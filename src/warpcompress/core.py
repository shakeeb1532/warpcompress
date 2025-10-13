#!/usr/bin/env python3
"""
WarpCompress — Scalable, parallel, lossless compressor/decompressor.

- Writer uses FORMAT v2 (stores both compressed and original per-chunk sizes).
- Reader supports v2 and v1. v2 enables parallel scatter-gather mmap writes;
  v1 falls back to streamed sequential writes (still constant RAM).
- Throughput mode, integrity footer, and verbose per-chunk timings included.
- Performance (1.0.1): Zstd zero-copy decompress_into, thread-local dctx reuse,
  and MADV hints for input/output mmaps.
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import mmap
import os
import struct
import sys
import time
from typing import Iterable, List, Sequence, Tuple, Optional
import threading
import hashlib

try:
    import lz4.frame
    import zstandard as zstd
    import snappy
    import brotli
except ImportError:
    print("Error: Required libs missing. Install: pip install lz4 zstandard python-snappy brotli", file=sys.stderr)
    raise

# Optional hash libs
try:
    import blake3; _has_blake3 = True
except Exception:
    _has_blake3 = False
try:
    import xxhash; _has_xxhash = True
except Exception:
    _has_xxhash = False

# --------- Thread-local zstd dctx ---------
_tlocal = threading.local()
def _get_zstd_dctx():
    """Return a thread-local ZstdDecompressor (reuse across chunks)."""
    if not hasattr(_tlocal, "zstd_dctx"):
        _tlocal.zstd_dctx = zstd.ZstdDecompressor()
    return _tlocal.zstd_dctx

# --------- Format & constants ---------
MAGIC_NUMBER = 0x57415250  # "WARP"
FORMAT_VERSION = 2         # writer uses v2; reader supports v1 and v2

ALGO_LZ4, ALGO_ZSTD, ALGO_SNAPPY, ALGO_BROTLI = 0, 1, 2, 3
ALGO_NAMES = {ALGO_LZ4: "lz4", ALGO_ZSTD: "zstd", ALGO_SNAPPY: "snappy", ALGO_BROTLI: "brotli"}

TRAILER_MAGIC = 0x31544657  # "WFT1"
HASH_NONE, HASH_BLAKE3, HASH_XXH64, HASH_BLAKE2B = 0, 1, 2, 3
HASH_NAMES = {HASH_NONE: "none", HASH_BLAKE3: "blake3", HASH_XXH64: "xxh64", HASH_BLAKE2B: "blake2b"}

DEFAULT_CHUNK_SIZE = 4 * 1024 * 1024
DEFAULT_THREADS = min(32, os.cpu_count() or 4)
DEFAULT_ZSTD_LEVEL = 3
DEFAULT_ZSTD_THREADS = 0
DEFAULT_BROTLI_QUALITY = 5

# --------- Binary helpers ---------
def _pack_header_v2(algorithm_id: int, orig_size: int, comp_sizes: Sequence[int], decomp_sizes: Sequence[int]) -> bytes:
    if len(comp_sizes) != len(decomp_sizes):
        raise ValueError("comp/decomp sizes length mismatch")
    num_chunks = len(comp_sizes)
    head = struct.pack("<L B B Q L", MAGIC_NUMBER, 2, algorithm_id, orig_size, num_chunks)
    comp_bin = b"".join(struct.pack("<Q", s) for s in comp_sizes)
    decomp_bin = b"".join(struct.pack("<Q", s) for s in decomp_sizes)
    return head + comp_bin + decomp_bin

def _pack_header_v1(algorithm_id: int, orig_size: int, comp_sizes: Sequence[int]) -> bytes:
    head = struct.pack("<L B B Q L", MAGIC_NUMBER, 1, algorithm_id, orig_size, len(comp_sizes))
    comp_bin = b"".join(struct.pack("<Q", s) for s in comp_sizes)
    return head + comp_bin

def _unpack_header(f) -> Tuple[int, int, int, List[int], Optional[List[int]], int, int]:
    """
    Returns: (algo_id, orig_size, num_chunks, comp_sizes, decomp_sizes_or_None, data_start_offset, version)
    """
    fixed_len = struct.calcsize("<L B B Q L")
    fixed = f.read(fixed_len)
    if len(fixed) != fixed_len:
        raise ValueError("Input file too small / corrupt.")
    magic, ver, algo, orig_size, num_chunks = struct.unpack("<L B B Q L", fixed)
    if magic != MAGIC_NUMBER:
        raise ValueError("Bad magic; not a .warp file.")
    q = struct.calcsize("<Q")
    comp = []
    for _ in range(num_chunks):
        buf = f.read(q)
        if len(buf) != q:
            raise ValueError("Header truncated (comp sizes).")
        comp.append(struct.unpack("<Q", buf)[0])
    if ver == 1:
        return algo, orig_size, num_chunks, comp, None, f.tell(), 1
    if ver != 2:
        raise ValueError(f"Unsupported .warp version: {ver}")
    decomp = []
    for _ in range(num_chunks):
        buf = f.read(q)
        if len(buf) != q:
            raise ValueError("Header truncated (decomp sizes).")
        decomp.append(struct.unpack("<Q", buf)[0])
    return algo, orig_size, num_chunks, comp, decomp, f.tell(), 2

def _pack_footer(hash_algo_id: int, digest_bytes: bytes) -> bytes:
    return struct.pack("<L B H", TRAILER_MAGIC, hash_algo_id, len(digest_bytes)) + digest_bytes

def _unpack_footer(mm: mmap.mmap, offset: int) -> Optional[Tuple[int, bytes]]:
    if offset + 7 > len(mm):
        return None
    magic, = struct.unpack_from("<L", mm, offset)
    if magic != TRAILER_MAGIC:
        return None
    algo = struct.unpack_from("<B", mm, offset + 4)[0]
    slen = struct.unpack_from("<H", mm, offset + 5)[0]
    end = offset + 7 + slen
    if end > len(mm):
        raise ValueError("Corrupted footer: truncated digest.")
    return algo, bytes(mm[offset+7:end])

def _chunk_views(mm: mmap.mmap, chunk_size: int) -> Iterable[memoryview]:
    mv = memoryview(mm); n = len(mm)
    for off in range(0, n, chunk_size):
        yield mv[off: min(off + chunk_size, n)]

# --------- Hash abstraction ---------
class _Hasher:
    def __init__(self, algo_id: int):
        self.algo_id = algo_id
        if algo_id == HASH_NONE:
            self._h = None
        elif algo_id == HASH_BLAKE3:
            if not _has_blake3: raise RuntimeError("blake3 not installed. pip install blake3")
            self._h = blake3.blake3()
        elif algo_id == HASH_XXH64:
            if not _has_xxhash: raise RuntimeError("xxhash not installed. pip install xxhash")
            self._h = xxhash.xxh64()
        elif algo_id == HASH_BLAKE2B:
            self._h = hashlib.blake2b(digest_size=32)
        else:
            raise ValueError("Unknown hash algo id")
    def update(self, b: bytes | memoryview):
        if self._h: self._h.update(b)
    def digest(self) -> bytes:
        return self._h.digest() if self._h else b""

def _algo_name_to_id(name: str) -> int:
    m = {"none": HASH_NONE, "blake3": HASH_BLAKE3, "xxh64": HASH_XXH64, "blake2b": HASH_BLAKE2B}
    if name.lower() not in m:
        raise ValueError("Unknown checksum algo. Choose: none, blake3, xxh64, blake2b.")
    return m[name.lower()]

# --------- Policy helpers ---------
def _auto_choose_algorithm(sample: bytes, zstd_level: int) -> int:
    if not sample:
        return ALGO_SNAPPY
    sample = sample[: 1 * 1024 * 1024]
    zstd_c = zstd.ZstdCompressor(level=zstd_level).compress(sample)
    snp_c  = snappy.compress(sample)
    return ALGO_ZSTD if len(zstd_c) <= len(snp_c) * 0.90 else ALGO_SNAPPY

def _throughput_plan(file_size: int, cpu_cores: int) -> Tuple[int, int, int, int]:
    """Return (chunk_size, host_threads, zstd_threads, algorithm_id) tuned for wall-clock throughput."""
    MiB = 1024 * 1024; GiB = 1024 * MiB; cores = max(1, cpu_cores)
    if file_size < 256 * MiB:
        return 2 * MiB, min(4, cores), 0, (ALGO_LZ4 if file_size >= 32 * MiB else ALGO_SNAPPY)
    if file_size < 1 * GiB:
        return (8 * MiB if cores >= 8 else 4 * MiB), cores, max(1, cores // 2), (ALGO_ZSTD if cores >= 6 else ALGO_LZ4)
    if file_size < 10 * GiB:
        return 16 * MiB, cores, min(8, cores), ALGO_ZSTD
    return 32 * MiB, cores, min(12, cores), ALGO_ZSTD

# --------- Manager ---------
class CompressionManager:
    def __init__(self, **kwargs):
        self.threads = max(1, kwargs.get("threads", DEFAULT_THREADS))
        self.chunk_size = max(128 * 1024, kwargs.get("chunk_size", DEFAULT_CHUNK_SIZE))
        self.zstd_level = kwargs.get("zstd_level", DEFAULT_ZSTD_LEVEL)
        self.zstd_threads = kwargs.get("zstd_threads", DEFAULT_ZSTD_THREADS)
        self.brotli_quality = kwargs.get("brotli_quality", DEFAULT_BROTLI_QUALITY)
        self.verbose = kwargs.get("verbose", False)
        self.checksum_algo = kwargs.get("checksum_algo", HASH_NONE)

    # per-chunk workers
    def _compress_job(self, args: Tuple[int, memoryview, int, int]) -> Tuple[int, bytes, memoryview]:
        i, view, algo, zt = args
        t0 = time.perf_counter()
        if algo == ALGO_ZSTD:
            out = zstd.ZstdCompressor(level=self.zstd_level, threads=zt).compress(view)
        elif algo == ALGO_LZ4:
            out = lz4.frame.compress(view)
        elif algo == ALGO_SNAPPY:
            out = snappy.compress(view)
        elif algo == ALGO_BROTLI:
            out = brotli.compress(bytes(view), quality=self.brotli_quality)
        else:
            raise ValueError(f"Unknown algo id: {algo}")
        t1 = time.perf_counter()
        if self.verbose:
            mbps = (len(view) / (1024 * 1024)) / max(1e-9, (t1 - t0))
            print(f"[compress] chunk {i:5d}: {len(view)/1e6:8.2f}MB → {len(out)/1e6:8.2f}MB | {mbps:8.2f} MB/s")
        return i, out, view

    def _decompress_job(self, args: Tuple[int, memoryview, int, Optional[Tuple[int, int, mmap.mmap]]]) -> Tuple[int, int | bytes]:
        """
        Decompress one chunk.
        If a destination slice is provided (v2 + zstd), write directly into mm_out[start:end]
        and return (i, written_len). Otherwise, return (i, bytes) for caller to write.
        """
        i, view, algo, dest = args
        t0 = time.perf_counter()

        # Fast path: Zstd can decompress directly into destination buffer
        if algo == ALGO_ZSTD and dest is not None:
            start, end, mm_out = dest
            dctx = _get_zstd_dctx()
            dst = memoryview(mm_out)[start:end]
            written = dctx.decompress_into(dst, view)
            t1 = time.perf_counter()
            if self.verbose:
                mbps = (written / (1024 * 1024)) / max(1e-9, (t1 - t0))
                print(f"[decomp ] chunk {i:5d}: {len(view)/1e6:8.2f}MB → {written/1e6:8.2f}MB | {mbps:8.2f} MB/s (into)")
            return i, written

        # Fallback: allocate an intermediate object
        if algo == ALGO_ZSTD:
            out = _get_zstd_dctx().decompress(view)
        elif algo == ALGO_LZ4:
            out = lz4.frame.decompress(view)
        elif algo == ALGO_SNAPPY:
            out = snappy.decompress(view)
        elif algo == ALGO_BROTLI:
            out = brotli.decompress(view)
        else:
            raise ValueError(f"Unknown algo id: {algo}")

        t1 = time.perf_counter()
        if self.verbose:
            mbps = (len(out) / (1024 * 1024)) / max(1e-9, (t1 - t0))
            print(f"[decomp ] chunk {i:5d}: {len(view)/1e6:8.2f}MB → {len(out)/1e6:8.2f}MB | {mbps:8.2f} MB/s")
        return i, out

    # compress
    def compress_file(self, input_filename: str, output_filename: str, level: str):
        with open(input_filename, "rb") as f_in, mmap.mmap(f_in.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            file_size = len(mm)
            if file_size == 0:
                with open(output_filename, "wb") as f_out:
                    f_out.write(_pack_header_v2(ALGO_SNAPPY, 0, [], []))
                return

            # policy
            if level == "fastest":
                algo, chunk_size, threads, zt = ALGO_SNAPPY, self.chunk_size, self.threads, 0
            elif level == "max":
                algo, chunk_size, threads, zt = ALGO_BROTLI, self.chunk_size, self.threads, 0
            elif level == "throughput":
                chunk_size, threads, zt, algo = _throughput_plan(file_size, os.cpu_count() or 4)
                if self.verbose:
                    print(f"INFO: Throughput plan → algo={ALGO_NAMES[algo]}, chunk={chunk_size//1024//1024}MiB, threads={threads}, zstd_threads={zt}")
            else:
                algo = _auto_choose_algorithm(mm[: min(file_size, 1024*1024)], self.zstd_level)
                chunk_size, threads, zt = self.chunk_size, self.threads, self.zstd_threads
                if self.verbose:
                    print(f"INFO: Auto-selected {ALGO_NAMES[algo]}.")

            # chunk views
            try:
                mm.madvise(mmap.MADV_SEQUENTIAL)  # type: ignore[attr-defined]
            except Exception:
                pass
            chunk_views = list(_chunk_views(mm, chunk_size))
            decomp_sizes = [len(v) for v in chunk_views]

            hasher = _Hasher(self.checksum_algo)
            comp_chunks: List[bytes] = [b""] * len(chunk_views)
            tasks = [(i, v, algo, zt) for i, v in enumerate(chunk_views)]
            if len(tasks) == 1 or threads == 1:
                for t in tasks:
                    i, out, view = self._compress_job(t)
                    comp_chunks[i] = out
                    if self.checksum_algo != HASH_NONE: hasher.update(view)
            else:
                with cf.ThreadPoolExecutor(max_workers=threads) as ex:
                    for i, out, view in ex.map(self._compress_job, tasks, chunksize=1):
                        comp_chunks[i] = out
                        if self.checksum_algo != HASH_NONE: hasher.update(view)

        comp_sizes = [len(c) for c in comp_chunks]
        with open(output_filename, "wb") as f_out:
            f_out.write(_pack_header_v2(algo, file_size, comp_sizes, decomp_sizes))
            for c in comp_chunks:
                f_out.write(c)
            if self.checksum_algo != HASH_NONE:
                f_out.write(_pack_footer(self.checksum_algo, hasher.digest()))

    # decompress
    def decompress_file(self, input_filename: str, output_filename: str):
        with open(input_filename, "rb") as f_in, mmap.mmap(f_in.fileno(), 0, access=mmap.ACCESS_READ) as mm_in:
            algo, orig_size, num_chunks, comp_sizes, decomp_sizes, data_start, ver = _unpack_header(f_in)
            if orig_size == 0:
                with open(output_filename, "wb"): pass
                return

            # I/O hints to kernel
            try:
                mm_in.madvise(mmap.MADV_SEQUENTIAL)  # type: ignore[attr-defined]
                mm_in.madvise(mmap.MADV_WILLNEED)    # type: ignore[attr-defined]
            except Exception:
                pass

            payload_len = sum(comp_sizes)
            footer = _unpack_footer(mm_in, data_start + payload_len)

            comp_views: List[memoryview] = []
            off = data_start
            for s in comp_sizes:
                comp_views.append(memoryview(mm_in)[off:off+s]); off += s

            with open(output_filename, "w+b") as f_out:
                f_out.truncate(orig_size)
                with mmap.mmap(f_out.fileno(), 0, access=mmap.ACCESS_WRITE) as mm_out:
                    try:
                        mm_out.madvise(mmap.MADV_SEQUENTIAL)  # type: ignore[attr-defined]
                    except Exception:
                        pass

                    if ver == 2 and decomp_sizes is not None:
                        # prefix sums → per-chunk offsets
                        offsets = [0] * num_chunks
                        acc = 0
                        for i, sz in enumerate(decomp_sizes):
                            offsets[i] = acc; acc += sz

                        def task(i_view):
                            i, view = i_view
                            start = offsets[i]
                            end   = start + decomp_sizes[i]
                            if algo == ALGO_ZSTD:
                                # zero-copy into the output slice
                                return self._decompress_job((i, view, algo, (start, end, mm_out)))
                            else:
                                # one allocation, then copy to the slice
                                i2, out = self._decompress_job((i, view, algo, None))
                                mv = memoryview(mm_out)[start:start+len(out)]
                                mv[:] = out
                                return i2, len(out)

                        if num_chunks == 1 or self.threads == 1:
                            for i, view in enumerate(comp_views):
                                task((i, view))
                        else:
                            with cf.ThreadPoolExecutor(max_workers=self.threads) as ex:
                                for _ in ex.map(task, enumerate(comp_views), chunksize=1):
                                    pass
                    else:
                        # v1: stream sequentially to output (constant RAM)
                        write_off = 0
                        for i, view in enumerate(comp_views):
                            _, out = self._decompress_job((i, view, algo, None))
                            end = write_off + len(out)
                            mm_out[write_off:end] = out
                            write_off = end

            # Optional integrity verification
            if footer:
                hash_algo_id, digest_expected = footer
                hasher = _Hasher(hash_algo_id)
                with open(output_filename, "rb") as f_chk:
                    while True:
                        buf = f_chk.read(8 * 1024 * 1024)
                        if not buf: break
                        hasher.update(buf)
                calc = hasher.digest()
                if calc != digest_expected:
                    raise ValueError(f"Integrity check FAILED: expected {digest_expected.hex()}, got {calc.hex()}.")

# ---- CLI builder (re-exported by warpcompress.cli) ----
def _build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="WarpCompress: massive-file-ready lossless compression.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    sub = p.add_subparsers(dest="mode", required=True)

    def add_common(sp):
        sp.add_argument("input_file")
        sp.add_argument("output_file")
        sp.add_argument("--threads", type=int, default=DEFAULT_THREADS)
        sp.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE)
        sp.add_argument("--zstd-level", type=int, default=DEFAULT_ZSTD_LEVEL)
        sp.add_argument("--zstd-threads", type=int, default=DEFAULT_ZSTD_THREADS)
        sp.add_argument("--brotli-quality", type=int, default=DEFAULT_BROTLI_QUALITY)
        sp.add_argument("--verbose", action="store_true")

    sp_c = sub.add_parser("compress", help="Compress a file.")
    add_common(sp_c)
    sp_c.add_argument("--level", choices=["auto", "fastest", "max", "throughput"], default="auto")
    sp_c.add_argument("--checksum", choices=["none", "blake3", "xxh64", "blake2b"], default="none")

    sp_d = sub.add_parser("decompress", help="Decompress a .warp file.")
    add_common(sp_d)
    return p


