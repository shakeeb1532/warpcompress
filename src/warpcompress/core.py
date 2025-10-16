# Copyright 2025
"""
warpcompress.core

Pure-Python core for Warp container:
- Header: b"WARP" + version + chunk_size
- Per-chunk header: algo(1 byte) | orig_len(uint32 LE) | comp_len(uint32 LE)
- Parallel chunk compression/decompression
- Zero-chunk elision
- Zstd/LZ4/Snappy strategy levels

This module is intentionally dependency-light and thread-safe.
"""

from __future__ import annotations

import io
import os
import sys
import mmap
import math
import struct
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Iterable, List, Optional, Tuple

# Optional codecs
try:
    import zstandard as zstd  # type: ignore
except Exception:  # pragma: no cover
    zstd = None  # type: ignore

try:
    import lz4.frame as lz4f  # type: ignore
except Exception:  # pragma: no cover
    lz4f = None  # type: ignore

try:
    import snappy  # type: ignore
except Exception:  # pragma: no cover
    snappy = None  # type: ignore


# -------------------------
# Format constants
# -------------------------

MAGIC = b"WARP"
VERSION = 2

# Algorithms (1 byte)
ALGO_ZSTD = 1
ALGO_LZ4 = 2
ALGO_SNAPPY = 3
ALGO_COPY = 4
ALGO_ZERO = 5

CHUNK_MIN = 256 * 1024      # 256 KiB
CHUNK_DEF = 1 * 1024 * 1024  # 1 MiB
CHUNK_MAX = 16 * 1024 * 1024 # 16 MiB

# per-chunk header: algo(1) + orig_len(4) + comp_len(4) => 9 bytes
CHDR = struct.Struct("<BII")

# file header: MAGIC(4) + version(1) + flags(1 reserved) + chunk_size(uint32) + reserved(uint32)
FHDR = struct.Struct("<4sBBII")


# -------------------------
# Utilities
# -------------------------

def _is_all_zero(b: memoryview) -> bool:
    # Fast-ish zero check without allocations
    # short-circuit on first non-zero byte
    mv = memoryview(b)
    # Scan in 8K stripes to help branch prediction
    step = 8192
    for i in range(0, len(mv), step):
        if mv[i : i + step].tobytes().strip(b"\x00"):
            return False
    return True


def _coerce_chunk_size(chunk_size: Optional[int]) -> int:
    if not chunk_size:
        return CHUNK_DEF
    return max(CHUNK_MIN, min(CHUNK_MAX, int(chunk_size)))


def _cpu_workers(default: int = 0) -> int:
    n = os.cpu_count() or 4
    if default:
        return default
    # mild over-subscription can help when codecs release the GIL
    return min(max(2, n), 64)


def _log(verbose: bool, msg: str) -> None:
    if verbose:
        print(msg, flush=True)


# -------------------------
# Thread-local codec caches (bug-fixed)
# -------------------------

_tls = threading.local()

def _tls_cache() -> dict:
    c = getattr(_tls, "_cache", None)
    if c is None:
        c = {}
        setattr(_tls, "_cache", c)
    return c

def _zstd_c(level: int, threads: int) -> Callable[[bytes], bytes]:
    if zstd is None:
        raise RuntimeError("zstandard not available")
    cache = _tls_cache()
    key = ("zc", int(level), int(threads or 0))
    zc = cache.get(key)
    if zc is None:
        zc = zstd.ZstdCompressor(level=level, threads=threads or 0)
        cache[key] = zc
    return zc.compress

def _zstd_d() -> Callable[[bytes], bytes]:
    if zstd is None:
        raise RuntimeError("zstandard not available")
    cache = _tls_cache()
    key = ("zd",)
    zd = cache.get(key)
    if zd is None:
        zd = zstd.ZstdDecompressor()
        cache[key] = zd
    return zd.decompress

def _lz4_c(accel: int = 1) -> Callable[[bytes], bytes]:
    if lz4f is None:
        raise RuntimeError("lz4.frame not available")
    def _c(data: bytes) -> bytes:
        return lz4f.compress(data, compression_level=accel, block_linked=True)
    return _c

def _lz4_d() -> Callable[[bytes], bytes]:
    if lz4f is None:
        raise RuntimeError("lz4.frame not available")
    def _d(data: bytes) -> bytes:
        return lz4f.decompress(data)
    return _d

def _snappy_c() -> Callable[[bytes], bytes]:
    if snappy is None:
        raise RuntimeError("python-snappy not available")
    return snappy.compress

def _snappy_d() -> Callable[[bytes], bytes]:
    if snappy is None:
        raise RuntimeError("python-snappy not available")
    return snappy.uncompress


# -------------------------
# Planning
# -------------------------

class Plan:
    __slots__ = ("level", "chunk", "workers", "zstd_threads", "lz4_accel", "verbose")

    def __init__(self, level: str, *, chunk: int, workers: int, zstd_threads: int, lz4_accel: int, verbose: bool):
        self.level = level
        self.chunk = chunk
        self.workers = workers
        self.zstd_threads = zstd_threads
        self.lz4_accel = lz4_accel
        self.verbose = verbose


def _make_plan(level: str, *, chunk: Optional[int], workers: Optional[int],
               zstd_hybrid: str = "auto", verbose: bool = False) -> Plan:
    lvl = (level or "throughput").lower()
    csize = _coerce_chunk_size(chunk)
    w = _cpu_workers(workers or 0)

    # Heuristic: zstd threads (internal) usually 0/auto is fine. Keep 0.
    zt = 0

    # lz4 accel: 1..12 (higher is faster, worse ratio). Default a little higher on throughput.
    lz_acc = 4 if lvl in ("throughput", "lz4") else 1

    if lvl not in ("throughput", "zstd", "lz4", "ratio"):
        raise ValueError(f"Unknown level: {level}")

    if verbose:
        base = "snappy" if lvl == "throughput" else lvl
        print(f"Base hint: {base} (final codec is per-chunk adaptive)", flush=True)

    _log(verbose, f"INFO: base plan -> chunk={csize/1024/1024:.2f} MB (auto), "
                  f"workers={w}, zstd_threads={zt}, lz4_accel={lz_acc}")

    return Plan(lvl, chunk=csize, workers=w, zstd_threads=zt, lz4_accel=lz_acc, verbose=verbose)


# -------------------------
# Codec choice per chunk
# -------------------------

def _choose_alg(plan: Plan, block: memoryview) -> Tuple[int, Callable[[bytes], bytes]]:
    """
    Choose codec for a block. Simple heuristics:
      - If all zero: ALGO_ZERO
      - throughput: snappy; if not available, lz4; else zstd-1
      - lz4: lz4
      - zstd: zstd level 1
      - ratio: zstd level 3
    """
    if _is_all_zero(block):
        return ALGO_ZERO, lambda _: b""

    lvl = plan.level
    if lvl == "throughput":
        if snappy is not None:
            return ALGO_SNAPPY, _snappy_c()
        elif lz4f is not None:
            return ALGO_LZ4, _lz4_c(plan.lz4_accel)
        elif zstd is not None:
            return ALGO_ZSTD, _zstd_c(level=1, threads=plan.zstd_threads)
        else:
            return ALGO_COPY, lambda b: bytes(b)
    elif lvl == "lz4":
        if lz4f is None:
            return ALGO_COPY, lambda b: bytes(b)
        return ALGO_LZ4, _lz4_c(plan.lz4_accel)
    elif lvl == "zstd":
        if zstd is None:
            return ALGO_COPY, lambda b: bytes(b)
        return ALGO_ZSTD, _zstd_c(level=1, threads=plan.zstd_threads)
    else:  # ratio
        if zstd is None:
            return ALGO_COPY, lambda b: bytes(b)
        return ALGO_ZSTD, _zstd_c(level=3, threads=plan.zstd_threads)


def _decomp_fn(algo: int) -> Callable[[bytes], bytes]:
    if algo == ALGO_ZERO:
        return lambda _b: b""
    if algo == ALGO_COPY:
        return lambda b: b
    if algo == ALGO_SNAPPY:
        return _snappy_d()
    if algo == ALGO_LZ4:
        return _lz4_d()
    if algo == ALGO_ZSTD:
        return _zstd_d()
    raise ValueError(f"Unknown algo {algo}")


# -------------------------
# File I/O helpers
# -------------------------

def _write_fhdr(fout: io.BufferedWriter, chunk: int) -> None:
    # flags currently unused (0); reserved 0
    hdr = FHDR.pack(MAGIC, VERSION, 0, chunk, 0)
    fout.write(hdr)

def _read_fhdr(fin: io.BufferedReader) -> Tuple[int, int]:
    raw = fin.read(FHDR.size)
    if len(raw) != FHDR.size:
        raise ValueError("Invalid WARP header (truncated)")
    magic, ver, _flags, chunk, _reserved = FHDR.unpack(raw)
    if magic != MAGIC:
        raise ValueError("Bad magic; not a warp file")
    if ver not in (1, 2):
        raise ValueError(f"Unsupported WARP version {ver}")
    return ver, chunk


# -------------------------
# Public API
# -------------------------

def compress_file(input_filename: str,
                  output_filename: str,
                  *,
                  level: str = "throughput",
                  workers: Optional[int] = None,
                  chunk: Optional[int] = None,
                  zstd_hybrid: str = "auto",
                  verbose: bool = False) -> None:
    """
    Compress input_filename into Warp container at output_filename.
    """
    plan = _make_plan(level, chunk=chunk, workers=workers, zstd_hybrid=zstd_hybrid, verbose=verbose)

    total_in = 0
    zero_blocks = 0

    with open(input_filename, "rb") as f_in, open(output_filename, "wb") as f_out:
        _write_fhdr(f_out, plan.chunk)

        # mmap when helpful; otherwise stream
        try:
            fileno = f_in.fileno()
            size = os.fstat(fileno).st_size
            mm = mmap.mmap(fileno, 0, access=mmap.ACCESS_READ)
            use_mm = True
        except Exception:
            mm = None
            use_mm = False

        def read_block(i: int) -> memoryview:
            if use_mm:
                off = i * plan.chunk
                end = min(off + plan.chunk, size)
                return memoryview(mm)[off:end]
            else:
                # stream read
                return memoryview(f_in.read(plan.chunk))

        def job(i: int, block: memoryview) -> Tuple[int, bytes]:
            # choose codec & compress
            algo, comp = _choose_alg(plan, block)
            if algo == ALGO_ZERO:
                comp_data = b""
            elif algo == ALGO_COPY:
                comp_data = bytes(block)
            else:
                comp_data = comp(block.tobytes())
            hdr = CHDR.pack(algo, len(block), len(comp_data))
            return i, hdr + comp_data

        # total blocks
        if use_mm:
            blocks = math.ceil(size / plan.chunk) if size else 0
        else:
            # streaming: determine by reading progressively
            # but we will push tasks as we read instead
            blocks = None

        next_idx = 0
        pending = {}
        written = 0

        with ThreadPoolExecutor(max_workers=plan.workers) as ex:
            futures = []
            i = 0

            if use_mm:
                while i < blocks:
                    blk = read_block(i)
                    if len(blk) == 0:
                        break
                    if _is_all_zero(blk):
                        zero_blocks += 1
                    total_in += len(blk)
                    futures.append(ex.submit(job, i, blk))
                    i += 1
            else:
                while True:
                    chunk_bytes = f_in.read(plan.chunk)
                    if not chunk_bytes:
                        break
                    blk = memoryview(chunk_bytes)
                    if _is_all_zero(blk):
                        zero_blocks += 1
                    total_in += len(blk)
                    futures.append(ex.submit(job, i, blk))
                    i += 1

            # ordered writer
            for fut in as_completed(futures):
                idx, payload = fut.result()
                pending[idx] = payload
                while next_idx in pending:
                    f_out.write(pending.pop(next_idx))
                    written += 1
                    next_idx += 1

    if verbose:
        if zero_blocks:
            print(f"[compress] 0+ zero x{zero_blocks}", flush=True)


def decompress_file(input_filename: str,
                    output_filename: str,
                    *,
                    workers: Optional[int] = None,
                    verbose: bool = False) -> None:
    """
    Decompress Warp container into raw output file.
    """
    w = _cpu_workers(workers or 0)

    with open(input_filename, "rb") as f_in:
        _ver, chunk_size = _read_fhdr(f_in)

        # We'll compute the output size on the fly and preallocate lazily.
        # Decode stream:
        jobs = []
        blocks_meta: List[Tuple[int, int, int, int]] = []  # (idx, algo, orig, comp_off)

        # Read all chunk headers first (keeps code simple)
        data_off = FHDR.size
        idx = 0
        while True:
            hdr = f_in.read(CHDR.size)
            if not hdr:
                break
            if len(hdr) != CHDR.size:
                raise ValueError("Truncated container (chunk header)")
            algo, orig_len, comp_len = CHDR.unpack(hdr)
            blocks_meta.append((idx, algo, orig_len, data_off))
            data_off += comp_len
            # skip payload
            if comp_len:
                f_in.seek(comp_len, io.SEEK_CUR)
            idx += 1

        # Prepare threadpool
        def djob(meta: Tuple[int, int, int, int]) -> Tuple[int, bytes]:
            i, algo, orig_len, comp_off = meta
            if algo == ALGO_ZERO:
                return i, b"\x00" * orig_len
            if algo == ALGO_COPY:
                with open(input_filename, "rb") as fin:
                    fin.seek(comp_off)
                    return i, fin.read(orig_len)
            decomp = _decomp_fn(algo)
            with open(input_filename, "rb") as fin:
                fin.seek(comp_off)
                comp = fin.read(CHDR.size)  # wrong on purpose? No – we stored just payload
            # Correct: read only comp payload
            with open(input_filename, "rb") as fin2:
                fin2.seek(comp_off)
                comp_payload = fin2.read(blocks_comp_len[ i ])
            return i, decomp(comp_payload)

        # We need comp_len per index for random reads
        blocks_comp_len = {}
        data_off = FHDR.size
        for i, (_, _, _orig, _off) in enumerate(blocks_meta):
            # re-read header: we already have comp_len implicit via deltas
            # In first pass, we didn't store comp_len; store it now:
            f_in.seek(data_off)
            hdr = f_in.read(CHDR.size)
            algo, orig_len, comp_len = CHDR.unpack(hdr)
            blocks_comp_len[i] = comp_len
            data_off += CHDR.size + comp_len

        # Now decompress in parallel and write in order
        with open(output_filename, "wb") as f_out, ThreadPoolExecutor(max_workers=w) as ex:
            # Pre-size file if we can estimate quickly
            total_out = sum(orig for (_i, _a, orig, _off) in blocks_meta)
            try:
                f_out.truncate(total_out)
            except Exception:
                pass

            # Prepare jobs using the offsets after each header
            # Recompute payload offsets = header_offset + CHDR.size
            hdr_off = FHDR.size
            payload_offs = []
            for i in range(len(blocks_meta)):
                f_in.seek(hdr_off)
                algo, orig, comp = CHDR.unpack(f_in.read(CHDR.size))
                payload_offs.append(hdr_off + CHDR.size)
                hdr_off += CHDR.size + comp

            # Fix blocks_meta to carry payload offsets
            fixed_meta = []
            for i, (idx, algo, orig, _dummy) in enumerate(blocks_meta):
                fixed_meta.append((idx, algo, orig, payload_offs[i]))

            futures = { ex.submit(djob, m): m[0] for m in fixed_meta }
            next_idx = 0
            pending = {}
            while futures:
                fut = next(as_completed(futures))
                i, data = fut.result()
                del futures[fut]
                pending[i] = data
                while next_idx in pending:
                    f_out.seek(next_idx * chunk_size)
                    f_out.write(pending.pop(next_idx))
                    next_idx += 1

    if verbose:
        print("[decompress] done", flush=True)

