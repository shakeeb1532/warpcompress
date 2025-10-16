
# Copyright 2025
# WarpCompress core — parallel (de)compression, faster compression path
from __future__ import annotations
import os
import time
import mmap
import struct
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Tuple, List, Optional, Dict

# External codecs
import snappy
import lz4.frame as lz4f
import zstandard as zstd
import brotli

# ===== Container format =====
MAGIC = b"WARP"                  # 4 bytes
VERSION = 1                      # 1 byte
HEADER_FMT = ">4sB B I Q"        # MAGIC, VER, ALGO_ID, CHUNK_SIZE, ORIG_SIZE
HEADER_SIZE = struct.calcsize(HEADER_FMT)

CHUNK_HDR_FMT = ">II"            # ORIG_LEN (uint32), COMP_LEN (uint32)
CHUNK_HDR_SIZE = struct.calcsize(CHUNK_HDR_FMT)

# Bigger default chunk for better throughput
CHUNK_SIZE_DEFAULT = 8 * 1024 * 1024  # 8 MiB

# ===== Algorithms =====
ALGO_SNAPPY = 1
ALGO_LZ4    = 2
ALGO_ZSTD   = 3
ALGO_BROTLI = 4

ALGO_NAMES = {
    ALGO_SNAPPY: "snappy",
    ALGO_LZ4:    "lz4",
    ALGO_ZSTD:   "zstd",
    ALGO_BROTLI: "brotli",
}
NAME_TO_ID = {v: k for k, v in ALGO_NAMES.items()}

# ===== Helpers =====
def _fmt_bytes(n: int) -> str:
    x = float(n)
    for u in ("B", "KB", "MB", "GB", "TB"):
        if x < 1024.0:
            return f"{x:.2f} {u}"
        x /= 1024.0
    return f"{x:.2f} PB"

def _cpu_workers(default: Optional[int] = None) -> int:
    # default may be None; choose a sensible number
    if isinstance(default, int) and default > 0:
        return default
    cpus = os.cpu_count() or 4
    return max(2, min(32, cpus))

def _plan_for(level: Optional[str],
              chunk_size: Optional[int],
              workers: Optional[int]) -> Tuple[int, int, int, int, int]:
    """
    Returns: (algo_id, chunk, workers, zstd_level, zstd_threads)

    Tuned levels:
      - throughput/auto/snappy  -> snappy, chunk=8 MiB, workers=cpu
      - lz4                     -> lz4,    chunk=8 MiB, workers=cpu
      - zstd/balanced/default   -> zstd,   chunk=8 MiB, level=3
      - ratio/brotli/max        -> brotli, chunk=8 MiB
    """
    lvl = (level or "throughput").lower()
    w = _cpu_workers(workers)
    c = int(chunk_size or CHUNK_SIZE_DEFAULT)

    if lvl in ("throughput", "auto", "snappy", "speed", "fast"):
        return (ALGO_SNAPPY, c, w, 0, 0)
    if lvl in ("lz4",):
        return (ALGO_LZ4, c, w, 0, 0)
    if lvl in ("zstd", "balanced", "default"):
        # When using our own thread pool (w>1), keep Zstd internal threads=0 to avoid oversubscription.
        # If single-threaded, give Zstd its own threads (per library guidance).
        zstd_threads = 0 if w > 1 else max(1, _cpu_workers(0))
        return (ALGO_ZSTD, c, w, 3, zstd_threads)
    if lvl in ("ratio", "brotli", "max"):
        return (ALGO_BROTLI, c, w, 0, 0)
    # Fallback to throughput
    return (ALGO_SNAPPY, c, w, 0, 0)

# ---- per-thread codec providers (avoid shared-state contention) ----
_tls = threading.local()

def _thread_comp_fn(algo_id: int, zstd_level: int, zstd_threads: int) -> Callable[[bytes], bytes]:
    if algo_id == ALGO_SNAPPY:
        return snappy.compress
    if algo_id == ALGO_LZ4:
        return lz4f.compress
    if algo_id == ALGO_ZSTD:
        key = (zstd_level, zstd_threads)
        zc = getattr(_tls, "zc", None)
        zck = getattr(_tls, "zck", None)
        if zc is None or zck != key:
            _tls.zc = zstd.ZstdCompressor(level=zstd_level, threads=zstd_threads or 0)
            _tls.zck = key
        return _tls.zc.compress
    if algo_id == ALGO_BROTLI:
        # quality 5 is a good speed/ratio balance; adjust if you expose a flag
        return lambda b: brotli.compress(b, quality=5)
    raise ValueError(f"Unknown algorithm id: {algo_id}")

def _thread_decomp_fn(algo_id: int) -> Callable[[bytes], bytes]:
    if algo_id == ALGO_SNAPPY:
        return snappy.uncompress
    if algo_id == ALGO_LZ4:
        return lz4f.decompress
    if algo_id == ALGO_ZSTD:
        zd = getattr(_tls, "zd", None)
        if zd is None:
            _tls.zd = zstd.ZstdDecompressor()
        return _tls.zd.decompress
    if algo_id == ALGO_BROTLI:
        return brotli.decompress
    raise ValueError(f"Unknown algorithm id: {algo_id}")

# ========= Parallel COMPRESSION =========
def compress_file(
    input_filename: str,
    output_filename: str,
    level: str = "throughput",
    *,
    chunk_size: Optional[int] = None,
    workers: Optional[int] = None,
    verbose: bool = False,
) -> None:
    """
    Threaded compression: mmapped input, per-chunk jobs in a pool, in-order write-out.
    We copy the mmap slice to bytes() to avoid exported-pointer issues on Py3.12/3.13.
    """
    file_size = os.path.getsize(input_filename)
    algo_id, csize, worker_count, zstd_level, zstd_threads = _plan_for(level, chunk_size, workers)
    algo_name = ALGO_NAMES[algo_id]

    if verbose:
        print(f"Compressing {os.path.basename(input_filename)} → {os.path.basename(output_filename)} (level={level}) …")
        print(f"INFO: plan → algo={algo_name}, chunk={_fmt_bytes(csize)}, workers={worker_count}, zstd_threads={zstd_threads}")

    # Memory-map input
    with open(input_filename, "rb") as f_in, open(output_filename, "wb") as f_out:
        # Write header
        f_out.write(struct.pack(HEADER_FMT, MAGIC, VERSION, algo_id, csize, file_size))

        mm = mmap.mmap(f_in.fileno(), 0, access=mmap.ACCESS_READ)
        try:
            mv = memoryview(mm)
            try:
                mv_len = len(mv)
                n_chunks = (mv_len + csize - 1) // csize

                def job(idx: int) -> Tuple[int, int, bytes, float]:
                    # slice → copy to bytes → compress with a per-thread codec
                    start = idx * csize
                    end = min(start + csize, mv_len)
                    t0 = time.perf_counter()
                    view = mv[start:end]
                    try:
                        data = bytes(view)  # break exported-pointer tie
                    finally:
                        view.release()

                    comp = _thread_comp_fn(algo_id, zstd_level, zstd_threads)
                    cbytes = comp(data)
                    t1 = time.perf_counter()
                    return idx, len(data), cbytes, (t1 - t0)

                # Bounded in-flight to keep the pipeline full without blowing memory
                next_to_write = 0
                pending: Dict[int, Tuple[int, bytes, float]] = {}
                submitted = 0
                max_inflight = max(worker_count * 4, 8)  # more aggressive than before

                with ThreadPoolExecutor(max_workers=worker_count) as ex:
                    futures = []
                    # Prime the queue
                    while submitted < n_chunks and len(futures) < max_inflight:
                        futures.append(ex.submit(job, submitted)); submitted += 1

                    while futures:
                        for fut in as_completed(futures):
                            idx, orig_len, cbytes, dt = fut.result()
                            pending[idx] = (orig_len, cbytes, dt)
                            futures.remove(fut)

                            # Keep submitting new work
                            if submitted < n_chunks:
                                futures.append(ex.submit(job, submitted)); submitted += 1

                            # Write any contiguous ready chunks
                            while next_to_write in pending:
                                o_len, c_b, c_dt = pending.pop(next_to_write)
                                f_out.write(struct.pack(CHUNK_HDR_FMT, o_len, len(c_b)))
                                f_out.write(c_b)
                                if verbose:
                                    mb = o_len / (1024 * 1024)
                                    thr = mb / max(c_dt, 1e-9)
                                    print(f"[compress] chunk {next_to_write:>4}: {_fmt_bytes(o_len)} → {_fmt_bytes(len(c_b))} | {thr:,.2f} MB/s")
                                next_to_write += 1
            finally:
                mv.release()
        finally:
            mm.close()

# ========= Parallel DECOMPRESSION =========
def decompress_file(
    input_filename: str,
    output_filename: str,
    *,
    workers: Optional[int] = None,
    verbose: bool = False,
) -> None:
    """
    Threaded decompression:
      1) read header, scan chunk table to build (in_off, comp_len, out_off, orig_len)
      2) pre-allocate output and mmap
      3) thread-pool to decode chunks and write to output offsets
    """
    with open(input_filename, "rb") as f_in:
        # --- header ---
        hdr = f_in.read(HEADER_SIZE)
        if len(hdr) != HEADER_SIZE:
            raise ValueError("Input too small to be a valid .warp file")

        magic, ver, algo_id, csize, orig_size = struct.unpack(HEADER_FMT, hdr)
        if magic != MAGIC:  raise ValueError("Bad magic; not a warp file")
        if ver != VERSION:  raise ValueError(f"Unsupported version: {ver}")
        if algo_id not in ALGO_NAMES:  raise ValueError(f"Unknown algorithm id: {algo_id}")

        algo_name = ALGO_NAMES[algo_id]
        decomp = _thread_decomp_fn(algo_id)

        # --- build index ---
        chunk_index: List[Tuple[int, int, int, int]] = []  # (file_off, comp_len, out_off, orig_len)
        out_off = 0
        file_off = HEADER_SIZE
        while True:
            f_in.seek(file_off)
            ch = f_in.read(CHUNK_HDR_SIZE)
            if not ch:
                break
            if len(ch) != CHUNK_HDR_SIZE:
                raise ValueError("Truncated chunk header")
            orig_len, comp_len = struct.unpack(CHUNK_HDR_FMT, ch)
            data_off = file_off + CHUNK_HDR_SIZE
            chunk_index.append((data_off, comp_len, out_off, orig_len))
            file_off = data_off + comp_len
            out_off += orig_len

        # --- prepare output file ---
        with open(output_filename, "wb") as f_out:
            f_out.truncate(orig_size)

        if verbose:
            print(f"Decompressing with {algo_name} | chunk={_fmt_bytes(csize)} | orig={_fmt_bytes(orig_size)} | chunks={len(chunk_index)}")

        # mmap input & output
        with open(input_filename, "rb") as fin2:
            mm_in = mmap.mmap(fin2.fileno(), 0, access=mmap.ACCESS_READ)
            try:
                with open(output_filename, "r+b") as fout2:
                    mm_out = mmap.mmap(fout2.fileno(), 0, access=mmap.ACCESS_WRITE)
                    try:
                        worker_count = _cpu_workers(workers)

                        def job(rec):
                            data_off, comp_len, out_off, orig_len = rec
                            cbytes = bytes(mm_in[data_off:data_off+comp_len])
                            t0 = time.perf_counter()
                            data = decomp(cbytes)
                            t1 = time.perf_counter()
                            if len(data) != orig_len:
                                raise ValueError("Decompressed length mismatch")
                            return out_off, data, (t1 - t0), orig_len

                        max_inflight = max(worker_count * 4, 8)
                        next_idx = 0
                        in_flight: List = []
                        total_written = 0

                        with ThreadPoolExecutor(max_workers=worker_count) as ex:
                            while next_idx < len(chunk_index) and len(in_flight) < max_inflight:
                                in_flight.append(ex.submit(job, chunk_index[next_idx])); next_idx += 1

                            while in_flight:
                                for fut in as_completed(in_flight):
                                    out_off, data, dt, o_len = fut.result()
                                    mm_out[out_off:out_off+len(data)] = data
                                    if verbose:
                                        mb = o_len / (1024*1024)
                                        thr = mb / max(dt, 1e-9)
                                        print(f"[decompress] chunk @+{out_off}: {_fmt_bytes(o_len)} | {thr:,.2f} MB/s")
                                    total_written += len(data)
                                    in_flight.remove(fut)

                                    if next_idx < len(chunk_index):
                                        in_flight.append(ex.submit(job, chunk_index[next_idx])); next_idx += 1

                        if total_written != orig_size:
                            raise ValueError(f"Size mismatch: expected {orig_size}, wrote {total_written}")
                    finally:
                        mm_out.flush()
                        mm_out.close()
            finally:
                mm_in.close()

# Convenience for CLI messaging
def detect_algo_name(level: str) -> str:
    algo_id, _, _, _, _ = _plan_for(level, None, None)
    return ALGO_NAMES[algo_id]

