
# Copyright 2025
# WarpCompress core — parallel (de)compression with safe mmap usage
from __future__ import annotations
import os
import io
import time
import mmap
import struct
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

def _cpu_workers(default: int = 0) -> int:
    if default > 0:
        return default
    cpus = os.cpu_count() or 4
    return max(2, min(32, cpus))  # sensible ceiling

def _plan_for(level: Optional[str], chunk_size: Optional[int], workers: Optional[int]):
    """
    Returns: (algo_id, chunk, workers, zstd_level, zstd_threads)
    Tuned levels:
      - throughput/auto/snappy  -> snappy, 8 MiB, workers=cpu
      - lz4                     -> lz4,    8 MiB, workers=cpu
      - zstd / balanced / default -> zstd (level 3), 8 MiB
      - ratio / brotli          -> brotli (q=5), 8 MiB
    """
    lvl = (level or "throughput").lower()
    if lvl in ("throughput", "auto", "snappy", "speed", "fast"):
        return (ALGO_SNAPPY, int(chunk_size or CHUNK_SIZE_DEFAULT), _cpu_workers(workers), 0, 0)
    if lvl in ("lz4",):
        return (ALGO_LZ4, int(chunk_size or CHUNK_SIZE_DEFAULT), _cpu_workers(workers), 0, 0)
    if lvl in ("zstd", "balanced", "default"):
        return (ALGO_ZSTD, int(chunk_size or CHUNK_SIZE_DEFAULT), _cpu_workers(workers), 3, 0)
    if lvl in ("ratio", "brotli", "max"):
        return (ALGO_BROTLI, int(chunk_size or CHUNK_SIZE_DEFAULT), _cpu_workers(workers), 0, 0)
    # Fallback to throughput
    return (ALGO_SNAPPY, int(chunk_size or CHUNK_SIZE_DEFAULT), _cpu_workers(workers), 0, 0)

def _codec_for(algo_id: int, *, zstd_level: int = 3, zstd_threads: int = 0
               ) -> Tuple[Callable[[bytes], bytes], Callable[[bytes], bytes]]:
    """Return (compress, decompress) callables. Objects are reused where it helps."""
    if algo_id == ALGO_SNAPPY:
        return snappy.compress, snappy.uncompress
    if algo_id == ALGO_LZ4:
        return (lambda b: lz4f.compress(b)), (lambda b: lz4f.decompress(b))
    if algo_id == ALGO_ZSTD:
        # One compressor/decompressor reused from the outer scope
        zc = zstd.ZstdCompressor(level=zstd_level, threads=zstd_threads or 0)
        zd = zstd.ZstdDecompressor()
        return zc.compress, zd.decompress
    if algo_id == ALGO_BROTLI:
        return (lambda b: brotli.compress(b, quality=5)), brotli.decompress
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
    Threaded compression: read input mmapped, submit chunk compress tasks, and
    write records in-order. Uses bytes() copies to avoid exported-pointer issues.
    """
    file_size = os.path.getsize(input_filename)
    algo_id, csize, worker_count, zstd_level, zstd_threads = _plan_for(level, chunk_size, workers)
    algo_name = ALGO_NAMES[algo_id]
    comp, _ = _codec_for(algo_id, zstd_level=zstd_level, zstd_threads=zstd_threads)

    if verbose:
        print(f"Compressing {os.path.basename(input_filename)} → {os.path.basename(output_filename)} (level={level}) …")
        print(f"INFO: plan → algo={algo_name}, chunk={_fmt_bytes(csize)}, workers={worker_count}, zstd_threads={zstd_threads}")

    # Memory-map input
    with open(input_filename, "rb") as f_in, open(output_filename, "wb") as f_out:
        # Write file header
        f_out.write(struct.pack(HEADER_FMT, MAGIC, VERSION, algo_id, csize, file_size))

        mm = mmap.mmap(f_in.fileno(), 0, access=mmap.ACCESS_READ)
        try:
            mv = memoryview(mm)
            try:
                mv_len = len(mv)
                n_chunks = (mv_len + csize - 1) // csize

                def job(idx: int) -> Tuple[int, int, bytes, float]:
                    start = idx * csize
                    end = min(start + csize, mv_len)
                    # copy slice to bytes to break ties with mmap
                    t0 = time.perf_counter()
                    view = mv[start:end]
                    try:
                        data = bytes(view)
                    finally:
                        view.release()
                    cbytes = comp(data)
                    t1 = time.perf_counter()
                    return idx, len(data), cbytes, (t1 - t0)

                # Bounded in-flight to control memory usage
                next_to_write = 0
                pending: Dict[int, Tuple[int, bytes, float]] = {}
                submitted = 0

                with ThreadPoolExecutor(max_workers=worker_count) as ex:
                    futures = []
                    # Prime the queue
                    max_inflight = worker_count * 2
                    while submitted < n_chunks and len(futures) < max_inflight:
                        futures.append(ex.submit(job, submitted)); submitted += 1

                    while futures:
                        for fut in as_completed(futures):
                            idx, orig_len, cbytes, dt = fut.result()
                            pending[idx] = (orig_len, cbytes, dt)
                            futures.remove(fut)
                            # Keep submitting while we have capacity
                            if submitted < n_chunks:
                                futures.append(ex.submit(job, submitted)); submitted += 1

                            # Write any ready-in-order chunks
                            while next_to_write in pending:
                                o_len, c_b, c_dt = pending.pop(next_to_write)
                                f_out.write(struct.pack(CHUNK_HDR_FMT, o_len, len(c_b)))
                                f_out.write(c_b)
                                if verbose:
                                    mb = o_len / (1024 * 1024)
                                    thr = mb / max(c_dt, 1e-9)
                                    print(f"[compress] chunk {next_to_write:>4}: {_fmt_bytes(o_len)} → {_fmt_bytes(len(c_b))} | {thr:,.2f} MB/s")
                                next_to_write += 1
                        # loop continues until all futures resolved & written
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
        _, decomp = _codec_for(algo_id)

        # --- scan chunk records to build index ---
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

        # --- prepare output file (pre-allocate & mmap) ---
        with open(output_filename, "wb") as f_out:
            f_out.truncate(orig_size)

        if verbose:
            print(f"Decompressing with {algo_name} | chunk={_fmt_bytes(csize)} | orig={_fmt_bytes(orig_size)} | chunks={len(chunk_index)}")

        # mmap source once for fast slicing
        with open(input_filename, "rb") as fin2:
            mm_in = mmap.mmap(fin2.fileno(), 0, access=mmap.ACCESS_READ)
            try:
                # mmap destination for random writes
                with open(output_filename, "r+b") as fout2:
                    mm_out = mmap.mmap(fout2.fileno(), 0, access=mmap.ACCESS_WRITE)
                    try:
                        worker_count = _cpu_workers(workers)

                        def job(rec):
                            data_off, comp_len, out_off, orig_len = rec
                            # copy compressed bytes (avoid exported-pointer ties)
                            cview = mm_in[data_off:data_off+comp_len]
                            cbytes = bytes(cview)  # copy
                            t0 = time.perf_counter()
                            data = decomp(cbytes)
                            t1 = time.perf_counter()
                            if len(data) != orig_len:
                                raise ValueError("Decompressed length mismatch")
                            return out_off, data, (t1 - t0), orig_len

                        # Submit all jobs (bound inflight for memory)
                        max_inflight = _cpu_workers(workers) * 2
                        next_idx = 0
                        in_flight: List = []
                        total_written = 0

                        with ThreadPoolExecutor(max_workers=worker_count) as ex:
                            # prime
                            while next_idx < len(chunk_index) and len(in_flight) < max_inflight:
                                in_flight.append(ex.submit(job, chunk_index[next_idx]))
                                next_idx += 1

                            while in_flight:
                                for fut in as_completed(in_flight):
                                    out_off, data, dt, o_len = fut.result()
                                    # unordered write is fine; we write to the correct offset
                                    mm_out[out_off:out_off+len(data)] = data
                                    if verbose:
                                        mb = o_len / (1024*1024)
                                        thr = mb / max(dt, 1e-9)
                                        # compute chunk ordinal (best-effort)
                                        print(f"[decompress] chunk @+{out_off}: {_fmt_bytes(o_len)} | {thr:,.2f} MB/s")
                                    total_written += len(data)
                                    in_flight.remove(fut)

                                    # keep pipeline full
                                    if next_idx < len(chunk_index):
                                        in_flight.append(ex.submit(job, chunk_index[next_idx]))
                                        next_idx += 1
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

