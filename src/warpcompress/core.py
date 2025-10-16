
# Copyright 2025
# src/warpcompress/core.py
# WarpCompress core — mmap + safe memoryview release (Py 3.12/3.13 compatible)

from __future__ import annotations
import os
import time
import mmap
import struct
from typing import Callable, Tuple, Optional

# External codecs
import snappy
import lz4.frame as lz4f
import zstandard as zstd
import brotli

# =========================
# Container / wire constants
# =========================

MAGIC = b"WARP"                 # 4 bytes
VERSION = 1                     # 1 byte
# MAGIC(4s) | VER(B) | ALGO(B) | CHUNK_SIZE(I) | ORIG_SIZE(Q)
HEADER_FMT = ">4sB B I Q"
HEADER_SIZE = struct.calcsize(HEADER_FMT)

# Per-chunk header: ORIG_LEN(uint32), COMP_LEN(uint32)
CHUNK_HDR_FMT = ">II"
CHUNK_HDR_SIZE = struct.calcsize(CHUNK_HDR_FMT)

# Default chunk
CHUNK_SIZE_DEFAULT = 2 * 1024 * 1024  # 2 MiB (matches your throughput log)

# =================
# Algorithms mapping
# =================

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

# =========
# Utilities
# =========

def _fmt_bytes(n: int) -> str:
    x = float(n)
    for u in ("B", "KB", "MB", "GB", "TB"):
        if x < 1024.0:
            return f"{x:.2f} {u}"
        x /= 1024.0
    return f"{x:.2f} PB"

def _choose_plan(level: Optional[str]) -> tuple[int, int, int, int]:
    """
    Return (algo_id, chunk_size, threads, zstd_threads) for a given 'level'.
    - 'throughput'/'auto' → snappy, 2MiB, threads=4
    - 'lz4'               → lz4,    2MiB
    - 'balanced'/'zstd'   → zstd,   2MiB
    - 'ratio'/'brotli'    → brotli, 2MiB
    """
    lvl = (level or "auto").lower()
    if lvl in ("throughput", "auto", "speed", "fast", "snappy"):
        return (ALGO_SNAPPY, CHUNK_SIZE_DEFAULT, 4, 0)
    if lvl in ("lz4",):
        return (ALGO_LZ4, CHUNK_SIZE_DEFAULT, 0, 0)
    if lvl in ("balanced", "default", "zstd"):
        return (ALGO_ZSTD, CHUNK_SIZE_DEFAULT, 0, 0)  # set zstd_threads>0 if you want MT
    if lvl in ("ratio", "max", "brotli"):
        return (ALGO_BROTLI, CHUNK_SIZE_DEFAULT, 0, 0)
    # Fallback to throughput plan
    return (ALGO_SNAPPY, CHUNK_SIZE_DEFAULT, 4, 0)

# =======
# Codecs
# =======

def _codec_for(algo_id: int, *, zstd_level: int = 3, zstd_threads: int = 0
               ) -> Tuple[Callable[[bytes], bytes], Callable[[bytes], bytes]]:
    if algo_id == ALGO_SNAPPY:
        return snappy.compress, snappy.uncompress
    if algo_id == ALGO_LZ4:
        return (lambda b: lz4f.compress(b)), (lambda b: lz4f.decompress(b))
    if algo_id == ALGO_ZSTD:
        if zstd_threads and zstd_threads > 0:
            zc = zstd.ZstdCompressor(level=zstd_level, threads=zstd_threads)
        else:
            zc = zstd.ZstdCompressor(level=zstd_level)
        zd = zstd.ZstdDecompressor()
        return zc.compress, zd.decompress
    if algo_id == ALGO_BROTLI:
        return (lambda b: brotli.compress(b, quality=5)), brotli.decompress
    raise ValueError(f"Unknown algorithm id: {algo_id}")

# ===========
# Public API
# ===========

def compress_file(
    input_filename: str,
    output_filename: str,
    level: str = "auto",
    *,
    chunk_size: Optional[int] = None,
    verbose: bool = False,
) -> None:
    """
    Compress `input_filename` into a .warp container at `output_filename`.
    Uses mmap + memoryviews but **always** releases views before closing mmap
    to avoid BufferError: "cannot close exported pointers exist".
    """
    file_size = os.path.getsize(input_filename)

    algo_id, plan_chunk, threads, zstd_threads = _choose_plan(level)
    csize = int(chunk_size or plan_chunk)
    algo_name = ALGO_NAMES[algo_id]

    if verbose:
        # Match your previous log style
        print(f"Compressing {os.path.basename(input_filename)} → {os.path.basename(output_filename)} (level={level}) …")
        print(f"INFO: Throughput plan → algo={algo_name}, chunk={_fmt_bytes(csize)}, threads={threads}, zstd_threads={zstd_threads}")

    comp, _ = _codec_for(algo_id, zstd_level=3, zstd_threads=zstd_threads)

    with open(input_filename, "rb") as f_in, open(output_filename, "wb") as f_out:
        # Write container header
        f_out.write(struct.pack(HEADER_FMT, MAGIC, VERSION, algo_id, csize, file_size))

        # IMPORTANT: manage mmap manually (no `with mmap(...) as mm:`)
        mm = mmap.mmap(f_in.fileno(), 0, access=mmap.ACCESS_READ)
        try:
            mv = memoryview(mm)
            try:
                pos = 0
                idx = 0
                mv_len = len(mv)
                while pos < mv_len:
                    end = min(pos + csize, mv_len)

                    # Take a slice view → copy to bytes → RELEASE view immediately
                    t0 = time.perf_counter()
                    chunk_view = mv[pos:end]
                    try:
                        data = bytes(chunk_view)  # breaks exported-pointer tie with mm
                    finally:
                        chunk_view.release()      # critical for Py 3.12/3.13

                    cbytes = comp(data)
                    t1 = time.perf_counter()

                    # Write per-chunk header + payload
                    f_out.write(struct.pack(CHUNK_HDR_FMT, len(data), len(cbytes)))
                    f_out.write(cbytes)

                    if verbose:
                        dt = max(t1 - t0, 1e-9)
                        mb = len(data) / (1024 * 1024)
                        print(f"[compress] chunk {idx:>4}: {_fmt_bytes(len(data))} → {_fmt_bytes(len(cbytes))} | {mb/dt:,.2f} MB/s")
                    idx += 1
                    pos = end
            finally:
                # Release top-level view before closing the mmap
                mv.release()
        finally:
            mm.close()


def decompress_file(
    input_filename: str,
    output_filename: str,
    *,
    verbose: bool = False,
) -> None:
    """
    Decompress a .warp file created by `compress_file`.
    Uses simple streaming I/O (no mmap needed).
    """
    with open(input_filename, "rb") as f_in:
        hdr = f_in.read(HEADER_SIZE)
        if len(hdr) != HEADER_SIZE:
            raise ValueError("Input too small to be a valid .warp file")

        magic, ver, algo_id, csize, orig_size = struct.unpack(HEADER_FMT, hdr)
        if magic != MAGIC:
            raise ValueError("Bad magic; not a warp file")
        if ver != VERSION:
            raise ValueError(f"Unsupported version: {ver}")
        if algo_id not in ALGO_NAMES:
            raise ValueError(f"Unknown algorithm id in file: {algo_id}")

        algo_name = ALGO_NAMES[algo_id]
        _, decomp = _codec_for(algo_id)

        if verbose:
            print(f"Decompressing with {algo_name} | chunk={_fmt_bytes(csize)} | orig={_fmt_bytes(orig_size)}")

        total_out = 0
        with open(output_filename, "wb") as f_out:
            idx = 0
            while True:
                ch = f_in.read(CHUNK_HDR_SIZE)
                if not ch:
                    break  # EOF
                if len(ch) != CHUNK_HDR_SIZE:
                    raise ValueError("Truncated chunk header")

                orig_len, comp_len = struct.unpack(CHUNK_HDR_FMT, ch)
                cbytes = f_in.read(comp_len)
                if len(cbytes) != comp_len:
                    raise ValueError("Truncated chunk data")

                t0 = time.perf_counter()
                data = decomp(cbytes)
                t1 = time.perf_counter()

                if len(data) != orig_len:
                    raise ValueError("Decompressed length mismatch")

                f_out.write(data)
                total_out += len(data)

                if verbose:
                    dt = max(t1 - t0, 1e-9)
                    mb = len(data) / (1024 * 1024)
                    print(f"[decompress] chunk {idx:>4}: {_fmt_bytes(comp_len)} → {_fmt_bytes(len(data))} | {mb/dt:,.2f} MB/s")
                idx += 1

        if total_out != orig_size:
            raise ValueError(f"Size mismatch: expected {orig_size}, wrote {total_out}")

# ---------------------------
# Small helper for CLI/UX text
# ---------------------------

def detect_algo_name(level: str) -> str:
    algo_id, _, _, _ = _choose_plan(level)
    return ALGO_NAMES[algo_id]

