
# Copyright 2025
# core.py — WarpCompress core (mmap + safe memoryview release)
from __future__ import annotations
import os
import time
import mmap
import struct
from typing import Callable, Tuple

import snappy
import lz4.frame as lz4f
import zstandard as zstd
import brotli

# -----------------------------
# Container / protocol constants
# -----------------------------
MAGIC = b"WARP"            # 4 bytes
VERSION = 1                # 1 byte
HEADER_FMT = ">4sB B I Q"  # MAGIC, VER, ALGO_ID, CHUNK_SIZE, ORIG_SIZE
HEADER_SIZE = struct.calcsize(HEADER_FMT)

CHUNK_SIZE_DEFAULT = 4 * 1024 * 1024  # 4 MiB per chunk

# Per-chunk header: orig_len (uint32), comp_len (uint32)
CHUNK_HDR_FMT = ">II"
CHUNK_HDR_SIZE = struct.calcsize(CHUNK_HDR_FMT)

# Algorithms
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

# -----------------------------
# Utilities
# -----------------------------
def _fmt_bytes(n: int) -> str:
    for u in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024.0:
            return f"{n:.2f} {u}"
        n /= 1024.0
    return f"{n:.2f} PB"

def _choose_algo(mode: str) -> int:
    m = (mode or "auto").lower()
    if m in ("snappy", "throughput", "speed", "fast"):
        return ALGO_SNAPPY
    if m in ("lz4",):
        return ALGO_LZ4
    if m in ("balanced", "default", "zstd"):
        return ALGO_ZSTD
    if m in ("ratio", "max", "brotli"):
        return ALGO_BROTLI
    return ALGO_SNAPPY  # auto → throughput

# -----------------------------
# Codecs
# -----------------------------
def _codec_for(algo_id: int) -> Tuple[Callable[[bytes], bytes], Callable[[bytes], bytes]]:
    if algo_id == ALGO_SNAPPY:
        return snappy.compress, snappy.uncompress
    if algo_id == ALGO_LZ4:
        return (lambda b: lz4f.compress(b)), (lambda b: lz4f.decompress(b))
    if algo_id == ALGO_ZSTD:
        zc = zstd.ZstdCompressor(level=3)
        zd = zstd.ZstdDecompressor()
        return zc.compress, zd.decompress
    if algo_id == ALGO_BROTLI:
        return (lambda b: brotli.compress(b, quality=5)), brotli.decompress
    raise ValueError(f"Unknown algorithm id: {algo_id}")

# -----------------------------
# Public API
# -----------------------------
def compress_file(
    input_filename: str,
    output_filename: str,
    level: str = "auto",
    *,
    chunk_size: int = CHUNK_SIZE_DEFAULT,
    verbose: bool = False,
) -> None:
    """Compress using mmap + memoryviews (released safely each loop)."""
    file_size = os.path.getsize(input_filename)
    algo_id = _choose_algo(level)
    algo_name = ALGO_NAMES[algo_id]
    comp, _ = _codec_for(algo_id)

    if verbose:
        print(f"Compressing {os.path.basename(input_filename)} → "
              f"{os.path.basename(output_filename)} (level={level}) …")
        if level.lower() in ("auto", "throughput", "fast", "speed"):
            print(f"INFO: Auto-selected {algo_name}.")

    with open(input_filename, "rb") as f_in, open(output_filename, "wb") as f_out:
        # File header
        f_out.write(struct.pack(HEADER_FMT, MAGIC, VERSION, algo_id, int(chunk_size), int(file_size)))

        mm = mmap.mmap(f_in.fileno(), 0, access=mmap.ACCESS_READ)
        try:
            mv = memoryview(mm)
            try:
                pos = 0
                idx = 0
                mv_len = len(mv)
                while pos < mv_len:
                    end = min(pos + chunk_size, mv_len)

                    # Take a view on the chunk, copy bytes, then RELEASE the view.
                    t0 = time.perf_counter()
                    chunk_view = mv[pos:end]
                    try:
                        data = bytes(chunk_view)          # copy → no exported pointer
                    finally:
                        chunk_view.release()              # critical on 3.12/3.13

                    c_bytes = comp(data)
                    t1 = time.perf_counter()

                    f_out.write(struct.pack(CHUNK_HDR_FMT, len(data), len(c_bytes)))
                    f_out.write(c_bytes)

                    if verbose:
                        dt = max(t1 - t0, 1e-9)
                        mb = len(data) / (1024 * 1024)
                        print(f"[compress] chunk {idx:>4} : "
                              f"{_fmt_bytes(len(data))} → {_fmt_bytes(len(c_bytes))} | {mb/dt:,.2f} MB/s")
                    idx += 1
                    pos = end
            finally:
                mv.release()  # release top-level view before closing mmap
        finally:
            mm.close()

def decompress_file(
    input_filename: str,
    output_filename: str,
    *,
    verbose: bool = False,
) -> None:
    """Decompress a .warp made by compress_file()."""
    with open(input_filename, "rb") as f_in:
        hdr = f_in.read(HEADER_SIZE)
        if len(hdr) != HEADER_SIZE:
            raise ValueError("Input too small to be a valid .warp file")

        magic, ver, algo_id, chunk_size, orig_size = struct.unpack(HEADER_FMT, hdr)
        if magic != MAGIC:
            raise ValueError("Bad magic; not a warp file")
        if ver != VERSION:
            raise ValueError(f"Unsupported version: {ver}")
        if algo_id not in ALGO_NAMES:
            raise ValueError(f"Unknown algorithm id in file: {algo_id}")

        algo_name = ALGO_NAMES[algo_id]
        _, decomp = _codec_for(algo_id)

        if verbose:
            print(f"Decompressing with {algo_name} | chunk={_fmt_bytes(chunk_size)} | "
                  f"orig={_fmt_bytes(orig_size)}")

        remaining = orig_size
        total_out = 0

        with open(output_filename, "wb") as f_out:
            idx = 0
            while True:
                ch = f_in.read(CHUNK_HDR_SIZE)
                if not ch:
                    break
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
                remaining = max(0, remaining - len(data))

                if verbose:
                    dt = max(t1 - t0, 1e-9)
                    mb = len(data) / (1024 * 1024)
                    print(f"[decompress] chunk {idx:>4} : "
                          f"{_fmt_bytes(comp_len)} → {_fmt_bytes(len(data))} | {mb/dt:,.2f} MB/s")
                idx += 1

        if total_out != orig_size:
            raise ValueError(f"Size mismatch: expected {orig_size}, wrote {total_out}")

# Convenience for CLI messaging
def detect_algo_name(level: str) -> str:
    return ALGO_NAMES[_choose_algo(level)]


