# Copyright 2025
"""
warpcompress.core (v0.7.4)

Pure-Python core for a simple "WARP" container:

File header (FHDR):
  MAGIC(4) | ver(u8) | flags(u8) | chunk_size(u32 LE) | reserved(u32 LE)

Per-chunk header (CHDR):
  algo(u8) | orig_len(u32 LE) | comp_len(u32 LE)
  Followed by 'comp_len' payload bytes.

Features:
- Parallel chunk compression/decompression with ordered writes.
- Per-chunk codec choice (snappy/lz4/zstd/copy/zero).
- Zero-block elision.
- Auto chunk policy (size- and sparsity-aware) + tiny-input fast paths.
- Pure Python I/O; optional C codecs for speed.
"""

from __future__ import annotations
import io
import os
import mmap
import math
import struct
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, List, Optional, Tuple

# ----------------------------
# Optional codecs (import if available)
# ----------------------------
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

# ----------------------------
# Format constants
# ----------------------------
MAGIC = b"WARP"
VERSION = 2

ALGO_ZSTD   = 1
ALGO_LZ4    = 2
ALGO_SNAPPY = 3
ALGO_COPY   = 4
ALGO_ZERO   = 5

# Chunk limits (bytes)
CHUNK_MIN = 256 * 1024          # 256 KiB
CHUNK_DEF = 1 * 1024 * 1024     # default if nothing else applies
DEFAULT_CHUNK_MAX = 64 * 1024 * 1024  # 64 MiB default cap
CHUNK_MAX = int(os.environ.get("WARP_CHUNK_MAX", DEFAULT_CHUNK_MAX))

FHDR = struct.Struct("<4sBBII")  # magic, ver, flags, chunk_size, reserved
CHDR = struct.Struct("<BII")     # algo, orig_len, comp_len

# ----------------------------
# Small utilities
# ----------------------------
def _is_all_zero(b: memoryview) -> bool:
    """Fast-ish zero check without allocating."""
    step = 8192
    for i in range(0, len(b), step):
        if b[i:i+step].tobytes().strip(b"\x00"):
            return False
    return True

def _coerce_chunk_size(n: Optional[int]) -> int:
    if not n:
        return CHUNK_DEF
    return max(CHUNK_MIN, min(CHUNK_MAX, int(n)))

def _cpu_workers(n: Optional[int]) -> int:
    if n and n > 0:
        return n
    c = os.cpu_count() or 4
    return min(max(2, c), 64)

# ----------------------------
# TLS codec caches (bug-fixed: dict on thread-local)
# ----------------------------
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
    obj = cache.get(key)
    if obj is None:
        obj = zstd.ZstdCompressor(level=level, threads=threads or 0)
        cache[key] = obj
    return obj.compress

def _zstd_d() -> Callable[[bytes], bytes]:
    if zstd is None:
        raise RuntimeError("zstandard not available")
    cache = _tls_cache()
    key = ("zd",)
    obj = cache.get(key)
    if obj is None:
        obj = zstd.ZstdDecompressor()
        cache[key] = obj
    return obj.decompress

def _lz4_c(accel: int) -> Callable[[bytes], bytes]:
    if lz4f is None:
        raise RuntimeError("lz4.frame not available")
    def _c(b: bytes) -> bytes:
        return lz4f.compress(b, compression_level=accel, block_linked=True)
    return _c

def _lz4_d() -> Callable[[bytes], bytes]:
    if lz4f is None:
        raise RuntimeError("lz4.frame not available")
    return lz4f.decompress

def _snappy_c() -> Callable[[bytes], bytes]:
    if snappy is None:
        raise RuntimeError("python-snappy not available")
    return snappy.compress

def _snappy_d() -> Callable[[bytes], bytes]:
    if snappy is None:
        raise RuntimeError("python-snappy not available")
    return snappy.uncompress

# ----------------------------
# Helper used by the CLI
# ----------------------------
def detect_algo_name(algo: int) -> str:
    m = {
        ALGO_ZSTD:   "zstd",
        ALGO_LZ4:    "lz4",
        ALGO_SNAPPY: "snappy",
        ALGO_COPY:   "copy",
        ALGO_ZERO:   "zero",
    }
    return m.get(int(algo), f"unknown({algo})")

# ----------------------------
# Auto-chunk policy (size & sparsity aware)
# ----------------------------
def _align_up(n: int, base: int) -> int:
    return ((n + base - 1) // base) * base

def _sample_zero_ratio(path: str, *, sample_window: int = 256 * 1024, max_windows: int = 3) -> float:
    """
    Estimate how 'zero-heavy' a file is by sampling up to 3 small windows:
    start, middle, and end. Returns ratio in [0,1].
    """
    try:
        size = os.path.getsize(path)
        if size <= 0:
            return 0.0
        offs = [0]
        if size > sample_window:
            offs.append(max(0, (size // 2) - sample_window // 2))
        if size > 2 * sample_window:
            offs.append(max(0, size - sample_window))
        offs = offs[:max_windows]
        zeros = total = 0
        with open(path, "rb") as f:
            for off in offs:
                f.seek(off)
                chunk = f.read(sample_window)
                if not chunk:
                    continue
                zeros += chunk.count(0)
                total += len(chunk)
        return (zeros / total) if total else 0.0
    except Exception:
        return 0.0

def _policy_chunk_size(
    file_size: int,
    *,
    requested: Optional[int],
    level: str,
    workers: int,
    zero_ratio_hint: float | None = None,
) -> int:
    """
    Rule-based policy for picking chunk size.

    Goals:
      - Tiny/small files: ~1–8 chunks total (reduce Python/syscall overhead).
      - Medium/large files: scale chunk up to keep chunk count reasonable.
      - Very sparse: bump to max to minimize header/syscall cost.
      - Explicit --chunk always wins.
    """
    # 1) explicit override wins
    if requested:
        return _coerce_chunk_size(requested)

    # 2) small-file bands (choose chunk to hit a target chunk count)
    if file_size <= (2 * 1024**2):            # <= 2 MiB → 1 chunk
        chunk = _align_up(file_size, 256 * 1024) or (256 * 1024)
    elif file_size <= (8 * 1024**2):          # <= 8 MiB → ~2 chunks
        chunk = _align_up((file_size + 1) // 2, 256 * 1024)
    elif file_size <= (32 * 1024**2):         # <= 32 MiB → ~4–8 chunks
        chunk = _align_up((file_size + 3) // 4, 512 * 1024)
    elif file_size <= (128 * 1024**2):        # <= 128 MiB → ~8–16 chunks
        chunk = _align_up((file_size + 7) // 8, 1 * 1024 * 1024)
    else:
        # 3) medium/large bands (clamped to CHUNK_MAX)
        if file_size <= (1 * 1024**3):        # <= 1 GiB
            chunk = 4 * 1024 * 1024
        elif file_size <= (5 * 1024**3):      # <= 5 GiB
            chunk = 8 * 1024 * 1024
        elif file_size <= (10 * 1024**3):     # <= 10 GiB
            chunk = 16 * 1024 * 1024
        elif file_size <= (50 * 1024**3):     # <= 50 GiB
            chunk = 32 * 1024 * 1024
        elif file_size <= (100 * 1024**3):    # <= 100 GiB
            chunk = 48 * 1024 * 1024
        else:                                  # > 100 GiB
            chunk = 64 * 1024 * 1024

    # 4) very sparse -> prefer max
    zr = zero_ratio_hint if zero_ratio_hint is not None else 0.0
    if zr >= 0.90:
        chunk = max(chunk, CHUNK_MAX)

    return _coerce_chunk_size(chunk)

# ----------------------------
# Planning & codec choice
# ----------------------------
class Plan:
    __slots__ = ("level", "chunk", "workers", "zstd_threads", "lz4_accel", "verbose")
    def __init__(self, level: str, *, chunk: int, workers: int,
                 zstd_threads: int, lz4_accel: int, verbose: bool):
        self.level = level
        self.chunk = chunk
        self.workers = workers
        self.zstd_threads = zstd_threads
        self.lz4_accel = lz4_accel
        self.verbose = verbose

def _make_plan(level: str, *, chunk: Optional[int], workers: Optional[int],
               zstd_hybrid: str = "auto", verbose: bool = False) -> Plan:
    lvl = (level or "throughput").lower()
    if lvl not in ("throughput", "zstd", "lz4", "ratio"):
        raise ValueError(f"Unknown level: {level}")
    csize = _coerce_chunk_size(chunk)
    w = _cpu_workers(workers)
    zt = 0
    lz_acc = 4 if lvl in ("throughput", "lz4") else 1
    if verbose:
        base = "snappy" if lvl == "throughput" else lvl
        print(f"Base hint: {base} (per-chunk adaptive)", flush=True)
        print(
            f"INFO: base plan -> chunk={csize/1048576:.2f} MiB, "
            f"workers={w}, zstd_threads={zt}, lz4_accel={lz_acc}",
            flush=True,
        )
    return Plan(lvl, chunk=csize, workers=w, zstd_threads=zt, lz4_accel=lz_acc, verbose=verbose)

def _choose_alg(plan: Plan, block: memoryview) -> Tuple[int, Callable[[bytes], bytes]]:
    if _is_all_zero(block):
        return ALGO_ZERO, (lambda _b: b"")
    lvl = plan.level
    if lvl == "throughput":
        if snappy is not None:
            return ALGO_SNAPPY, _snappy_c()
        if lz4f is not None:
            return ALGO_LZ4, _lz4_c(plan.lz4_accel)
        if zstd is not None:
            return ALGO_ZSTD, _zstd_c(1, plan.zstd_threads)
        return ALGO_COPY, (lambda b: bytes(b))
    if lvl == "lz4":
        return (ALGO_LZ4, _lz4_c(plan.lz4_accel)) if lz4f else (ALGO_COPY, lambda b: bytes(b))
    if lvl == "zstd":
        return (ALGO_ZSTD, _zstd_c(1, plan.zstd_threads)) if zstd else (ALGO_COPY, lambda b: bytes(b))
    # ratio
    return (ALGO_ZSTD, _zstd_c(3, plan.zstd_threads)) if zstd else (ALGO_COPY, lambda b: bytes(b))

def _decomp_fn(algo: int) -> Callable[[bytes], bytes]:
    if algo == ALGO_ZERO:   return lambda _b: b""
    if algo == ALGO_COPY:   return lambda b: b
    if algo == ALGO_SNAPPY: return _snappy_d()
    if algo == ALGO_LZ4:    return _lz4_d()
    if algo == ALGO_ZSTD:   return _zstd_d()
    raise ValueError(f"Unknown algo {algo}")

# ----------------------------
# Public API
# ----------------------------
def compress_file(
    input_filename: str,
    output_filename: str,
    *,
    level: str = "throughput",
    workers: Optional[int] = None,
    chunk: Optional[int] = None,
    zstd_hybrid: str = "auto",
    verbose: bool = False,
) -> None:
    """Compress input_filename into a Warp container at output_filename."""

    # Compute workers early for policy use
    w = _cpu_workers(workers)

    # Auto-chunk policy (can be disabled via env)
    auto_on = os.environ.get("WARP_AUTO_CHUNK", "1") not in ("0", "false", "False", "")
    if auto_on:
        try:
            file_size = os.path.getsize(input_filename)
        except Exception:
            file_size = 0
        zero_hint = _sample_zero_ratio(input_filename) if file_size > 0 else 0.0
        chosen_chunk = _policy_chunk_size(
            file_size,
            requested=chunk,
            level=level,
            workers=w,
            zero_ratio_hint=zero_hint,
        )
        if verbose:
            print(
                f"Policy: size={file_size}B zero≈{zero_hint:.2%} -> chunk={chosen_chunk/1048576:.1f} MiB",
                flush=True,
            )
    else:
        chosen_chunk = _coerce_chunk_size(chunk)

    # Build plan using the chosen chunk
    plan = _make_plan(level, chunk=chosen_chunk, workers=workers, zstd_hybrid=zstd_hybrid, verbose=verbose)

    with open(input_filename, "rb") as f_in, open(output_filename, "wb") as f_out:
        # Write container header
        _write_fhdr(f_out, plan.chunk)

        # Prefer mmap for read parallelism
        try:
            fileno = f_in.fileno()
            total = os.fstat(fileno).st_size
            mm = mmap.mmap(fileno, 0, access=mmap.ACCESS_READ)
            use_mm = True
        except Exception:
            mm = None
            total = 0
            use_mm = False

        def read_block(i: int) -> memoryview:
            if use_mm:
                off = i * plan.chunk
                end = min(off + plan.chunk, total)
                return memoryview(mm)[off:end]
            else:
                return memoryview(f_in.read(plan.chunk))

        # estimate block count
        blocks = math.ceil(total / plan.chunk) if use_mm and total else None
        zero_blocks = 0

        def job(i: int, blk: memoryview) -> Tuple[int, bytes]:
            algo, enc = _choose_alg(plan, blk)
            if algo == ALGO_ZERO:
                comp = b""
            elif algo == ALGO_COPY:
                comp = bytes(blk)
            else:
                comp = enc(blk.tobytes())
            return i, CHDR.pack(algo, len(blk), len(comp)) + comp

        # --- Tiny/low-chunk fast path (sequential, avoids thread overhead) ---
        est_blocks = blocks
        if est_blocks is None:
            try:
                fsz = os.path.getsize(input_filename)
                est_blocks = max(1, (fsz + plan.chunk - 1) // plan.chunk)
            except Exception:
                est_blocks = 0

        if (est_blocks and est_blocks <= 2) or (total and total <= (32 * 1024 * 1024)):
            i = 0
            while True:
                blk = read_block(i)
                if not blk:
                    break
                if _is_all_zero(blk):
                    zero_blocks += 1
                _idx, payload = job(i, blk)
                f_out.write(payload)
                i += 1
            if verbose and zero_blocks:
                print(f"[compress] (seq) 0+ zero x{zero_blocks}", flush=True)
            return  # done, skip threaded path

        # --- Threaded path ---
        next_idx = 0
        pending = {}
        with ThreadPoolExecutor(max_workers=plan.workers) as ex:
            futs = []
            if use_mm:
                blocks_mm = math.ceil(total / plan.chunk) if total else 0
                for i in range(blocks_mm):
                    blk = read_block(i)
                    if not blk:
                        break
                    if _is_all_zero(blk):
                        zero_blocks += 1
                    futs.append(ex.submit(job, i, blk))
            else:
                i = 0
                while True:
                    data = f_in.read(plan.chunk)
                    if not data:
                        break
                    blk = memoryview(data)
                    if _is_all_zero(blk):
                        zero_blocks += 1
                    futs.append(ex.submit(job, i, blk))
                    i += 1

            for fut in as_completed(futs):
                idx, payload = fut.result()
                pending[idx] = payload
                while next_idx in pending:
                    f_out.write(pending.pop(next_idx))
                    next_idx += 1

    if verbose and zero_blocks:
        print(f"[compress] 0+ zero x{zero_blocks}", flush=True)

def decompress_file(
    input_filename: str,
    output_filename: str,
    *,
    workers: Optional[int] = None,
    verbose: bool = False,
) -> None:
    """Decompress a Warp container into raw output file."""
    w = _cpu_workers(workers)

    # 1) Scan the chunk table: (algo, orig_len, comp_len, payload_off)
    meta: List[Tuple[int, int, int, int]] = []
    with open(input_filename, "rb") as f:
        _ver, _chunk_size = _read_fhdr(f)
        file_off = FHDR.size
        while True:
            hdr = f.read(CHDR.size)
            if not hdr:
                break
            if len(hdr) != CHDR.size:
                raise ValueError("Truncated container (chunk header)")
            algo, orig_len, comp_len = CHDR.unpack(hdr)
            payload_off = file_off + CHDR.size
            meta.append((algo, orig_len, comp_len, payload_off))
            f.seek(comp_len, io.SEEK_CUR)
            file_off += CHDR.size + comp_len

    # 2) Compute output offsets (prefix sum)
    offs = [0] * len(meta)
    for i in range(1, len(meta)):
        offs[i] = offs[i - 1] + meta[i - 1][1]
    total_out = sum(m[1] for m in meta)

    # --- Tiny/low-chunk fast path ---
    if len(meta) <= 2 or total_out <= (32 * 1024 * 1024):
        with open(output_filename, "wb") as f_out, open(input_filename, "rb") as fi:
            try:
                f_out.truncate(total_out)
            except Exception:
                pass
            for i, (algo, orig, comp, payload_off) in enumerate(meta):
                if algo == ALGO_ZERO:
                    data = b"\x00" * orig
                elif algo == ALGO_COPY:
                    fi.seek(payload_off); data = fi.read(orig)
                else:
                    dec = _decomp_fn(algo)
                    fi.seek(payload_off); payload = fi.read(comp)
                    data = dec(payload)
                f_out.seek(offs[i]); f_out.write(data)
        if verbose:
            print("[decompress] (seq) done", flush=True)
        return

    # 3) Worker to decode a single chunk
    def djob(i: int, algo: int, orig: int, comp: int, payload_off: int) -> Tuple[int, bytes]:
        if algo == ALGO_ZERO:
            return i, b"\x00" * orig
        if algo == ALGO_COPY:
            with open(input_filename, "rb") as fi:
                fi.seek(payload_off)
                return i, fi.read(orig)
        dec = _decomp_fn(algo)
        with open(input_filename, "rb") as fi:
            fi.seek(payload_off)
            payload = fi.read(comp)
        return i, dec(payload)

    # 4) Parallel decode; write in order
    pending = {}
    next_idx = 0
    with open(output_filename, "wb") as f_out, ThreadPoolExecutor(max_workers=w) as ex:
        try:
            f_out.truncate(total_out)
        except Exception:
            pass
        futures = {ex.submit(djob, i, *m): i for i, m in enumerate(meta)}
        while futures:
            fut = next(as_completed(futures))
            idx, data = fut.result()
            del futures[fut]
            pending[idx] = data
            while next_idx in pending:
                f_out.seek(offs[next_idx])
                f_out.write(pending.pop(next_idx))
                next_idx += 1

    if verbose:
        print("[decompress] done", flush=True)

# ----------------------------
# I/O helpers (last to keep code tidy)
# ----------------------------
def _write_fhdr(fout: io.BufferedWriter, chunk: int) -> None:
    fout.write(FHDR.pack(MAGIC, VERSION, 0, chunk, 0))

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
