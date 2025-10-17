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

"""
warpcompress.core (v0.7.5)

Pure-Python core for a simple "WARP" container.

Container layout
----------------
File header (FHDR):
  MAGIC(4) | ver(u8) | flags(u8) | chunk_size(u32 LE) | reserved(u32 LE)

Per-chunk header (CHDR):
  algo(u8) | orig_len(u32 LE) | comp_len(u32 LE)
  Followed by 'comp_len' payload bytes.

Algorithms:
  1 = zstd, 2 = lz4, 3 = snappy, 4 = copy (no compression), 5 = zero (elide)

This version focuses on speed:
- Prefer LZ4 (block API) for --level throughput (faster than snappy on Apple Silicon)
- Threaded zstd on compression
- Single-syscall writes with os.writev (avoid header+payload concatenation)
- Zero-copy COPY blocks (memoryview of the mmap slice)
- Small-file fast path (<=32 MiB): one chunk, no thread pool
- Simple incompressible heuristic to skip hopeless chunks
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
# Optional codecs
# ----------------------------
try:
    import zstandard as zstd  # type: ignore
except Exception:  # pragma: no cover
    zstd = None  # type: ignore

# Prefer lz4.block (lower overhead) but fall back to lz4.frame
try:
    import lz4.block as lz4b  # type: ignore
except Exception:  # pragma: no cover
    lz4b = None  # type: ignore

try:
    import lz4.frame as lz4f  # type: ignore
except Exception:  # pragma: no cover
    lz4f = None  # type: ignore

try:
    import snappy as pysnappy  # type: ignore
except Exception:  # pragma: no cover
    pysnappy = None  # type: ignore

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

FHDR = struct.Struct("<4sBBII")  # magic, ver, flags, chunk_size, reserved
CHDR = struct.Struct("<BII")     # algo, orig_len, comp_len

MiB = 1024 * 1024
GiB = 1024 * MiB

# Policy caps (can be overridden by env)
DEFAULT_CHUNK_MAX = 64 * MiB
CHUNK_MAX = int(os.environ.get("WARP_CHUNK_MAX", DEFAULT_CHUNK_MAX))
CHUNK_MIN = 256 * 1024

# ----------------------------
# Small utilities
# ----------------------------
def _align_up(n: int, base: int) -> int:
    return ((n + base - 1) // base) * base

def _is_all_zero(mv: memoryview) -> bool:
    """Fast-ish check without allocating; scan in steps and strip zeros."""
    step = 8192
    for i in range(0, len(mv), step):
        if mv[i:i+step].tobytes().strip(b"\x00"):
            return False
    return True

def _looks_incompressible(mv: memoryview) -> bool:
    """Quick heuristic: sample <=16 KiB and check byte diversity."""
    n = min(len(mv), 16 * 1024)
    if n == 0:
        return False
    sample = mv[:n].tobytes()
    uniq = len(set(sample))
    return uniq > (0.90 * len(sample))

def _coerce_chunk_size(n: Optional[int]) -> int:
    if not n:
        return 1 * MiB
    return max(CHUNK_MIN, min(CHUNK_MAX, int(n)))

def _cpu_workers(n: Optional[int]) -> int:
    if n and n > 0:
        return n
    c = os.cpu_count() or 4
    return min(max(2, c), 64)

def _writev(fd: int, parts) -> None:
    """
    Single-syscall write of multiple buffers when supported.
    Falls back to sequential writes.
    """
    try:
        os.writev(fd, [p if isinstance(p, memoryview) else memoryview(p) for p in parts])
    except (AttributeError, OSError):
        for p in parts:
            if isinstance(p, memoryview):
                os.write(fd, p)
            else:
                os.write(fd, memoryview(p))

# ----------------------------
# TLS codec caches
# ----------------------------
_tls = threading.local()

def _tls_cache() -> dict:
    c = getattr(_tls, "_cache", None)
    if c is None:
        c = {}
        setattr(_tls, "_cache", c)
    return c

def _zstd_c(level: int, threads: int):
    if zstd is None:
        raise RuntimeError("zstandard not available")
    cache = _tls_cache()
    key = ("zc", level, max(1, threads))
    obj = cache.get(key)
    if obj is None:
        obj = zstd.ZstdCompressor(level=level, threads=max(1, threads))
        cache[key] = obj
    return obj.compress

def _zstd_d():
    if zstd is None:
        raise RuntimeError("zstandard not available")
    cache = _tls_cache()
    key = ("zd",)
    obj = cache.get(key)
    if obj is None:
        obj = zstd.ZstdDecompressor()
        cache[key] = obj
    return obj.decompress

def _lz4b_c(accel: int) -> Callable[[bytes], bytes]:
    if lz4b is None:
        raise RuntimeError("lz4.block not available")
    # store_size=False since we already store orig_len in CHDR
    return lambda b: lz4b.compress(b, store_size=False, mode="default", acceleration=accel or 1)

def _lz4b_d(uncompressed_size: int) -> Callable[[bytes], bytes]:
    if lz4b is None:
        raise RuntimeError("lz4.block not available")
    return lambda b: lz4b.decompress(b, uncompressed_size=uncompressed_size)

def _lz4f_c(accel: int) -> Callable[[bytes], bytes]:
    if lz4f is None:
        raise RuntimeError("lz4.frame not available")
    return lambda b: lz4f.compress(b, compression_level=accel or 0, content_checksum=False)

def _lz4f_d() -> Callable[[bytes], bytes]:
    if lz4f is None:
        raise RuntimeError("lz4.frame not available")
    return lz4f.decompress

def _snappy_c() -> Callable[[bytes], bytes]:
    if pysnappy is None:
        raise RuntimeError("snappy not available")
    return pysnappy.compress

def _snappy_d() -> Callable[[bytes], bytes]:
    if pysnappy is None:
        raise RuntimeError("snappy not available")
    return pysnappy.decompress

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
# Auto-chunk policy (from your runs)
# ----------------------------
def _sample_zero_ratio(path: str, *, sample_window: int = 256 * 1024, max_windows: int = 3) -> float:
    """Estimate zero fraction by sampling up to three small windows."""
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
    zero_ratio_hint: float = 0.0,
) -> int:
    """
    Choose chunk bytes. Tuned for Apple Silicon based on your data:
    - <= 32 MiB: one chunk (avoid thread overhead)
    - 32..128 MiB: 16 MiB
    - 128..512 MiB: 32 MiB
    - 512 MiB..2 GiB: 48 MiB (mixed ~1 GiB peaked around here)
    - > 2 GiB: 64 MiB (cap)
    - Very sparse (zstd level): keep larger chunks
    """
    if requested:
        return _coerce_chunk_size(requested)

    if file_size <= 32 * MiB:
        return _coerce_chunk_size(file_size or 1 * MiB)

    if level == "zstd" and zero_ratio_hint >= 0.90:
        if file_size <= 256 * MiB:
            return _coerce_chunk_size(16 * MiB)
        elif file_size <= 1 * GiB:
            return _coerce_chunk_size(32 * MiB)
        elif file_size <= 2 * GiB:
            return _coerce_chunk_size(48 * MiB)
        else:
            return _coerce_chunk_size(64 * MiB)

    if file_size <= 128 * MiB:
        return _coerce_chunk_size(16 * MiB)
    elif file_size <= 512 * MiB:
        return _coerce_chunk_size(32 * MiB)
    elif file_size <= 2 * GiB:
        return _coerce_chunk_size(48 * MiB)
    else:
        return _coerce_chunk_size(64 * MiB)

# ----------------------------
# Planning & codec choice
# ----------------------------
class Plan:
    __slots__ = ("level", "chunk", "workers", "zstd_threads", "lz4_accel", "verbose")
    def __init__(self, level: str, *, chunk: int, workers: int, zstd_threads: int, lz4_accel: int, verbose: bool):
        self.level = level
        self.chunk = chunk
        self.workers = workers
        self.zstd_threads = zstd_threads
        self.lz4_accel = lz4_accel
        self.verbose = verbose

def _make_plan(level: str, *, chunk: Optional[int], workers: Optional[int], verbose: bool = False) -> Plan:
    lvl = (level or "throughput").lower()
    if lvl not in ("throughput", "zstd", "lz4", "ratio"):
        raise ValueError(f"Unknown level: {level}")
    csize = _coerce_chunk_size(chunk)
    w = _cpu_workers(workers)
    # zstd threads: use workers for max parallelism
    zt = max(1, w)
    # lz4 accel: mild acceleration for lz4 paths
    lz_acc = 1 if lvl in ("throughput", "lz4") else 0
    if verbose:
        print(f"INFO: plan -> level={lvl}, chunk={csize/1048576:.1f} MiB, workers={w}, zstd_threads={zt}, lz4_accel={lz_acc}", flush=True)
    return Plan(lvl, chunk=csize, workers=w, zstd_threads=zt, lz4_accel=lz_acc, verbose=verbose)

def _choose_compressor(plan: Plan, orig_len: int) -> Tuple[int, Callable[[bytes], bytes], Optional[Callable[[bytes], bytes]]]:
    """
    Return (algo_code, compress_fn, optional_decomp_provider)
    Decompression provider is only needed for lz4.block (needs uncompressed_size).
    """
    lvl = plan.level
    if lvl == "throughput":
        # Prefer lz4.block
        if lz4b is not None:
            return ALGO_LZ4, _lz4b_c(plan.lz4_accel), None  # decoder needs orig_len; handled in decomp path
        if lz4f is not None:
            return ALGO_LZ4, _lz4f_c(plan.lz4_accel), _lz4f_d()
        if pysnappy is not None:
            return ALGO_SNAPPY, _snappy_c(), _snappy_d()
        if zstd is not None:
            return ALGO_ZSTD, _zstd_c(1, plan.zstd_threads), _zstd_d()
        return ALGO_COPY, (lambda b: b), None

    if lvl == "lz4":
        if lz4b is not None:
            return ALGO_LZ4, _lz4b_c(plan.lz4_accel), None
        if lz4f is not None:
            return ALGO_LZ4, _lz4f_c(plan.lz4_accel), _lz4f_d()
        return ALGO_COPY, (lambda b: b), None

    if lvl == "zstd":
        if zstd is not None:
            return ALGO_ZSTD, _zstd_c(1, plan.zstd_threads), _zstd_d()
        return ALGO_COPY, (lambda b: b), None

    # ratio mode: zstd level 3 if available
    if zstd is not None:
        return ALGO_ZSTD, _zstd_c(3, plan.zstd_threads), _zstd_d()
    return ALGO_COPY, (lambda b: b), None

def _decomp_fn(algo: int, orig_len: int) -> Callable[[bytes], bytes]:
    if algo == ALGO_ZERO:
        return lambda _b: b""
    if algo == ALGO_COPY:
        return lambda b: b
    if algo == ALGO_SNAPPY:
        return _snappy_d()
    if algo == ALGO_LZ4:
        if lz4b is not None:
            return _lz4b_d(orig_len)
        if lz4f is not None:
            return _lz4f_d()
        raise RuntimeError("LZ4 not available for decompression")
    if algo == ALGO_ZSTD:
        return _zstd_d()
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
    verbose: bool = False,
) -> None:
    """Compress input_filename into a Warp container at output_filename."""
    # Workers (compute now for policy)
    w = _cpu_workers(workers)

    # Auto-chunk policy
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
                f"Policy: size={file_size} bytes zero≈{zero_hint:.2%} -> chunk={chosen_chunk/1048576:.1f} MiB",
                flush=True,
            )
    else:
        chosen_chunk = _coerce_chunk_size(chunk)

    plan = _make_plan(level, chunk=chosen_chunk, workers=workers, verbose=verbose)

    with open(input_filename, "rb") as f_in, open(output_filename, "wb", buffering=0) as f_out:
        out_fd = f_out.fileno()
        # Write container header
        f_out.write(FHDR.pack(MAGIC, VERSION, 0, plan.chunk, 0))

        # Prefer mmap for parallel reads
        try:
            fileno = f_in.fileno()
            total = os.fstat(fileno).st_size
            mm = mmap.mmap(fileno, 0, access=mmap.ACCESS_READ)
            use_mm = True
        except Exception:
            mm = None
            total = 0
            use_mm = False

        def get_block(i: int) -> memoryview:
            if use_mm:
                off = i * plan.chunk
                end = min(off + plan.chunk, total)
                if end <= off:
                    return memoryview(b"")
                return memoryview(mm)[off:end]
            else:
                data = f_in.read(plan.chunk)
                return memoryview(data) if data else memoryview(b"")

        # Estimate blocks
        if use_mm and total:
            est_blocks = math.ceil(total / plan.chunk)
        else:
            # fallback: rough estimate
            try:
                fsz = os.path.getsize(input_filename)
                est_blocks = max(1, (fsz + plan.chunk - 1) // plan.chunk)
            except Exception:
                est_blocks = 0

        # Small/tiny fast path: write sequentially, no pool
        if (est_blocks and est_blocks <= 2) or (total and total <= 32 * MiB):
            i = 0
            zero_blocks = 0
            while True:
                blk = get_block(i)
                if not blk:
                    break
                if _is_all_zero(blk):
                    zero_blocks += 1
                    hdr = CHDR.pack(ALGO_ZERO, len(blk), 0)
                    _writev(out_fd, (hdr,))
                else:
                    # incompressible short-circuit
                    if _looks_incompressible(blk):
                        hdr = CHDR.pack(ALGO_COPY, len(blk), len(blk))
                        _writev(out_fd, (hdr, blk))
                    else:
                        algo, enc, _ = _choose_compressor(plan, len(blk))
                        if algo == ALGO_COPY:
                            hdr = CHDR.pack(ALGO_COPY, len(blk), len(blk))
                            _writev(out_fd, (hdr, blk))
                        else:
                            comp = enc(blk.tobytes())
                            hdr = CHDR.pack(algo, len(blk), len(comp))
                            _writev(out_fd, (hdr, memoryview(comp)))
                i += 1
            if verbose and zero_blocks:
                print(f"[compress] (seq) zero x{zero_blocks}", flush=True)
            if use_mm:
                mm.close()
            return

        # Threaded path
        zero_blocks = 0

        def job(i: int, blk: memoryview):
            """
            Return per-chunk tuple for ordered write:
              (i, algo, orig_len, comp_len, payload_memoryview_or_bytes)
            """
            if _is_all_zero(blk):
                return (i, ALGO_ZERO, len(blk), 0, b"")
            if _looks_incompressible(blk):
                # COPY block: zero-copy payload
                return (i, ALGO_COPY, len(blk), len(blk), blk)
            algo, enc, _ = _choose_compressor(plan, len(blk))
            if algo == ALGO_COPY:
                return (i, ALGO_COPY, len(blk), len(blk), blk)
            comp = enc(blk.tobytes())
            return (i, algo, len(blk), len(comp), comp)

        next_idx = 0
        pending = {}

        with ThreadPoolExecutor(max_workers=plan.workers) as ex:
            futs = []
            if use_mm:
                for i in range(est_blocks):
                    blk = get_block(i)
                    if not blk:
                        break
                    if _is_all_zero(blk):
                        zero_blocks += 1  # for stats only
                    futs.append(ex.submit(job, i, blk))
            else:
                i = 0
                while True:
                    blk = get_block(i)
                    if not blk:
                        break
                    if _is_all_zero(blk):
                        zero_blocks += 1
                    futs.append(ex.submit(job, i, blk))
                    i += 1

            for fut in as_completed(futs):
                idx, algo, orig_len, comp_len, payload = fut.result()
                pending[idx] = (algo, orig_len, comp_len, payload)
                while next_idx in pending:
                    a, o, c, p = pending.pop(next_idx)
                    hdr = CHDR.pack(a, o, c)
                    if c == 0:
                        _writev(out_fd, (hdr,))
                    else:
                        # COPY may carry a memoryview; others are bytes
                        if isinstance(p, memoryview):
                            _writev(out_fd, (hdr, p))
                        else:
                            _writev(out_fd, (hdr, memoryview(p)))
                    next_idx += 1

        if verbose and zero_blocks:
            print(f"[compress] zero x{zero_blocks}", flush=True)
        if use_mm:
            mm.close()

def decompress_file(
    input_filename: str,
    output_filename: str,
    *,
    workers: Optional[int] = None,
    verbose: bool = False,
) -> None:
    """Decompress a WARP container to raw output."""
    w = _cpu_workers(workers)

    # 1) Scan chunk table
    meta: List[Tuple[int, int, int, int]] = []
    with open(input_filename, "rb") as f:
        raw = f.read(FHDR.size)
        if len(raw) != FHDR.size:
            raise ValueError("Invalid WARP header (truncated)")
        magic, ver, _flags, _chunk, _reserved = FHDR.unpack(raw)
        if magic != MAGIC:
            raise ValueError("Bad magic; not a WARP file")
        if ver not in (1, 2):
            raise ValueError(f"Unsupported WARP version {ver}")

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

    # 2) Compute output offsets and total
    offs = [0] * len(meta)
    for i in range(1, len(meta)):
        offs[i] = offs[i - 1] + meta[i - 1][1]
    total_out = sum(m[1] for m in meta)

    # 3) Tiny fast path: sequential
    if len(meta) <= 2 or total_out <= 32 * MiB:
        with open(output_filename, "wb", buffering=0) as f_out, open(input_filename, "rb") as fi:
            try:
                f_out.truncate(total_out)
            except Exception:
                pass
            for i, (algo, orig, comp, payload_off) in enumerate(meta):
                if algo == ALGO_ZERO:
                    if orig > 0:
                        f_out.write(b"\x00" * orig)
                    continue
                fi.seek(payload_off)
                payload = fi.read(comp)
                if algo == ALGO_COPY:
                    f_out.write(payload)  # already raw
                else:
                    dec = _decomp_fn(algo, orig)
                    f_out.write(dec(payload))
        if verbose:
            print("[decompress] (seq) done", flush=True)
        return

    # 4) Parallel decode; ordered write
    pending = {}
    next_idx = 0

    def djob(i: int, algo: int, orig: int, comp: int, payload_off: int) -> Tuple[int, bytes]:
        if algo == ALGO_ZERO:
            return i, b"\x00" * orig
        with open(input_filename, "rb") as fi:
            fi.seek(payload_off)
            payload = fi.read(comp)
        if algo == ALGO_COPY:
            return i, payload
        dec = _decomp_fn(algo, orig)
        return i, dec(payload)

    with open(output_filename, "wb", buffering=0) as f_out, ThreadPoolExecutor(max_workers=w) as ex:
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
