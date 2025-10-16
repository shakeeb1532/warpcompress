# Copyright 2025
"""
warpcompress.core

Pure-Python core for Warp container:
- File header:  MAGIC(4) | ver(1) | flags(1) | chunk_size(u32) | reserved(u32)
- Chunk header: algo(1)  | orig_len(u32)     | comp_len(u32)
- Parallel chunk (de)compression with per-chunk codec choice.
- Zero-block elision.
"""

from __future__ import annotations
import io, os, mmap, math, struct, threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, List, Optional, Tuple

# ---- Optional codecs ---------------------------------------------------------
try:
    import zstandard as zstd      # type: ignore
except Exception:
    zstd = None                   # type: ignore
try:
    import lz4.frame as lz4f      # type: ignore
except Exception:
    lz4f = None                   # type: ignore
try:
    import snappy                 # type: ignore
except Exception:
    snappy = None                 # type: ignore

# ---- Container constants -----------------------------------------------------
MAGIC   = b"WARP"
VERSION = 2

ALGO_ZSTD   = 1
ALGO_LZ4    = 2
ALGO_SNAPPY = 3
ALGO_COPY   = 4
ALGO_ZERO   = 5

CHUNK_MIN = 256 * 1024
CHUNK_DEF = 1 * 1024 * 1024
CHUNK_MAX = 16 * 1024 * 1024

# file header + chunk header
FHDR = struct.Struct("<4sBBII")   # MAGIC, ver, flags, chunk_size, reserved
CHDR = struct.Struct("<BII")      # algo,  orig_len,  comp_len

# ---- Small utils -------------------------------------------------------------
def _is_all_zero(b: memoryview) -> bool:
    # fast zero test without allocating
    step = 8192
    for i in range(0, len(b), step):
        if b[i:i+step].tobytes().strip(b"\x00"):
            return False
    return True

def _coerce_chunk_size(n: Optional[int]) -> int:
    return max(CHUNK_MIN, min(CHUNK_MAX, int(n or CHUNK_DEF)))

def _cpu_workers(n: Optional[int]) -> int:
    if n and n > 0: return n
    c = os.cpu_count() or 4
    return min(max(2, c), 64)

# ---- Thread-local codec caches (bug-fixed; dict on TLS) ----------------------
_tls = threading.local()

def _tls_cache() -> dict:
    c = getattr(_tls, "_cache", None)
    if c is None:
        c = {}
        setattr(_tls, "_cache", c)
    return c

def _zstd_c(level: int, threads: int) -> Callable[[bytes], bytes]:
    if zstd is None: raise RuntimeError("zstandard not available")
    cache = _tls_cache()
    key = ("zc", int(level), int(threads or 0))
    obj = cache.get(key)
    if obj is None:
        obj = zstd.ZstdCompressor(level=level, threads=threads or 0)
        cache[key] = obj
    return obj.compress

def _zstd_d() -> Callable[[bytes], bytes]:
    if zstd is None: raise RuntimeError("zstandard not available")
    cache = _tls_cache()
    key = ("zd",)
    obj = cache.get(key)
    if obj is None:
        obj = zstd.ZstdDecompressor()
        cache[key] = obj
    return obj.decompress

def _lz4_c(accel: int) -> Callable[[bytes], bytes]:
    if lz4f is None: raise RuntimeError("lz4.frame not available")
    def _c(b: bytes) -> bytes:
        return lz4f.compress(b, compression_level=accel, block_linked=True)
    return _c

def _lz4_d() -> Callable[[bytes], bytes]:
    if lz4f is None: raise RuntimeError("lz4.frame not available")
    return lz4f.decompress

def _snappy_c() -> Callable[[bytes], bytes]:
    if snappy is None: raise RuntimeError("python-snappy not available")
    return snappy.compress

def _snappy_d() -> Callable[[bytes], bytes]:
    if snappy is None: raise RuntimeError("python-snappy not available")
    return snappy.uncompress

# ---- Public helper (used by CLI) --------------------------------------------
def detect_algo_name(algo: int) -> str:
    m = {
        ALGO_ZSTD:   "zstd",
        ALGO_LZ4:    "lz4",
        ALGO_SNAPPY: "snappy",
        ALGO_COPY:   "copy",
        ALGO_ZERO:   "zero",
    }
    return m.get(int(algo), f"unknown({algo})")

# ---- Planning ----------------------------------------------------------------
class Plan:
    __slots__ = ("level","chunk","workers","zstd_threads","lz4_accel","verbose")
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
    if lvl not in ("throughput","zstd","lz4","ratio"):
        raise ValueError(f"Unknown level: {level}")
    csize  = _coerce_chunk_size(chunk)
    w      = _cpu_workers(workers)
    zt     = 0
    lz_acc = 4 if lvl in ("throughput","lz4") else 1
    if verbose:
        base = "snappy" if lvl == "throughput" else lvl
        print(f"Base hint: {base} (final codec is per-chunk adaptive)", flush=True)
        print(f"INFO: base plan -> chunk={csize/1048576:.2f} MB (auto), "
              f"workers={w}, zstd_threads={zt}, lz4_accel={lz_acc}", flush=True)
    return Plan(lvl, chunk=csize, workers=w, zstd_threads=zt, lz4_accel=lz_acc, verbose=verbose)

# ---- Codec choice per chunk --------------------------------------------------
def _choose_alg(plan: Plan, block: memoryview) -> Tuple[int, Callable[[bytes], bytes]]:
    if _is_all_zero(block):
        return ALGO_ZERO, (lambda _b: b"")
    if plan.level == "throughput":
        if snappy is not None: return ALGO_SNAPPY, _snappy_c()
        if lz4f   is not None: return ALGO_LZ4,    _lz4_c(plan.lz4_accel)
        if zstd   is not None: return ALGO_ZSTD,   _zstd_c(1, plan.zstd_threads)
        return ALGO_COPY, (lambda b: bytes(b))
    if plan.level == "lz4":
        return (ALGO_LZ4, _lz4_c(plan.lz4_accel)) if lz4f else (ALGO_COPY, lambda b: bytes(b))
    if plan.level == "zstd":
        return (ALGO_ZSTD, _zstd_c(1, plan.zstd_threads)) if zstd else (ALGO_COPY, lambda b: bytes(b))
    # ratio
    return (ALGO_ZSTD, _zstd_c(3, plan.zstd_threads)) if zstd else (ALGO_COPY, lambda b: bytes(b))

def _decomp_fn(algo: int) -> Callable[[bytes], bytes]:
    return {
        ALGO_ZERO:   (lambda _b: b""),
        ALGO_COPY:   (lambda b: b),
        ALGO_SNAPPY: _snappy_d(),
        ALGO_LZ4:    _lz4_d(),
        ALGO_ZSTD:   _zstd_d(),
    }[algo]

# ---- I/O helpers -------------------------------------------------------------
def _write_fhdr(fout: io.BufferedWriter, chunk: int) -> None:
    fout.write(FHDR.pack(MAGIC, VERSION, 0, chunk, 0))

def _read_fhdr(fin: io.BufferedReader) -> Tuple[int,int]:
    raw = fin.read(FHDR.size)
    if len(raw) != FHDR.size: raise ValueError("Invalid WARP header (truncated)")
    magic, ver, _flags, chunk, _res = FHDR.unpack(raw)
    if magic != MAGIC:         raise ValueError("Bad magic; not a warp file")
    if ver not in (1,2):       raise ValueError(f"Unsupported WARP version {ver}")
    return ver, chunk

# ---- Public API --------------------------------------------------------------
def compress_file(input_filename: str, output_filename: str, *,
                  level: str = "throughput", workers: Optional[int] = None,
                  chunk: Optional[int] = None, zstd_hybrid: str = "auto",
                  verbose: bool = False) -> None:
    plan = _make_plan(level, chunk=chunk, workers=workers,
                      zstd_hybrid=zstd_hybrid, verbose=verbose)

    with open(input_filename, "rb") as f_in, open(output_filename, "wb") as f_out:
        _write_fhdr(f_out, plan.chunk)

        # Read strategy: use mmap if possible
        try:
            fileno = f_in.fileno()
            total  = os.fstat(fileno).st_size
            mm     = mmap.mmap(fileno, 0, access=mmap.ACCESS_READ)
            use_mm = True
        except Exception:
            mm = None; total = 0; use_mm = False

        def read_block(i: int) -> memoryview:
            if use_mm:
                off = i * plan.chunk
                end = min(off + plan.chunk, total)
                return memoryview(mm)[off:end]
            else:
                return memoryview(f_in.read(plan.chunk))

        blocks = math.ceil(total / plan.chunk) if use_mm else None
        zero_blocks = 0
        next_idx = 0
        pending = {}

        def job(i: int, blk: memoryview) -> Tuple[int, bytes]:
            algo, enc = _choose_alg(plan, blk)
            comp = b"" if algo == ALGO_ZERO else (bytes(blk) if algo == ALGO_COPY else enc(blk.tobytes()))
            return i, CHDR.pack(algo, len(blk), len(comp)) + comp

        with ThreadPoolExecutor(max_workers=plan.workers) as ex:
            futs = []
            if use_mm:
                for i in range(blocks):
                    blk = read_block(i)
                    if not blk: break
                    if _is_all_zero(blk): zero_blocks += 1
                    futs.append(ex.submit(job, i, blk))
            else:
                i = 0
                while True:
                    data = f_in.read(plan.chunk)
                    if not data: break
                    blk = memoryview(data)
                    if _is_all_zero(blk): zero_blocks += 1
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

def decompress_file(input_filename: str, output_filename: str, *,
                    workers: Optional[int] = None, verbose: bool = False) -> None:
    w = _cpu_workers(workers)

    # 1) Scan chunk table (algo, lengths, payload offset)
    meta: List[Tuple[int,int,int,int]] = []  # (algo, orig_len, comp_len, payload_off)
    with open(input_filename, "rb") as f:
        _ver, chunk_size = _read_fhdr(f)
        file_off = FHDR.size
        while True:
            hdr = f.read(CHDR.size)
            if not hdr: break
            if len(hdr) != CHDR.size:
                raise ValueError("Truncated container (chunk header)")
            algo, orig_len, comp_len = CHDR.unpack(hdr)
            payload_off = file_off + CHDR.size
            meta.append((algo, orig_len, comp_len, payload_off))
            f.seek(comp_len, io.SEEK_CUR)
            file_off += CHDR.size + comp_len

    # 2) Precompute output offsets (prefix sum of orig_len)
    offs = [0] * len(meta)
    for i in range(1, len(meta)):
        offs[i] = offs[i-1] + meta[i-1][1]
    total_out = sum(m[1] for m in meta)

    # 3) Parallel decode
    def djob(i: int, algo: int, orig: int, comp: int, off: int) -> Tuple[int, bytes]:
        if algo == ALGO_ZERO: return i, b"\x00" * orig
        if algo == ALGO_COPY:
            with open(input_filename, "rb") as fi:
                fi.seek(off); return i, fi.read(orig)
        dec = _decomp_fn(algo)
        with open(input_filename, "rb") as fi:
            fi.seek(off); payload = fi.read(comp)
        return i, dec(payload)

    pending = {}
    next_idx = 0
    with open(output_filename, "wb") as f_out, ThreadPoolExecutor(max_workers=w) as ex:
        try: f_out.truncate(total_out)
        except Exception: pass
        futs = { ex.submit(djob, i, *m, offs[i]): i for i, m in enumerate(meta) }
        while futs:
            fut = next(as_completed(futs))
            idx, data = fut.result()
            del futs[fut]
            pending[idx] = data
            while next_idx in pending:
                f_out.seek(offs[next_idx])
                f_out.write(pending.pop(next_idx))
                next_idx += 1
    if verbose:
        print("[decompress] done", flush=True)

