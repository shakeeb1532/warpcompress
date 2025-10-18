# Copyright 2025
"""
warpcompress.core (v0.8.0)

Pure-Python core for a simple "WARP" container with optional trailers:
- WIX1: seekable index table for random access / fast open
- WCHK: whole-file checksum trailer (xxh64 / blake3 / sha256)
- WFTR: tiny footer pointing to WIX/WCHK offsets (so we don't scan)

Format (v2):
  File header (FHDR):
    MAGIC(4)='WARP' | ver(u8)=2 | flags(u8)=0 | chunk_size(u32 LE) | reserved(u32 LE)
  Repeated chunks:
    CHDR: algo(u8) | orig_len(u32 LE) | comp_len(u32 LE)
    PAYLOAD: comp_len bytes (omitted for zero chunks)
  Optional trailers (any order; discover via WFTR):
    WIX1: "WIX1" | count(u32) | entries[count] | crc32(u32)
          entry: payload_off(u64) | orig(u32) | comp(u32) | algo(u8)
    WCHK: "WCHK" | algo(u8) | dlen(u8) | digest(dlen bytes)
  Footer (always if any trailer present):
    WFTR: "WFTR" | wix_off(u64) | wchk_off(u64)

Algorithms:
  1 = zstd, 2 = lz4, 3 = snappy, 4 = copy (no compression), 5 = zero (all-zero chunk)
"""

from __future__ import annotations
import io
import os
import mmap
import math
import struct
import threading
import zlib
from typing import Callable, List, Optional, Tuple

# ----------------------------
# Optional codecs
# ----------------------------
try:
    import zstandard as zstd  # type: ignore
except Exception:
    zstd = None  # type: ignore

# Prefer lz4.block (lower overhead) but fall back to lz4.frame
try:
    import lz4.block as lz4b  # type: ignore
except Exception:
    lz4b = None  # type: ignore

try:
    import lz4.frame as lz4f  # type: ignore
except Exception:
    lz4f = None  # type: ignore

try:
    import snappy as pysnappy  # type: ignore
except Exception:
    pysnappy = None  # type: ignore

# Optional checksum libs
try:
    import xxhash as _xxh  # type: ignore
except Exception:
    _xxh = None  # type: ignore

try:
    import blake3 as _blake3  # type: ignore
except Exception:
    _blake3 = None  # type: ignore

import hashlib

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

FHDR = struct.Struct("<4sBBII")   # magic, ver, flags, chunk_size, reserved
CHDR = struct.Struct("<BII")      # algo, orig_len, comp_len

# Trailers/footers
WIX_MAGIC  = b"WIX1"
WCHK_MAGIC = b"WCHK"
WFTR_MAGIC = b"WFTR"

WIX_HDR  = struct.Struct("<4sI")   # magic, count
WIX_ENT  = struct.Struct("<QII B") # payload_off, orig, comp, algo
WFTR     = struct.Struct("<4sQQ")  # magic, wix_off, wchk_off

MiB = 1024 * 1024
GiB = 1024 * MiB

# Policy caps (can be overridden by env)
DEFAULT_CHUNK_MAX = 64 * MiB
CHUNK_MAX = int(os.environ.get("WARP_CHUNK_MAX", DEFAULT_CHUNK_MAX))
CHUNK_MIN = 256 * 1024

# ----------------------------
# Small utilities
# ----------------------------
def _is_all_zero(mv: memoryview) -> bool:
    step = 8192
    for i in range(0, len(mv), step):
        if mv[i:i+step].tobytes().strip(b"\x00"):
            return False
    return True

def _looks_incompressible(mv: memoryview) -> bool:
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

def _tell(fd: int) -> int:
    return os.lseek(fd, 0, os.SEEK_CUR)

def _write(fd: int, b: bytes | memoryview) -> None:
    if isinstance(b, memoryview):
        os.write(fd, b)
    else:
        os.write(fd, memoryview(b))

def _writev(fd: int, parts) -> None:
    try:
        os.writev(fd, [p if isinstance(p, memoryview) else memoryview(p) for p in parts])
    except (AttributeError, OSError):
        for p in parts:
            _write(fd, p)

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

def _zstd_c(level: int, threads: int, dict_bytes: Optional[bytes] = None):
    if zstd is None:
        raise RuntimeError("zstandard not available")
    cache = _tls_cache()
    key = ("zc", level, max(1, threads), bool(dict_bytes))
    obj = cache.get(key)
    if obj is None:
        cd = zstd.ZstdCompressionDict(dict_bytes) if (dict_bytes and len(dict_bytes) > 0) else None
        obj = zstd.ZstdCompressor(level=level, threads=max(1, threads), dict_data=cd)
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
# Header helpers
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
# Auto-chunk policy (kept simple)
# ----------------------------
def _sample_zero_ratio(path: str, *, sample_window: int = 256 * 1024, max_windows: int = 3) -> float:
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

def _policy_chunk_size(file_size: int, *, requested: Optional[int], level: str, workers: int, zero_ratio_hint: float = 0.0) -> int:
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
    __slots__ = ("level", "chunk", "workers", "zstd_threads", "lz4_accel", "verbose",
                 "checksum", "dict_bytes", "make_index")
    def __init__(self, level: str, *, chunk: int, workers: int, zstd_threads: int, lz4_accel: int,
                 verbose: bool, checksum: str, dict_bytes: Optional[bytes], make_index: bool):
        self.level = level
        self.chunk = chunk
        self.workers = workers
        self.zstd_threads = zstd_threads
        self.lz4_accel = lz4_accel
        self.verbose = verbose
        self.checksum = checksum  # "none" | "xxh64" | "blake3" | "sha256"
        self.dict_bytes = dict_bytes
        self.make_index = make_index

def _make_plan(level: str, *, chunk: Optional[int], workers: Optional[int], verbose: bool = False,
               checksum: str = "none", dict_bytes: Optional[bytes] = None, make_index: bool = False) -> Plan:
    lvl = (level or "throughput").lower()
    if lvl not in ("throughput", "zstd", "lz4", "ratio"):
        raise ValueError(f"Unknown level: {level}")
    csize = _coerce_chunk_size(chunk)
    w = _cpu_workers(workers)
    zt = max(1, w)
    lz_acc = 1 if lvl in ("throughput", "lz4") else 0
    cs = (checksum or "none").lower()
    if cs not in ("none", "xxh64", "blake3", "sha256"):
        raise ValueError(f"Unknown checksum: {checksum}")
    if verbose:
        print(f"INFO: plan -> level={lvl}, chunk={csize/1048576:.1f} MiB, workers={w}, zstd_threads={zt}, index={make_index}, checksum={cs}", flush=True)
    return Plan(lvl, chunk=csize, workers=w, zstd_threads=zt, lz4_accel=lz_acc, verbose=verbose,
                checksum=cs, dict_bytes=dict_bytes, make_index=make_index)

def _choose_compressor(plan: Plan, orig_len: int) -> Tuple[int, Callable[[bytes], bytes], Optional[Callable[[bytes], bytes]]]:
    lvl = plan.level
    if lvl == "throughput":
        if lz4b is not None:
            return ALGO_LZ4, _lz4b_c(plan.lz4_accel), None
        if lz4f is not None:
            return ALGO_LZ4, _lz4f_c(plan.lz4_accel), _lz4f_d()
        if pysnappy is not None:
            return ALGO_SNAPPY, _snappy_c(), _snappy_d()
        if zstd is not None:
            return ALGO_ZSTD, _zstd_c(1, plan.zstd_threads, plan.dict_bytes), _zstd_d()
        return ALGO_COPY, (lambda b: b), None
    if lvl == "lz4":
        if lz4b is not None:
            return ALGO_LZ4, _lz4b_c(plan.lz4_accel), None
        if lz4f is not None:
            return ALGO_LZ4, _lz4f_c(plan.lz4_accel), _lz4f_d()
        return ALGO_COPY, (lambda b: b), None
    if lvl == "zstd":
        if zstd is not None:
            return ALGO_ZSTD, _zstd_c(1, plan.zstd_threads, plan.dict_bytes), _zstd_d()
        return ALGO_COPY, (lambda b: b), None
    # ratio mode
    if zstd is not None:
        return ALGO_ZSTD, _zstd_c(3, plan.zstd_threads, plan.dict_bytes), _zstd_d()
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
# Checksums (whole-file)
# ----------------------------
_CHK_NONE   = 0
_CHK_XXH64  = 1
_CHK_BLAKE3 = 2
_CHK_SHA256 = 3

class _Checksummer:
    def __init__(self, algo: str):
        a = algo.lower()
        self.algo_name = a
        self.code = _CHK_NONE
        if a == "none":
            self._h = None
            self.code = _CHK_NONE
        elif a == "xxh64":
            if _xxh is not None:
                self._h = _xxh.xxh64()
                self.code = _CHK_XXH64
            else:
                self._h = hashlib.sha256()
                self.code = _CHK_SHA256
        elif a == "blake3":
            if _blake3 is not None:
                self._h = _blake3.blake3()
                self.code = _CHK_BLAKE3
            else:
                self._h = hashlib.sha256()
                self.code = _CHK_SHA256
        elif a == "sha256":
            self._h = hashlib.sha256()
            self.code = _CHK_SHA256
        else:
            raise ValueError(a)

    def update(self, b: bytes | memoryview):
        if self._h is None:
            return
        if isinstance(b, memoryview):
            self._h.update(b.tobytes())
        else:
            self._h.update(b)

    def update_zeros(self, n: int):
        if self._h is None or n <= 0:
            return
        blk = b"\x00" * (1 << 20)
        while n > 0:
            take = blk if n >= len(blk) else blk[:n]
            self._h.update(take)
            n -= len(take)

    def digest(self) -> bytes:
        if self._h is None:
            return b""
        # blake3 has .digest()
        return self._h.digest()

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
    checksum: str = "none",
    make_index: bool = False,
    zstd_dict: Optional[bytes] = None,
) -> None:
    """Compress input_filename into a Warp container at output_filename."""
    # Workers (compute now for policy)
    w = _cpu_workers(workers)

    # Auto-chunk policy
    auto_on = os.environ.get("WARP_AUTO_CHUNK", "1") not in ("0", "false", "False", "")
    try:
        file_size = os.path.getsize(input_filename)
    except Exception:
        file_size = 0
    zero_hint = _sample_zero_ratio(input_filename) if (auto_on and file_size > 0) else 0.0
    chosen_chunk = _policy_chunk_size(file_size, requested=chunk, level=level, workers=w, zero_ratio_hint=zero_hint)

    plan = _make_plan(level, chunk=chosen_chunk, workers=workers, verbose=verbose,
                      checksum=checksum, dict_bytes=zstd_dict, make_index=make_index)

    chk = _Checksummer(plan.checksum)
    index_entries: List[Tuple[int, int, int, int]] = []  # (payload_off, orig, comp, algo)

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

        # Iterate chunks sequentially (threading omitted here to keep index/checksum logic simple and robust)
        # NOTE: threading is still used in 0.7.x; we can reintroduce parallel compress later with ordered writes.
        offset = FHDR.size
        i = 0
        while True:
            if use_mm:
                off = i * plan.chunk
                end = min(off + plan.chunk, total)
                if end <= off:
                    break
                blk = memoryview(mm)[off:end]
            else:
                data = f_in.read(plan.chunk)
                if not data:
                    break
                blk = memoryview(data)

            if _is_all_zero(blk):
                # zero chunk: no payload; update checksum and index
                chk.update_zeros(len(blk))
                hdr = CHDR.pack(ALGO_ZERO, len(blk), 0)
                _write(out_fd, hdr)
                payload_off = _tell(out_fd)  # payload starts after header; for zero it equals current
                index_entries.append((payload_off, len(blk), 0, ALGO_ZERO))
                offset += CHDR.size
            else:
                # choose codec
                if _looks_incompressible(blk):
                    algo = ALGO_COPY
                    comp_payload = blk  # zero-copy
                else:
                    algo, enc, _ = _choose_compressor(plan, len(blk))
                    if algo == ALGO_COPY:
                        comp_payload = blk
                    else:
                        comp_payload = enc(blk.tobytes())
                # checksum original
                chk.update(blk)

                # index needs payload offset (after header)
                cur = _tell(out_fd)
                payload_off = cur + CHDR.size
                if isinstance(comp_payload, memoryview):
                    hdr = CHDR.pack(algo, len(blk), len(blk))
                    _writev(out_fd, (hdr, comp_payload))
                    comp_len = len(blk)
                else:
                    hdr = CHDR.pack(algo, len(blk), len(comp_payload))
                    _writev(out_fd, (hdr, memoryview(comp_payload)))
                    comp_len = len(comp_payload)

                index_entries.append((payload_off, len(blk), comp_len, algo))
                offset = payload_off + comp_len
            i += 1

        if use_mm:
            mm.close()

        # Trailers and footer
        wix_off = 0
        wchk_off = 0

        if plan.make_index and index_entries:
            buf = bytearray()
            buf += WIX_HDR.pack(WIX_MAGIC, len(index_entries))
            entries_bytes = bytearray()
            for (poff, orig, comp, algo) in index_entries:
                entries_bytes += WIX_ENT.pack(poff, orig, comp, algo)
            crc = zlib.crc32(entries_bytes) & 0xFFFFFFFF
            buf += entries_bytes
            buf += struct.pack("<I", crc)
            wix_off = _tell(out_fd)
            _write(out_fd, buf)

        if chk.code != _CHK_NONE:
            dig = chk.digest()
            wchk_off = _tell(out_fd)
            _write(out_fd, WCHK_MAGIC + struct.pack("<BB", chk.code, len(dig)) + dig)

        if wix_off or wchk_off:
            _write(out_fd, WFTR.pack(WFTR_MAGIC, wix_off, wchk_off))

def _read_footer_and_trailers(fi) -> tuple[list[tuple[int,int,int,int]] | None, tuple[int,int] | None]:
    """
    Return (index_entries or None, (chk_code, chk_len, chk_digest) or None)
    """
    # Try footer
    try:
        fi.seek(-WFTR.size, os.SEEK_END)
        data = fi.read(WFTR.size)
        if len(data) == WFTR.size:
            magic, wix_off, wchk_off = WFTR.unpack(data)
            if magic == WFTR_MAGIC:
                idx = None
                chk = None
                if wix_off:
                    fi.seek(wix_off)
                    hdr = fi.read(WIX_HDR.size)
                    mg, count = WIX_HDR.unpack(hdr)
                    if mg == WIX_MAGIC:
                        entries = []
                        for _ in range(count):
                            e = fi.read(WIX_ENT.size)
                            po, orig, comp, algo = WIX_ENT.unpack(e)
                            entries.append((po, orig, comp, algo))
                        # ignore crc for now
                        _ = fi.read(4)
                        idx = entries
                if wchk_off:
                    fi.seek(wchk_off)
                    mg = fi.read(4)
                    if mg == WCHK_MAGIC:
                        meta = fi.read(2)
                        code, dlen = struct.unpack("<BB", meta)
                        dig = fi.read(dlen)
                        chk = (code, dlen, dig)
                return idx, chk
    except Exception:
        pass
    return None, None

def _chk_from_code(code: int) -> _Checksummer:
    if code == _CHK_XXH64:
        return _Checksummer("xxh64")
    if code == _CHK_BLAKE3:
        return _Checksummer("blake3")
    if code == _CHK_SHA256:
        return _Checksummer("sha256")
    return _Checksummer("none")

def decompress_file(
    input_filename: str,
    output_filename: str,
    *,
    workers: Optional[int] = None,
    verbose: bool = False,
    verify: bool = False,
) -> None:
    """Decompress a WARP container to raw output."""
    w = _cpu_workers(workers)

    # 0) Try indexed open via footer
    with open(input_filename, "rb") as f:
        raw = f.read(FHDR.size)
        if len(raw) != FHDR.size:
            raise ValueError("Invalid WARP header (truncated)")
        magic, ver, _flags, _chunk, _reserved = FHDR.unpack(raw)
        if magic != MAGIC:
            raise ValueError("Bad magic; not a WARP file")
        if ver not in (1, 2):
            raise ValueError(f"Unsupported WARP version {ver}")

        idx, chk_meta = _read_footer_and_trailers(f)

        if idx is None:
            # fallback: linear scan
            meta: List[Tuple[int, int, int, int]] = []
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
        else:
            # fast path: translate index to meta tuples
            meta = []
            for (poff, orig, comp, algo) in idx:
                meta.append((algo, orig, comp, poff))

    # 1) Compute output offsets and total
    offs = [0] * len(meta)
    for i in range(1, len(meta)):
        offs[i] = offs[i - 1] + meta[i - 1][1]
    total_out = sum(m[1] for m in meta)

    # 2) Prepare checksum (if verify and trailer present)
    chk = None
    want_digest = None
    if verify:
        if 'chk_meta' in locals() and chk_meta:
            code, dlen, dig = chk_meta
            chk = _chk_from_code(code)
            want_digest = dig
        else:
            # no stored checksum; we can still proceed without verification
            chk = _Checksummer("none")

    # 3) Parallel decode; ordered write (simple threadpool)
    from concurrent.futures import ThreadPoolExecutor, as_completed
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
            idx = futures.pop(fut)
            i, data = fut.result()
            pending[i] = data
            while next_idx in pending:
                chunk_bytes = pending.pop(next_idx)
                # checksum (if needed)
                if chk:
                    if meta[next_idx][0] == ALGO_ZERO:
                        chk.update_zeros(len(chunk_bytes))
                    else:
                        chk.update(chunk_bytes)
                f_out.seek(offs[next_idx])
                f_out.write(chunk_bytes)
                next_idx += 1

    # 4) Verify digest
    if verify and want_digest is not None and chk:
        have = chk.digest()
        if have != want_digest:
            raise ValueError("Checksum verification failed")
    if verbose:
        print("[decompress] done", flush=True)


