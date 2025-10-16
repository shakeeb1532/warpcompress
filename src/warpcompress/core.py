# Copyright 2025
# WarpCompress core — v0.7.0 ultra throughput, batched I/O, hybrid zstd, coalescing, NumPy fast paths (pure Python)
from __future__ import annotations
import os, time, mmap, struct, threading, ctypes
from typing import Tuple, List, Optional, Dict, Set
from concurrent.futures import ThreadPoolExecutor, as_completed

# C-backed wheels (pure-Python packaging remains)
import snappy
import lz4.block as lz4b
import zstandard as zstd
import brotli

# ========= Container =========
MAGIC = b"WARP"
VERSION_V1 = 1
VERSION_V2 = 2
HEADER_FMT = ">4sB B I Q"   # MAGIC, VER, algo_hint, chunk_size, orig_size
HEADER_SIZE = struct.calcsize(HEADER_FMT)

CHUNK_V1_FMT = ">II"        # orig_len, comp_len
CHUNK_V1_SIZE = struct.calcsize(CHUNK_V1_FMT)

CHUNK_V2_FMT = ">BII"       # algo_id, orig_len, comp_len
CHUNK_V2_SIZE = struct.calcsize(CHUNK_V2_FMT)

MIXED_ALGO = 255

# ========= Algorithms =========
ALGO_RAW    = 0
ALGO_SNAPPY = 1
ALGO_LZ4    = 2
ALGO_ZSTD   = 3
ALGO_BROTLI = 4
ALGO_ZERO   = 5

ALGO_NAMES = {
    ALGO_RAW:    "raw",
    ALGO_SNAPPY: "snappy",
    ALGO_LZ4:    "lz4",
    ALGO_ZSTD:   "zstd",
    ALGO_BROTLI: "brotli",
    ALGO_ZERO:   "zero",
}

CHUNK_SIZE_DEFAULT = 8 * 1024 * 1024

# ========= Tunables =========
INCOMPRESSIBLE_THRESH = 0.985     # if all fast-codec ratios > this → RAW
ZSTD_BETTER_FACTOR    = 0.92      # zstd must beat snappy/lz4 by ~8%

# --- Env toggles / fallbacks ---
SAFE_BYTES    = os.getenv("WARP_SAFE_BYTES") == "1"         # force bytes() path (disable memoryview fast-path)
NO_SENDFILE   = os.getenv("WARP_NO_SENDFILE") == "1"        # disable sendfile
INFLIGHT_CAP  = int(os.getenv("WARP_INFLIGHT", "0"))        # cap inflight items (0 = auto)
ZSTD_HYBRID   = (os.getenv("WARP_ZSTD_HYBRID") or "auto").lower()  # "auto" | "on" | "off"
WRITEV_BATCH_MAX  = int(os.getenv("WARP_WRITEV_BATCH_MAX", "32"))  # vectors per flush
WRITEV_BYTES_MAX  = int(os.getenv("WARP_WRITEV_BYTES_MAX", str(8<<20)))  # ~8 MiB per flush

# Optional NumPy speed-ups
try:
    import numpy as _np
    _HAVE_NUMPY = True
except Exception:
    _HAVE_NUMPY = False

def _writev(fd: int, parts):
    try:
        return os.writev(fd, parts)  # type: ignore[attr-defined]
    except AttributeError:
        total = 0
        for p in parts:
            total += os.write(fd, p)
        return total

def _call_codec(fn, buf):
    # try buffer protocol; fall back to bytes() if codec dislikes memoryview
    try:
        return fn(buf)
    except (TypeError, BufferError, ValueError, AttributeError):
        return fn(bytes(buf))

# ========= Helpers =========
def _fmt_bytes(n: int) -> str:
    x = float(n)
    for u in ("B","KB","MB","GB","TB"):
        if x < 1024: return f"{x:.2f} {u}"
        x /= 1024
    return f"{x:.2f} PB"

def _cpu_workers(default: Optional[int] = None) -> int:
    if isinstance(default, int) and default > 0: return default
    cpus = os.cpu_count() or 4
    return max(2, min(32, cpus))

def _is_all_zero_mv(view) -> bool:
    if _HAVE_NUMPY and len(view) >= 8:
        arr = _np.frombuffer(view, dtype=_np.uint64, count=len(view)//8)
        if arr.size and arr.any():
            return False
        tail = len(view) - arr.size*8
        return not any(view[-tail:]) if tail else True
    else:
        return not any(view)

def _entropy_hint_bytes(b: bytes) -> float:
    # Shannon entropy bits/byte, used to bias codec/accel
    if not b:
        return 8.0
    if _HAVE_NUMPY:
        arr = _np.frombuffer(b, dtype=_np.uint8)
        counts = _np.bincount(arr, minlength=256)
        total = float(len(arr))
        p = counts[counts > 0].astype(_np.float64) / total
        return float(-_np.sum(p * _np.log2(p)))
    else:
        from collections import Counter
        import math
        c = Counter(b); total = len(b)
        return -sum((v/total) * math.log2(v/total) for v in c.values())

# ---- compressibility probe (few MiB) ----
def _compressibility_hint(path: str, samples: int = 3, sample_bytes: int = 1*1024*1024) -> float:
    try:
        size = os.path.getsize(path)
        if size <= 0: return 1.0
        offs: List[int] = [0]
        if size > sample_bytes: offs.append(max(0, size//2 - sample_bytes//2))
        if size > 2*sample_bytes: offs.append(max(0, size - sample_bytes))
        offs = offs[:samples]
        ratios: List[float] = []
        with open(path, "rb") as f:
            for off in offs:
                f.seek(off)
                raw = f.read(sample_bytes)
                if not raw: continue
                c = snappy.compress(raw)
                ratios.append(len(c)/max(1, len(raw)))
        return sum(ratios)/len(ratios) if ratios else 1.0
    except Exception:
        return 1.0

def _auto_chunk_size(file_size: int, algo_hint: int, workers: int, comp_hint_ratio: float) -> int:
    if algo_hint in (ALGO_SNAPPY, ALGO_LZ4):
        min_chunk, max_chunk, est_mbps, target_ms = 1<<20, 32<<20, 700.0, 35.0
    elif algo_hint == ALGO_ZSTD:
        min_chunk, max_chunk, est_mbps, target_ms = 2<<20, 64<<20, 500.0, 45.0
    else:
        min_chunk, max_chunk, est_mbps, target_ms = 1<<20, 32<<20, 150.0, 60.0
    slow_factor = max(0.4, min(1.0, comp_hint_ratio))
    adj_mbps = est_mbps * slow_factor
    target_bytes = int(adj_mbps * (target_ms/1000.0) * 1024*1024)
    if workers <= 0: workers = _cpu_workers(None)
    if file_size > 0 and workers > 0:
        max_for_conc = max(min_chunk, file_size // max(1, workers*8))
    else:
        max_for_conc = max_chunk
    chunk = max(min_chunk, min(max_chunk, target_bytes))
    chunk = min(chunk, max_for_conc)
    if file_size and chunk > file_size: chunk = max(min_chunk, file_size)
    return max(min_chunk, (chunk // (1<<20)) * (1<<20))  # align 1 MiB

def _auto_or_fixed_chunk(path: str, algo_hint: int, chunk_size: Optional[int], workers: Optional[int]) -> int:
    if chunk_size and chunk_size > 0: return int(chunk_size)
    hint = _compressibility_hint(path)
    return _auto_chunk_size(os.path.getsize(path), algo_hint, _cpu_workers(workers), hint)

# ---- thread-local compressors/decompressors ----
_tls = threading.local()
def _snappy_c(): return snappy.compress
def _snappy_d(): return snappy.uncompress
def _lz4_c(accel: int): return lambda b: lz4b.compress(b, mode="fast", acceleration=max(1, accel), store_size=False)
def _lz4_d(expected: int): return lambda b: lz4b.decompress(b, uncompressed_size=expected)
def _zstd_c(level: int, threads: int):
    key = ("zc", level, threads)
    zc = getattr(_tls, key, None)
    if zc is None:
        zc = zstd.ZstdCompressor(level=level, threads=threads or 0)
        setattr(_tls, key, zc)
    return zc.compress
def _zstd_d():
    key = ("zd",)
    zd = getattr(_tls, key, None)
    if zd is None:
        zd = zstd.ZstdDecompressor()
        setattr(_tls, key, zd)
    return zd.decompress

# ===== Plan selection (hint only) =====
def _plan_for(level: Optional[str], chunk_size: Optional[int], workers: Optional[int],
              *, path_for_autosize: Optional[str] = None) -> Tuple[int,int,int,int,int,int]:
    lvl = (level or "throughput").lower()
    w = _cpu_workers(workers)
    if lvl in ("throughput","auto","snappy","speed","fast"):
        algo_hint, zstd_level, zstd_threads, lz4_accel = ALGO_SNAPPY, 0, 0, 4
    elif lvl in ("lz4",):
        algo_hint, zstd_level, zstd_threads, lz4_accel = ALGO_LZ4, 0, 0, 4
    elif lvl in ("zstd","balanced","default"):
        algo_hint, zstd_level, zstd_threads, lz4_accel = ALGO_ZSTD, 3, (0 if w > 1 else max(1, _cpu_workers(0))), 2
    elif lvl in ("ratio","brotli","max"):
        algo_hint, zstd_level, zstd_threads, lz4_accel = ALGO_BROTLI, 5, 0, 1
    else:
        algo_hint, zstd_level, zstd_threads, lz4_accel = ALGO_SNAPPY, 0, 0, 4
    if path_for_autosize:
        c = _auto_or_fixed_chunk(path_for_autosize, algo_hint, chunk_size, w)
    else:
        c = int(chunk_size or CHUNK_SIZE_DEFAULT)
    return (algo_hint, c, w, zstd_level, zstd_threads, lz4_accel)

# ===== codec choice (prefix sample; ZERO detection is whole-chunk) =====
def _pick_codec_for_chunk(level: str, sample: bytes) -> int:
    s = sample[:131072] if len(sample) > 131072 else sample
    H = _entropy_hint_bytes(s)
    c_sn  = snappy.compress(s);                 r_sn  = len(c_sn)/max(1,len(s))
    c_lz4 = lz4b.compress(s, store_size=False); r_lz4 = len(c_lz4)/max(1,len(s))
    try_zst = (min(r_sn, r_lz4) < 0.9) or ("zstd" in level.lower()) or ("ratio" in level.lower()) or (H < 5.5)
    r_zst = 1.0
    if try_zst:
        zc = zstd.ZstdCompressor(level=1, threads=0)
        c_z = zc.compress(s)
        r_zst = len(c_z)/max(1,len(s))
    # very high entropy → RAW
    if H > 7.8 and r_sn > INCOMPRESSIBLE_THRESH and r_lz4 > INCOMPRESSIBLE_THRESH and r_zst > INCOMPRESSIBLE_THRESH:
        return ALGO_RAW
    # low entropy or explicitly zstd requested and zstd meaningfully better
    if (H < 5.5) or (("zstd" in level.lower() or "ratio" in level.lower()) and (r_zst <= min(r_sn, r_lz4)*ZSTD_BETTER_FACTOR)):
        return ALGO_ZSTD
    # choose faster of snappy/lz4 by sample ratio (tie → snappy)
    return ALGO_LZ4 if r_lz4 < r_sn else ALGO_SNAPPY

# ===== OS I/O hints (best-effort) =====
def _try_madvise(mm: mmap.mmap):
    try:
        if hasattr(mmap, "MADV_SEQUENTIAL"):
            mm.madvise(mmap.MADV_SEQUENTIAL)
        if hasattr(mmap, "MADV_WILLNEED"):
            mm.madvise(mmap.MADV_WILLNEED)
    except Exception:
        pass

def _posix_fadvise(fd: int, offset: int, length: int, advice: int) -> None:
    try:
        libc = ctypes.CDLL(None)
        func = getattr(libc, "posix_fadvise", None)
        if func is None: return
        func.argtypes = [ctypes.c_int, ctypes.c_longlong, ctypes.c_longlong, ctypes.c_int]
        func.restype = ctypes.c_int
        func(fd, ctypes.c_longlong(offset), ctypes.c_longlong(length), ctypes.c_int(advice))
    except Exception:
        pass

_POSIX_FADV_SEQUENTIAL = 2
_POSIX_FADV_WILLNEED   = 3

# ===== kernel copy helpers =====
def _sendfile_copy(out_fd: int, in_fd: int, offset: int, count: int) -> int:
    if NO_SENDFILE:
        return -1
    try:
        sent_total, off, rem = 0, offset, count
        while rem > 0:
            res = os.sendfile(out_fd, in_fd, off, rem)
            if isinstance(res, tuple):
                sent, off = res
            else:
                sent = res; off += sent
            if sent == 0: break
            sent_total += sent; rem -= sent
        return sent_total
    except Exception:
        return -1

def _copy_file_range(out_fd: int, in_fd: int, offset: int, count: int) -> int:
    try:
        sent_total, off, rem = 0, offset, count
        while rem > 0:
            n = os.copy_file_range(in_fd, out_fd, off, None, rem)  # type: ignore[attr-defined]
            if n == 0: break
            off += n; sent_total += n; rem -= n
        return sent_total
    except Exception:
        return -1

# ===== Hybrid Zstd probe =====
def _hybrid_mode_from(arg: Optional[str]) -> str:
    v = (arg or ZSTD_HYBRID or "auto").lower()
    return v if v in ("auto","on","off") else "auto"

def _choose_zstd_hybrid_from_sample(sample: bytes, level: int, base_workers: int) -> Tuple[int, int]:
    if not sample:
        return (base_workers, 0)
    # A) threads=0
    t0 = time.perf_counter()
    _ = zstd.ZstdCompressor(level=level, threads=0).compress(sample)
    dt0 = max(1e-6, time.perf_counter() - t0)
    thr0 = len(sample)/dt0
    # B) threads>0  (cap to avoid oversubscription)
    tz = max(2, min( max(1, (_cpu_workers(0)//2)), 8 ))
    t1 = time.perf_counter()
    _ = zstd.ZstdCompressor(level=level, threads=tz).compress(sample)
    dt1 = max(1e-6, time.perf_counter() - t1)
    thr1 = len(sample)/dt1
    if thr1 >= thr0 * 1.15:
        py_workers = min(2, base_workers)
        return (py_workers, tz)
    return (base_workers, 0)

# ===== COMPRESSION =====
def compress_file(
    input_filename: str,
    output_filename: str,
    level: str = "throughput",
    *,
    chunk_size: Optional[int] = None,
    workers: Optional[int] = None,
    verbose: bool = False,
    io_hints: bool = True,
    zstd_hybrid: str = "auto",   # "auto" | "on" | "off"
) -> None:
    file_size = os.path.getsize(input_filename)
    algo_hint, csize, worker_count, zstd_level, zstd_threads, lz4_accel = _plan_for(
        level, chunk_size, workers, path_for_autosize=input_filename
    )

    if verbose:
        auto_lbl = "(manual)" if (chunk_size and chunk_size>0) else "(auto)"
        print(f"Compressing {os.path.basename(input_filename)} → {os.path.basename(output_filename)} (level={level}) …")
        print(f"INFO: base plan → chunk={_fmt_bytes(csize)} {auto_lbl}, workers={worker_count}, zstd_threads={zstd_threads}, lz4_accel={lz4_accel}")

    with open(input_filename, "rb") as f_in, open(output_filename, "wb") as f_out:
        in_fd, out_fd = f_in.fileno(), f_out.fileno()
        f_out.write(struct.pack(HEADER_FMT, MAGIC, VERSION_V2, MIXED_ALGO, csize, file_size))

        if io_hints:
            _posix_fadvise(in_fd, 0, 0, _POSIX_FADV_SEQUENTIAL)
            _posix_fadvise(in_fd, 0, 0, _POSIX_FADV_WILLNEED)

        mm = mmap.mmap(in_fd, 0, access=mmap.ACCESS_READ)
        try:
            if io_hints: _try_madvise(mm)
            mv = memoryview(mm)
            try:
                total = len(mv)

                # Hybrid Zstd — reuse prefix already in memory (up to 8 MiB)
                zstd_hybrid_mode = _hybrid_mode_from(zstd_hybrid)
                if zstd_hybrid_mode != "off":
                    comp_hint = _compressibility_hint(input_filename)
                    likely_zstd = (algo_hint == ALGO_ZSTD) or ("zstd" in (level or "").lower()) or ("ratio" in (level or "").lower()) or (comp_hint < 0.85)
                    if likely_zstd or zstd_hybrid_mode == "on":
                        probe = bytes(mv[:min(8*1024*1024, total)])
                        py_w, zt = _choose_zstd_hybrid_from_sample(probe, zstd_level or 1, worker_count)
                        if zt > 0:
                            worker_count = py_w
                            zstd_threads = zt
                        else:
                            zstd_threads = 0
                        if verbose:
                            print(f"INFO: zstd hybrid → workers={worker_count}, zstd_threads={zstd_threads}")

                n_chunks = (total + csize - 1) // csize

                def job(idx: int):
                    start = idx * csize
                    end = min(start + csize, total)

                    if SAFE_BYTES:
                        raw = bytes(mv[start:end])
                        if not any(raw):  # exact ZERO check on bytes
                            return idx, ALGO_ZERO, start, len(raw), b"", 0.0
                        algo_id = _pick_codec_for_chunk(level, raw[:131072])
                        t0 = time.perf_counter()
                        if algo_id == ALGO_RAW:
                            payload = None
                        elif algo_id == ALGO_SNAPPY:
                            payload = snappy.compress(raw)
                        elif algo_id == ALGO_LZ4:
                            # entropy-biased acceleration
                            ent = _entropy_hint_bytes(raw[:131072])
                            acc = max(1, min(12, (8 if ent > 7.2 else 3)))
                            payload = lz4b.compress(raw, mode="fast", acceleration=acc, store_size=False)
                        elif algo_id == ALGO_ZSTD:
                            payload = _zstd_c(zstd_level or 1, zstd_threads or 0)(raw)
                        else:
                            payload = brotli.compress(raw, quality=5); algo_id = ALGO_BROTLI
                        dt = time.perf_counter() - t0
                        return idx, algo_id, start, len(raw), payload, dt

                    view = mv[start:end]
                    try:
                        if _is_all_zero_mv(view):
                            return idx, ALGO_ZERO, start, (end - start), b"", 0.0
                        prefix = bytes(view[:131072]) if (end-start) > 0 else b""
                        algo_id = _pick_codec_for_chunk(level, prefix)
                        t0 = time.perf_counter()
                        if algo_id == ALGO_RAW:
                            payload = None
                        elif algo_id == ALGO_SNAPPY:
                            payload = _call_codec(snappy.compress, view)
                        elif algo_id == ALGO_LZ4:
                            ent = _entropy_hint_bytes(prefix)
                            acc = max(1, min(12, (8 if ent > 7.2 else 3)))
                            payload = _call_codec(lambda b: lz4b.compress(b, mode="fast", acceleration=acc, store_size=False), view)
                        elif algo_id == ALGO_ZSTD:
                            zc = _zstd_c(zstd_level or 1, zstd_threads or 0)
                            payload = _call_codec(zc, view)
                        else:
                            payload = brotli.compress(bytes(view), quality=5); algo_id = ALGO_BROTLI
                        dt = time.perf_counter() - t0
                        return idx, algo_id, start, (end - start), payload, dt
                    finally:
                        view.release()

                next_to_write = 0
                pending: Dict[int, Tuple[int,int,Optional[bytes],float,int,int]] = {}  # idx -> (algo, start, payload, dt, o_len, start_end_guard)
                submitted = 0
                max_inflight = max(_cpu_workers(worker_count) * 16, 32)
                if INFLIGHT_CAP > 0:
                    max_inflight = min(max_inflight, INFLIGHT_CAP)

                vec: List[bytes] = []   # batched writev
                vec_bytes = 0
                def _flush_vec():
                    nonlocal vec, vec_bytes
                    if vec:
                        _writev(out_fd, vec)
                        vec = []
                        vec_bytes = 0

                with ThreadPoolExecutor(max_workers=worker_count) as ex:
                    futures: Set = set()
                    def fill():
                        nonlocal submitted
                        while submitted < n_chunks and len(futures) < max_inflight:
                            futures.add(ex.submit(job, submitted)); submitted += 1
                    fill()
                    while futures:
                        for fut in as_completed(futures):
                            idx, algo_id, start, o_len, payload, dt = fut.result()
                            pending[idx] = (algo_id, start, payload, dt, o_len, start+o_len)
                            futures.remove(fut)
                            break

                        # Drain in order with RAW/ZERO coalescing and batched writev
                        while next_to_write in pending:
                            algo_id, start, payload, c_dt, o_len, end_pos = pending.pop(next_to_write)

                            # Coalesce consecutive RAW
                            if algo_id == ALGO_RAW and payload is None:
                                _flush_vec()  # maintain order around kernel copy
                                total_len = o_len
                                first_start = start
                                k = next_to_write + 1
                                while k in pending:
                                    a2, s2, p2, _, l2, e2 = pending[k]
                                    if a2 == ALGO_RAW and p2 is None and s2 == (first_start + total_len):
                                        total_len += l2
                                        del pending[k]
                                        k += 1
                                    else:
                                        break
                                hdr = struct.pack(CHUNK_V2_FMT, ALGO_RAW, total_len, total_len)
                                _writev(out_fd, [hdr])
                                sent = _copy_file_range(out_fd, in_fd, first_start, total_len)
                                if sent < 0:
                                    sent = _sendfile_copy(out_fd, in_fd, first_start, total_len)
                                if sent < 0:
                                    _writev(out_fd, [mm[first_start:first_start+total_len]])
                                if verbose:
                                    mb = total_len/(1024*1024); thr = mb/max(c_dt,1e-9) if c_dt else float("inf")
                                    print(f"[compress] {next_to_write:>4}+ raw x{(k-next_to_write)}: {_fmt_bytes(total_len)} | {thr:,.2f} MB/s")
                                next_to_write = k
                                continue

                            # Coalesce consecutive ZERO
                            if algo_id == ALGO_ZERO:
                                total_len = o_len
                                k = next_to_write + 1
                                while k in pending:
                                    a2, s2, p2, _, l2, e2 = pending[k]
                                    if a2 == ALGO_ZERO:
                                        total_len += l2
                                        del pending[k]
                                        k += 1
                                    else:
                                        break
                                hdr = struct.pack(CHUNK_V2_FMT, ALGO_ZERO, total_len, 0)
                                vec.append(hdr); vec_bytes += len(hdr)
                                if verbose:
                                    print(f"[compress] {next_to_write:>4}+ zero x{(k-next_to_write)}: {_fmt_bytes(total_len)} → 0 B")
                                next_to_write = k
                                # flush if large header batch
                                if len(vec) >= WRITEV_BATCH_MAX or vec_bytes >= WRITEV_BYTES_MAX:
                                    _flush_vec()
                                continue

                            # Normal compressed chunk → batch header+payload
                            hdr = struct.pack(CHUNK_V2_FMT, algo_id, o_len, len(payload))
                            vec.append(hdr); vec.append(payload)
                            vec_bytes += len(hdr) + len(payload)
                            if verbose:
                                mb = o_len/(1024*1024); thr = mb/max(c_dt,1e-9)
                                print(f"[compress] {next_to_write:>4} {ALGO_NAMES[algo_id]:<5}: {_fmt_bytes(o_len)} → {_fmt_bytes(len(payload))} | {thr:,.2f} MB/s")
                            next_to_write += 1

                            if len(vec) >= WRITEV_BATCH_MAX or vec_bytes >= WRITEV_BYTES_MAX:
                                _flush_vec()
                        fill()
                    _flush_vec()
            finally:
                mv.release()
        finally:
            mm.close()

# ===== DECOMPRESSION (v1 + v2) =====
def _choose_decomp_mode(mode: Optional[str], algo_hint: int) -> str:
    m = (mode or "auto").lower()
    if m in ("seq","sequential"): return "seq"
    if m in ("par","parallel"):   return "par"
    return "seq"

def decompress_file(
    input_filename: str,
    output_filename: str,
    *,
    workers: Optional[int] = None,
    decomp_mode: str = "auto",
    verbose: bool = False,
    sparse_output: bool = True,
    io_hints: bool = True,
) -> None:
    with open(input_filename, "rb") as f_in:
        hdr = f_in.read(HEADER_SIZE)
        if len(hdr) != HEADER_SIZE: raise ValueError("Input too small to be a valid .warp file")
        magic, ver, header_algo, csize, orig_size = struct.unpack(HEADER_FMT, hdr)
        if magic != MAGIC: raise ValueError("Bad magic; not a warp file")
        if ver not in (VERSION_V1, VERSION_V2): raise ValueError(f"Unsupported version: {ver}")

        metas: List[Tuple[int,int,int,int,int,int]] = []  # (idx, algo, data_off, comp_len, out_off, orig_len)
        file_off = HEADER_SIZE
        idx = 0
        out_off = 0
        if ver == VERSION_V2:
            while True:
                f_in.seek(file_off)
                ch = f_in.read(CHUNK_V2_SIZE)
                if not ch: break
                if len(ch) != CHUNK_V2_SIZE: raise ValueError("Truncated v2 chunk header")
                algo_id, orig_len, comp_len = struct.unpack(CHUNK_V2_FMT, ch)
                data_off = file_off + CHUNK_V2_SIZE
                metas.append((idx, algo_id, data_off, comp_len, out_off, orig_len))
                file_off = data_off + comp_len
                out_off += orig_len; idx += 1
        else:
            while True:
                f_in.seek(file_off)
                ch = f_in.read(CHUNK_V1_SIZE)
                if not ch: break
                if len(ch) != CHUNK_V1_SIZE: raise ValueError("Truncated v1 chunk header")
                orig_len, comp_len = struct.unpack(CHUNK_V1_FMT, ch)
                data_off = file_off + CHUNK_V1_SIZE
                metas.append((idx, header_algo, data_off, comp_len, out_off, orig_len))
                file_off = data_off + comp_len
                out_off += orig_len; idx += 1

    mode = _choose_decomp_mode(decomp_mode, header_algo if ver==VERSION_V1 else MIXED_ALGO)
    worker_count = _cpu_workers(workers)

    if verbose:
        print(f"Decompressing (v{ver}) | mode={mode} | chunk={_fmt_bytes(csize)} | orig={_fmt_bytes(orig_size)} | chunks={len(metas)}")

    with open(input_filename, "rb") as fin2:
        in_fd = fin2.fileno()
        if io_hints:
            _posix_fadvise(in_fd, 0, 0, _POSIX_FADV_SEQUENTIAL)
            _posix_fadvise(in_fd, 0, 0, _POSIX_FADV_WILLNEED)
        mm_in = mmap.mmap(in_fd, 0, access=mmap.ACCESS_READ)
        try:
            def job(rec):
                idx, algo, data_off, comp_len, out_off, orig_len = rec
                if algo in (ALGO_ZERO, ALGO_RAW):
                    return idx, algo, b"", 0.0, orig_len, data_off, comp_len, out_off
                buf = bytes(mm_in[data_off:data_off+comp_len])
                t0 = time.perf_counter()
                if algo == ALGO_SNAPPY:
                    data = _snappy_d()(buf)
                elif algo == ALGO_LZ4:
                    data = _lz4_d(orig_len)(buf)
                elif algo == ALGO_ZSTD:
                    data = _zstd_d()(buf)
                elif algo == ALGO_BROTLI:
                    data = brotli.decompress(buf)
                else:
                    data = buf; algo = ALGO_RAW
                dt = time.perf_counter() - t0
                if len(data) != orig_len: raise ValueError("Decompressed length mismatch")
                return idx, algo, data, dt, orig_len, data_off, comp_len, out_off

            if mode == "seq":
                with open(output_filename, "wb") as f_out:
                    f_out.truncate(orig_size)
                    out_fd = f_out.fileno()
                    max_inflight = max(worker_count*16, 32)
                    if INFLIGHT_CAP > 0:
                        max_inflight = min(max_inflight, INFLIGHT_CAP)
                    next_idx = 0
                    pending: Dict[int, Tuple[int, bytes, float, int, int, int, int]] = {}
                    submitted = 0
                    in_flight: Set = set()
                    with ThreadPoolExecutor(max_workers=worker_count) as ex:
                        def fill():
                            nonlocal submitted
                            while submitted < len(metas) and len(in_flight) < max_inflight:
                                in_flight.add(ex.submit(job, metas[submitted])); submitted += 1
                        fill()
                        while in_flight:
                            for fut in as_completed(in_flight):
                                res = fut.result()
                                pending[res[0]] = res
                                in_flight.remove(fut)
                                break
                            while next_idx in pending:
                                _, algo, data, dt, olen, data_off, comp_len, out_off = pending.pop(next_idx)
                                if algo == ALGO_ZERO and sparse_output:
                                    f_out.seek(olen, os.SEEK_CUR)  # hole
                                    if verbose:
                                        print(f"[decompress] {next_idx:>4} zero  : {_fmt_bytes(olen)} | hole")
                                elif algo == ALGO_RAW:
                                    # Prefer copy_file_range, then sendfile, then memcpy
                                    sent = _copy_file_range(out_fd, in_fd, data_off, comp_len)
                                    if sent < 0:
                                        sent = _sendfile_copy(out_fd, in_fd, data_off, comp_len)
                                    if sent < 0:
                                        os.write(out_fd, mm_in[data_off:data_off+comp_len])
                                    if verbose:
                                        print(f"[decompress] {next_idx:>4} raw   : {_fmt_bytes(olen)} | copy")
                                else:
                                    f_out.write(data)
                                    if verbose:
                                        mb = olen/(1024*1024); thr = mb/max(dt,1e-9)
                                        print(f"[decompress] {next_idx:>4} {ALGO_NAMES[algo]:<5}: {_fmt_bytes(olen)} | {thr:,.2f} MB/s")
                                next_idx += 1
                            fill()
            else:
                with open(output_filename, "wb") as f_out:
                    f_out.truncate(orig_size)
                with open(output_filename, "r+b") as fout2:
                    mm_out = mmap.mmap(fout2.fileno(), 0, access=mmap.ACCESS_WRITE)
                    try:
                        max_inflight = max(worker_count*16, 32)
                        if INFLIGHT_CAP > 0:
                            max_inflight = min(max_inflight, INFLIGHT_CAP)
                        submitted = 0
                        in_flight: Set = set()
                        with ThreadPoolExecutor(max_workers=worker_count) as ex:
                            def fill():
                                nonlocal submitted
                                while submitted < len(metas) and len(in_flight) < max_inflight:
                                    in_flight.add(ex.submit(job, metas[submitted])); submitted += 1
                            fill()
                            while in_flight:
                                for fut in as_completed(in_flight):
                                    idx, algo, data, dt, olen, data_off, comp_len, out_off = fut.result()
                                    if algo == ALGO_ZERO and sparse_output:
                                        pass
                                    elif algo == ALGO_RAW:
                                        mm_out[out_off:out_off+comp_len] = mm_in[data_off:data_off+comp_len]
                                    else:
                                        mm_out[out_off:out_off+len(data)] = data
                                    if verbose:
                                        name = "zero(hole)" if (algo==ALGO_ZERO and sparse_output) else ALGO_NAMES[algo]
                                        mb = olen/(1024*1024); thr = (mb/max(dt,1e-9)) if algo not in (ALGO_ZERO, ALGO_RAW) else float("inf")
                                        print(f"[decompress] {idx:>4} {name:<9}: {_fmt_bytes(olen)} | {thr if thr!=float('inf') else 'copy'}")
                                    in_flight.remove(fut)
                                    break
                                fill()
                    finally:
                        mm_out.flush(); mm_out.close()
        finally:
            mm_in.close()

def detect_algo_name(level: str) -> str:
    hint, *_ = _plan_for(level, None, None)
    return ALGO_NAMES.get(hint, "snappy")

