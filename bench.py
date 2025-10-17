#!/usr/bin/env python3
"""
Simple compressor benchmark:
- warp-compress (your CLI)
- lz4 -1
- zstd -1
- pigz -1

Writes/append CSV with columns:
input, tool, ratio, comp_MBps, decomp_MBps, workers, warp_level, zstd_hybrid,
orig_bytes, comp_bytes, notes
"""
from __future__ import annotations
import argparse, csv, os, shutil, subprocess, sys, tempfile, time
from pathlib import Path

MB = 1024 * 1024

def mbps(bytes_: int, secs: float) -> float:
    return (bytes_ / secs) / MB if secs > 0 else 0.0

def size_of(p: Path) -> int:
    return p.stat().st_size

def have(bin_name: str) -> bool:
    return shutil.which(bin_name) is not None

def run(cmd: list[str], **kw) -> subprocess.CompletedProcess:
    kw.setdefault("stdout", subprocess.PIPE)
    kw.setdefault("stderr", subprocess.PIPE)
    return subprocess.run(cmd, check=True, **kw)

def safe_tmp(prefix: str) -> Path:
    return Path(tempfile.mkdtemp(prefix=prefix))

def bench_warp(input_path: Path, workers: int, level: str, chunk_bytes: int | None):
    tool = f"warp-{level}"
    outdir = safe_tmp("warp-")
    try:
        out_warp = outdir / "out.warp"
        out_raw  = outdir / "out.bin"
        args = ["warp-compress","compress",str(input_path),str(out_warp),"--level",level,"--workers",str(workers)]
        if chunk_bytes:
            args += ["--chunk", str(int(chunk_bytes))]
        t0 = time.perf_counter(); run(args); t1 = time.perf_counter()
        orig_bytes = size_of(input_path); comp_bytes = size_of(out_warp)
        t2 = time.perf_counter(); run(["warp-compress","decompress",str(out_warp),str(out_raw),"--workers",str(workers)]); t3 = time.perf_counter()
        ratio = comp_bytes / orig_bytes if orig_bytes else 1.0
        return tool, ratio, mbps(orig_bytes, t1 - t0), mbps(orig_bytes, t3 - t2), comp_bytes, ""
    finally:
        shutil.rmtree(outdir, ignore_errors=True)

def bench_lz4(input_path: Path, workers: int):
    if not have("lz4"): return None
    tool = "lz4-1"
    outdir = safe_tmp("lz4-")
    try:
        out_c  = outdir / "out.lz4"; out_raw = outdir / "out.bin"; orig_bytes = size_of(input_path)
        t0 = time.perf_counter(); run(["lz4","-1","-f",str(input_path),str(out_c)]); t1 = time.perf_counter()
        t2 = time.perf_counter(); run(["lz4","-d","-f",str(out_c),str(out_raw)]); t3 = time.perf_counter()
        comp_bytes = size_of(out_c); ratio = comp_bytes / orig_bytes if orig_bytes else 1.0
        return tool, ratio, mbps(orig_bytes, t1 - t0), mbps(orig_bytes, t3 - t2), comp_bytes, ""
    finally:
        shutil.rmtree(outdir, ignore_errors=True)

def bench_zstd(input_path: Path, workers: int):
    if not have("zstd"): return None
    tool = "zstd-1"
    outdir = safe_tmp("zstd-")
    try:
        out_c  = outdir / "out.zst"; out_raw = outdir / "out.bin"; orig_bytes = size_of(input_path)
        t0 = time.perf_counter(); run(["zstd","-1",f"-T{workers}",str(input_path),"-o",str(out_c)]); t1 = time.perf_counter()
        t2 = time.perf_counter(); run(["zstd","-d",f"-T{workers}",str(out_c),"-o",str(out_raw)]); t3 = time.perf_counter()
        comp_bytes = size_of(out_c); ratio = comp_bytes / orig_bytes if orig_bytes else 1.0
        return tool, ratio, mbps(orig_bytes, t1 - t0), mbps(orig_bytes, t3 - t2), comp_bytes, ""
    finally:
        shutil.rmtree(outdir, ignore_errors=True)

def bench_pigz(input_path: Path, workers: int):
    if not have("pigz"): return None
    tool = "pigz-1"
    outdir = safe_tmp("pigz-")
    try:
        out_gz  = outdir / "out.gz"; out_raw = outdir / "out.bin"; orig_bytes = size_of(input_path)
        t0 = time.perf_counter()
        with out_gz.open("wb") as f: run(["pigz","-1","-p",str(workers),"-c",str(input_path)], stdout=f)
        t1 = time.perf_counter()
        comp_bytes = size_of(out_gz)
        t2 = time.perf_counter()
        with out_raw.open("wb") as f: run(["pigz","-d","-p",str(workers),"-c",str(out_gz)], stdout=f)
        t3 = time.perf_counter()
        ratio = comp_bytes / orig_bytes if orig_bytes else 1.0
        return tool, ratio, mbps(orig_bytes, t1 - t0), mbps(orig_bytes, t3 - t2), comp_bytes, ""
    finally:
        shutil.rmtree(outdir, ignore_errors=True)

def main():
    ap = argparse.ArgumentParser(description="Benchmark warp + competitors")
    ap.add_argument("input", help="input file to compress")
    ap.add_argument("--csv", required=True, help="CSV output (append)")
    ap.add_argument("--workers", type=int, default=os.cpu_count() or 8)
    ap.add_argument("--warp-level", choices=["throughput","lz4","zstd","ratio"], default="throughput")
    ap.add_argument("--warp-chunk", type=int, default=0, help="override warp chunk bytes (0=auto)")
    ap.add_argument("--only", nargs="*", default=None, help="limit tools, e.g. --only warp-throughput zstd-1")
    args = ap.parse_args()

    ip = Path(args.input)
    if not ip.exists():
        print(f"Input not found: {ip}", file=sys.stderr); sys.exit(2)

    tool_fns = [
        (f"warp-{args.warp_level}", lambda: bench_warp(ip, args.workers, args.warp_level, args.warp_chunk or None)),
        ("lz4-1",  lambda: bench_lz4(ip, args.workers)),
        ("zstd-1", lambda: bench_zstd(ip, args.workers)),
        ("pigz-1", lambda: bench_pigz(ip, args.workers)),
    ]
    if args.only:
        only = set(args.only)
        tool_fns = [(n,f) for (n,f) in tool_fns if n in only]
        if not tool_fns:
            print(f"No matching tools in --only: {args.only}", file=sys.stderr); sys.exit(2)

    rows = []
    for label, fn in tool_fns:
        try:
            r = fn()
        except subprocess.CalledProcessError as e:
            print(f"[skip] {label}: failed: {e}", file=sys.stderr); continue
        if r is None:
            print(f"[skip] {label}: tool not installed", file=sys.stderr); continue
        tool, ratio, cMBps, dMBps, comp_bytes, notes = r
        print(f"{tool:14s} ratio={ratio:.3f}  comp={cMBps:.1f} MB/s  decomp={dMBps:.1f} MB/s")
        rows.append({
            "input": str(ip),
            "tool": tool,
            "ratio": ratio,
            "comp_MBps": cMBps,
            "decomp_MBps": dMBps,
            "workers": args.workers,
            "warp_level": args.warp_level,
            "zstd_hybrid": "auto",
            "orig_bytes": size_of(ip),
            "comp_bytes": comp_bytes,
            "notes": notes,
        })

    if not rows:
        print("Nothing to write (all tools skipped).", file=sys.stderr); sys.exit(1)

    out = Path(args.csv)
    write_header = not out.exists()
    with out.open("a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        if write_header: w.writeheader()
        for r in rows: w.writerow(r)

if __name__ == "__main__":
    main()
