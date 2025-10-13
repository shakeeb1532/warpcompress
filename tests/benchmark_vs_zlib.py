import os, time, zlib, argparse, statistics as stats
from pathlib import Path
from warpcompress.core import CompressionManager

def make_data(size_mb: int) -> bytes:
    # moderately compressible synthetic
    import random
    r = random.Random(42)
    chunks = []
    for _ in range(size_mb):
        s = bytearray(os.urandom(512*1024))
        # sprinkle zeros & repeated patterns
        for i in range(0, len(s), 4096):
            if r.random() < 0.2:
                s[i:i+1024] = b"\x00" * min(1024, len(s)-i)
        chunks.append(bytes(s))
    return b"".join(chunks)

def bench_once_zlib(data: bytes):
    t0 = time.perf_counter()
    comp = zlib.compress(data, 6)
    t1 = time.perf_counter()
    decomp = zlib.decompress(comp)
    t2 = time.perf_counter()
    assert decomp == data
    return (t1 - t0), (t2 - t1)

def bench_once_warp(data: bytes, tmp: Path, level: str = "throughput", checksum: str = "none"):
    inp = tmp / "in.bin"; out = tmp / "out.warp"; back = tmp / "back.bin"
    inp.write_bytes(data)
    cm = CompressionManager(verbose=False)
    t0 = time.perf_counter()
    cm.compress_file(str(inp), str(out), level=level)
    t1 = time.perf_counter()
    cm.decompress_file(str(out), str(back))
    t2 = time.perf_counter()
    assert back.read_bytes() == data
    return (t1 - t0), (t2 - t1)

def bench(size_mb: int, runs: int, tmp: Path):
    data = make_data(size_mb)
    zc, zd, wc, wd = [], [], [], []
    for _ in range(runs):
        c, d = bench_once_zlib(data); zc.append(c); zd.append(d)
        c, d = bench_once_warp(data, tmp); wc.append(c); wd.append(d)
    def fmt(xs): return f"{stats.mean(xs):.4f} s"
    return fmt(zc), fmt(zd), fmt(wc), fmt(wd)

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--sizes", type=int, nargs="+", default=[10, 50, 100, 200])
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--tmpdir", type=Path, default=Path("./.bench_tmp"))
    args = ap.parse_args()
    args.tmpdir.mkdir(parents=True, exist_ok=True)

    print("\nHere are the detailed benchmark results (average of", args.runs, "runs per size):\n")
    print("Size (MB)\tzlib compress\tzlib decompress\twarpcompress compress\twarpcompress decompress")
    for sz in args.sizes:
        zc, zd, wc, wd = bench(sz, args.runs, args.tmpdir)
        print(f"{sz}\t{zc}\t{zd}\t{wc}\t{wd}")
    print()
