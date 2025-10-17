# 🚀 WarpCompress
warpcompress








Pure-Python, thread-parallel block compressor with a simple container.

Fast by default: --level throughput uses LZ4 (block API) for quick writes and strong read speed.

Parallel both ways: compression and decompression use threads (C libs release the GIL).

Smart chunks: tuned policy (small-file fast path; 16–64 MiB for medium/large).

Low overhead: zero-copy COPY blocks and single-syscall writes (os.writev).

Cross-platform: macOS, Linux, Windows (Python 3.9+).

Container format v2. CLI name is warp-compress. Current package version: 0.7.5.

TL;DR
# Install (with speed codecs)
pip install warpcompress zstandard lz4 python-snappy

# Fast compress (throughput mode)
warp-compress compress input.bin out.warp --level throughput --workers 8 --verbose

# Decompress
warp-compress decompress out.warp roundtrip.bin --workers 8

Install
PyPI
pip install warpcompress
# Optional but recommended:
pip install zstandard lz4 python-snappy

From Git tag (GitHub)
pip install "warpcompress @ git+https://github.com/shakeeb1532/warpcompress@v0.7.5"
pip install zstandard lz4 python-snappy

(Optional) Container (GHCR)

If you publish a GHCR image:

docker pull ghcr.io/shakeeb1532/warpcompress:latest
docker run --rm -v "$PWD:/work" ghcr.io/shakeeb1532/warpcompress:latest \
  compress /work/input.bin /work/out.warp --level throughput --workers 8 --verbose

CLI
usage: warp-compress [--version] {compress,decompress} ...

compress:
  warp-compress compress INPUT OUTPUT.warp
    --level {throughput,zstd,lz4,ratio}   # default: throughput
    --workers INT                         # default: cpu_count()
    --chunk BYTES                         # override policy chunk
    --verbose

decompress:
  warp-compress decompress INPUT.warp OUTPUT
    --workers INT
    --verbose

Levels

throughput → LZ4 block (fastest writes, great reads)

zstd → Zstandard level=1, threads=workers (great ratio; very fast on zeros)

lz4 → explicit LZ4 block

ratio → Zstandard level=3 (better compression, still quick)

Examples
# Fast & simple
warp-compress compress big.bin big.warp --level throughput --workers 8 --verbose

# Better ratio
warp-compress compress big.bin big-zstd.warp --level zstd --workers 8

# Force a chunk (e.g., 48 MiB) to experiment
warp-compress compress data.bin data.warp --level throughput --chunk 50331648

Env toggles

WARP_CHUNK_MAX (bytes) — cap the chunk size (default 64 MiB)

WARP_AUTO_CHUNK=0 — disable policy and require --chunk

Getting the best chunk automatically (optional)

Warp’s built-in policy is tuned for typical machines (≤32 MiB → single chunk, then 16/32/48/64 MiB).
If you want peak performance on a specific machine/file mix:

Option A — Tiny manual sweep (works on all versions)

# Try a few chunks and pick the fastest for your data
for c in 4194304 8388608 16777216 33554432 50331648 67108864; do
  ./bench.py input.bin --csv results.csv --workers 8 \
    --warp-level throughput --only warp-throughput --warp-chunk $c
done


Option B — Autotune (if enabled in your build)
If you enabled an autotune hook in core.py, set:

export WARP_AUTOTUNE=1
# Then run as usual; Warp will test a few chunk sizes on a small prefix and choose the best.
warp-compress compress input.bin out.warp --level throughput --workers 8


If your build doesn’t include autotune, the env var is simply ignored (no errors).

Container format (v2)

File header (FHDR):
MAGIC='WARP'(4) | ver(u8)=2 | flags(u8)=0 | chunk_size(u32 LE) | reserved(u32 LE)

Per-chunk:
algo(u8) | orig_len(u32 LE) | comp_len(u32 LE) | payload(comp_len bytes)

Algorithms:
1=zstd, 2=lz4, 3=snappy, 4=copy (verbatim), 5=zero (no payload)

zero chunks expand to orig_len zero bytes and have comp_len=0.

Quick sanity check
python - <<'PY'
import os, hashlib
# 128 MiB random
with open("rand-128M.bin","wb") as f:
    f.write(os.urandom(128*1024*1024))
PY

warp-compress compress rand-128M.bin rand.warp --level throughput --workers 8 --verbose
warp-compress decompress rand.warp round.bin --workers 8

# verify
python - <<'PY'
import hashlib
def sha256(p):
    m=hashlib.sha256()
    with open(p,'rb') as f:
        for b in iter(lambda:f.read(1<<20), b''):
            m.update(b)
    return m.hexdigest()
print("input :", sha256("rand-128M.bin"))
print("output:", sha256("round.bin"))
PY

Benchmarking

A simple bench.py compares Warp vs lz4 -1, zstd -1, pigz -1.

Make a 1 GiB mixed dataset (≈35% random, rest zeros)
python - <<'PY'
import os, random
MiB=1024*1024
with open("input-mixed-1G.bin","wb") as f:
    z=b"\x00"*MiB
    for _ in range(1024):
        f.write(os.urandom(MiB) if random.randrange(100)<35 else z)
PY

Run the bench
# Install competitor CLIs (macOS example; adjust for your OS)
brew install lz4 zstd pigz

# Warp + competitors
./bench.py input-mixed-1G.bin --csv results.csv --workers 8 --warp-level throughput

# Warp-only chunk sweep (find your local peak)
for c in 4194304 8388608 16777216 33554432 50331648 67108864; do
  ./bench.py input-mixed-1G.bin --csv results.csv --workers 8 \
    --warp-level throughput --only warp-throughput --warp-chunk $c
done

Cross-platform benchmark in GitHub Actions

This repo includes .github/workflows/crossbench.yml to run identical benches on Ubuntu, macOS, Windows and upload CSVs.

Go to Actions → Cross-platform Bench → Run workflow

sizes_mib: 20 200 1024

mix_pct: 35

workers: 8

When it finishes, download artifacts:

results-Ubuntu.csv, results-macOS.csv, results-Windows.csv

Missing competitor tools on a runner are skipped gracefully; CSV still writes other tools.

Past benchmark results (your runs)

All runs used workers=8. Numbers are best observed.
Hardware (as reported by your bench): macOS 11.2.3, ARM64, 8 cores.

A) 1 GiB mixed (≈35% random) — policy exploration & competitor compare
tool	ratio	comp MB/s	decomp MB/s
warp-throughput (best)	~0.396	1655	1884
lz4 -1	0.370	1797	1024
zstd -1	0.366	4891	2490
pigz -1	0.369	665	1617

Warp chunk sweep (compression MB/s):

chunk	4 MiB	8 MiB	16 MiB	32 MiB	48 MiB	64 MiB
comp	453	728	1109	1349	1655	1601
B) 5 GiB datasets — end-to-end compare

Input: mixed-5G (5120 MiB)

tool	ratio	comp MB/s	decomp MB/s
warp-throughput	0.358	152	452
lz4 -1	0.361	1074	341
zstd -1	0.358	855	728
pigz -1	0.361	582	455

Input: zero-5G (5120 MiB)

tool	ratio	comp MB/s	decomp MB/s
warp-zstd	0.000	147	500
lz4 -1	0.004	2147	1721
zstd -1	0.000	2901	1951
pigz -1	0.005	2652	1327

Input: rand-5G (5120 MiB)

tool	ratio	comp MB/s	decomp MB/s
warp-throughput	1.000	204	377
lz4 -1	1.000	727	630
zstd -1	1.000	579	364
pigz -1	1.000	150	150

Notes:
• The 1 GiB mixed run shows Warp’s best chunk ≈ 48 MiB on this Mac.
• warp-zstd compress speed on zeros benefits from threaded Zstd (enabled in v0.7.5).
• Competitor tools are highly optimized C; Warp aims for strong read speed and competitive write speed while staying pure-Python.

Development
git clone https://github.com/shakeeb1532/warpcompress
cd warpcompress
python -m venv .venv && source .venv/bin/activate
python -m pip install -U pip
python -m pip install -e . zstandard lz4 python-snappy
# optional
pytest -q

Releasing

Bump version in:

src/warpcompress/__init__.py → __version__

pyproject.toml → version

Tag & release

git add -A
git commit -m "v0.7.5"
git tag -a v0.7.5 -m "v0.7.5"
git push origin main --tags

PyPI (Trusted Publisher)

On pypi.org → your project → Settings → Publishing, add a Pending Trusted Publisher for this repo/workflow.

Create the tag (v0.7.5) and your publish workflow will push the wheel/sdist.

GHCR (optional)

With the provided Dockerfile + publish-ghcr.yml, a tag push builds:

ghcr.io/shakeeb1532/warpcompress:latest

ghcr.io/shakeeb1532/warpcompress:v0.7.5

License

MIT (see LICENSE).

Maintainers’ checklist

 Version bumped in pyproject.toml and __init__.py

 bench.py present at repo root

 Workflows present (crossbench.yml, optional publish-pypi.yml, publish-ghcr.yml)

 Tag + GitHub Release created (triggers PyPI/GHCR if configured)

If you want me to include a badge for PyPI downloads or add usage gifs/screenshots, I can slot them at the top under the existing badges.
