# 🚀 WarpCompress

**Massive-file-ready (100GB+), parallel, lossless compressor/decompressor** with mmap scatter–gather I/O, zero-copy Zstd decode, and an adaptive throughput mode.

[![CI](https://github.com/shakeeb1532/warpcompress/actions/workflows/ci.yml/badge.svg)](https://github.com/shakeeb1532/warpcompress/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/python-3.9%2B-blue.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)
![Platform](https://img.shields.io/badge/platform-Linux%20%7C%20macOS-lightgrey.svg)

> ⚡️ **Why WarpCompress?**
>
> - **Fast**: C-extension codecs + multi-threading + zero-copy Zstd `decompress_into()`  
> - **Scalable**: mmap chunking and scatter–gather decompression → constant RAM, even for 100GB  
> - **Practical**: Integrity footer (BLAKE3/xxhash), `--verbose` per-chunk timings, and an auto **throughput** mode

---

## ✨ Features

- **Format v2** writer: stores both compressed sizes and original per-chunk sizes → **true parallel** scatter–gather **decompression**  
- **Backward-compatible** reader: v1 files stream sequentially (still constant RAM)  
- **Throughput mode** (`--level throughput`): adapts chunk size, thread counts, and zstd threads by input size/cores  
- **Integrity footer**: `--checksum {none,blake3,xxh64,blake2b}`  
- **Verbose profiling**: `--verbose` prints per-chunk throughput

---

## 🔧 Install

```bash
pip install -e .[hashes]
# Base deps: lz4, zstandard, python-snappy, brotli
# Extras:    blake3, xxhash

## 🏁 Quick Start
# Compress for max wall-clock speed (throughput plan) + BLAKE3 footer
warp-compress compress input.bin out.warp --level throughput --checksum blake3 --verbose

# Decompress (parallel scatter-gather for v2; streamed for v1)
warp-compress decompress out.warp restored.bin --verbose

## 🧪 Benchmarks

Average of 3 runs per size (from tests/benchmark_vs_zlib.py):

| Size (MB) | zlib compress | zlib decompress | warpcompress compress | warpcompress decompress |
| --------: | ------------: | --------------: | --------------------: | ----------------------: |
|        10 |      0.2353 s |        0.0070 s |          **0.0158 s** |                0.0187 s |
|        50 |      1.5433 s |        0.0492 s |          **0.0853 s** |                0.1027 s |
|       100 |      2.4197 s |        0.1284 s |          **0.1872 s** |                0.2025 s |
|       200 |      6.2520 s |        0.2508 s |          **0.6333 s** |                0.7307 s |

Notes

WarpCompress outpaces zlib on compression across sizes.

zlib can decode small inputs faster; WarpCompress narrows the gap as size grows.

For decode-heavy workloads, try compressing with LZ4 or Zstd-1..3 for even faster reads.

## 🧠 How it works (visuals)

Parallel Compression (high level)

flowchart LR
  A[Input file (mmap)] --> B[Chunk views]
  B -->|Thread pool| C1[Codec compress]
  B -->|Thread pool| C2[Codec compress]
  B -->|Thread pool| C3[Codec compress]
  C1 --> D[Collect compressed chunks]
  C2 --> D
  C3 --> D
  D --> E[Write v2 header (sizes + orig chunk sizes)]
  E --> F[Write chunks + optional footer]

Parallel Scatter–Gather Decompression (v2)
sequenceDiagram
  participant IN as .warp (mmap)
  participant POOL as Thread Pool
  participant OUT as Output mmap

  IN->>POOL: Read per-chunk compressed sizes
  POOL->>POOL: Compute output offsets (prefix sums of decomp sizes)
  loop chunks
    POOL->>IN: Slice compressed chunk view
    alt Zstd (zero-copy)
      POOL->>OUT: decompress_into( OUT[start:end], IN[chunk] )
    else Others
      POOL->>POOL: out = decompress(IN[chunk])
      POOL->>OUT: OUT[start:end] = out
    end
  end

🧰 CLI Reference
warp-compress compress [-h] [--threads N] [--chunk-size BYTES]
                      [--zstd-level N] [--zstd-threads N]
                      [--brotli-quality N] [--verbose]
                      [--level {auto,fastest,max,throughput}]
                      [--checksum {none,blake3,xxh64,blake2b}]
                      input_file output_file

warp-compress decompress [-h] [--threads N] [--chunk-size BYTES]
                        [--zstd-level N] [--zstd-threads N]
                        [--brotli-quality N] [--verbose]
                        input_file output_file
🧩 File Format (v2)

[MAGIC u32='WARP'] [VER u8=2] [ALGO u8] [ORIG_SIZE u64] [NUM_CHUNKS u32]
[COMP_SIZE_0 u64] ... [COMP_SIZE_N-1 u64]
[DECOMP_SIZE_0 u64] ... [DECOMP_SIZE_N-1 u64]
[CHUNK_0 bytes] ... [CHUNK_N-1 bytes]
# optional trailer:
[TRAILER_MAGIC u32='WFT1'] [HASH_ALGO u8] [SUM_LEN u16] [SUM bytes]

- v2 stores both compressed sizes and expected decompressed sizes → enables per-chunk offset precompute and zero-copy Zstd decompress_into().

🧑‍💻 Development

# Setup
python -m venv .venv && source .venv/bin/activate
pip install -e .[hashes]
pip install -U pytest

# Run tests
pytest -q

# Benchmark against zlib
PYTHONPATH=src python tests/benchmark_vs_zlib.py --sizes 10 50 100 200 --runs 3

Repo pointers

- Core engine: src/warpcompress/core.py

- CLI entrypoint: src/warpcompress/cli.py

- Tests & benchmark: tests/

🐛 Troubleshooting

ImportError: zstandard/lz4/snappy/brotli: run pip install -e .[hashes] again (base + extras)

Slow HDD or network FS? Use bigger chunks (e.g., --chunk-size 16_777_216) and --level throughput

Low-RAM machine? WarpCompress uses constant RAM; if paging occurs, try fewer threads (--threads 4)
