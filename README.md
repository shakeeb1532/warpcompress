# WarpCompress

A fast, **massive-file-ready** (100GB+), parallel, lossless compressor/decompressor.

## Highlights
- **Format v2** header stores both *compressed sizes* and *original per-chunk sizes* → **true parallel scatter-gather decompression** using `mmap` (constant RAM).
- **Backward compatible** reader for v1 files (safe streamed writes).
- **Throughput mode** (`--level throughput`) auto-tunes chunk size, host threads, and zstd internal threads based on file size and cores.
- **Integrity footer** (`--checksum {none,blake3,xxh64,blake2b}`) verifies the original data.
- **Verbose per-chunk timings** (`--verbose`) for profiling.
- Zero-copy chunking with `mmap` + `memoryview`; threaded C-extensions (release GIL).

## Install
```bash
pip install -e .[hashes]
# base: lz4, zstandard, python-snappy, brotli
# extras [hashes]: blake3, xxhash
CLI
bash
Copy code
# Compress (v2 writer) for max wall-clock speed, with integrity footer
warp-compress compress input.bin output.warp --level throughput --checksum blake3 --verbose

# Decompress (parallel scatter-gather for v2; streamed for v1)
warp-compress decompress output.warp restored.bin --verbose
Common options:

scss
Copy code
--threads N            Host threads for (de)compression (default: auto up to 32)
--chunk-size BYTES     Chunk size (writer). Throughput mode may override this.
--zstd-level N         Zstandard level (default: 3)
--zstd-threads N       Zstd internal threads (default: 0 -> auto)
--brotli-quality N     Brotli quality 0–11 (default: 5)
--checksum {none,blake3,xxh64,blake2b}
--verbose
Python API
python
Copy code
from warpcompress.core import CompressionManager, HASH_BLAKE3

cm = CompressionManager(verbose=True, checksum_algo=HASH_BLAKE3)
cm.compress_file("input.bin", "out.warp", level="throughput")
cm.decompress_file("out.warp", "back.bin")
File Format
Little-endian, versioned.

v2 (writer default):

less
Copy code
[MAGIC u32='WARP'] [VER u8=2] [ALGO u8] [ORIG_SIZE u64] [NUM_CHUNKS u32]
[COMP_SIZE_0 u64] ... [COMP_SIZE_N-1 u64]
[DECOMP_SIZE_0 u64] ... [DECOMP_SIZE_N-1 u64]
[CHUNK_0 bytes] ... [CHUNK_N-1 bytes]
# optional trailer:
[TRAILER_MAGIC u32='WFT1'] [HASH_ALGO u8] [SUM_LEN u16] [SUM bytes]
v1 (legacy, still supported for reading):

css
Copy code
[MAGIC u32='WARP'] [VER u8=1] [ALGO u8] [ORIG_SIZE u64] [NUM_CHUNKS u32]
[COMP_SIZE_0 u64] ... [COMP_SIZE_N-1 u64]
[CHUNK_0 bytes] ... [CHUNK_N-1 bytes]
# optional trailer as above
v1.0.1 (performance)
Zstd zero-copy decompress_into into output mmap slices (v2 format).

Thread-local Zstd decompressor reuse (less per-chunk overhead).

Added MADV_WILLNEED/SEQUENTIAL hints for input/output mmaps.

License
MIT

css
Copy code

---

# Replace: `tests/test_roundtrip.py` (kept minimal; covers v2 + throughput)

```python
import os, pathlib
from warpcompress.core import CompressionManager, HASH_BLAKE3

def test_roundtrip_small(tmp_path: pathlib.Path):
    p = tmp_path / "small.bin"
    data = os.urandom(256 * 1024)
    p.write_bytes(data)
    cm = CompressionManager(verbose=False, checksum_algo=HASH_BLAKE3)
    out = tmp_path / "small.warp"
    back = tmp_path / "small.back"
    cm.compress_file(str(p), str(out), level="auto")
    cm.decompress_file(str(out), str(back))
    assert back.read_bytes() == data

def test_roundtrip_throughput(tmp_path: pathlib.Path):
    p = tmp_path / "med.bin"
    data = os.urandom(3 * 1024 * 1024)
    p.write_bytes(data)
    cm = CompressionManager(verbose=False)
    out = tmp_path / "med.warp"
    back = tmp_path / "med.back"
    cm.compress_file(str(p), str(out), level="throughput")
    cm.decompress_file(str(out), str(back))
    assert back.read_bytes() == data
