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
