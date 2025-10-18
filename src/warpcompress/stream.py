# Stream API (v0.8.0). Safe temp-file backed; we can inline real streaming later.
from __future__ import annotations
from typing import BinaryIO, Optional
import os, tempfile, shutil
from .core import compress_file, decompress_file

def compress_stream(src: BinaryIO, dst: BinaryIO, *, level: str = "throughput",
                    workers: Optional[int] = None, chunk: Optional[int] = None,
                    verify: bool = False, dict_bytes: Optional[bytes] = None,
                    checksum: str = "none", make_index: bool = False) -> None:
    with tempfile.NamedTemporaryFile(prefix="warp-stream-in-", delete=False) as in_tmp:
        in_name = in_tmp.name
        shutil.copyfileobj(src, in_tmp)
    with tempfile.NamedTemporaryFile(prefix="warp-stream-out-", delete=False) as out_tmp:
        out_name = out_tmp.name
    try:
        compress_file(in_name, out_name, level=level, workers=workers, chunk=chunk,
                      verbose=False, checksum=checksum, make_index=make_index, zstd_dict=dict_bytes)
        with open(out_name, "rb") as f:
            shutil.copyfileobj(f, dst)
    finally:
        for p in (in_name, out_name):
            try: os.unlink(p)
            except Exception: pass

def decompress_stream(src: BinaryIO, dst: BinaryIO, *, workers: Optional[int] = None, verify: bool = False) -> None:
    with tempfile.NamedTemporaryFile(prefix="warp-stream-in-", delete=False) as in_tmp:
        in_name = in_tmp.name
        shutil.copyfileobj(src, in_tmp)
    with tempfile.NamedTemporaryFile(prefix="warp-stream-out-", delete=False) as out_tmp:
        out_name = out_tmp.name
    try:
        decompress_file(in_name, out_name, workers=workers, verbose=False, verify=verify)
        with open(out_name, "rb") as f:
            shutil.copyfileobj(f, dst)
    finally:
        for p in (in_name, out_name):
            try: os.unlink(p)
            except Exception: pass
