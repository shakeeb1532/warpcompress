from __future__ import annotations
from typing import Iterable
import os, pathlib

try:
    import zstandard as zstd  # type: ignore
except Exception:
    zstd = None  # type: ignore

def gather_samples(root: str, *, max_files: int = 5000, max_size: int = 256*1024) -> list[bytes]:
    """Collect small file samples under root (breadth-first)."""
    rootp = pathlib.Path(root)
    out: list[bytes] = []
    for p in rootp.rglob("*"):
        if len(out) >= max_files:
            break
        if p.is_file():
            try:
                if p.stat().st_size <= max_size:
                    out.append(p.read_bytes())
            except Exception:
                continue
    return out

def train_zstd_dict(sample_dir: str, *, dict_size: int = 131072) -> bytes:
    if zstd is None:
        raise RuntimeError("zstandard not available (pip install zstandard)")
    samples = gather_samples(sample_dir)
    if not samples:
        raise ValueError("No suitable samples found for dictionary training")
    d = zstd.train_dictionary(dict_size, samples)
    return bytes(d.as_bytes())
