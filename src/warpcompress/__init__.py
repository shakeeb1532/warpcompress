"""
WarpCompress package
"""
from .core import (
    CompressionManager,
    HASH_NONE, HASH_BLAKE3, HASH_XXH64, HASH_BLAKE2B,
)
__all__ = [
    "CompressionManager",
    "HASH_NONE", "HASH_BLAKE3", "HASH_XXH64", "HASH_BLAKE2B",
]
__version__ = "1.0.1"
