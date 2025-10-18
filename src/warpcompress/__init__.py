"""
WarpCompress package
"""
__all__ = [
    "compress_file",
    "decompress_file",
    "detect_algo_name",
    "compress_stream",
    "decompress_stream",
    "__version__",
]

__version__ = "0.8.0"

from .core import compress_file, decompress_file, detect_algo_name
from .stream import compress_stream, decompress_stream



