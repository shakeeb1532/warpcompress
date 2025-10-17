"""
WarpCompress package
"""
__all__ = ["compress_file", "decompress_file", "detect_algo_name", "__version__"]

__version__ = "0.7.3"

from .core import compress_file, decompress_file, detect_algo_name


