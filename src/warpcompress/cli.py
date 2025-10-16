# WarpCompress CLI
import argparse
import sys
from .core import (
    compress_file, decompress_file, detect_algo_name,
    CHUNK_SIZE_DEFAULT, _fmt_bytes
)

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="warp-compress",
        description="WarpCompress: fast, chunked compression with parallel (de)compression."
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    pc = sub.add_parser("compress", help="Compress a file")
    pc.add_argument("input_file")
    pc.add_argument("output_file")
    pc.add_argument("--level", default="throughput",
                    help="throughput(snappy) | lz4 | zstd | ratio(brotli)")
    pc.add_argument("--chunk-size", type=int, default=None,
                    help=f"Chunk size in bytes (default: {_fmt_bytes(CHUNK_SIZE_DEFAULT)})")
    pc.add_argument("--workers", type=int, default=None,
                    help="Worker threads (default: CPU count)")
    pc.add_argument("--verbose", action="store_true")

    pd = sub.add_parser("decompress", help="Decompress a .warp file")
    pd.add_argument("input_file")
    pd.add_argument("output_file")
    pd.add_argument("--workers", type=int, default=None,
                    help="Worker threads (default: CPU count)")
    pd.add_argument("--decomp-mode", default="auto", choices=["auto","seq","par"],
                    help="Decompression mode: auto (default), seq=parallel decode + sequential writes, par=parallel mmap writes")
    pd.add_argument("--verbose", action="store_true")
    return p

def main() -> int:
    args = _build_parser().parse_args()
    try:
        if args.cmd == "compress":
            if args.verbose:
                print(f"Using algorithm: {detect_algo_name(args.level)}")
            compress_file(
                args.input_file,
                args.output_file,
                level=args.level,
                chunk_size=args.chunk_size,
                workers=args.workers,
                verbose=args.verbose,
            )
        elif args.cmd == "decompress":
            decompress_file(
                args.input_file,
                args.output_file,
                workers=args.workers,
                decomp_mode=args.decomp_mode,
                verbose=args.verbose,
            )
        else:
            return 2
        return 0
    except KeyboardInterrupt:
        return 130
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

if __name__ == "__main__":
    sys.exit(main())



