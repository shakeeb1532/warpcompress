# cli.py — WarpCompress CLI
import argparse
import sys
from .core import compress_file, decompress_file, detect_algo_name

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="warp-compress", description="WarpCompress: massive-file-ready lossless compression.")
    sub = p.add_subparsers(dest="cmd", required=True)

    pc = sub.add_parser("compress", help="Compress a file.")
    pc.add_argument("input_file")
    pc.add_argument("output_file")
    pc.add_argument("--level", default="auto",
                    help="snappy|lz4|zstd|brotli or modes: auto/throughput/balanced/ratio (default: auto)")
    pc.add_argument("--chunk-size", type=int, default=None, help="Chunk size in bytes (default: 4MiB).")
    pc.add_argument("--verbose", action="store_true")

    pd = sub.add_parser("decompress", help="Decompress a .warp file.")
    pd.add_argument("input_file")
    pd.add_argument("output_file")
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
                chunk_size=args.chunk_size if args.chunk_size else None or 4 * 1024 * 1024,
                verbose=args.verbose,
            )
        elif args.cmd == "decompress":
            decompress_file(args.input_file, args.output_file, verbose=args.verbose)
        else:
            raise SystemExit(2)
        return 0
    except KeyboardInterrupt:
        return 130
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

if __name__ == "__main__":
    sys.exit(main())


