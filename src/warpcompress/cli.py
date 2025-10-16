# WarpCompress CLI — v0.7.0 hybrid zstd + fallbacks + batched I/O
import argparse, sys
from .core import (
    compress_file, decompress_file, detect_algo_name,
    CHUNK_SIZE_DEFAULT, _fmt_bytes
)

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="warp-compress",
        description="WarpCompress: adaptive, parallel (de)compression in pure Python."
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    pc = sub.add_parser("compress", help="Compress a file")
    pc.add_argument("input_file")
    pc.add_argument("output_file")
    pc.add_argument("--level", default="throughput",
                    help="throughput(snappy-biased) | lz4 | zstd | ratio(brotli)")
    pc.add_argument("--chunk-size", type=int, default=None,
                    help=f"Chunk size in bytes (omit to auto-tune; fallback: {_fmt_bytes(CHUNK_SIZE_DEFAULT)})")
    pc.add_argument("--workers", type=int, default=None,
                    help="Worker threads (default: CPU count)")
    pc.add_argument("--zstd-hybrid", default="auto", choices=["auto","on","off"],
                    help="Auto-pick between Python chunk threads vs Zstd internal threads (default: auto)")
    pc.add_argument("--no-io-hints", action="store_true",
                    help="Disable madvise/posix_fadvise I/O hints")
    pc.add_argument("--verbose", action="store_true")

    pd = sub.add_parser("decompress", help="Decompress a .warp file (v1 or v2)")
    pd.add_argument("input_file")
    pd.add_argument("output_file")
    pd.add_argument("--workers", type=int, default=None,
                    help="Worker threads (default: CPU count)")
    pd.add_argument("--decomp-mode", default="auto", choices=["auto","seq","par"],
                    help="auto (default), seq=parallel decode + sequential writes, par=parallel mmap writes")
    pd.add_argument("--no-sparse", action="store_true",
                    help="Disable sparse output optimization for ZERO chunks")
    pd.add_argument("--no-io-hints", action="store_true",
                    help="Disable madvise/posix_fadvise I/O hints")
    pd.add_argument("--verbose", action="store_true")
    return p

def main() -> int:
    args = _build_parser().parse_args()
    try:
        if args.cmd == "compress":
            if args.verbose:
                print(f"Base hint: {detect_algo_name(args.level)} (final codec is per-chunk adaptive)")
            compress_file(
                args.input_file, args.output_file,
                level=args.level,
                chunk_size=args.chunk_size,
                workers=args.workers,
                verbose=args.verbose,
                io_hints=(not args.no_io_hints),
                zstd_hybrid=args.zstd_hybrid,
            )
        elif args.cmd == "decompress":
            decompress_file(
                args.input_file, args.output_file,
                workers=args.workers,
                decomp_mode=args.decomp_mode,
                verbose=args.verbose,
                sparse_output=(not args.no_sparse),
                io_hints=(not args.no_io_hints),
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
