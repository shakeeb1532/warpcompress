from __future__ import annotations
import argparse
from .core import compress_file, decompress_file, detect_algo_name  # detect is imported for CLI help/use
from . import __version__

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="warp-compress", description="Warp container CLI")
    p.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    sp = p.add_subparsers(dest="cmd", required=True)

    c = sp.add_parser("compress", help="Compress a file into .warp")
    c.add_argument("input_file")
    c.add_argument("output_file")
    c.add_argument("--level", choices=["throughput", "zstd", "lz4", "ratio"], default="throughput")
    c.add_argument("--workers", type=int, default=None)
    c.add_argument("--chunk", type=int, default=None, help="Chunk size in bytes (min/max enforced)")
    c.add_argument("--zstd-hybrid", choices=["auto", "on", "off"], default="auto")
    c.add_argument("--verbose", action="store_true")

    d = sp.add_parser("decompress", help="Decompress a .warp file to raw")
    d.add_argument("input_file")
    d.add_argument("output_file")
    d.add_argument("--workers", type=int, default=None)
    d.add_argument("--verbose", action="store_true")
    return p

def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.cmd == "compress":
        compress_file(
            args.input_file,
            args.output_file,
            level=args.level,
            workers=args.workers,
            chunk=args.chunk,
            zstd_hybrid=args.zstd_hybrid,
            verbose=args.verbose,
        )
        return 0
    if args.cmd == "decompress":
        decompress_file(args.input_file, args.output_file, workers=args.workers, verbose=args.verbose)
        return 0
    return 2

if __name__ == "__main__":
    raise SystemExit(main())

