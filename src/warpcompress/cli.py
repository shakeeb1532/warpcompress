from __future__ import annotations
import argparse, os, sys, tarfile, tempfile, pathlib
from .core import compress_file, decompress_file
from .stream import compress_stream, decompress_stream
from .dictutil import train_zstd_dict
from . import __version__

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="warp-compress", description="Warp container CLI")
    p.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    sp = p.add_subparsers(dest="cmd", required=True)

    c = sp.add_parser("compress", help="Compress a file or directory into .warp")
    c.add_argument("input_path")
    c.add_argument("output_file")
    c.add_argument("--level", choices=["throughput", "zstd", "lz4", "ratio"], default="throughput")
    c.add_argument("--workers", type=int, default=None)
    c.add_argument("--chunk", type=int, default=None, help="Chunk size in bytes (min/max enforced)")
    c.add_argument("--checksum", choices=["none","xxh64","blake3","sha256"], default="none")
    c.add_argument("--verify", action="store_true", help="Compute & store checksum; verify during decompress (if used with --checksum)")
    c.add_argument("--index", action="store_true", help="Write seek index trailer for fast open/random access")
    c.add_argument("--dict", dest="dict_path", default=None, help="Use a zstd dictionary file (zstd modes)")
    c.add_argument("--train-dict", dest="train_dir", default=None, help="Train a zstd dictionary from this directory, then use it")

    d = sp.add_parser("decompress", help="Decompress a .warp file to raw")
    d.add_argument("input_file")
    d.add_argument("output_file")
    d.add_argument("--workers", type=int, default=None)
    d.add_argument("--verify", action="store_true", help="Verify checksum if present")
    d.add_argument("--verbose", action="store_true")

    return p

def _read_dict(path: str | None) -> bytes | None:
    if not path:
        return None
    p = pathlib.Path(path)
    return p.read_bytes()

def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    if args.cmd == "compress":
        src = args.input_path
        dst = args.output_file
        dict_bytes = None

        # optional: train dict
        if args.train_dir:
            # decide dict file path (default next to output)
            dict_out = args.dict_path or (dst + ".dict")
            data = train_zstd_dict(args.train_dir)
            pathlib.Path(dict_out).write_bytes(data)
            dict_bytes = data
        else:
            dict_bytes = _read_dict(args.dict_path)

        if os.path.isdir(src):
            # tar the directory to a temp file and stream-compress (safe; no temp in RAM)
            with tempfile.NamedTemporaryFile(prefix="warp-arch-", delete=False) as tmp_tar:
                tar_name = tmp_tar.name
            try:
                with tarfile.open(tar_name, mode="w") as tf:
                    tf.add(src, arcname=os.path.basename(src))
                with open(tar_name, "rb") as s, open(dst, "wb") as d:
                    compress_stream(s, d, level=args.level, workers=args.workers, chunk=args.chunk,
                                    verify=args.verify, dict_bytes=dict_bytes,
                                    checksum=args.checksum, make_index=args.index)
            finally:
                try: os.unlink(tar_name)
                except Exception: pass
        else:
            compress_file(src, dst, level=args.level, workers=args.workers, chunk=args.chunk,
                          verbose=True, checksum=args.checksum if args.verify else "none",
                          make_index=args.index, zstd_dict=dict_bytes)
        return 0

    if args.cmd == "decompress":
        decompress_file(args.input_file, args.output_file, workers=args.workers,
                        verbose=args.verbose, verify=args.verify)
        return 0

    return 2

if __name__ == "__main__":
    raise SystemExit(main())
