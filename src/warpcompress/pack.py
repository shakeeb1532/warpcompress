# src/warpcompress/pack.py
# MIT-style helper CLI to add folder (multi-file) support on top of WarpCompress.
import argparse
import fnmatch
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path
from typing import Iterable, List, Optional


def _should_include(path: Path, include: List[str], exclude: List[str], root: Path) -> bool:
    rel = str(path.relative_to(root))
    if include:
        ok = any(fnmatch.fnmatch(rel, pat) for pat in include)
        if not ok:
            return False
    if exclude:
        if any(fnmatch.fnmatch(rel, pat) for pat in exclude):
            return False
    return True


def _build_tar(src: Path, out_tar: Path, include: List[str], exclude: List[str],
               dereference: bool, root_name: Optional[str]) -> None:
    """
    Create an uncompressed tar of `src` at `out_tar`.
    """
    src = src.resolve()
    if root_name is None:
        root_name = src.name

    def _filter(ti: tarfile.TarInfo) -> tarfile.TarInfo:
        # Keep perms & mtimes as-is; you can normalize here if needed.
        return ti

    mode = "w"  # uncompressed tar
    with tarfile.open(out_tar, mode) as tf:
        # Walk manually to apply include/exclude against relative paths
        for p in src.rglob("*"):
            if not _should_include(p, include, exclude, src):
                continue
            arcname = Path(root_name) / p.relative_to(src)
            tf.add(
                p,
                arcname=str(arcname),
                recursive=False,
                filter=_filter,
                dereference=dereference,
            )
        # Also add the root itself (empty dir entry) if not excluded
        if _should_include(src, include, exclude, src):
            ti = tf.gettarinfo(str(src), arcname=root_name)
            if ti is not None and ti.isdir():
                tf.addfile(ti)


def _is_tar(path: Path) -> bool:
    try:
        return tarfile.is_tarfile(path)
    except Exception:
        return False


def _run_warpcompress(args: List[str]) -> None:
    """
    Call the installed warp-compress CLI. We rely on the documented CLI:
    warp-compress compress <in> <out>, warp-compress decompress <in> <out>
    """
    proc = subprocess.run(["warp-compress", *args], check=False)
    if proc.returncode != 0:
        raise SystemExit(proc.returncode)


def _pack(args: argparse.Namespace) -> None:
    src = Path(args.input_path)
    out_warp = Path(args.output_warp)
    out_warp.parent.mkdir(parents=True, exist_ok=True)

    if src.is_file():
        # Single file → compress directly (best speed/size, no tar needed)
        wc_args = [
            "compress", str(src), str(out_warp),
            "--level", args.level,
        ]
        if args.threads is not None:
            wc_args += ["--threads", str(args.threads)]
        if args.chunk_size is not None:
            wc_args += ["--chunk-size", str(args.chunk_size)]
        if args.zstd_level is not None:
            wc_args += ["--zstd-level", str(args.zstd_level)]
        if args.zstd_threads is not None:
            wc_args += ["--zstd-threads", str(args.zstd_threads)]
        if args.brotli_quality is not None:
            wc_args += ["--brotli-quality", str(args.brotli_quality)]
        if args.checksum is not None:
            wc_args += ["--checksum", args.checksum]
        if args.verbose:
            wc_args += ["--verbose"]
        _run_warpcompress(wc_args)
        return

    if not src.is_dir():
        raise SystemExit(f"Input does not exist or is not a file/dir: {src}")

    # Directory → create tar, then compress tar
    with tempfile.TemporaryDirectory(prefix="warp-pack-") as td:
        tar_path = Path(td) / (src.name + ".tar")
        _build_tar(
            src=src,
            out_tar=tar_path,
            include=args.include or [],
            exclude=args.exclude or [],
            dereference=args.dereference,
            root_name=args.root_name,
        )

        wc_args = [
            "compress", str(tar_path), str(out_warp),
            "--level", args.level,
        ]
        if args.threads is not None:
            wc_args += ["--threads", str(args.threads)]
        if args.chunk_size is not None:
            wc_args += ["--chunk-size", str(args.chunk_size)]
        if args.zstd_level is not None:
            wc_args += ["--zstd-level", str(args.zstd_level)]
        if args.zstd_threads is not None:
            wc_args += ["--zstd-threads", str(args.zstd_threads)]
        if args.brotli_quality is not None:
            wc_args += ["--brotli-quality", str(args.brotli_quality)]
        if args.checksum is not None:
            wc_args += ["--checksum", args.checksum]
        if args.verbose:
            wc_args += ["--verbose"]
        _run_warpcompress(wc_args)


def _unpack(args: argparse.Namespace) -> None:
    in_warp = Path(args.input_warp)
    out_target = Path(args.output_path)

    # Always decompress to a temp file first; then decide whether to extract
    with tempfile.TemporaryDirectory(prefix="warp-unpack-") as td:
        tmp_out = Path(td) / "payload.bin"
        wc_args = ["decompress", str(in_warp), str(tmp_out)]
        if args.threads is not None:
            wc_args += ["--threads", str(args.threads)]
        if args.chunk_size is not None:
            wc_args += ["--chunk-size", str(args.chunk_size)]
        if args.zstd_level is not None:
            wc_args += ["--zstd-level", str(args.zstd_level)]
        if args.zstd_threads is not None:
            wc_args += ["--zstd-threads", str(args.zstd_threads)]
        if args.brotli_quality is not None:
            wc_args += ["--brotli-quality", str(args.brotli_quality)]
        if args.verbose:
            wc_args += ["--verbose"]
        _run_warpcompress(wc_args)

        # If payload is a tar (or --extract was forced), extract to folder
        should_extract = args.extract or _is_tar(tmp_out)
        if should_extract:
            out_dir = out_target
            out_dir.mkdir(parents=True, exist_ok=True)
            with tarfile.open(tmp_out, "r") as tf:
                # Security note: tarfile.extractall can be vulnerable to path traversal.
                # Here we enforce safe extraction.
                def is_within_directory(directory, target):
                    abs_directory = os.path.abspath(directory)
                    abs_target = os.path.abspath(target)
                    return os.path.commonprefix([abs_directory, abs_target]) == abs_directory

                for member in tf.getmembers():
                    target_path = out_dir / member.name
                    if not is_within_directory(out_dir, target_path):
                        raise SystemExit(f"Unsafe path in tar: {member.name}")
                tf.extractall(out_dir)
        else:
            # Single-file payload → move into place
            out_target.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(tmp_out), str(out_target))


def main(argv: Optional[List[str]] = None) -> None:
    p = argparse.ArgumentParser(
        prog="warp-pack",
        description="Folder-ready wrapper for WarpCompress: pack directories to tar + warp, and unpack."
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    common_comp = argparse.ArgumentParser(add_help=False)
    common_comp.add_argument("--threads", type=int, default=None)
    common_comp.add_argument("--chunk-size", type=int, default=None)
    common_comp.add_argument("--zstd-level", type=int, default=None)
    common_comp.add_argument("--zstd-threads", type=int, default=None)
    common_comp.add_argument("--brotli-quality", type=int, default=None)
    common_comp.add_argument("--checksum", choices=["none","blake3","xxh64","blake2b"], default=None)
    common_comp.add_argument("--level", choices=["auto","fastest","max","throughput"], default="throughput")
    common_comp.add_argument("--verbose", action="store_true")

    # pack
    sp = sub.add_parser("pack", parents=[common_comp], help="Compress a file or directory to .warp")
    sp.add_argument("input_path", help="File or directory to compress")
    sp.add_argument("output_warp", help="Output .warp file")
    sp.add_argument("--include", action="append", default=[], help="Glob(s) to include (evaluated relative to dir)")
    sp.add_argument("--exclude", action="append", default=[], help="Glob(s) to exclude")
    sp.add_argument("--dereference", action="store_true", help="Follow symlinks; default preserves links")
    sp.add_argument("--root-name", default=None, help="Override top-level folder name inside the archive")
    sp.set_defaults(func=_pack)

    # unpack
    su = sub.add_parser("unpack", parents=[common_comp], help="Decompress .warp; auto-extract if it contains a tar")
    su.add_argument("input_warp", help="Input .warp file")
    su.add_argument("output_path", help="Folder (for extracted dirs) OR file path (for single-file payloads)")
    su.add_argument("--extract", action="store_true", help="Force extract to folder (even if payload looks like a file)")
    su.set_defaults(func=_unpack)

    args = p.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
