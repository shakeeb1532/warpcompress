from .core import CompressionManager, _build_argparser, _algo_name_to_id
import time

def main() -> None:
    parser = _build_argparser()
    args = parser.parse_args()

    checksum_algo = _algo_name_to_id(getattr(args, "checksum", "none")) if args.mode == "compress" else 0

    cm = CompressionManager(
        threads=args.threads,
        chunk_size=args.chunk_size,
        zstd_level=args.zstd_level,
        zstd_threads=args.zstd_threads,
        brotli_quality=args.brotli_quality,
        verbose=args.verbose,
        checksum_algo=checksum_algo,
    )

    t0 = time.perf_counter()
    if args.mode == "compress":
        print(f"Compressing {args.input_file} \u2192 {args.output_file} (level={args.level}) …")
        cm.compress_file(args.input_file, args.output_file, level=args.level)
    else:
        print(f"Decompressing {args.input_file} \u2192 {args.output_file} …")
        cm.decompress_file(args.input_file, args.output_file)
    dt = time.perf_counter() - t0
    print(f"Done in {dt:.3f}s.")



