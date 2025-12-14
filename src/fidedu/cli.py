from __future__ import annotations

import argparse
import os
from pathlib import Path

from .core import find_duplicates, human_bytes, perform_hardlinking, plan_stats


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description=(
            "Deduplicate files across one or more folders by replacing duplicates with hardlinks to a"
            " chosen canonical original (in place). Duplicates are detected via a BLAKE2b hash over file"
            " content and attributes (mode, uid, gid, size, mtime_sec)."
        )
    )
    ap.add_argument("folders", nargs="+", type=Path, help="One or more folders to scan recursively.")
    ap.add_argument(
        "-c",
        "--compress",
        action="store_true",
        help="Apply changes (relink duplicates to canonical originals). Default: dry-run.",
    )
    ap.add_argument("-v", "--verbose", action="store_true", help="Verbose output.")
    ap.add_argument(
        "-w",
        "--workers",
        type=int,
        default=os.cpu_count() or 4,
        help="Number of parallel worker processes for hashing (default: CPU count).",
    )
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    roots = [p.resolve() for p in args.folders]

    if args.verbose:
        print(f"[cfg] roots={', '.join(map(str, roots))}")
        print(f"[cfg] workers={args.workers}")
        print(f"[cfg] mode={'EXECUTE' if args.compress else 'DRY-RUN'}")

    dup_map_dev, size_map, finfo = find_duplicates(roots=roots, workers=args.workers, verbose=args.verbose)

    savings, relinks, files = plan_stats(dup_map_dev, size_map, finfo)

    if not dup_map_dev:
        print("No duplicate files found.")
        return

    # Count duplicate sets as sum of per-device groups
    dup_sets = sum(len(g) for g in dup_map_dev.values())

    print(f"Duplicate sets found: {dup_sets}")
    print(f"Files involved:       {files}")
    print(f"Planned relinks:      {relinks}")
    print(f"Estimated savings:    {human_bytes(savings)} ({savings} bytes)")

    if args.verbose:
        print("\nDetails per duplicate set:")
        for digest, dev_groups in dup_map_dev.items():
            size = size_map[digest]
            for dev, paths in dev_groups.items():
                print(f"  digest={digest[:16]}... dev={dev} size={size} bytes count={len(paths)}")
                for p in paths:
                    print(f"    - {p}")

    if args.compress:
        print("\n[execute] Relinking duplicates to canonical originals...")
        perform_hardlinking(dup_map_dev, size_map, finfo, verbose=args.verbose)
        print("[done] Hardlinking complete.")
    else:
        print("\n[dry-run] Use --compress to apply these changes.")


if __name__ == "__main__":
    main()
