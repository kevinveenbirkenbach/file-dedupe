#!/usr/bin/env python3
"""
file-dedupe — In-place hardlink deduplication across one or more folders.

- Scans the given folders recursively.
- Finds duplicate files by hashing *content + attributes* (mode, uid, gid, size, mtime_sec).
- For each duplicate set (per filesystem/volume), chooses one canonical original
  and replaces the others with hardlinks to it (only when --compress is given).
- Parallel hashing via ProcessPoolExecutor.

Usage (dry-run):
    fidedu DIR [DIR ...]
    python3 main.py DIR1 DIR2

Apply changes:
    fidedu DIR1 DIR2 --compress -v
"""

import argparse
import concurrent.futures as cf
import hashlib
import os
import struct
from pathlib import Path
from typing import Dict, List, Tuple, Iterator, Optional, Iterable, DefaultDict
from collections import defaultdict

BUF_SIZE = 1024 * 1024  # 1 MiB


# ----------------------- Helpers -----------------------

def iter_files(roots: Iterable[Path]) -> Iterator[Path]:
    """Yield regular files under the provided roots; skip symlinks."""
    seen_dirs = set()
    for root in roots:
        root = root.resolve()
        if not root.exists() or not root.is_dir():
            continue
        for dirpath, dirnames, filenames in os.walk(root):
            d = Path(dirpath)
            # prevent cycles if the same root is passed multiple times
            rp = d.resolve()
            if rp in seen_dirs:
                dirnames[:] = []
                continue
            seen_dirs.add(rp)

            for fname in filenames:
                p = d / fname
                try:
                    st_l = p.lstat()
                except FileNotFoundError:
                    continue
                if not os.path.isfile(p) or os.path.islink(p):
                    continue
                yield p


def human_bytes(n: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    i = 0
    v = float(n)
    while v >= 1024 and i < len(units) - 1:
        v /= 1024.0
        i += 1
    return f"{v:.2f} {units[i]}"


# ----------------------- Hashing -----------------------

def hash_file_with_attrs(path: Path, bufsize: int = BUF_SIZE) -> Optional[Tuple[str, int]]:
    """
    Return (hex_digest, size) or None if unreadable.
    Hash covers:
      - st_mode (lower 16 bits), st_uid, st_gid, st_size, st_mtime (seconds precision)
      - file content
    """
    try:
        st = path.stat()
        h = hashlib.blake2b(digest_size=32)

        # Use SECOND precision mtime for robustness across filesystems/tools.
        mtime_sec = int(st.st_mtime)

        meta = struct.pack(
            "<IIQQQ",  # mode, uid, gid, size, mtime_sec
            st.st_mode & 0xFFFF,
            st.st_uid & 0xFFFFFFFF,
            st.st_gid & 0xFFFFFFFF,
            st.st_size & 0xFFFFFFFFFFFFFFFF,
            mtime_sec & 0xFFFFFFFFFFFFFFFF,
        )
        h.update(meta)

        with path.open("rb") as f:
            while True:
                chunk = f.read(bufsize)
                if not chunk:
                    break
                h.update(chunk)

        return h.hexdigest(), st.st_size
    except (PermissionError, FileNotFoundError, OSError):
        return None


# ----------------------- Core logic -----------------------

def collect_by_size(roots: List[Path], verbose: bool) -> Tuple[Dict[int, List[Path]], Dict[Path, Tuple[int, int, int]]]:
    """
    Return:
      by_size: size -> [paths]
      finfo:   path -> (size, dev, ino)
    """
    by_size: DefaultDict[int, List[Path]] = defaultdict(list)
    finfo: Dict[Path, Tuple[int, int, int]] = {}
    total = 0
    for p in iter_files(roots):
        try:
            st = p.stat()
        except FileNotFoundError:
            continue
        if not stat_is_regular_file(st.st_mode):
            continue
        by_size[st.st_size].append(p)
        finfo[p] = (st.st_size, st.st_dev, st.st_ino)
        total += 1
    if verbose:
        print(f"[scan] Total files considered: {total}")
    # keep only sizes with >1 files
    return {s: ps for s, ps in by_size.items() if len(ps) > 1}, finfo


def stat_is_regular_file(mode: int) -> bool:
    return (mode & 0o170000) == 0o100000


def compute_hashes_parallel(paths: List[Path], workers: int, verbose: bool) -> Dict[str, List[Path]]:
    """For given same-size candidates, return digest -> [paths]."""
    result: DefaultDict[str, List[Path]] = defaultdict(list)
    with cf.ProcessPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(hash_file_with_attrs, p): p for p in paths}
        for fut in cf.as_completed(futures):
            p = futures[fut]
            ret = fut.result()
            if ret is None:
                if verbose:
                    print(f"[warn] Skipping unreadable file: {p}")
                continue
            digest, _ = ret
            result[digest].append(p)
    return dict(result)


def find_duplicates(
    roots: List[Path],
    workers: int,
    verbose: bool,
) -> Tuple[Dict[str, Dict[int, List[Path]]], Dict[str, int], Dict[Path, Tuple[int, int, int]]]:
    """
    Find duplicate sets across all roots.

    Returns:
      dup_map_dev: digest -> { st_dev -> [paths ...] }  (only lists with >=2 are meaningful)
      size_map:    digest -> file_size
      finfo:       path  -> (size, dev, ino)
    """
    by_size, finfo = collect_by_size(roots, verbose)
    dup_map_dev: Dict[str, Dict[int, List[Path]]] = {}
    size_map: Dict[str, int] = {}

    for size, paths in by_size.items():
        if verbose:
            print(f"[group] Hashing {len(paths)} candidates with size={size} bytes")
        by_digest = compute_hashes_parallel(paths, workers, verbose)
        for digest, same in by_digest.items():
            if len(same) >= 2:
                # partition by device (hardlinks must stay on same filesystem)
                by_dev: DefaultDict[int, List[Path]] = defaultdict(list)
                for p in same:
                    _sz, dev, _ino = finfo[p]
                    by_dev[dev].append(p)
                # keep only device-groups with >=2 files
                filtered = {dev: ps for dev, ps in by_dev.items() if len(ps) >= 2}
                if filtered:
                    dup_map_dev[digest] = filtered
                    size_map[digest] = size

    return dup_map_dev, size_map, finfo


def plan_stats(
    dup_map_dev: Dict[str, Dict[int, List[Path]]],
    size_map: Dict[str, int],
    finfo: Dict[Path, Tuple[int, int, int]],
) -> Tuple[int, int, int]:
    """
    Compute:
      total_savings_bytes
      total_relinks_needed
      total_files_involved
    Uses unique inode counts so already-hardlinked files don't inflate savings.
    """
    savings = 0
    relinks = 0
    files = 0

    for digest, dev_groups in dup_map_dev.items():
        size = size_map[digest]
        for dev, paths in dev_groups.items():
            files += len(paths)
            # Count unique inodes
            inode_groups: DefaultDict[int, List[Path]] = defaultdict(list)
            for p in paths:
                _sz, _dev, ino = finfo[p]
                inode_groups[ino].append(p)
            unique_inodes = len(inode_groups)
            if unique_inodes <= 1:
                continue
            savings += (unique_inodes - 1) * size
            # choose canonical as the inode group with the most paths (minimize relinks)
            canonical_inode, canonical_paths = max(inode_groups.items(), key=lambda kv: len(kv[1]))
            relinks += (len(paths) - len(canonical_paths))

    return savings, relinks, files


def perform_hardlinking(
    dup_map_dev: Dict[str, Dict[int, List[Path]]],
    size_map: Dict[str, int],
    finfo: Dict[Path, Tuple[int, int, int]],
    verbose: bool,
) -> None:
    """
    For each digest and device group:
      - pick the inode group with most members as canonical
      - relink all other files to the first path of the canonical group
    """
    for digest, dev_groups in dup_map_dev.items():
        for dev, paths in dev_groups.items():
            if len(paths) < 2:
                continue

            # Build inode groups
            inode_groups: DefaultDict[int, List[Path]] = defaultdict(list)
            for p in paths:
                _sz, _dev, ino = finfo[p]
                inode_groups[ino].append(p)

            if len(inode_groups) <= 1:
                continue  # already all hardlinked

            # canonical = inode group with most paths (fewer relinks)
            canonical_inode, canonical_paths = max(inode_groups.items(), key=lambda kv: len(kv[1]))
            canonical_target = canonical_paths[0]  # any path in the canonical inode

            if verbose:
                print(f"[canon] digest={digest[:16]}... dev={dev} keep={canonical_target}")

            # For every other inode group, relink each path to canonical_target
            for ino, group_paths in inode_groups.items():
                if ino == canonical_inode:
                    continue
                for p in group_paths:
                    try:
                        # remove and create hardlink to canonical
                        if verbose:
                            print(f"[repl] {p} -> hardlink to {canonical_target}")
                        os.remove(p)
                        os.link(canonical_target, p)
                    except FileNotFoundError:
                        # concurrently removed? skip
                        continue
                    except PermissionError as e:
                        if verbose:
                            print(f"[warn] Permission error relinking {p}: {e}")
                    except OSError as e:
                        if verbose:
                            print(f"[warn] OSError relinking {p}: {e}")


# ----------------------- CLI -----------------------

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description=(
            "Deduplicate files across one or more folders by replacing duplicates with hardlinks to a"
            " chosen canonical original (in place). Duplicates are detected via a BLAKE2b hash over file"
            " content and attributes (mode, uid, gid, size, mtime_sec)."
        )
    )
    ap.add_argument("folders", nargs="+", type=Path, help="One or more folders to scan recursively.")
    ap.add_argument("-c", "--compress", action="store_true",
                    help="Apply changes (relink duplicates to canonical originals). Default: dry-run.")
    ap.add_argument("-v", "--verbose", action="store_true", help="Verbose output.")
    ap.add_argument("-w", "--workers", type=int, default=os.cpu_count() or 4,
                    help="Number of parallel worker processes for hashing (default: CPU count).")
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    roots = [p.resolve() for p in args.folders]

    if args.verbose:
        print(f"[cfg] roots={', '.join(map(str, roots))}")
        print(f"[cfg] workers={args.workers}")
        print(f"[cfg] mode={'EXECUTE' if args.compress else 'DRY-RUN'}")

    dup_map_dev, size_map, finfo = find_duplicates(
        roots=roots, workers=args.workers, verbose=args.verbose
    )

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
