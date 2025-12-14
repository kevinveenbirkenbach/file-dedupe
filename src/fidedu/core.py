# ./src/fidedu/core.py
from __future__ import annotations

import concurrent.futures as cf
import hashlib
import os
import struct
from collections import defaultdict
from collections.abc import Iterable, Iterator
from pathlib import Path

BUF_SIZE = 1024 * 1024  # 1 MiB


def iter_files(roots: Iterable[Path]) -> Iterator[Path]:
    """Yield regular files under the provided roots; skip symlinks."""
    seen_dirs: set[Path] = set()
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
                    p.lstat()
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


def hash_file_with_attrs(path: Path, bufsize: int = BUF_SIZE) -> tuple[str, int] | None:
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


def stat_is_regular_file(mode: int) -> bool:
    return (mode & 0o170000) == 0o100000


def collect_by_size(
    roots: list[Path],
    verbose: bool,
) -> tuple[dict[int, list[Path]], dict[Path, tuple[int, int, int]]]:
    """
    Return:
      by_size: size -> [paths]
      finfo:   path -> (size, dev, ino)
    """
    by_size: defaultdict[int, list[Path]] = defaultdict(list)
    finfo: dict[Path, tuple[int, int, int]] = {}
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


def compute_hashes_parallel(paths: list[Path], workers: int, verbose: bool) -> dict[str, list[Path]]:
    """For given same-size candidates, return digest -> [paths]."""
    result: defaultdict[str, list[Path]] = defaultdict(list)
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
    roots: list[Path],
    workers: int,
    verbose: bool,
) -> tuple[dict[str, dict[int, list[Path]]], dict[str, int], dict[Path, tuple[int, int, int]]]:
    """
    Find duplicate sets across all roots.

    Returns:
      dup_map_dev: digest -> { st_dev -> [paths ...] }  (only lists with >=2 are meaningful)
      size_map:    digest -> file_size
      finfo:       path  -> (size, dev, ino)
    """
    by_size, finfo = collect_by_size(roots, verbose)
    dup_map_dev: dict[str, dict[int, list[Path]]] = {}
    size_map: dict[str, int] = {}

    for size, paths in by_size.items():
        if verbose:
            print(f"[group] Hashing {len(paths)} candidates with size={size} bytes")
        by_digest = compute_hashes_parallel(paths, workers, verbose)
        for digest, same in by_digest.items():
            if len(same) >= 2:
                # partition by device (hardlinks must stay on same filesystem)
                by_dev: defaultdict[int, list[Path]] = defaultdict(list)
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
    dup_map_dev: dict[str, dict[int, list[Path]]],
    size_map: dict[str, int],
    finfo: dict[Path, tuple[int, int, int]],
) -> tuple[int, int, int]:
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
        for _dev, paths in dev_groups.items():
            files += len(paths)
            # Count unique inodes
            inode_groups: defaultdict[int, list[Path]] = defaultdict(list)
            for p in paths:
                _sz, _dev2, ino = finfo[p]
                inode_groups[ino].append(p)

            unique_inodes = len(inode_groups)
            if unique_inodes <= 1:
                continue

            savings += (unique_inodes - 1) * size

            # choose canonical as the inode group with the most paths (minimize relinks)
            _canonical_inode, canonical_paths = max(inode_groups.items(), key=lambda kv: len(kv[1]))
            relinks += len(paths) - len(canonical_paths)

    return savings, relinks, files


def perform_hardlinking(
    dup_map_dev: dict[str, dict[int, list[Path]]],
    size_map: dict[str, int],
    finfo: dict[Path, tuple[int, int, int]],
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
            inode_groups: defaultdict[int, list[Path]] = defaultdict(list)
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
