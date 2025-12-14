from __future__ import annotations

import os
import re
import stat
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path


class FileDedupeTests(unittest.TestCase):
    def setUp(self) -> None:
        # Isolated workspace with two roots to verify cross-folder dedupe
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.dir1 = self.root / "dir1"
        self.dir2 = self.root / "dir2"
        self.dir1.mkdir()
        self.dir2.mkdir()

        # --- Test data ---
        # Two true duplicates across dir1 and dir2 (same content + aligned attrs)
        self.payload = b"HELLO-WORLD" * 1234  # ~13.6 KB
        self.a = self.dir1 / "a.txt"
        self.d = self.dir2 / "d.bin"
        self.a.write_bytes(self.payload)
        self.d.write_bytes(self.payload)

        # Align attributes (mtime + mode)
        st_a = self.a.stat()
        mt = int(st_a.st_mtime)
        os.utime(self.d, (mt, mt))
        self.d.chmod(st_a.st_mode)

        # Different content file (should not dedupe)
        (self.dir1 / "c.txt").write_text("something else entirely")

        # Same content BUT different attributes → should NOT dedupe
        self.e = self.dir2 / "e.txt"
        self.e.write_bytes(self.payload)
        now = time.time()
        os.utime(self.e, (now - 3600, now - 3600))  # different mtime
        self.e.chmod(stat.S_IRUSR | stat.S_IWUSR)  # 0o600

        self.dup_size = self.a.stat().st_size
        self.expected_savings = self.dup_size  # a.txt + d.bin → save one file's size

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def run_cli(self, *args: str, check: bool = True, verbose: bool = False) -> subprocess.CompletedProcess[str]:
        cmd = [
            sys.executable,
            "-m",
            "fidedu.cli",
            str(self.dir1),
            str(self.dir2),
            *args,
        ]
        proc = subprocess.run(cmd, text=True, capture_output=True)
        if verbose:
            print("STDOUT:\n", proc.stdout)
            print("STDERR:\n", proc.stderr)
        if check and proc.returncode != 0:
            self.fail(
                f"Command failed ({proc.returncode}):\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
            )
        return proc

    def parse_savings_bytes(self, stdout: str) -> int:
        m = re.search(r"Estimated savings:\s.*\((\d+)\s+bytes\)", stdout)
        self.assertIsNotNone(m, f"Could not parse savings from output:\n{stdout}")
        return int(m.group(1))

    def test_dry_run_reports_expected_savings(self) -> None:
        """Dry run should report exactly one file-size saved (two true-duplicates across dirs)."""
        proc = self.run_cli()
        if "No duplicate files found." in proc.stdout:
            self.fail(f"Expected duplicates but found none.\n{proc.stdout}")
        savings = self.parse_savings_bytes(proc.stdout)
        self.assertEqual(savings, self.expected_savings, f"Unexpected savings.\n{proc.stdout}")

    def test_compress_creates_hardlinks_only_for_true_duplicates(self) -> None:
        """After --compress, a.txt and d.bin are hardlinked; e.txt remains separate."""
        self.run_cli("--compress", "-v")
        # a and d should now share the same inode
        ino_a = os.stat(self.a).st_ino
        ino_d = os.stat(self.d).st_ino
        self.assertEqual(ino_a, ino_d, "a.txt and d.bin should be hardlinked")

        # e.txt must not be linked to a/d (different attributes)
        ino_e = os.stat(self.e).st_ino
        self.assertNotEqual(
            ino_e, ino_a, "e.txt should NOT be deduplicated due to attribute differences"
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
