# file-dedupe

[![PyPI](https://img.shields.io/pypi/v/fidedu.svg)](https://pypi.org/project/fidedu/)
[![Python Versions](https://img.shields.io/pypi/pyversions/fidedu.svg)](https://pypi.org/project/fidedu/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![CI](https://github.com/kevinveenbirkenbach/file-dedupe/actions/workflows/ci-and-mark-stable.yml/badge.svg)](https://github.com/kevinveenbirkenbach/file-dedupe/actions/workflows/ci-and-mark-stable.yml)

[![GitHub Sponsors](https://img.shields.io/badge/Sponsor-GitHub%20Sponsors-blue?logo=github)](https://github.com/sponsors/kevinveenbirkenbach)
[![Patreon](https://img.shields.io/badge/Support-Patreon-orange?logo=patreon)](https://www.patreon.com/c/kevinveenbirkenbach)
[![Buy Me a Coffee](https://img.shields.io/badge/Buy%20me%20a%20Coffee-Funding-yellow?logo=buymeacoffee)](https://buymeacoffee.com/kevinveenbirkenbach)
[![PayPal](https://img.shields.io/badge/Donate-PayPal-blue?logo=paypal)](https://s.veen.world/paypaldonate)

**Find and replace duplicate files across one or more folders with hardlinks.**  
Uses fast parallel hashing with file attribute awareness to safely deduplicate files *in-place*.  
Part of the [Infinito.Nexus](https://github.com/kevinveenbirkenbach/infinito.nexus) ecosystem.

---

## 📦 Installation

The tool is published on **PyPI** and can be installed using pip:

```bash
pip install fidedu
````

This installs the global command:

```bash
fidedu
```

You can also invoke it explicitly as a module:

```bash
python -m fidedu --help
```

---

## 🚀 Usage

### Basic Examples

Analyze duplicates across one or more directories (dry-run mode):

```bash
fidedu ~/Documents ~/Downloads ~/Pictures
```

Replace duplicates with **hardlinks** to one canonical copy (in-place deduplication):

```bash
fidedu ~/Documents ~/Downloads ~/Pictures --compress
```

Verbose output (show exactly what happens):

```bash
fidedu ~/Documents ~/Downloads ~/Pictures --compress -v
```

Equivalent module invocation:

```bash
python -m fidedu ~/Documents ~/Downloads --compress -v
```

---

## ⚙️ Behavior

1. Scans all provided folders recursively.
2. Groups files with identical size.
3. For each candidate group, computes a **BLAKE2b hash** including file attributes
   (mode, UID, GID, size, and modification time).
4. Files with matching hashes are treated as **true duplicates**.
5. One canonical copy is kept; all others are **replaced by hardlinks** pointing to it.
6. When `--compress` is *not* used, the tool reports potential savings only
   (no changes are made).

---

### Command Line Options

| Option              | Description                                                                       |
| ------------------- | --------------------------------------------------------------------------------- |
| `--compress`        | Apply deduplication (replace duplicates with hardlinks). Default is dry-run mode. |
| `-v`, `--verbose`   | Verbose output (show detailed linking actions).                                   |
| `-w`, `--workers N` | Number of parallel hashing processes (default: CPU count).                        |

---

### Example Output

```text
Duplicate sets found: 12
Files involved:       57
Planned relinks:      45
Estimated savings:    1.2 GB (1,234,567,890 bytes)

[dry-run] Use --compress to apply these changes.
```

---

## 🧪 Testing

Run all tests locally:

```bash
make test
```

Lint the codebase:

```bash
make lint
```

---

## 📄 License

This project is licensed under the **MIT License**.
See the [LICENSE](LICENSE) file for details.

---

## 👤 Author

**Kevin Veen-Birkenbach**
[https://veen.world](https://veen.world)
