# file-dedupe

**Find and replace duplicate files across one or more folders with hardlinks.**  
Uses fast parallel hashing with file attribute awareness to safely deduplicate files in-place.  
Part of the [Infinito.Nexus](https://github.com/kevinveenbirkenbach/infinito.nexus) ecosystem.

---

## 📦 Installation

`file-dedupe` can be installed using the custom package manager  
[`pkgmgr`](https://github.com/kevinveenbirkenbach/package-manager):

```bash
pkgmgr install fidedu
````

This provides the global command:

```bash
fidedu
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

---

## ⚙️ Behavior

1. Scans all provided folders recursively.
2. Groups files with identical size.
3. For each candidate group, computes a **BLAKE2b hash** including file attributes
   (mode, UID, GID, size, and modification time).
4. Files with matching hashes are treated as **true duplicates**.
5. One canonical copy is kept; all others are **replaced by hardlinks** pointing to it.
6. When `--compress` is *not* used, the tool reports potential savings only (no changes are made).

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
Scanning 3 folders...
Duplicate sets found: 12
Files involved:       57
Planned hardlinks:    45
Estimated savings:    1.2 GB (1,234,567,890 bytes)

[dry-run] Use --compress to apply these changes.
```

---

## 🧪 Testing

Run all tests using the built-in `Makefile`:

```bash
make test
```

---

## 👤 Author

**Kevin Veen-Birkenbach**
[https://veen.world](https://veen.world)

---

## 💬 Credits and References

Developed with assistance from **ChatGPT (GPT-5)** as part of an iterative design and implementation [session on *October 18, 2025*](https://chatgpt.com/share/68f3612f-d620-800f-8c43-5fa8a3d564b9), focusing on parallel, attribute-aware, in-place hardlink deduplication.
