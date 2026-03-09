[![Tests][badge-ci]][link-ci]
[![Coverage][badge-cov]][link-cov]

[badge-cov]: https://codecov.io/github/saezlab/cache-manager/graph/badge.svg
[link-cov]: https://codecov.io/github/saezlab/cache-manager
[badge-ci]: https://img.shields.io/github/actions/workflow/status/saezlab/cache-manager/ci.yml?branch=main
[link-ci]: https://github.com/saezlab/cache-manager/actions/workflows/ci.yml

# cache-manager

## Description
cache-manager is a lightweight, Pythonic cache for files with an SQLite registry. It lets you:

- Store files under a cache directory while tracking metadata in SQLite.
- Version entries automatically based on a stable key derived from a URI/parameters.
- Track and query item status (UNINITIALIZED, WRITE, READY, FAILED, TRASH).
- Attach type-aware attributes (int, float, varchar, datetime, text) and search by them.
- Open plain and compressed files (gz, zip, tar(.gz|.bz2)) via a unified interface.
- Clean up stale records/files and keep the best/most recent ready entries.

This is ideal for reproducible data pipelines and ETL steps where you want deterministic, discoverable artifacts.

## Table of Contents
- [cache-manager](#cache-manager)
  - [Description](#description)
  - [Table of Contents](#table-of-contents)
  - [Installation](#installation)
  - [Usage](#usage)
  - [Configuration](#configuration)
  - [Examples](#examples)
  - [Contributing](#contributing)
  - [License](#license)
  - [Contact](#contact)

## Installation
Clone and install in editable mode (no extra tools required):

```bash
git clone https://github.com/saezlab/cache-manager.git
cd cache-manager
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

Alternatively, if you prefer Poetry:

```bash
git clone https://github.com/saezlab/cache-manager.git
cd cache-manager
poetry install
```

## Usage
The API centers around two types: `Cache` (manager) and `CacheItem` (one file + metadata). Create a cache, create or retrieve items, write files, mark them READY, and open them later.

Minimal example:

```python
import cache_manager as cm
from cache_manager._status import Status

cache = cm.Cache(path="./my_cache")
item = cache.create(
    uri="https://example.org/data.tsv",
    params={"dataset": "demo", "version": 1},
    attrs={"species": "human", "rows": 1200},
    status=Status.WRITE.value,
    filename="data.tsv",
)

with open(item.path, "w", encoding="utf-8") as f:
    f.write("col_a\tcol_b\n1\t2\n")

item.ready()
best = cache.best(
    uri="https://example.org/data.tsv",
    params={"dataset": "demo", "version": 1},
)
print(best, best.path)
```

Run the included example script which downloads a real dataset and caches it:

```bash
python scripts/hello_cache_manager.py
```

## Configuration
There is no global config file; you configure the cache per instance:

- `Cache(path: str | None = None, pkg: str | None = None)`
  - `path`: explicit directory for cache (contains the SQLite registry and files).
  - `pkg`: if set, uses an OS-specific cache directory for that application name via `platformdirs` (e.g., on Linux: `~/.cache/<pkg>`).

Common item fields:
- `uri` (str): a canonical identifier used for the hash key (together with `params`).
- `params` (dict): serialized to the stable key; changing them yields a new key.
- `attrs` (dict): typed attributes persisted to attribute tables for rich queries.
- `status` (int): from `cache_manager._status.Status` (READY, WRITE, etc.).
- `filename` (str): filename to be used in the cache; extension is auto-inferred.

Logging/session helpers are available under `cache_manager.session` and `cache_manager.log` if you want simple trace output.

## Examples

1. Create or reuse an item with `best_or_new`.

```python
import os
import cache_manager as cm
from cache_manager._status import Status

cache = cm.Cache(path="./my_cache")
uri = "https://example.org/report.csv"
params = {"year": 2026, "cohort": "A"}

item = cache.best_or_new(
    uri=uri,
    params=params,
    attrs={"kind": "report", "format": "csv"},
    filename="report.csv",
    new_status=Status.WRITE.value,
)

if item.status != Status.READY.value or not os.path.exists(item.path):
    with open(item.path, "w", encoding="utf-8") as f:
        f.write("id,value\n1,42\n")
    item.ready()

print("Using:", item.path)
```

2. Query by attributes and metadata.

```python
import cache_manager as cm
from cache_manager._status import Status

cache = cm.Cache(path="./my_cache")

cache.create(
    uri="demo://sample-1",
    attrs={"project": "alpha", "batch": 1, "score": 0.95},
    status=Status.READY.value,
)
cache.create(
    uri="demo://sample-2",
    attrs={"project": "alpha", "batch": 2, "score": 0.71},
    status=Status.READY.value,
)

ids = cache.by_attrs({"project": "alpha", "batch": 2})
print("matching ids:", ids)

items = cache.search(uri="demo://sample-2", status=Status.READY.value)
for it in items:
    print(it.version_id, it.attrs)
```

3. Open a cached file through `CacheItem.open`.

```python
import cache_manager as cm
from cache_manager._status import Status

cache = cm.Cache(path="./my_cache")
item = cache.create(
    uri="demo://text-file",
    filename="hello.txt",
    status=Status.WRITE.value,
)

with open(item.path, "w", encoding="utf-8") as f:
    f.write("hello\nworld\n")

item.ready()

opened = item.open(default_mode="r", encoding="utf-8", large=True)
print(next(iter(opened.result)).strip())
```

## Contributing
Contributions are welcome! A typical flow:

Please open issues and pull requests on GitHub. If you plan a larger change, consider discussing it in an issue first. (A dedicated `CONTRIBUTING.md` may be added later.)

## License
This project is licensed under the GNU General Public License v3.0. See the [LICENSE](LICENSE) file for details.

## Contact
OmniPath Team - omnipathdb@gmail.com

Project page: https://github.com/saezlab/cache-manager
