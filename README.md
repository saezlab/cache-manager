[![Tests][badge-ci]][link-ci]
[![Coverage][badge-cov]][link-cov]

[badge-cov]: https://codecov.io/github/saezlab/cache-manager/graph/badge.svg
[link-cov]: https://codecov.io/github/saezlab/cache-manager
[badge-ci]: https://img.shields.io/github/actions/workflow/status/saezlab/cache-manager/ci.yml?branch=main
[link-ci]: https://github.com/saezlab/cache-manager/actions/workflows/ci.yml

# cache-manager

## Description
cache-manager is a lightweight, Pythonic cache for files with a SQLite registry. It lets you:

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

```

Run the included example script which downloads a real dataset and caches it:

```bash
python scripts/hello_cache_manager.py
```

## Configuration
There’s no global config file; you configure the cache per instance:

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

1) TODO: Add a minimal example.

```python

```

2) TODO: Add another minimal example.

```python

```

1) TODO: Add a more complete example.

```python

```

## Contributing
Contributions are welcome! A typical flow:

```bash
git clone https://github.com/saezlab/cache-manager.git
cd cache-manager
python -m venv .venv && source .venv/bin/activate
pip install -e .[dev]
pytest -q
```

Please open issues and pull requests on GitHub. If you plan a larger change, consider discussing it in an issue first. (A dedicated `CONTRIBUTING.md` may be added later.)

## License
This project is licensed under the GNU General Public License v3.0. See the [LICENSE](LICENSE) file for details.

## Contact
OmniPath Team — omnipathdb@gmail.com

Project page: https://github.com/saezlab/cache-manager
Documentation: https://cache-manager.readthedocs.io/
