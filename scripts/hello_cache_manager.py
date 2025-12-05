import os
import csv
from urllib.request import urlopen
from contextlib import closing

import cache_manager as cm
from cache_manager._status import Status


URL = (
    "https://zenodo.org/records/16902349/files/"
    "synthetic_protein_interactions.tsv?download=1"
)
FILENAME = "synthetic_protein_interactions.tsv"


def download_to(path: str, url: str, chunk_size: int = 1024 * 1024) -> None:
    """Download URL to `path` using stdlib only (streamed)."""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with closing(urlopen(url, timeout=120)) as resp, open(path, "wb") as out:
        while True:
            chunk = resp.read(chunk_size)
            if not chunk:
                break
            out.write(chunk)


def main():
    # 1) Create / open cache in a local directory
    cache = cm.Cache(path="./my_cache")

    # 2) Get best READY item for this URI or create a new one with WRITE status
    params = {
        "source": "Zenodo",
        "record": 16902349,
        "file": FILENAME,
    }
    attrs = {
        "project": "demo",
        "dataset": "synthetic_protein_interactions",
        "format": "tsv",
    }

    item = cache.best_or_new(
        uri=URL,
        params=params,
        attrs=attrs,
        filename=FILENAME,
        new_status=Status.WRITE.value,
    )

    print("Item:", item)
    print("Cache dir:", cache.dir)
    print("Target path:", item.path)

    # 3) Download if not READY or file missing
    need_download = item.status != Status.READY.value or not os.path.exists(item.path)
    if need_download:
        try:
            print("Downloading from:", URL)
            download_to(item.path, URL)
            item.ready()
            print("Download complete. Marked READY.")
        except Exception as e:
            item.failed()
            # Best effort cleanup of partial download
            try:
                if os.path.exists(item.path):
                    os.remove(item.path)
            except Exception:
                pass
            raise RuntimeError(f"Download failed: {e}")
    else:
        print("Already READY in cache; skipping download.")

    # 4) Open and read a few lines (header + 5 rows)
    fobj = item.open(mode="r", encoding="utf-8")
    try:
        reader = csv.reader(fobj, delimiter="\t")
        rows = []
        for i, row in enumerate(reader):
            rows.append(row)
            if i >= 5:  # header + 5
                break
        print("Preview (first 6 lines):")
        for r in rows:
            print(r)
    finally:
        if hasattr(fobj, "close"):
            fobj.close()

    # 5) Show how to find this item again (best READY)
    best_ready = cache.best(uri=URL)  # defaults to status=READY
    print("Best READY version:", best_ready)

    # 6) Attribute-based search
    print("By attrs (project=demo):", cache.by_attrs({"project": "demo"}))


if __name__ == "__main__":
    main()