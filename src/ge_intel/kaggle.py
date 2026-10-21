"""The pinned Kaggle dump `antonygarand/osrs-ge-5m` v1: download and verify (ADR-0005, ADR-0018).

Every file is pinned by its listed size and SHA-256 in `sources/kaggle_osrs_ge_5m_v1.json`. A
download streams to `<file>.part`, is checked against the pin and only then takes its real
name, so a file under its real name is always a verified one. A pin without a SHA-256 is
recorded by the first verified download (trust on first download; the month checks against
the Wiki's `/1h` are what establish trust in the content) and must then be committed, after
which a different file is refused. Files are fetched one at a time with a pause between them,
never in parallel, and a re-run skips files already verified.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
import urllib.parse
import urllib.request
import zipfile
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path

from ge_intel.wiki import USER_AGENT

DATASET = "antonygarand/osrs-ge-5m"
VERSION = 1
SOURCE = f"kaggle:{DATASET}@v{VERSION}"
PINS = Path(__file__).parent / "sources" / "kaggle_osrs_ge_5m_v1.json"
API = "https://www.kaggle.com/api/v1/datasets/download"
PAUSE_S = 5.0
CHUNK = 1 << 20

# (url, headers) -> iterable of body chunks; raises on HTTP errors.
Fetch = Callable[[str, dict[str, str]], Iterable[bytes]]


class PinMismatch(ValueError):
    """A downloaded or local file differs from its pin."""


@dataclass(frozen=True)
class Pin:
    name: str  # path inside the dataset, e.g. prices_5m/year=2021/month=03/data.parquet
    bytes: int
    sha256: str | None


def load_pins(path: Path = PINS) -> list[Pin]:
    doc = json.loads(path.read_text())
    if doc["dataset"] != DATASET or doc["version"] != VERSION:
        raise ValueError(f"{path} pins {doc['dataset']} v{doc['version']}, not {SOURCE}")
    return [Pin(f["name"], f["bytes"], f["sha256"]) for f in doc["files"]]


def record_pins(found: dict[str, str], path: Path = PINS) -> None:
    """Write first-download hashes into the pins file; never overwrites an existing pin."""
    doc = json.loads(path.read_text())
    for f in doc["files"]:
        if f["name"] in found and f["sha256"] is None:
            f["sha256"] = found[f["name"]]
    path.write_text(json.dumps(doc, indent=2) + "\n")


def download_url(name: str) -> str:
    return f"{API}/{DATASET}/{urllib.parse.quote(name, safe='')}?datasetVersionNumber={VERSION}"


def local_path(dest: Path, name: str) -> Path:
    path = (dest / name).resolve()
    if dest.resolve() not in path.parents:
        raise ValueError(f"pinned name {name!r} leaves {dest}")
    return path


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        while chunk := fh.read(CHUNK):
            digest.update(chunk)
    return digest.hexdigest()


def check(path: Path, pin: Pin, sha: str | None = None) -> str:
    """The file's SHA-256 if it matches its pin (size always, hash once pinned)."""
    size = path.stat().st_size
    if size != pin.bytes:
        raise PinMismatch(f"{pin.name}: {size:,} bytes, pinned {pin.bytes:,}")
    if pin.name.endswith(".parquet"):
        with path.open("rb") as fh:
            head = fh.read(4)
            fh.seek(-4, os.SEEK_END)
            if head != b"PAR1" or fh.read(4) != b"PAR1":
                raise PinMismatch(f"{pin.name}: not a Parquet file")
    sha = sha or sha256_file(path)
    if pin.sha256 is not None and sha != pin.sha256:
        raise PinMismatch(f"{pin.name}: SHA-256 {sha[:12]}…, pinned {pin.sha256[:12]}…")
    return sha


def urllib_fetch(url: str, headers: dict[str, str]) -> Iterable[bytes]:
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=120) as resp:  # follows Kaggle's storage redirect
        while chunk := resp.read(CHUNK):
            yield chunk


def _unzip_single(path: Path) -> None:
    """Kaggle may serve a file zipped; keep its single member under the same name."""
    with zipfile.ZipFile(path) as zf:
        members = [m for m in zf.infolist() if not m.is_dir()]
        if len(members) != 1:
            raise PinMismatch(f"{path.name}: zip with {len(members)} members, expected 1")
        body = zf.read(members[0])
    path.write_bytes(body)


def download(
    dest: Path,
    *,
    dry_run: bool = False,
    pins: list[Pin] | None = None,
    pins_path: Path = PINS,
    fetch: Fetch = urllib_fetch,
    sleep: Callable[[float], None] = time.sleep,
    pause_s: float = PAUSE_S,
) -> dict:
    """Fetch every pinned file not yet verified under `dest`. Dry run: report, send nothing."""
    pins = pins if pins is not None else load_pins(pins_path)
    have, todo = [], []
    for pin in pins:
        path = local_path(dest, pin.name)
        if path.exists():
            try:
                check(path, pin)
                have.append(pin)
                continue
            except PinMismatch:
                if dry_run:
                    todo.append(pin)
                    continue
                raise  # a local file that fails its pin is never silently replaced
        todo.append(pin)
    report = {
        "dataset": SOURCE,
        "files_total": len(pins),
        "files_verified": len(have),
        "files_to_download": len(todo),
        "bytes_to_download": sum(p.bytes for p in todo),
        "unpinned_hashes": sum(p.sha256 is None for p in pins),
        "requests": len(todo),
    }
    if dry_run or not todo:
        return report
    headers = {"User-Agent": USER_AGENT}
    recorded: dict[str, str] = {}
    for i, pin in enumerate(todo):
        if i:
            sleep(pause_s)
        path = local_path(dest, pin.name)
        path.parent.mkdir(parents=True, exist_ok=True)
        part = path.with_name(path.name + ".part")
        digest = hashlib.sha256()
        with part.open("wb") as fh:
            for chunk in fetch(download_url(pin.name), headers):
                fh.write(chunk)
                digest.update(chunk)
        sha = digest.hexdigest()
        with part.open("rb") as fh:
            zipped = fh.read(4) == b"PK\x03\x04" and not pin.name.endswith(".zip")
        if zipped:
            _unzip_single(part)
            sha = None
        try:
            sha = check(part, pin, sha)
        except PinMismatch:
            part.unlink()
            raise
        part.replace(path)
        if pin.sha256 is None:
            recorded[pin.name] = sha
    if recorded:
        record_pins(recorded, pins_path)
    return {**report, "downloaded": len(todo), "pins_recorded": len(recorded)}
