"""The pinned Kaggle dump: the shipped pins, verified downloads, refusals."""

import hashlib
import io
import json
import shutil
import zipfile
from pathlib import Path

import pytest

from ge_intel import kaggle

README = Path(__file__).parent / "fixtures" / "kaggle" / "README.md"
PARQUET = b"PAR1" + b"\x00" * 16 + b"PAR1"


def test_shipped_pins_cover_version_1_and_pin_the_real_readme():
    pins = kaggle.load_pins()
    months = [p.name for p in pins if p.name.startswith("prices_5m/")]
    assert len(pins) == 70 and len(months) == 68
    assert months[0] == "prices_5m/year=2021/month=03/data.parquet"
    assert months[-1] == "prices_5m/year=2026/month=10/data.parquet"
    assert sum(p.bytes for p in pins) == 4_802_290_041
    readme = next(p for p in pins if p.name == "README.md")
    assert kaggle.check(README, readme) == readme.sha256  # the README as downloaded


class Fake:
    def __init__(self, bodies):
        self.bodies = bodies
        self.urls = []

    def __call__(self, url, headers):
        assert headers["User-Agent"].startswith("osrs-ge-market-intel/")
        self.urls.append(url)
        name = next(n for n in self.bodies if kaggle.download_url(n) == url)
        body = self.bodies[name]
        yield from (body[:7], body[7:])


@pytest.fixture
def pins_file(tmp_path):
    path = tmp_path / "pins.json"
    shutil.copy(kaggle.PINS, path)
    return path


def pins_for(pins_file, bodies, pinned):
    doc = json.loads(pins_file.read_text())
    doc["files"] = [
        {
            "name": n,
            "bytes": len(b),
            "sha256": hashlib.sha256(b).hexdigest() if n in pinned else None,
        }
        for n, b in bodies.items()
    ]
    pins_file.write_text(json.dumps(doc))
    return kaggle.load_pins(pins_file)


def test_download_verifies_records_first_hashes_and_resumes(tmp_path, pins_file):
    bodies = {"README.md": b"# readme\n", "prices_5m/year=2021/month=03/data.parquet": PARQUET}
    pins = pins_for(pins_file, bodies, pinned={"README.md"})
    fake = Fake(bodies)
    dest = tmp_path / "v1"
    dry = kaggle.download(dest, dry_run=True, pins=pins, pins_path=pins_file, fetch=fake)
    assert dry["files_to_download"] == 2 and dry["bytes_to_download"] == len(PARQUET) + 9
    assert fake.urls == [] and not dest.exists()

    out = kaggle.download(dest, pins=pins, pins_path=pins_file, fetch=fake, sleep=lambda s: None)
    assert out["downloaded"] == 2 and out["pins_recorded"] == 1
    assert (dest / "prices_5m/year=2021/month=03/data.parquet").read_bytes() == PARQUET
    recorded = {p.name: p.sha256 for p in kaggle.load_pins(pins_file)}
    assert (
        recorded["prices_5m/year=2021/month=03/data.parquet"] == hashlib.sha256(PARQUET).hexdigest()
    )
    again = kaggle.download(dest, pins=kaggle.load_pins(pins_file), pins_path=pins_file, fetch=fake)
    assert again["files_to_download"] == 0 and len(fake.urls) == 2
    assert "%2F" in fake.urls[1]  # nested names are URL-encoded


def test_a_file_that_differs_from_its_pin_is_refused(tmp_path, pins_file):
    name = "prices_5m/year=2021/month=03/data.parquet"
    pins = pins_for(pins_file, {name: PARQUET}, pinned={name})
    tampered = b"PAR1" + b"\x01" * 16 + b"PAR1"
    with pytest.raises(kaggle.PinMismatch, match="SHA-256"):
        kaggle.download(tmp_path, pins=pins, pins_path=pins_file, fetch=Fake({name: tampered}))
    assert list((tmp_path / "prices_5m/year=2021/month=03").iterdir()) == []  # no .part left
    with pytest.raises(kaggle.PinMismatch, match="bytes"):
        kaggle.download(
            tmp_path, pins=pins, pins_path=pins_file, fetch=Fake({name: PARQUET + b"x"})
        )


def test_a_zipped_response_is_unwrapped_and_checked(tmp_path, pins_file):
    name = "prices_5m/year=2021/month=03/data.parquet"
    pins = pins_for(pins_file, {name: PARQUET}, pinned={name})
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("data.parquet", PARQUET)
    kaggle.download(tmp_path, pins=pins, pins_path=pins_file, fetch=Fake({name: buf.getvalue()}))
    assert (tmp_path / name).read_bytes() == PARQUET


def test_pinned_names_cannot_leave_the_download_folder(tmp_path):
    with pytest.raises(ValueError, match="leaves"):
        kaggle.local_path(tmp_path, "../outside.parquet")
