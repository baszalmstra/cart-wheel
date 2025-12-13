"""Tests for channel management module."""

from __future__ import annotations

import io
import json
import tarfile
import zipfile
from pathlib import Path

import zstandard as zstd

from cart_wheel.channel import index_channel


def _create_test_conda_package(
    output_path: Path,
    name: str = "test-pkg",
    version: str = "1.0.0",
    build: str = "py_0",
    depends: list[str] | None = None,
) -> Path:
    """Create a minimal test .conda package."""
    if depends is None:
        depends = ["python >=3.8"]

    index_json = {
        "name": name,
        "version": version,
        "build": build,
        "build_number": 0,
        "depends": depends,
        "subdir": "noarch",
    }

    info_tar_buffer = io.BytesIO()
    with tarfile.open(fileobj=info_tar_buffer, mode="w") as tar:
        index_data = json.dumps(index_json).encode()
        index_info = tarfile.TarInfo(name="info/index.json")
        index_info.size = len(index_data)
        tar.addfile(index_info, io.BytesIO(index_data))

    info_tar_data = info_tar_buffer.getvalue()
    cctx = zstd.ZstdCompressor()
    info_zst_data = cctx.compress(info_tar_data)

    pkg_tar_buffer = io.BytesIO()
    with tarfile.open(fileobj=pkg_tar_buffer, mode="w") as tar:
        pass
    pkg_tar_data = pkg_tar_buffer.getvalue()
    pkg_zst_data = cctx.compress(pkg_tar_data)

    conda_path = output_path / f"{name}-{version}-{build}.conda"
    with zipfile.ZipFile(conda_path, "w") as zf:
        metadata = {"conda_pkg_format_version": 2}
        zf.writestr("metadata.json", json.dumps(metadata))
        zf.writestr(f"info-{name}-{version}-{build}.tar.zst", info_zst_data)
        zf.writestr(f"pkg-{name}-{version}-{build}.tar.zst", pkg_zst_data)

    return conda_path


def test_index_channel_creates_repodata_json(tmp_path: Path):
    """Should create repodata.json in noarch subdir."""
    channel_dir = tmp_path / "channel"
    noarch_dir = channel_dir / "noarch"
    noarch_dir.mkdir(parents=True)

    _create_test_conda_package(noarch_dir, name="test-pkg", version="1.0.0")

    index_channel(channel_dir)

    repodata_path = noarch_dir / "repodata.json"
    assert repodata_path.exists()

    with open(repodata_path) as f:
        repodata = json.load(f)

    assert "packages.conda" in repodata or "packages" in repodata
    all_packages = {
        **repodata.get("packages", {}),
        **repodata.get("packages.conda", {}),
    }
    assert "test-pkg-1.0.0-py_0.conda" in all_packages


def test_index_channel_indexes_multiple_packages(tmp_path: Path):
    """Should index all packages in subdir."""
    channel_dir = tmp_path / "channel"
    noarch_dir = channel_dir / "noarch"
    noarch_dir.mkdir(parents=True)

    _create_test_conda_package(noarch_dir, name="pkg-a", version="1.0.0")
    _create_test_conda_package(noarch_dir, name="pkg-b", version="2.0.0")
    _create_test_conda_package(noarch_dir, name="pkg-b", version="2.1.0")

    index_channel(channel_dir)

    with open(noarch_dir / "repodata.json") as f:
        repodata = json.load(f)

    all_packages = {
        **repodata.get("packages", {}),
        **repodata.get("packages.conda", {}),
    }
    assert len(all_packages) == 3
    assert "pkg-a-1.0.0-py_0.conda" in all_packages
    assert "pkg-b-2.0.0-py_0.conda" in all_packages
    assert "pkg-b-2.1.0-py_0.conda" in all_packages


def test_index_channel_repodata_has_package_metadata(tmp_path: Path):
    """Repodata should contain package metadata."""
    channel_dir = tmp_path / "channel"
    noarch_dir = channel_dir / "noarch"
    noarch_dir.mkdir(parents=True)

    _create_test_conda_package(
        noarch_dir,
        name="my-pkg",
        version="2.0.0",
        depends=["numpy", "requests >=2.0"],
    )

    index_channel(channel_dir)

    with open(noarch_dir / "repodata.json") as f:
        repodata = json.load(f)

    all_packages = {
        **repodata.get("packages", {}),
        **repodata.get("packages.conda", {}),
    }
    pkg_info = all_packages["my-pkg-2.0.0-py_0.conda"]

    assert pkg_info["name"] == "my-pkg"
    assert pkg_info["version"] == "2.0.0"
    assert pkg_info["build"] == "py_0"
    assert "depends" in pkg_info
    assert "sha256" in pkg_info
    assert "size" in pkg_info


def test_index_channel_creates_compressed_repodata(tmp_path: Path):
    """Should create repodata.json.zst compressed file."""
    channel_dir = tmp_path / "channel"
    noarch_dir = channel_dir / "noarch"
    noarch_dir.mkdir(parents=True)

    _create_test_conda_package(noarch_dir)

    index_channel(channel_dir)

    repodata_zst = noarch_dir / "repodata.json.zst"
    assert repodata_zst.exists()
