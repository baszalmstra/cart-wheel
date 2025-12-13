"""Tests for sync orchestration module."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pytest

from cart_wheel.conda import ConversionResult
from cart_wheel.pypi import PyPIRelease, WheelInfo
from cart_wheel.sync import (
    _extract_dependencies,
    check_for_updates,
    sync_all,
    sync_package,
)


@pytest.fixture
def setup_package_dir(tmp_path: Path) -> Path:
    """Set up a packages directory with test config."""
    packages_dir = tmp_path / "packages"
    packages_dir.mkdir()

    config = """
version_constraint = ">=1.0"
skip_versions = []

wheels = [
  { filename = "test_pkg-1.1.0-py3-none-any.whl" },
  { filename = "test_pkg-1.0.0-py3-none-any.whl" },
]
"""
    (packages_dir / "test-pkg.toml").write_text(config)
    return packages_dir


@pytest.fixture
def mock_pypi_release() -> PyPIRelease:
    """Create a mock PyPI release."""
    return PyPIRelease(
        version="1.1.0",
        upload_time=datetime(2024, 1, 1),
        wheels=[
            WheelInfo(
                filename="test_pkg-1.1.0-py3-none-any.whl",
                url="https://files.pythonhosted.org/test_pkg-1.1.0-py3-none-any.whl",
                python_requires=">=3.8",
                sha256="abc123",
                size=10000,
            ),
        ],
        yanked=False,
    )


def test_extract_dependencies_extracts_required():
    """Should extract required dependencies."""
    result = ConversionResult(
        path=Path("test.conda"),
        name="test",
        version="1.0.0",
        dependencies=["requests >=2.0", "click", "python >=3.8"],
    )

    deps = _extract_dependencies(result)

    assert "requests" in deps.required
    assert "click" in deps.required
    assert "python" not in deps.required


def test_extract_dependencies_extracts_optional():
    """Should extract optional dependencies."""
    result = ConversionResult(
        path=Path("test.conda"),
        name="test",
        version="1.0.0",
        dependencies=["requests"],
        extra_depends={
            "dev": ["pytest >=7.0", "ruff"],
            "docs": ["sphinx"],
        },
    )

    deps = _extract_dependencies(result)

    assert deps.optional["dev"] == ["pytest", "ruff"]
    assert deps.optional["docs"] == ["sphinx"]


def test_sync_package_dry_run(setup_package_dir: Path, tmp_path: Path):
    """Dry run should not convert or update state."""
    state_dir = tmp_path / "state"
    output_dir = tmp_path / "output"

    result = sync_package(
        "test-pkg",
        setup_package_dir,
        state_dir,
        output_dir,
        dry_run=True,
    )

    assert len(result.converted) == 2
    assert "1.1.0" in result.converted
    assert "1.0.0" in result.converted

    assert not state_dir.exists() or not list(state_dir.glob("*.json"))
    assert not output_dir.exists()


def test_sync_package_skips_converted(setup_package_dir: Path, tmp_path: Path):
    """Should skip already converted wheels."""
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    output_dir = tmp_path / "output"

    state_file = state_dir / "test-pkg.json"
    state_file.write_text(
        json.dumps(
            {
                "test_pkg-1.1.0-py3-none-any.whl": {
                    "status": "converted",
                    "converted_at": "2024-01-01T00:00:00",
                    "conda_file": "test-pkg-1.1.0-py_0.conda",
                    "subdir": "noarch",
                }
            }
        )
    )

    result = sync_package(
        "test-pkg",
        setup_package_dir,
        state_dir,
        output_dir,
        dry_run=True,
    )

    assert len(result.converted) == 1
    assert "1.0.0" in result.converted


@patch("cart_wheel.sync.download_wheel")
@patch("cart_wheel.sync.convert_wheel")
@patch("cart_wheel.sync.get_matching_versions")
def test_sync_package_converts_pending(
    mock_get_versions,
    mock_convert,
    mock_download,
    setup_package_dir: Path,
    tmp_path: Path,
    mock_pypi_release: PyPIRelease,
):
    """Should convert pending wheels."""
    state_dir = tmp_path / "state"
    output_dir = tmp_path / "output"

    mock_get_versions.return_value = iter([mock_pypi_release])
    mock_download.return_value = iter([b"wheel content"])
    mock_convert.return_value = ConversionResult(
        path=output_dir / "noarch" / "test_pkg-1.1.0-py_0.conda",
        name="test-pkg",
        version="1.1.0",
        dependencies=["dep1"],
        subdir="noarch",
    )

    result = sync_package(
        "test-pkg",
        setup_package_dir,
        state_dir,
        output_dir,
        dry_run=False,
    )

    assert "1.1.0" in result.converted
    assert (state_dir / "test-pkg.json").exists()


def test_sync_all_multiple_packages(tmp_path: Path):
    """Should sync all packages in directory."""
    packages_dir = tmp_path / "packages"
    packages_dir.mkdir()
    state_dir = tmp_path / "state"
    output_dir = tmp_path / "output"

    (packages_dir / "pkg-a.toml").write_text(
        """
wheels = [
  { filename = "pkg_a-1.0.0-py3-none-any.whl", sha256 = "abc" },
]
"""
    )
    (packages_dir / "pkg-b.toml").write_text(
        """
wheels = [
  { filename = "pkg_b-2.0.0-py3-none-any.whl", sha256 = "def" },
]
"""
    )

    results = sync_all(
        packages_dir,
        state_dir,
        output_dir,
        dry_run=True,
    )

    assert len(results) == 2
    assert any(r.package == "pkg-a" for r in results)
    assert any(r.package == "pkg-b" for r in results)


@patch("cart_wheel.sync.get_matching_versions")
def test_check_for_updates_finds_new_versions(
    mock_get_versions, setup_package_dir: Path, tmp_path: Path
):
    """Should find versions not in config."""
    state_dir = tmp_path / "state"

    new_release = PyPIRelease(
        version="1.2.0",
        upload_time=datetime(2024, 2, 1),
        wheels=[
            WheelInfo(
                filename="test_pkg-1.2.0-py3-none-any.whl",
                url="https://example.com/1.2.0",
                python_requires=">=3.8",
                sha256="ghi789",
                size=10000,
            ),
        ],
        yanked=False,
    )

    mock_get_versions.return_value = iter([new_release])

    updates = check_for_updates(setup_package_dir, state_dir)

    assert "test-pkg" in updates
    assert "1.2.0" in updates["test-pkg"]


@patch("cart_wheel.sync.get_matching_versions")
def test_check_for_updates_ignores_known_versions(
    mock_get_versions, setup_package_dir: Path, tmp_path: Path
):
    """Should not report versions already in config."""
    state_dir = tmp_path / "state"

    known_release = PyPIRelease(
        version="1.1.0",
        upload_time=datetime(2024, 1, 1),
        wheels=[
            WheelInfo(
                filename="test_pkg-1.1.0-py3-none-any.whl",
                url="https://example.com/1.1.0",
                python_requires=">=3.8",
                sha256="abc123",
                size=10000,
            ),
        ],
        yanked=False,
    )

    mock_get_versions.return_value = iter([known_release])

    updates = check_for_updates(setup_package_dir, state_dir)

    assert "test-pkg" not in updates
