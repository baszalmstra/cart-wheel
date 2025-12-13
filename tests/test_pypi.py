"""Tests for PyPI client module."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from cart_wheel.pypi import (
    PyPIError,
    WheelInfo,
    download_wheel,
    get_matching_versions,
    get_package_releases,
    select_best_wheel,
)


@pytest.fixture
def sample_pypi_response() -> dict:
    """Sample PyPI JSON API response."""
    return {
        "info": {"name": "example-package"},
        "releases": {
            "1.0.0": [
                {
                    "filename": "example_package-1.0.0-py3-none-any.whl",
                    "url": "https://files.pythonhosted.org/example_package-1.0.0-py3-none-any.whl",
                    "requires_python": ">=3.8",
                    "digests": {"sha256": "abc123"},
                    "size": 10000,
                    "upload_time": "2024-01-01T10:00:00Z",
                    "yanked": False,
                },
            ],
            "2.0.0": [
                {
                    "filename": "example_package-2.0.0-py3-none-any.whl",
                    "url": "https://files.pythonhosted.org/example_package-2.0.0-py3-none-any.whl",
                    "requires_python": ">=3.9",
                    "digests": {"sha256": "def456"},
                    "size": 12000,
                    "upload_time": "2024-06-01T10:00:00Z",
                    "yanked": False,
                },
                {
                    "filename": "example_package-2.0.0.tar.gz",
                    "url": "https://files.pythonhosted.org/example_package-2.0.0.tar.gz",
                    "requires_python": ">=3.9",
                    "digests": {"sha256": "ghi789"},
                    "size": 15000,
                    "upload_time": "2024-06-01T10:00:00Z",
                    "yanked": False,
                },
            ],
            "2.1.0": [
                {
                    "filename": "example_package-2.1.0-py3-none-any.whl",
                    "url": "https://files.pythonhosted.org/example_package-2.1.0-py3-none-any.whl",
                    "requires_python": ">=3.9",
                    "digests": {"sha256": "jkl012"},
                    "size": 13000,
                    "upload_time": "2024-08-01T10:00:00Z",
                    "yanked": True,
                },
            ],
            "0.9.0": [],  # Release with no files
        },
    }


def test_get_package_releases_yields_in_descending_order(sample_pypi_response: dict):
    """Releases should be yielded newest first."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = sample_pypi_response

    mock_client = MagicMock()
    mock_client.get.return_value = mock_response

    with patch("cart_wheel.pypi.get_cached_client", return_value=mock_client):
        releases = list(get_package_releases("example-package"))

    assert len(releases) == 3
    assert releases[0].version == "2.1.0"
    assert releases[1].version == "2.0.0"
    assert releases[2].version == "1.0.0"


def test_get_package_releases_extracts_wheel_info(sample_pypi_response: dict):
    """Wheel info should be extracted correctly."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = sample_pypi_response

    mock_client = MagicMock()
    mock_client.get.return_value = mock_response

    with patch("cart_wheel.pypi.get_cached_client", return_value=mock_client):
        releases = list(get_package_releases("example-package"))

    release_2_0 = next(r for r in releases if r.version == "2.0.0")
    assert len(release_2_0.wheels) == 1
    assert release_2_0.wheels[0].filename == "example_package-2.0.0-py3-none-any.whl"
    assert release_2_0.wheels[0].sha256 == "def456"
    assert release_2_0.wheels[0].size == 12000


def test_get_package_releases_skips_without_wheels(sample_pypi_response: dict):
    """Releases without wheel files should be skipped."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = sample_pypi_response

    mock_client = MagicMock()
    mock_client.get.return_value = mock_response

    with patch("cart_wheel.pypi.get_cached_client", return_value=mock_client):
        releases = list(get_package_releases("example-package"))

    versions = [r.version for r in releases]
    assert "0.9.0" not in versions


def test_get_package_releases_detects_yanked(sample_pypi_response: dict):
    """Yanked releases should be marked as such."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = sample_pypi_response

    mock_client = MagicMock()
    mock_client.get.return_value = mock_response

    with patch("cart_wheel.pypi.get_cached_client", return_value=mock_client):
        releases = list(get_package_releases("example-package"))

    release_2_1 = next(r for r in releases if r.version == "2.1.0")
    assert release_2_1.yanked is True

    release_2_0 = next(r for r in releases if r.version == "2.0.0")
    assert release_2_0.yanked is False


def test_get_package_releases_raises_for_missing():
    """Should raise PyPIError for non-existent package."""
    mock_response = MagicMock()
    mock_response.status_code = 404

    mock_client = MagicMock()
    mock_client.get.return_value = mock_response

    with patch("cart_wheel.pypi.get_cached_client", return_value=mock_client):
        with pytest.raises(PyPIError, match="not found on PyPI"):
            list(get_package_releases("nonexistent-package"))


def test_get_matching_versions_filters_by_constraint(sample_pypi_response: dict):
    """Should only yield versions matching the constraint."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = sample_pypi_response

    mock_client = MagicMock()
    mock_client.get.return_value = mock_response

    with patch("cart_wheel.pypi.get_cached_client", return_value=mock_client):
        releases = list(get_matching_versions("example-package", ">=2.0"))

    versions = [r.version for r in releases]
    assert "1.0.0" not in versions
    assert "2.0.0" in versions


def test_get_matching_versions_excludes_yanked_by_default(sample_pypi_response: dict):
    """Yanked releases should be excluded by default."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = sample_pypi_response

    mock_client = MagicMock()
    mock_client.get.return_value = mock_response

    with patch("cart_wheel.pypi.get_cached_client", return_value=mock_client):
        releases = list(get_matching_versions("example-package", ">=2.0"))

    versions = [r.version for r in releases]
    assert "2.1.0" not in versions


def test_get_matching_versions_includes_yanked_when_requested(sample_pypi_response: dict):
    """Yanked releases should be included when requested."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = sample_pypi_response

    mock_client = MagicMock()
    mock_client.get.return_value = mock_response

    with patch("cart_wheel.pypi.get_cached_client", return_value=mock_client):
        releases = list(
            get_matching_versions("example-package", ">=2.0", include_yanked=True)
        )

    versions = [r.version for r in releases]
    assert "2.1.0" in versions


def test_get_matching_versions_respects_max_versions(sample_pypi_response: dict):
    """Should limit results to max_versions."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = sample_pypi_response

    mock_client = MagicMock()
    mock_client.get.return_value = mock_response

    with patch("cart_wheel.pypi.get_cached_client", return_value=mock_client):
        releases = list(
            get_matching_versions("example-package", ">=1.0", max_versions=2)
        )

    assert len(releases) == 2


def test_select_best_wheel_prefers_pure_python():
    """Should prefer pure Python wheels when requested."""
    wheels = [
        WheelInfo(
            filename="pkg-1.0.0-cp311-cp311-linux_x86_64.whl",
            url="https://example.com/1",
            python_requires=">=3.8",
            sha256="abc",
            size=1000,
        ),
        WheelInfo(
            filename="pkg-1.0.0-py3-none-any.whl",
            url="https://example.com/2",
            python_requires=">=3.8",
            sha256="def",
            size=1000,
        ),
    ]

    best = select_best_wheel(wheels, prefer_pure_python=True)
    assert best is not None
    assert "py3-none-any" in best.filename


def test_select_best_wheel_returns_first_when_no_pure_python():
    """Should return first wheel when no pure Python wheel exists."""
    wheels = [
        WheelInfo(
            filename="pkg-1.0.0-cp311-cp311-linux_x86_64.whl",
            url="https://example.com/1",
            python_requires=">=3.8",
            sha256="abc",
            size=1000,
        ),
    ]

    best = select_best_wheel(wheels, prefer_pure_python=True)
    assert best is not None
    assert best.filename == "pkg-1.0.0-cp311-cp311-linux_x86_64.whl"


def test_select_best_wheel_returns_none_for_empty():
    """Should return None for empty wheel list."""
    assert select_best_wheel([]) is None


def test_download_wheel_streams_bytes():
    """Should yield bytes in chunks."""
    test_content = b"Hello, World!" * 1000

    mock_response = MagicMock()
    mock_response.iter_bytes.return_value = [
        test_content[i : i + 100] for i in range(0, len(test_content), 100)
    ]

    with patch("cart_wheel.pypi.httpx.Client") as mock_client:
        mock_client.return_value.__enter__.return_value.stream.return_value.__enter__.return_value = (
            mock_response
        )

        chunks = list(download_wheel("https://example.com/wheel.whl"))

    assert b"".join(chunks) == test_content
