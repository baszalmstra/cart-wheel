"""PyPI JSON API client for fetching package releases and streaming wheel downloads."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

import httpx
from packaging.specifiers import SpecifierSet
from packaging.version import InvalidVersion, Version

from .http import get_async_client, get_cached_client

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Generator

    from hishel.httpx import AsyncCacheClient

PYPI_JSON_URL = "https://pypi.org/pypi/{package}/json"


@dataclass(frozen=True)
class WheelInfo:
    """Information about a wheel file from PyPI."""

    filename: str
    url: str
    python_requires: str | None
    sha256: str
    size: int


@dataclass(frozen=True)
class PyPIRelease:
    """A release (version) of a package on PyPI."""

    version: str
    upload_time: datetime
    wheels: list[WheelInfo]
    yanked: bool


class PyPIError(Exception):
    """Error fetching from PyPI."""


def get_package_releases(package: str) -> Generator[PyPIRelease, None, None]:
    """Yield all releases for a package from PyPI.

    Releases are yielded in descending version order (newest first).
    Uses HTTP caching for faster repeated requests.
    """
    url = PYPI_JSON_URL.format(package=package)

    client = get_cached_client()
    response = client.get(url)
    if response.status_code == 404:
        raise PyPIError(f"Package '{package}' not found on PyPI")
    response.raise_for_status()
    data = response.json()

    releases = data.get("releases", {})

    # Filter out invalid versions and sort in descending order
    valid_versions = []
    invalid_versions = []
    for v in releases.keys():
        try:
            Version(v)
            valid_versions.append(v)
        except InvalidVersion:
            # Skip versions that don't conform to PEP 440
            invalid_versions.append(v)
            continue

    # Warn about invalid versions
    if invalid_versions:
        from rich.console import Console
        console = Console(stderr=True)
        console.print(
            f"[dim yellow]⚠ Skipping {len(invalid_versions)} invalid version(s) for {package}: "
            f"{', '.join(invalid_versions[:3])}{'...' if len(invalid_versions) > 3 else ''}[/]"
        )

    sorted_versions = sorted(
        valid_versions,
        key=lambda v: Version(v),
        reverse=True,
    )

    for version_str in sorted_versions:
        files = releases[version_str]
        wheels = []
        upload_time = None
        yanked = False

        for file_info in files:
            # Only consider wheel files
            if not file_info["filename"].endswith(".whl"):
                continue

            # Track upload time from any file
            if upload_time is None and file_info.get("upload_time"):
                upload_time = datetime.fromisoformat(
                    file_info["upload_time"].replace("Z", "+00:00")
                )

            # Track yanked status
            if file_info.get("yanked"):
                yanked = True

            # Get SHA256 from digests
            digests = file_info.get("digests", {})
            sha256 = digests.get("sha256", "")

            wheels.append(
                WheelInfo(
                    filename=file_info["filename"],
                    url=file_info["url"],
                    python_requires=file_info.get("requires_python"),
                    sha256=sha256,
                    size=file_info.get("size", 0),
                )
            )

        # Skip releases with no wheels
        if not wheels:
            continue

        yield PyPIRelease(
            version=version_str,
            upload_time=upload_time or datetime.min,
            wheels=wheels,
            yanked=yanked,
        )


def get_matching_versions(
    package: str,
    constraint: str,
    *,
    max_versions: int | None = None,
    include_yanked: bool = False,
) -> Generator[PyPIRelease, None, None]:
    """Yield releases matching a PEP 440 version constraint.

    Args:
        package: PyPI package name
        constraint: PEP 440 version specifier (e.g., ">=2.0", ">=1.0,<3.0")
        max_versions: Maximum number of versions to yield (None for unlimited)
        include_yanked: Whether to include yanked releases

    Yields:
        Releases matching the constraint, newest first
    """
    specifier = SpecifierSet(constraint)
    count = 0

    for release in get_package_releases(package):
        # Skip yanked releases unless explicitly requested
        if release.yanked and not include_yanked:
            continue

        # Check if version matches constraint
        if Version(release.version) not in specifier:
            continue

        yield release
        count += 1

        if max_versions is not None and count >= max_versions:
            return


def download_wheel(url: str, *, chunk_size: int = 65536) -> Iterator[bytes]:
    """Stream wheel file bytes from a URL.

    Args:
        url: URL to download from
        chunk_size: Size of chunks to yield

    Yields:
        Chunks of bytes from the wheel file
    """
    with httpx.Client() as client:
        with client.stream("GET", url) as response:
            response.raise_for_status()
            yield from response.iter_bytes(chunk_size=chunk_size)


def fetch_wheel_metadata(wheel_url: str) -> bytes | None:
    """Fetch wheel METADATA directly from PyPI using PEP 658.

    PyPI serves the METADATA file at <wheel_url>.metadata for wheels
    that have this feature enabled (PEP 658/714).
    Uses HTTP caching for faster repeated requests.

    Args:
        wheel_url: URL to the wheel file

    Returns:
        METADATA file contents as bytes, or None if not available
    """
    metadata_url = f"{wheel_url}.metadata"

    client = get_cached_client()
    response = client.get(metadata_url)
    if response.status_code == 404:
        return None
    response.raise_for_status()
    return response.content


def select_best_wheel(
    wheels: list[WheelInfo],
    *,
    prefer_pure_python: bool = True,
) -> WheelInfo | None:
    """Select the best wheel from a list of available wheels.

    Preference order:
    1. Pure Python wheels (py3-none-any) if prefer_pure_python is True
    2. Platform-specific wheels for current platform
    3. First available wheel

    Args:
        wheels: List of available wheels
        prefer_pure_python: Whether to prefer pure Python wheels

    Returns:
        The best wheel, or None if no wheels available
    """
    if not wheels:
        return None

    if prefer_pure_python:
        # Look for pure Python wheels first
        for wheel in wheels:
            if "py3-none-any" in wheel.filename or "py2.py3-none-any" in wheel.filename:
                return wheel

    # Return first available wheel
    return wheels[0]


# =============================================================================
# Async versions for concurrent fetching
# =============================================================================


def _parse_releases_response(
    data: dict, package: str
) -> tuple[list[PyPIRelease], list[str]]:
    """Parse PyPI JSON response into list of releases.

    Shared between sync and async implementations.

    Returns:
        Tuple of (releases, warnings) where warnings is a list of warning messages.
    """
    releases = data.get("releases", {})
    warnings: list[str] = []

    # Filter out invalid versions and sort in descending order
    valid_versions = []
    invalid_versions = []
    for v in releases.keys():
        try:
            Version(v)
            valid_versions.append(v)
        except InvalidVersion:
            invalid_versions.append(v)
            continue

    # Collect warning about invalid versions
    if invalid_versions:
        warnings.append(
            f"Skipping {len(invalid_versions)} invalid version(s) for {package}: "
            f"{', '.join(invalid_versions[:3])}{'...' if len(invalid_versions) > 3 else ''}"
        )

    sorted_versions = sorted(
        valid_versions,
        key=lambda v: Version(v),
        reverse=True,
    )

    result = []
    for version_str in sorted_versions:
        files = releases[version_str]
        wheels = []
        upload_time = None
        yanked = False

        for file_info in files:
            if not file_info["filename"].endswith(".whl"):
                continue

            if upload_time is None and file_info.get("upload_time"):
                upload_time = datetime.fromisoformat(
                    file_info["upload_time"].replace("Z", "+00:00")
                )

            if file_info.get("yanked"):
                yanked = True

            digests = file_info.get("digests", {})
            sha256 = digests.get("sha256", "")

            wheels.append(
                WheelInfo(
                    filename=file_info["filename"],
                    url=file_info["url"],
                    python_requires=file_info.get("requires_python"),
                    sha256=sha256,
                    size=file_info.get("size", 0),
                )
            )

        if not wheels:
            continue

        result.append(
            PyPIRelease(
                version=version_str,
                upload_time=upload_time or datetime.min,
                wheels=wheels,
                yanked=yanked,
            )
        )

    return result, warnings


async def get_package_releases_async(
    package: str,
    client: AsyncCacheClient,
) -> tuple[list[PyPIRelease], list[str]]:
    """Fetch all releases for a package from PyPI asynchronously.

    Returns releases in descending version order (newest first).

    Args:
        package: Package name
        client: Async HTTP client

    Returns:
        Tuple of (releases, warnings)
    """
    url = PYPI_JSON_URL.format(package=package)

    response = await client.get(url)
    if response.status_code == 404:
        raise PyPIError(f"Package '{package}' not found on PyPI")
    response.raise_for_status()
    data = response.json()

    return _parse_releases_response(data, package)


async def get_matching_versions_async(
    package: str,
    constraint: str,
    client: AsyncCacheClient,
    *,
    max_versions: int | None = None,
    include_yanked: bool = False,
) -> tuple[list[PyPIRelease], list[str]]:
    """Fetch releases matching a PEP 440 version constraint asynchronously.

    Args:
        package: PyPI package name
        constraint: PEP 440 version specifier (e.g., ">=2.0", ">=1.0,<3.0")
        client: Async HTTP client
        max_versions: Maximum number of versions to return (None for unlimited)
        include_yanked: Whether to include yanked releases

    Returns:
        Tuple of (releases, warnings) where releases match the constraint, newest first
    """
    releases, warnings = await get_package_releases_async(package, client)
    specifier = SpecifierSet(constraint)

    result = []
    for release in releases:
        if release.yanked and not include_yanked:
            continue

        if Version(release.version) not in specifier:
            continue

        result.append(release)

        if max_versions is not None and len(result) >= max_versions:
            break

    return result, warnings


async def fetch_wheel_metadata_async(
    wheel_url: str,
    client: AsyncCacheClient,
) -> bytes | None:
    """Fetch wheel METADATA directly from PyPI using PEP 658 asynchronously.

    Args:
        wheel_url: URL to the wheel file
        client: Async HTTP client

    Returns:
        METADATA file contents as bytes, or None if not available
    """
    metadata_url = f"{wheel_url}.metadata"

    response = await client.get(metadata_url)
    if response.status_code == 404:
        return None
    response.raise_for_status()
    return response.content


async def download_wheel_async(url: str) -> bytes:
    """Download wheel file asynchronously (no caching, streamed).

    Args:
        url: URL to download from

    Returns:
        Complete wheel file as bytes
    """
    import httpx

    async with httpx.AsyncClient(timeout=120.0) as client:
        async with client.stream("GET", url) as response:
            response.raise_for_status()
            chunks = []
            async for chunk in response.aiter_bytes():
                chunks.append(chunk)
            return b"".join(chunks)


def download_wheel_streaming(url: str) -> Iterable[bytes]:
    """Download wheel file with streaming (sync generator).

    Yields chunks as they arrive, suitable for passing directly
    to convert_wheel which uses stream_unzip.

    Args:
        url: URL to download from

    Yields:
        Chunks of bytes from the wheel file
    """
    import httpx

    with httpx.Client(timeout=120.0) as client:
        with client.stream("GET", url) as response:
            response.raise_for_status()
            yield from response.iter_bytes(chunk_size=65536)


async def download_wheel_streaming_async(url: str) -> "AsyncGenerator[bytes, None]":
    """Download wheel file with async streaming.

    Yields chunks as they arrive, suitable for passing directly
    to convert_wheel_async which uses async_stream_unzip.

    Args:
        url: URL to download from

    Yields:
        Chunks of bytes from the wheel file
    """
    import httpx

    async with httpx.AsyncClient(timeout=120.0) as client:
        async with client.stream("GET", url) as response:
            response.raise_for_status()
            async for chunk in response.aiter_bytes(chunk_size=65536):
                yield chunk
