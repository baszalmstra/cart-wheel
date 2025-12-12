"""Wheel parsing utilities."""

from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path

from packaging.utils import parse_wheel_filename
from pkginfo import Wheel


@dataclass
class WheelMetadata:
    """Parsed metadata from a wheel file."""

    name: str
    version: str
    summary: str | None = None
    description: str | None = None
    license: str | None = None
    requires_python: str | None = None
    dependencies: list[str] = field(default_factory=list)

    # URLs
    home_url: str | None = None
    doc_url: str | None = None
    dev_url: str | None = None
    source_url: str | None = None

    # Wheel-specific info
    wheel_path: Path | None = None
    is_pure_python: bool = True
    python_tag: str | None = None
    abi_tag: str | None = None
    platform_tag: str | None = None

    @property
    def conda_name(self) -> str:
        """Conda package names are lowercase with hyphens replaced by dashes."""
        return self.name.lower().replace("_", "-")

    @property
    def conda_subdir(self) -> str:
        """Determine conda subdir from platform tag."""
        if self.is_pure_python:
            return "noarch"

        platform = (self.platform_tag or "").lower()
        if "win_amd64" in platform or "win64" in platform:
            return "win-64"
        elif "win32" in platform:
            return "win-32"
        elif "linux_x86_64" in platform or "manylinux" in platform and "x86_64" in platform:
            return "linux-64"
        elif "linux_aarch64" in platform or "manylinux" in platform and "aarch64" in platform:
            return "linux-aarch64"
        elif "macosx" in platform and "x86_64" in platform:
            return "osx-64"
        elif "macosx" in platform and "arm64" in platform:
            return "osx-arm64"
        else:
            return "noarch"


def _parse_project_urls(project_urls: Sequence[str] | None) -> dict[str, str]:
    """Parse project_urls list into a dict mapping label to URL."""
    if not project_urls:
        return {}
    urls = {}
    for entry in project_urls:
        if ", " in entry:
            label, url = entry.split(", ", 1)
            urls[label.lower()] = url
    return urls


def parse_wheel(wheel_path: Path) -> WheelMetadata:
    """Parse metadata from a wheel file."""
    whl = Wheel(str(wheel_path))

    # Parse wheel filename for name, version, and tags
    filename_name, filename_version, _, tags = parse_wheel_filename(wheel_path.name)

    # Get the first tag (wheels can have multiple compatible tags)
    tag = next(iter(tags))
    python_tag = tag.interpreter
    abi_tag = tag.abi
    platform_tag = tag.platform

    is_pure = platform_tag == "any" and abi_tag == "none"

    # Parse project URLs
    project_urls = _parse_project_urls(whl.project_urls)

    # Use metadata values, falling back to filename if not available
    name = whl.name or str(filename_name)
    version = whl.version or str(filename_version)

    return WheelMetadata(
        name=name,
        version=version,
        summary=whl.summary,
        description=whl.description,
        license=whl.license,
        requires_python=whl.requires_python,
        dependencies=list(whl.requires_dist) if whl.requires_dist else [],
        home_url=whl.home_page or project_urls.get("homepage"),
        doc_url=project_urls.get("documentation"),
        dev_url=project_urls.get("repository"),
        source_url=project_urls.get("source"),
        wheel_path=wheel_path,
        is_pure_python=is_pure,
        python_tag=python_tag,
        abi_tag=abi_tag,
        platform_tag=platform_tag,
    )
