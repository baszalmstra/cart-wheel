"""Wheel parsing utilities."""

import configparser
from dataclasses import dataclass, field
from email.parser import Parser


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

    # Entry points
    console_scripts: list[str] = field(default_factory=list)
    gui_scripts: list[str] = field(default_factory=list)

    # Wheel-specific info
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
        elif "linux_x86_64" in platform or (
            "manylinux" in platform and "x86_64" in platform
        ):
            return "linux-64"
        elif "linux_aarch64" in platform or (
            "manylinux" in platform and "aarch64" in platform
        ):
            return "linux-aarch64"
        elif "macosx" in platform and "x86_64" in platform:
            return "osx-64"
        elif "macosx" in platform and "arm64" in platform:
            return "osx-arm64"
        else:
            return "noarch"


def _parse_metadata_bytes(content: bytes) -> dict:
    """Parse wheel METADATA file content.

    METADATA files use RFC 822 format (email-style headers).

    Args:
        content: Raw bytes of METADATA file.

    Returns:
        Dict with parsed metadata fields.
    """
    parser = Parser()
    msg = parser.parsestr(content.decode("utf-8"))

    # Get all Requires-Dist entries (can have multiple)
    requires_dist = msg.get_all("Requires-Dist") or []

    # Get all Project-URL entries
    project_urls_raw = msg.get_all("Project-URL") or []
    project_urls = {}
    for entry in project_urls_raw:
        if ", " in entry:
            label, url = entry.split(", ", 1)
            project_urls[label.lower()] = url

    return {
        "name": msg.get("Name", ""),
        "version": msg.get("Version", ""),
        "summary": msg.get("Summary"),
        "description": msg.get_payload() or None,  # Body is the description
        "license": msg.get("License"),
        "requires_python": msg.get("Requires-Python"),
        "requires_dist": requires_dist,
        "home_page": msg.get("Home-page"),
        "project_urls": project_urls,
    }


def _parse_wheel_bytes(content: bytes) -> dict:
    """Parse wheel WHEEL file content.

    Args:
        content: Raw bytes of WHEEL file.

    Returns:
        Dict with wheel metadata including tags.
    """
    parser = Parser()
    msg = parser.parsestr(content.decode("utf-8"))

    # Tag can appear multiple times for multi-platform wheels
    tags = msg.get_all("Tag") or []

    # Parse first tag into components
    python_tag = None
    abi_tag = None
    platform_tag = None

    if tags:
        parts = tags[0].split("-")
        if len(parts) >= 3:
            python_tag = parts[0]
            abi_tag = parts[1]
            platform_tag = parts[2]

    return {
        "wheel_version": msg.get("Wheel-Version"),
        "generator": msg.get("Generator"),
        "root_is_purelib": msg.get("Root-Is-Purelib", "false").lower() == "true",
        "tags": tags,
        "python_tag": python_tag,
        "abi_tag": abi_tag,
        "platform_tag": platform_tag,
    }


def _parse_entry_points_bytes(content: bytes) -> dict[str, list[str]]:
    """Parse entry_points.txt content.

    Args:
        content: Raw bytes of entry_points.txt file.

    Returns:
        Dict mapping section names to lists of entry point strings.
    """
    entry_points: dict[str, list[str]] = {}

    parser = configparser.ConfigParser()
    parser.read_string(content.decode("utf-8"))

    for section in parser.sections():
        entry_points[section] = [
            f"{name} = {value}" for name, value in parser.items(section)
        ]

    return entry_points


def parse_wheel_metadata(
    metadata_content: bytes,
    wheel_content: bytes,
    entry_points_content: bytes | None = None,
    filename: str | None = None,
) -> WheelMetadata:
    """Parse wheel metadata from raw file contents.

    Args:
        metadata_content: Contents of METADATA file.
        wheel_content: Contents of WHEEL file.
        entry_points_content: Contents of entry_points.txt (optional).
        filename: Original wheel filename for fallback parsing.

    Returns:
        WheelMetadata dataclass with parsed information.
    """
    metadata = _parse_metadata_bytes(metadata_content)
    wheel_info = _parse_wheel_bytes(wheel_content)

    # Parse entry points if provided
    entry_points = {}
    if entry_points_content:
        entry_points = _parse_entry_points_bytes(entry_points_content)

    # Determine if pure python
    platform_tag = wheel_info.get("platform_tag", "any")
    abi_tag = wheel_info.get("abi_tag", "none")
    is_pure = platform_tag == "any" and abi_tag == "none"

    # Get URLs from project_urls
    project_urls = metadata.get("project_urls", {})

    return WheelMetadata(
        name=metadata["name"],
        version=metadata["version"],
        summary=metadata.get("summary"),
        description=metadata.get("description"),
        license=metadata.get("license"),
        requires_python=metadata.get("requires_python"),
        dependencies=metadata.get("requires_dist", []),
        home_url=metadata.get("home_page") or project_urls.get("homepage"),
        doc_url=project_urls.get("documentation"),
        dev_url=project_urls.get("repository"),
        source_url=project_urls.get("source"),
        console_scripts=entry_points.get("console_scripts", []),
        gui_scripts=entry_points.get("gui_scripts", []),
        is_pure_python=is_pure,
        python_tag=wheel_info.get("python_tag"),
        abi_tag=wheel_info.get("abi_tag"),
        platform_tag=wheel_info.get("platform_tag"),
    )
