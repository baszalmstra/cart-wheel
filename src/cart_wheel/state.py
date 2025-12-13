"""State and config management for cart-wheel channel."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import tomllib

from packaging.utils import parse_wheel_filename

if TYPE_CHECKING:
    pass


def _extract_version_from_filename(filename: str) -> str:
    """Extract version from wheel filename using packaging library."""
    _, version, _, _ = parse_wheel_filename(filename)
    return str(version)


@dataclass
class WheelConfig:
    """Wheel information from package config."""

    filename: str

    @property
    def version(self) -> str:
        """Extract version from wheel filename."""
        return _extract_version_from_filename(self.filename)


@dataclass
class PackageConfig:
    """Configuration for a package from packages/<name>.toml."""

    name: str
    version_constraint: str
    skip_versions: list[str]
    wheels: list[WheelConfig]

    @classmethod
    def from_toml(cls, name: str, data: dict) -> PackageConfig:
        """Create PackageConfig from parsed TOML data."""
        wheels = [WheelConfig(filename=w["filename"]) for w in data.get("wheels", [])]
        return cls(
            name=name,
            version_constraint=data.get("version_constraint", ""),
            skip_versions=data.get("skip_versions", []),
            wheels=wheels,
        )


@dataclass
class Dependencies:
    """Dependencies extracted from wheel metadata."""

    required: list[str] = field(default_factory=list)
    optional: dict[str, list[str]] = field(default_factory=dict)

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        result: dict = {"required": self.required}
        if self.optional:
            result["optional"] = self.optional
        return result

    @classmethod
    def from_dict(cls, data: dict | None) -> Dependencies | None:
        """Create from dictionary."""
        if data is None:
            return None
        return cls(
            required=data.get("required", []),
            optional=data.get("optional", {}),
        )


@dataclass
class WheelState:
    """State of a wheel conversion."""

    status: Literal["pending", "converted", "failed", "skipped"]
    sha256: str | None = None
    upload_time: str | None = None
    converted_at: datetime | None = None
    conda_file: str | None = None
    subdir: str | None = None
    dependencies: Dependencies | None = None
    original_requirements: list[str] | None = None
    error: str | None = None
    retry_count: int = 0

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        result: dict = {"status": self.status}
        if self.sha256:
            result["sha256"] = self.sha256
        if self.upload_time:
            result["upload_time"] = self.upload_time
        if self.converted_at:
            result["converted_at"] = self.converted_at.isoformat()
        if self.conda_file:
            result["conda_file"] = self.conda_file
        if self.subdir:
            result["subdir"] = self.subdir
        if self.dependencies:
            result["dependencies"] = self.dependencies.to_dict()
        if self.original_requirements:
            result["original_requirements"] = self.original_requirements
        if self.error:
            result["error"] = self.error
        if self.retry_count > 0:
            result["retry_count"] = self.retry_count
        return result

    @classmethod
    def from_dict(cls, data: dict) -> WheelState:
        """Create from dictionary."""
        converted_at = None
        if data.get("converted_at"):
            converted_at = datetime.fromisoformat(data["converted_at"])

        return cls(
            status=data["status"],
            sha256=data.get("sha256"),
            upload_time=data.get("upload_time"),
            converted_at=converted_at,
            conda_file=data.get("conda_file"),
            subdir=data.get("subdir"),
            dependencies=Dependencies.from_dict(data.get("dependencies")),
            original_requirements=data.get("original_requirements"),
            error=data.get("error"),
            retry_count=data.get("retry_count", 0),
        )


class StateError(Exception):
    """Error related to state management."""


def load_package_config(packages_dir: Path, name: str) -> PackageConfig:
    """Load package configuration from TOML file.

    Args:
        packages_dir: Directory containing package config files
        name: Package name (without .toml extension)

    Returns:
        PackageConfig instance

    Raises:
        StateError: If config file doesn't exist or is invalid
    """
    config_path = packages_dir / f"{name}.toml"
    if not config_path.exists():
        raise StateError(f"Package config not found: {config_path}")

    with open(config_path, "rb") as f:
        data = tomllib.load(f)

    return PackageConfig.from_toml(name, data)


def list_packages(packages_dir: Path) -> list[str]:
    """List all packages in the packages directory.

    Args:
        packages_dir: Directory containing package config files

    Returns:
        List of package names (without .toml extension)
    """
    if not packages_dir.exists():
        return []

    return sorted(p.stem for p in packages_dir.glob("*.toml"))


def load_state(state_dir: Path, name: str) -> dict[str, WheelState]:
    """Load state for a package.

    Args:
        state_dir: Directory containing state files
        name: Package name (without .json extension)

    Returns:
        Dict mapping wheel filename to WheelState
    """
    state_path = state_dir / f"{name}.json"
    if not state_path.exists():
        return {}

    with open(state_path) as f:
        data = json.load(f)

    return {filename: WheelState.from_dict(state) for filename, state in data.items()}


def save_state(state_dir: Path, name: str, state: dict[str, WheelState]) -> None:
    """Save state for a package atomically.

    Args:
        state_dir: Directory containing state files
        name: Package name (without .json extension)
        state: Dict mapping wheel filename to WheelState
    """
    state_dir.mkdir(parents=True, exist_ok=True)
    state_path = state_dir / f"{name}.json"
    temp_path = state_path.with_suffix(".json.tmp")

    data = {filename: ws.to_dict() for filename, ws in state.items()}

    # Write to temp file first
    with open(temp_path, "w") as f:
        json.dump(data, f, indent=2)

    # Atomic rename
    temp_path.replace(state_path)


def validate_dependencies(
    deps: Dependencies,
    packages: list[str],
) -> list[str]:
    """Find missing dependencies.

    Args:
        deps: Dependencies to validate
        packages: List of packages in our channel (includes conda-forge references)

    Returns:
        List of missing dependency names
    """
    missing = []

    for dep in deps.required:
        # Extract package name (strip version specifiers)
        dep_name = dep.split("[")[0].split("<")[0].split(">")[0].split("=")[0].strip()
        normalized = dep_name.lower().replace("_", "-")
        if normalized not in packages:
            missing.append(dep_name)

    return missing


def validate_all_dependencies(
    state_dir: Path,
    packages_dir: Path,
) -> dict[str, list[str]]:
    """Validate dependencies for all packages.

    Args:
        state_dir: Directory containing state files
        packages_dir: Directory containing package configs

    Returns:
        Dict mapping package name to list of missing dependencies
    """
    packages = list_packages(packages_dir)

    results: dict[str, list[str]] = {}

    for pkg_name in packages:
        state = load_state(state_dir, pkg_name)

        # Find a converted wheel with dependencies
        for wheel_state in state.values():
            if wheel_state.status == "converted" and wheel_state.dependencies:
                missing = validate_dependencies(wheel_state.dependencies, packages)
                if missing:
                    results[pkg_name] = missing
                break

    return results


def get_pending_wheels(
    config: PackageConfig,
    state: dict[str, WheelState],
) -> list[WheelConfig]:
    """Get wheels that need to be converted.

    Args:
        config: Package configuration
        state: Current state

    Returns:
        List of WheelConfig for wheels not yet converted
    """
    pending = []

    for wheel in config.wheels:
        if wheel.filename in state:
            wheel_state = state[wheel.filename]
            # Skip if already converted or permanently failed
            if wheel_state.status == "converted":
                continue
            if wheel_state.status == "skipped":
                continue
            if wheel_state.status == "failed" and wheel_state.retry_count >= 3:
                continue

        pending.append(wheel)

    return pending
