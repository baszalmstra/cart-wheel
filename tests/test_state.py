"""Tests for state management module."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest

from cart_wheel.state import (
    Dependencies,
    PackageConfig,
    StateError,
    WheelConfig,
    WheelState,
    get_pending_wheels,
    list_packages,
    load_package_config,
    load_state,
    save_state,
    validate_dependencies,
)


def test_load_package_config_basic(tmp_path: Path):
    """Should load basic package config from TOML."""
    packages_dir = tmp_path / "packages"
    packages_dir.mkdir()

    config_content = """
version_constraint = ">=2.0"
skip_versions = ["2.0.1"]

wheels = [
  { filename = "pkg-2.1.0-py3-none-any.whl" },
  { filename = "pkg-2.0.0-py3-none-any.whl" },
]
"""
    (packages_dir / "example-pkg.toml").write_text(config_content)

    config = load_package_config(packages_dir, "example-pkg")

    assert config.name == "example-pkg"
    assert config.version_constraint == ">=2.0"
    assert config.skip_versions == ["2.0.1"]
    assert len(config.wheels) == 2
    assert config.wheels[0].version == "2.1.0"
    assert config.wheels[0].filename == "pkg-2.1.0-py3-none-any.whl"


def test_load_package_config_minimal(tmp_path: Path):
    """Should load minimal package config."""
    packages_dir = tmp_path / "packages"
    packages_dir.mkdir()

    (packages_dir / "minimal.toml").write_text("")

    config = load_package_config(packages_dir, "minimal")

    assert config.name == "minimal"
    assert config.version_constraint == ""
    assert config.skip_versions == []
    assert config.wheels == []


def test_load_package_config_not_found(tmp_path: Path):
    """Should raise error for missing config."""
    packages_dir = tmp_path / "packages"
    packages_dir.mkdir()

    with pytest.raises(StateError, match="Package config not found"):
        load_package_config(packages_dir, "nonexistent")


def test_list_packages_empty(tmp_path: Path):
    """Should return empty list for nonexistent directory."""
    assert list_packages(tmp_path / "packages") == []


def test_list_packages_multiple(tmp_path: Path):
    """Should list all packages alphabetically."""
    packages_dir = tmp_path / "packages"
    packages_dir.mkdir()

    (packages_dir / "zebra.toml").write_text("")
    (packages_dir / "alpha.toml").write_text("")
    (packages_dir / "beta.toml").write_text("")

    packages = list_packages(packages_dir)

    assert packages == ["alpha", "beta", "zebra"]


def test_list_packages_ignores_non_toml(tmp_path: Path):
    """Should ignore non-TOML files."""
    packages_dir = tmp_path / "packages"
    packages_dir.mkdir()

    (packages_dir / "valid.toml").write_text("")
    (packages_dir / "readme.md").write_text("")
    (packages_dir / "data.json").write_text("{}")

    packages = list_packages(packages_dir)

    assert packages == ["valid"]


def test_wheel_state_to_dict_converted():
    """Should serialize converted state."""
    state = WheelState(
        status="converted",
        converted_at=datetime(2024, 1, 15, 10, 30, 0),
        conda_file="pkg-1.0.0-py_0.conda",
        subdir="noarch",
        dependencies=Dependencies(
            required=["dep1", "dep2"],
            optional={"dev": ["pytest"]},
        ),
    )

    data = state.to_dict()

    assert data["status"] == "converted"
    assert data["converted_at"] == "2024-01-15T10:30:00"
    assert data["conda_file"] == "pkg-1.0.0-py_0.conda"
    assert data["subdir"] == "noarch"
    assert data["dependencies"]["required"] == ["dep1", "dep2"]


def test_wheel_state_to_dict_failed():
    """Should serialize failed state."""
    state = WheelState(
        status="failed",
        error="DependencyConversionError: unsupported marker",
        retry_count=2,
    )

    data = state.to_dict()

    assert data["status"] == "failed"
    assert data["error"] == "DependencyConversionError: unsupported marker"
    assert data["retry_count"] == 2
    assert "converted_at" not in data


def test_wheel_state_from_dict_roundtrip():
    """Should roundtrip through serialization."""
    original = WheelState(
        status="converted",
        converted_at=datetime(2024, 1, 15, 10, 30, 0),
        conda_file="pkg-1.0.0-py_0.conda",
        subdir="noarch",
        dependencies=Dependencies(required=["dep1"]),
    )

    data = original.to_dict()
    restored = WheelState.from_dict(data)

    assert restored.status == original.status
    assert restored.converted_at == original.converted_at
    assert restored.conda_file == original.conda_file
    assert restored.dependencies.required == original.dependencies.required


def test_save_and_load_state(tmp_path: Path):
    """Should save and load state correctly."""
    state_dir = tmp_path / "state"

    state = {
        "pkg-1.0.0-py3-none-any.whl": WheelState(
            status="converted",
            converted_at=datetime(2024, 1, 15, 10, 30, 0),
            conda_file="pkg-1.0.0-py_0.conda",
            subdir="noarch",
        ),
        "pkg-0.9.0-py3-none-any.whl": WheelState(
            status="pending",
        ),
    }

    save_state(state_dir, "example-pkg", state)
    loaded = load_state(state_dir, "example-pkg")

    assert len(loaded) == 2
    assert loaded["pkg-1.0.0-py3-none-any.whl"].status == "converted"
    assert loaded["pkg-0.9.0-py3-none-any.whl"].status == "pending"


def test_load_state_empty(tmp_path: Path):
    """Should return empty dict for nonexistent state."""
    state_dir = tmp_path / "state"
    state_dir.mkdir()

    state = load_state(state_dir, "nonexistent")

    assert state == {}


def test_save_state_creates_directory(tmp_path: Path):
    """Should create state directory if needed."""
    state_dir = tmp_path / "state" / "nested"

    state = {"wheel.whl": WheelState(status="pending")}
    save_state(state_dir, "pkg", state)

    assert state_dir.exists()
    assert (state_dir / "pkg.json").exists()


def test_validate_dependencies_all_found():
    """Should return empty list when all deps found."""
    deps = Dependencies(required=["requests", "numpy"])
    packages = ["requests", "numpy"]

    missing = validate_dependencies(deps, packages)

    assert missing == []


def test_validate_dependencies_some_missing():
    """Should return missing dependencies."""
    deps = Dependencies(required=["requests", "unknown-pkg", "another"])
    packages = ["requests"]

    missing = validate_dependencies(deps, packages)

    assert set(missing) == {"unknown-pkg", "another"}


def test_validate_dependencies_strips_version_specifiers():
    """Should strip version specifiers from dep names."""
    deps = Dependencies(required=["requests>=2.0", "numpy<2.0"])
    packages = ["requests", "numpy"]

    missing = validate_dependencies(deps, packages)

    assert missing == []


def test_validate_dependencies_normalizes_names():
    """Should normalize package names with underscores."""
    deps = Dependencies(required=["my_package"])
    packages = ["my-package"]

    missing = validate_dependencies(deps, packages)

    assert missing == []


def test_get_pending_wheels_all_pending_when_no_state():
    """All wheels pending when no state exists."""
    config = PackageConfig(
        name="pkg",
        version_constraint="",
        skip_versions=[],
        wheels=[
            WheelConfig(filename="pkg-1.0.0-py3-none-any.whl"),
            WheelConfig(filename="pkg-0.9.0-py3-none-any.whl"),
        ],
    )
    state: dict[str, WheelState] = {}

    pending = get_pending_wheels(config, state)

    assert len(pending) == 2


def test_get_pending_wheels_skips_converted():
    """Should skip already converted wheels."""
    config = PackageConfig(
        name="pkg",
        version_constraint="",
        skip_versions=[],
        wheels=[
            WheelConfig(filename="pkg-1.0.0-py3-none-any.whl"),
            WheelConfig(filename="pkg-0.9.0-py3-none-any.whl"),
        ],
    )
    state = {
        "pkg-1.0.0-py3-none-any.whl": WheelState(status="converted"),
    }

    pending = get_pending_wheels(config, state)

    assert len(pending) == 1
    assert pending[0].version == "0.9.0"


def test_get_pending_wheels_retries_failed_under_limit():
    """Should retry failed wheels under retry limit."""
    config = PackageConfig(
        name="pkg",
        version_constraint="",
        skip_versions=[],
        wheels=[
            WheelConfig(filename="pkg-1.0.0-py3-none-any.whl"),
        ],
    )
    state = {
        "pkg-1.0.0-py3-none-any.whl": WheelState(
            status="failed",
            retry_count=2,
        ),
    }

    pending = get_pending_wheels(config, state)

    assert len(pending) == 1


def test_get_pending_wheels_skips_failed_at_limit():
    """Should skip failed wheels at retry limit."""
    config = PackageConfig(
        name="pkg",
        version_constraint="",
        skip_versions=[],
        wheels=[
            WheelConfig(filename="pkg-1.0.0-py3-none-any.whl"),
        ],
    )
    state = {
        "pkg-1.0.0-py3-none-any.whl": WheelState(
            status="failed",
            retry_count=3,
        ),
    }

    pending = get_pending_wheels(config, state)

    assert len(pending) == 0
