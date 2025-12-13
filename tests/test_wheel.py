"""Tests for wheel parsing functionality."""

import zipfile

from cart_wheel.wheel import WheelMetadata, parse_wheel_metadata


def _parse_wheel_from_path(wheel_path):
    """Helper to parse wheel metadata from a wheel file path."""
    with zipfile.ZipFile(wheel_path, "r") as zf:
        # Find .dist-info directory
        dist_info_prefix = None
        for name in zf.namelist():
            if ".dist-info/" in name:
                dist_info_prefix = name.split("/")[0]
                break

        if dist_info_prefix is None:
            raise ValueError("No .dist-info directory found in wheel")

        metadata_content = zf.read(f"{dist_info_prefix}/METADATA")
        wheel_content = zf.read(f"{dist_info_prefix}/WHEEL")

        entry_points_content = None
        entry_points_path = f"{dist_info_prefix}/entry_points.txt"
        if entry_points_path in zf.namelist():
            entry_points_content = zf.read(entry_points_path)

        return parse_wheel_metadata(
            metadata_content=metadata_content,
            wheel_content=wheel_content,
            entry_points_content=entry_points_content,
            filename=wheel_path.name,
        )


# WheelMetadata tests


def test_wheel_metadata_conda_name_lowercase():
    metadata = WheelMetadata(name="MyPackage", version="1.0.0")
    assert metadata.conda_name == "mypackage"


def test_wheel_metadata_conda_name_underscore_to_hyphen():
    metadata = WheelMetadata(name="my_package", version="1.0.0")
    assert metadata.conda_name == "my-package"


def test_wheel_metadata_conda_name_mixed():
    metadata = WheelMetadata(name="My_Package_Name", version="1.0.0")
    assert metadata.conda_name == "my-package-name"


def test_wheel_metadata_conda_subdir_pure_python():
    metadata = WheelMetadata(
        name="pkg", version="1.0", is_pure_python=True, platform_tag="any"
    )
    assert metadata.conda_subdir == "noarch"


def test_wheel_metadata_conda_subdir_win64():
    metadata = WheelMetadata(
        name="pkg", version="1.0", is_pure_python=False, platform_tag="win_amd64"
    )
    assert metadata.conda_subdir == "win-64"


def test_wheel_metadata_conda_subdir_win32():
    metadata = WheelMetadata(
        name="pkg", version="1.0", is_pure_python=False, platform_tag="win32"
    )
    assert metadata.conda_subdir == "win-32"


def test_wheel_metadata_conda_subdir_linux64():
    metadata = WheelMetadata(
        name="pkg", version="1.0", is_pure_python=False, platform_tag="linux_x86_64"
    )
    assert metadata.conda_subdir == "linux-64"


def test_wheel_metadata_conda_subdir_linux_aarch64():
    metadata = WheelMetadata(
        name="pkg", version="1.0", is_pure_python=False, platform_tag="linux_aarch64"
    )
    assert metadata.conda_subdir == "linux-aarch64"


def test_wheel_metadata_conda_subdir_macos_x64():
    metadata = WheelMetadata(
        name="pkg",
        version="1.0",
        is_pure_python=False,
        platform_tag="macosx_10_9_x86_64",
    )
    assert metadata.conda_subdir == "osx-64"


def test_wheel_metadata_conda_subdir_macos_arm64():
    metadata = WheelMetadata(
        name="pkg",
        version="1.0",
        is_pure_python=False,
        platform_tag="macosx_11_0_arm64",
    )
    assert metadata.conda_subdir == "osx-arm64"


def test_wheel_metadata_conda_subdir_manylinux():
    metadata = WheelMetadata(
        name="pkg",
        version="1.0",
        is_pure_python=False,
        platform_tag="manylinux2014_x86_64",
    )
    assert metadata.conda_subdir == "linux-64"


def test_wheel_metadata_conda_subdir_unknown_fallback():
    metadata = WheelMetadata(
        name="pkg", version="1.0", is_pure_python=False, platform_tag="unknown"
    )
    assert metadata.conda_subdir == "noarch"


def test_wheel_metadata_conda_subdir_none_platform_tag():
    metadata = WheelMetadata(
        name="pkg", version="1.0", is_pure_python=False, platform_tag=None
    )
    assert metadata.conda_subdir == "noarch"


# parse_wheel_metadata tests


def test_parse_wheel_basic_metadata(sample_wheel):
    metadata = _parse_wheel_from_path(sample_wheel)
    assert metadata.name == "sample_package"
    assert metadata.version == "2.0.0"
    assert metadata.summary == "A sample package for testing"
    assert metadata.license == "Apache-2.0"
    assert metadata.requires_python == ">=3.10"


def test_parse_wheel_pure_python_detection(sample_wheel):
    metadata = _parse_wheel_from_path(sample_wheel)
    assert metadata.is_pure_python is True
    assert metadata.platform_tag == "any"
    assert metadata.abi_tag == "none"


def test_parse_wheel_dependencies(sample_wheel):
    metadata = _parse_wheel_from_path(sample_wheel)
    assert len(metadata.dependencies) == 3
    assert "requests>=2.0" in metadata.dependencies
    assert "click>=8.0" in metadata.dependencies


def test_parse_wheel_home_url(sample_wheel):
    metadata = _parse_wheel_from_path(sample_wheel)
    assert metadata.home_url == "https://example.com"


def test_parse_wheel_project_urls(sample_wheel):
    metadata = _parse_wheel_from_path(sample_wheel)
    assert metadata.doc_url == "https://docs.example.com"
    assert metadata.source_url == "https://github.com/example/sample"
    assert metadata.dev_url == "https://github.com/example/sample"


def test_parse_wheel_description(sample_wheel):
    metadata = _parse_wheel_from_path(sample_wheel)
    assert "# Sample Package" in (metadata.description or "")


def test_parse_wheel_minimal(minimal_wheel):
    metadata = _parse_wheel_from_path(minimal_wheel)
    assert metadata.name == "minimal"
    assert metadata.version == "0.1.0"
    assert metadata.summary is None or metadata.summary == ""
    assert metadata.dependencies == []


def test_parse_wheel_tags_from_filename(tmp_wheel):
    wheel_path = tmp_wheel(
        name="pkg",
        version="1.0.0",
        python_tag="py38",
        abi_tag="none",
        platform_tag="any",
    )
    metadata = _parse_wheel_from_path(wheel_path)
    assert metadata.python_tag == "py38"
    assert metadata.abi_tag == "none"
    assert metadata.platform_tag == "any"


def test_parse_wheel_platform_specific(tmp_wheel):
    wheel_path = tmp_wheel(
        name="pkg",
        version="1.0.0",
        python_tag="cp311",
        abi_tag="cp311",
        platform_tag="win_amd64",
    )
    metadata = _parse_wheel_from_path(wheel_path)
    assert metadata.is_pure_python is False
    assert metadata.platform_tag == "win_amd64"


def test_parse_wheel_conda_name(tmp_wheel):
    wheel_path = tmp_wheel(name="My_Test_Package", version="1.0.0")
    metadata = _parse_wheel_from_path(wheel_path)
    assert metadata.conda_name == "my-test-package"


def test_parse_wheel_name_from_metadata(tmp_wheel):
    wheel_path = tmp_wheel(name="fallback_test", version="1.0.0")
    metadata = _parse_wheel_from_path(wheel_path)
    assert metadata.name is not None
    assert len(metadata.name) > 0
