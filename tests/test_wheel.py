"""Tests for wheel parsing functionality."""

from pathlib import Path

import pytest

from cart_wheel.wheel import WheelMetadata, _parse_project_urls, parse_wheel

# _parse_project_urls tests


def test_parse_project_urls_empty_list():
    assert _parse_project_urls([]) == {}


def test_parse_project_urls_none():
    assert _parse_project_urls(None) == {}


def test_parse_project_urls_single_url():
    result = _parse_project_urls(["Documentation, https://docs.example.com"])
    assert result == {"documentation": "https://docs.example.com"}


def test_parse_project_urls_multiple_urls():
    urls = [
        "Documentation, https://docs.example.com",
        "Source, https://github.com/example/repo",
        "Homepage, https://example.com",
    ]
    result = _parse_project_urls(urls)
    assert result == {
        "documentation": "https://docs.example.com",
        "source": "https://github.com/example/repo",
        "homepage": "https://example.com",
    }


def test_parse_project_urls_case_insensitive_keys():
    urls = ["DOCUMENTATION, https://docs.example.com"]
    result = _parse_project_urls(urls)
    assert "documentation" in result


def test_parse_project_urls_url_with_comma():
    urls = ["Source, https://example.com/path?a=1,b=2"]
    result = _parse_project_urls(urls)
    assert result["source"] == "https://example.com/path?a=1,b=2"


def test_parse_project_urls_invalid_format_skipped():
    urls = ["InvalidEntry", "Valid, https://example.com"]
    result = _parse_project_urls(urls)
    assert result == {"valid": "https://example.com"}


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


# parse_wheel tests


def test_parse_wheel_basic_metadata(sample_wheel: Path):
    metadata = parse_wheel(sample_wheel)
    assert metadata.name == "sample_package"
    assert metadata.version == "2.0.0"
    assert metadata.summary == "A sample package for testing"
    assert metadata.license == "Apache-2.0"
    assert metadata.requires_python == ">=3.10"


def test_parse_wheel_path_set(sample_wheel: Path):
    metadata = parse_wheel(sample_wheel)
    assert metadata.wheel_path == sample_wheel


def test_parse_wheel_pure_python_detection(sample_wheel: Path):
    metadata = parse_wheel(sample_wheel)
    assert metadata.is_pure_python is True
    assert metadata.platform_tag == "any"
    assert metadata.abi_tag == "none"


def test_parse_wheel_dependencies(sample_wheel: Path):
    metadata = parse_wheel(sample_wheel)
    assert len(metadata.dependencies) == 3
    assert "requests>=2.0" in metadata.dependencies
    assert "click>=8.0" in metadata.dependencies


def test_parse_wheel_home_url(sample_wheel: Path):
    metadata = parse_wheel(sample_wheel)
    assert metadata.home_url == "https://example.com"


def test_parse_wheel_project_urls(sample_wheel: Path):
    metadata = parse_wheel(sample_wheel)
    assert metadata.doc_url == "https://docs.example.com"
    assert metadata.source_url == "https://github.com/example/sample"
    assert metadata.dev_url == "https://github.com/example/sample"


def test_parse_wheel_description(sample_wheel: Path):
    metadata = parse_wheel(sample_wheel)
    assert "# Sample Package" in (metadata.description or "")


def test_parse_wheel_minimal(minimal_wheel: Path):
    metadata = parse_wheel(minimal_wheel)
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
    metadata = parse_wheel(wheel_path)
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
    metadata = parse_wheel(wheel_path)
    assert metadata.is_pure_python is False
    assert metadata.platform_tag == "win_amd64"


def test_parse_wheel_conda_name(tmp_wheel):
    wheel_path = tmp_wheel(name="My_Test_Package", version="1.0.0")
    metadata = parse_wheel(wheel_path)
    assert metadata.conda_name == "my-test-package"


def test_parse_wheel_name_fallback_to_filename(tmp_wheel):
    wheel_path = tmp_wheel(name="fallback_test", version="1.0.0")
    metadata = parse_wheel(wheel_path)
    assert metadata.name is not None
    assert len(metadata.name) > 0
