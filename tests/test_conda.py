"""Tests for conda package building functionality."""

import hashlib
import io
import json
import tarfile
import zipfile
from pathlib import Path

import pytest
import zstandard as zstd
from packaging.requirements import Requirement

from cart_wheel.conda import (
    ConversionResult,
    _create_tar_zst,
    _requirement_to_conda_dep,
    convert_wheel,
)

# Helper functions


def _extract_info_file(conda_path: Path, filename: str) -> bytes:
    """Extract a file from the info archive of a .conda package."""
    with zipfile.ZipFile(conda_path) as z:
        for name in z.namelist():
            if name.startswith("info-") and name.endswith(".tar.zst"):
                info_zst = z.read(name)
                break
        else:
            raise ValueError("No info archive found")

    dctx = zstd.ZstdDecompressor()
    info_tar = dctx.decompress(info_zst)

    with tarfile.open(fileobj=io.BytesIO(info_tar)) as tar:
        member = tar.extractfile(filename)
        if member is None:
            raise ValueError(f"File {filename} not found in info archive")
        return member.read()


def _extract_pkg_file(conda_path: Path, filename: str) -> bytes:
    """Extract a file from the pkg archive of a .conda package."""
    with zipfile.ZipFile(conda_path) as z:
        for name in z.namelist():
            if name.startswith("pkg-") and name.endswith(".tar.zst"):
                pkg_zst = z.read(name)
                break
        else:
            raise ValueError("No pkg archive found")

    # Use streaming decompression (streaming compression doesn't include size in header)
    dctx = zstd.ZstdDecompressor()
    with dctx.stream_reader(io.BytesIO(pkg_zst)) as reader:
        pkg_tar = reader.read()

    with tarfile.open(fileobj=io.BytesIO(pkg_tar)) as tar:
        member = tar.extractfile(filename)
        if member is None:
            raise ValueError(f"File {filename} not found in pkg archive")
        return member.read()


# _create_tar_zst tests


def test_create_tar_zst_valid_zstd():
    """Output is valid zstd-compressed tar."""
    files = {"test.txt": b"hello world"}
    result = _create_tar_zst(files)

    dctx = zstd.ZstdDecompressor()
    decompressed = dctx.decompress(result)

    with tarfile.open(fileobj=io.BytesIO(decompressed)) as tar:
        assert "test.txt" in tar.getnames()


def test_create_tar_zst_multiple_files():
    """Multiple files are included in archive."""
    files = {
        "file1.txt": b"content1",
        "file2.txt": b"content2",
        "dir/file3.txt": b"content3",
    }
    result = _create_tar_zst(files)

    dctx = zstd.ZstdDecompressor()
    decompressed = dctx.decompress(result)

    with tarfile.open(fileobj=io.BytesIO(decompressed)) as tar:
        names = tar.getnames()
        assert "file1.txt" in names
        assert "file2.txt" in names
        assert "dir/file3.txt" in names


def test_create_tar_zst_content_preserved():
    """Binary content is preserved exactly."""
    content = b"test content with special chars: \x00\xff"
    files = {"test.bin": content}
    result = _create_tar_zst(files)

    dctx = zstd.ZstdDecompressor()
    decompressed = dctx.decompress(result)

    with tarfile.open(fileobj=io.BytesIO(decompressed)) as tar:
        extracted = tar.extractfile("test.bin")
        assert extracted.read() == content


def test_create_tar_zst_empty_dict():
    """Empty input creates valid empty archive."""
    result = _create_tar_zst({})
    dctx = zstd.ZstdDecompressor()
    decompressed = dctx.decompress(result)

    with tarfile.open(fileobj=io.BytesIO(decompressed)) as tar:
        assert tar.getnames() == []


# _requirement_to_conda_dep tests


def test_requirement_to_conda_dep_simple():
    """Simple requirement without version."""
    req = Requirement("requests")
    assert _requirement_to_conda_dep(req) == "requests"


def test_requirement_to_conda_dep_with_version():
    """Requirement with version specifier."""
    req = Requirement("requests>=2.0")
    assert _requirement_to_conda_dep(req) == "requests >=2.0"


def test_requirement_to_conda_dep_complex_version():
    """Requirement with multiple version constraints."""
    req = Requirement("requests>=2.0,<3.0")
    result = _requirement_to_conda_dep(req)
    assert "requests" in result
    assert ">=2.0" in result
    assert "<3.0" in result


def test_requirement_to_conda_dep_name_normalization():
    """Package names are normalized to lowercase with hyphens."""
    req = Requirement("Typing_Extensions>=4.0")
    result = _requirement_to_conda_dep(req)
    assert result.startswith("typing-extensions")


def test_requirement_to_conda_dep_underscore_to_hyphen():
    """Underscores in names become hyphens."""
    req = Requirement("my_package")
    assert _requirement_to_conda_dep(req) == "my-package"


def test_requirement_to_conda_dep_converts_extras():
    """Extras in dependencies are converted to conda format."""
    req = Requirement("requests[security]")
    result = _requirement_to_conda_dep(req)
    assert result == "requests[extras=[security]]"


def test_requirement_to_conda_dep_converts_multiple_extras():
    """Multiple extras are converted to conda format."""
    req = Requirement("httpx[http2,socks]>=0.24")
    result = _requirement_to_conda_dep(req)
    assert result == "httpx[extras=[http2,socks]] >=0.24"


def test_requirement_to_conda_dep_extras_and_condition():
    """Extras and conditions are both included."""
    req = Requirement("requests[security]>=2.0")
    result = _requirement_to_conda_dep(req, condition="__win")
    assert result == "requests[extras=[security]] >=2.0; if __win"


def test_requirement_to_conda_dep_with_condition():
    """Condition is appended to dependency string."""
    req = Requirement("pywin32>=300")
    result = _requirement_to_conda_dep(req, condition="__win")
    assert result == "pywin32 >=300; if __win"


# convert_wheel tests


def test_convert_wheel_creates_file(sample_wheel: Path, tmp_path: Path):
    """Output .conda file is created."""
    output_dir = tmp_path / "output"

    result = convert_wheel(sample_wheel, output_dir)

    assert result.path.exists()
    assert result.path.suffix == ".conda"


def test_convert_wheel_returns_conversion_result(sample_wheel: Path, tmp_path: Path):
    """convert_wheel returns ConversionResult with metadata."""
    output_dir = tmp_path / "output"

    result = convert_wheel(sample_wheel, output_dir)

    assert isinstance(result, ConversionResult)
    assert result.name == "sample-package"
    assert result.version == "2.0.0"
    assert result.subdir == "noarch"
    assert isinstance(result.dependencies, list)
    assert isinstance(result.extra_depends, dict)


def test_convert_wheel_file_name_format(sample_wheel: Path, tmp_path: Path):
    """Output filename follows conda naming convention."""
    output_dir = tmp_path / "output"

    result = convert_wheel(sample_wheel, output_dir)

    assert result.path.name == "sample-package-2.0.0-py_0.conda"


def test_convert_wheel_valid_zip(sample_wheel: Path, tmp_path: Path):
    """Output is valid ZIP with required archives."""
    output_dir = tmp_path / "output"

    result = convert_wheel(sample_wheel, output_dir)

    with zipfile.ZipFile(result.path) as z:
        names = z.namelist()
        assert "metadata.json" in names
        assert any("info-" in n and ".tar.zst" in n for n in names)
        assert any("pkg-" in n and ".tar.zst" in n for n in names)


def test_convert_wheel_metadata_json(sample_wheel: Path, tmp_path: Path):
    """metadata.json has correct format version."""
    output_dir = tmp_path / "output"

    result = convert_wheel(sample_wheel, output_dir)

    with zipfile.ZipFile(result.path) as z:
        meta = json.loads(z.read("metadata.json"))
        assert meta["conda_pkg_format_version"] == 2


def test_convert_wheel_index_json_contents(sample_wheel: Path, tmp_path: Path):
    """index.json contains required package metadata."""
    output_dir = tmp_path / "output"

    result = convert_wheel(sample_wheel, output_dir)

    index = _extract_info_file(result.path, "info/index.json")
    index_data = json.loads(index)

    assert index_data["name"] == "sample-package"
    assert index_data["version"] == "2.0.0"
    assert index_data["build"] == "py_0"
    assert index_data["build_number"] == 0
    assert index_data["subdir"] == "noarch"
    assert index_data["noarch"] == "python"


def test_convert_wheel_index_json_dependencies(sample_wheel: Path, tmp_path: Path):
    """index.json includes wheel dependencies."""
    output_dir = tmp_path / "output"

    result = convert_wheel(sample_wheel, output_dir)

    index = _extract_info_file(result.path, "info/index.json")
    index_data = json.loads(index)

    deps = index_data["depends"]
    assert any("python" in d for d in deps)
    assert any("requests" in d for d in deps)
    assert any("click" in d for d in deps)


def test_convert_wheel_excludes_extra_dependencies(sample_wheel: Path, tmp_path: Path):
    """Dependencies conditional on extras are excluded from main deps."""
    output_dir = tmp_path / "output"

    result = convert_wheel(sample_wheel, output_dir)

    index = _extract_info_file(result.path, "info/index.json")
    index_data = json.loads(index)

    deps = index_data["depends"]
    # typing-extensions has `extra == 'dev'` marker, should be in extra_depends
    assert not any("typing-extensions" in d for d in deps)


def test_convert_wheel_license_present(sample_wheel: Path, tmp_path: Path):
    """License is included when present in wheel."""
    output_dir = tmp_path / "output"

    result = convert_wheel(sample_wheel, output_dir)

    index = _extract_info_file(result.path, "info/index.json")
    index_data = json.loads(index)

    assert index_data["license"] == "Apache-2.0"


def test_convert_wheel_license_missing(minimal_wheel: Path, tmp_path: Path):
    """License is omitted when not in wheel metadata."""
    output_dir = tmp_path / "output"

    result = convert_wheel(minimal_wheel, output_dir)

    index = _extract_info_file(result.path, "info/index.json")
    index_data = json.loads(index)

    assert "license" not in index_data


def test_convert_wheel_paths_json_format(sample_wheel: Path, tmp_path: Path):
    """paths.json has correct structure."""
    output_dir = tmp_path / "output"

    result = convert_wheel(sample_wheel, output_dir)

    paths = _extract_info_file(result.path, "info/paths.json")
    paths_data = json.loads(paths)

    assert "paths" in paths_data
    assert "paths_version" in paths_data
    assert paths_data["paths_version"] == 1


def test_convert_wheel_paths_json_entry_format(sample_wheel: Path, tmp_path: Path):
    """paths.json entries have required fields."""
    output_dir = tmp_path / "output"

    result = convert_wheel(sample_wheel, output_dir)

    paths = _extract_info_file(result.path, "info/paths.json")
    paths_data = json.loads(paths)

    entry = paths_data["paths"][0]
    assert "_path" in entry
    assert "path_type" in entry
    assert "sha256" in entry
    assert "size_in_bytes" in entry
    assert entry["path_type"] == "hardlink"


def test_convert_wheel_installer_is_conda(sample_wheel: Path, tmp_path: Path):
    """INSTALLER file contains 'conda'."""
    output_dir = tmp_path / "output"

    result = convert_wheel(sample_wheel, output_dir)

    paths = _extract_info_file(result.path, "info/paths.json")
    paths_data = json.loads(paths)

    installer_paths = [p for p in paths_data["paths"] if "INSTALLER" in p["_path"]]
    assert len(installer_paths) == 1

    expected_hash = hashlib.sha256(b"conda\n").hexdigest()
    assert installer_paths[0]["sha256"] == expected_hash


def test_convert_wheel_original_installer_replaced(sample_wheel: Path, tmp_path: Path):
    """Original wheel INSTALLER is replaced with 'conda'."""
    output_dir = tmp_path / "output"

    result = convert_wheel(sample_wheel, output_dir)

    pkg_content = _extract_pkg_file(
        result.path, "site-packages/sample_package-2.0.0.dist-info/INSTALLER"
    )
    assert pkg_content == b"conda\n"


def test_convert_wheel_about_json_summary(sample_wheel: Path, tmp_path: Path):
    """about.json includes package summary."""
    output_dir = tmp_path / "output"

    result = convert_wheel(sample_wheel, output_dir)

    about = _extract_info_file(result.path, "info/about.json")
    about_data = json.loads(about)

    assert about_data["summary"] == "A sample package for testing"


def test_convert_wheel_about_json_urls(sample_wheel: Path, tmp_path: Path):
    """about.json includes project URLs."""
    output_dir = tmp_path / "output"

    result = convert_wheel(sample_wheel, output_dir)

    about = _extract_info_file(result.path, "info/about.json")
    about_data = json.loads(about)

    assert about_data["home"] == "https://example.com"
    assert about_data["doc_url"] == "https://docs.example.com"
    assert about_data["source_url"] == "https://github.com/example/sample"


def test_convert_wheel_about_json_description(sample_wheel: Path, tmp_path: Path):
    """about.json includes long description."""
    output_dir = tmp_path / "output"

    result = convert_wheel(sample_wheel, output_dir)

    about = _extract_info_file(result.path, "info/about.json")
    about_data = json.loads(about)

    assert "# Sample Package" in about_data["description"]


def test_convert_wheel_link_json(sample_wheel: Path, tmp_path: Path):
    """link.json marks package as noarch python."""
    output_dir = tmp_path / "output"

    result = convert_wheel(sample_wheel, output_dir)

    link = _extract_info_file(result.path, "info/link.json")
    link_data = json.loads(link)

    assert link_data["noarch"]["type"] == "python"
    assert link_data["package_metadata_version"] == 1


def test_convert_wheel_files_list(sample_wheel: Path, tmp_path: Path):
    """info/files lists all package files."""
    output_dir = tmp_path / "output"

    result = convert_wheel(sample_wheel, output_dir)

    files = _extract_info_file(result.path, "info/files")
    files_list = files.decode().strip().split("\n")

    assert any("site-packages" in f for f in files_list)


def test_convert_wheel_creates_output_directory(sample_wheel: Path, tmp_path: Path):
    """Output directory is created if it doesn't exist."""
    output_dir = tmp_path / "nested" / "output" / "dir"

    result = convert_wheel(sample_wheel, output_dir)

    assert output_dir.exists()
    assert result.path.exists()


def test_convert_wheel_converts_python_version_marker(tmp_wheel, tmp_path: Path):
    """Python version markers are converted to conda conditions."""
    wheel_path = tmp_wheel(
        name="marker_pkg",
        version="1.0.0",
        requires_dist=["typing-extensions; python_version < '3.11'"],
    )
    output_dir = tmp_path / "output"

    result = convert_wheel(wheel_path, output_dir)

    index = _extract_info_file(result.path, "info/index.json")
    index_data = json.loads(index)
    deps = index_data["depends"]

    typing_dep = next(d for d in deps if "typing-extensions" in d)
    assert "; if python <3.11" in typing_dep


def test_convert_wheel_converts_platform_marker(tmp_wheel, tmp_path: Path):
    """Platform markers are converted to conda conditions."""
    wheel_path = tmp_wheel(
        name="platform_pkg",
        version="1.0.0",
        requires_dist=["pywin32; sys_platform == 'win32'"],
    )
    output_dir = tmp_path / "output"

    result = convert_wheel(wheel_path, output_dir)

    index = _extract_info_file(result.path, "info/index.json")
    index_data = json.loads(index)
    deps = index_data["depends"]

    pywin32_dep = next(d for d in deps if "pywin32" in d)
    assert "; if __win" in pywin32_dep


def test_convert_wheel_converts_extras_in_dependency(tmp_wheel, tmp_path: Path):
    """Extras in dependencies are converted to conda format."""
    wheel_path = tmp_wheel(
        name="extras_pkg",
        version="1.0.0",
        requires_dist=["requests[security]>=2.0"],
    )
    output_dir = tmp_path / "output"

    result = convert_wheel(wheel_path, output_dir)

    index = _extract_info_file(result.path, "info/index.json")
    index_data = json.loads(index)
    deps = index_data["depends"]

    requests_dep = next(d for d in deps if "requests" in d)
    assert requests_dep == "requests[extras=[security]] >=2.0"


def test_convert_wheel_collects_extras(tmp_wheel, tmp_path: Path):
    """Extra-conditional dependencies are collected into extras field."""
    wheel_path = tmp_wheel(
        name="extras_pkg",
        version="1.0.0",
        requires_dist=[
            "requests>=2.0",
            "pytest; extra == 'test'",
            "sphinx; extra == 'docs'",
            "black; extra == 'dev'",
            "isort; extra == 'dev'",
        ],
    )
    output_dir = tmp_path / "output"

    result = convert_wheel(wheel_path, output_dir)

    # Check ConversionResult
    assert "test" in result.extra_depends
    assert "docs" in result.extra_depends
    assert "dev" in result.extra_depends

    # Check index.json
    index = _extract_info_file(result.path, "info/index.json")
    index_data = json.loads(index)

    deps = index_data["depends"]
    assert any("requests" in d for d in deps)
    assert not any("pytest" in d for d in deps)
    assert not any("sphinx" in d for d in deps)

    extra_depends = index_data["extra_depends"]
    assert "test" in extra_depends
    assert "pytest" in extra_depends["test"]
    assert "docs" in extra_depends
    assert "sphinx" in extra_depends["docs"]
    assert "dev" in extra_depends
    assert len(extra_depends["dev"]) == 2


def test_convert_wheel_extras_with_conditions(tmp_wheel, tmp_path: Path):
    """Extras can have conditional dependencies."""
    wheel_path = tmp_wheel(
        name="extras_cond_pkg",
        version="1.0.0",
        requires_dist=[
            "requests>=2.0",
            "pywin32; extra == 'dev' and sys_platform == 'win32'",
            "pytest; extra == 'test'",
        ],
    )
    output_dir = tmp_path / "output"

    result = convert_wheel(wheel_path, output_dir)

    index = _extract_info_file(result.path, "info/index.json")
    index_data = json.loads(index)

    extra_depends = index_data["extra_depends"]
    assert "dev" in extra_depends
    pywin32_dep = extra_depends["dev"][0]
    assert "pywin32" in pywin32_dep
    assert "; if __win" in pywin32_dep

    assert "test" in extra_depends
    pytest_dep = extra_depends["test"][0]
    assert pytest_dep == "pytest"
    assert "; if" not in pytest_dep


def test_convert_wheel_from_iterable(sample_wheel: Path, tmp_path: Path):
    """convert_wheel works with iterable of bytes."""
    output_dir = tmp_path / "output"

    def read_chunks():
        with open(sample_wheel, "rb") as f:
            while chunk := f.read(65536):
                yield chunk

    result = convert_wheel(read_chunks(), output_dir, filename=sample_wheel.name)

    assert result.path.exists()
    assert result.name == "sample-package"


def test_convert_wheel_requires_filename_for_iterable(
    sample_wheel: Path, tmp_path: Path
):
    """convert_wheel raises error if filename missing for iterable."""
    output_dir = tmp_path / "output"

    def read_chunks():
        with open(sample_wheel, "rb") as f:
            while chunk := f.read(65536):
                yield chunk

    with pytest.raises(ValueError, match="filename is required"):
        convert_wheel(read_chunks(), output_dir)
