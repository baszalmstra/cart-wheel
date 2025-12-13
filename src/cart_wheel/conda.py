"""Conda package building utilities."""

import io
import json
import re
import tempfile
import zipfile
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path

import zstandard as zstd
from packaging.markers import Marker
from packaging.requirements import Requirement

from .streaming import FileMetadata, StreamingTarZstWriter
from .wheel import parse_wheel_metadata


class DependencyConversionError(Exception):
    """Raised when a wheel dependency cannot be converted to conda format."""

    pass


@dataclass
class ConversionResult:
    """Result of converting a wheel to conda package."""

    path: Path
    name: str
    version: str
    dependencies: list[str] = field(default_factory=list)
    extra_depends: dict[str, list[str]] = field(default_factory=dict)
    entry_points: list[str] = field(default_factory=list)
    subdir: str = "noarch"
    original_requirements: list[str] = field(default_factory=list)


# Platform marker mappings to conda platform flags
_PLATFORM_MAP = {
    "win32": "__win",
    "linux": "__linux",
    "darwin": "__osx",
    "cygwin": "__win",
}

_PLATFORM_SYSTEM_MAP = {
    "Windows": "__win",
    "Linux": "__linux",
    "Darwin": "__osx",
}


def _convert_marker_atom(variable: str, op: str, value: str) -> str:
    """Convert a single marker comparison to conda condition."""
    if variable == "python_version":
        return f"python {op}{value}"

    if variable == "sys_platform":
        if op == "==":
            if value in _PLATFORM_MAP:
                return _PLATFORM_MAP[value]
            raise DependencyConversionError(f"Unknown sys_platform value: {value}")
        elif op == "!=":
            if value in _PLATFORM_MAP:
                return f"not {_PLATFORM_MAP[value]}"
            raise DependencyConversionError(f"Unknown sys_platform value: {value}")

    if variable == "platform_system":
        if op == "==":
            if value in _PLATFORM_SYSTEM_MAP:
                return _PLATFORM_SYSTEM_MAP[value]
            raise DependencyConversionError(f"Unknown platform_system value: {value}")
        elif op == "!=":
            if value in _PLATFORM_SYSTEM_MAP:
                return f"not {_PLATFORM_SYSTEM_MAP[value]}"
            raise DependencyConversionError(f"Unknown platform_system value: {value}")

    if variable == "os_name":
        if op == "==":
            if value == "nt":
                return "__win"
            elif value == "posix":
                return "__unix"
        elif op == "!=":
            if value == "nt":
                return "__unix"

    if variable == "platform_version":
        return f"__PLATFORM_VERSION__{op}{value}"

    if variable == "platform_python_implementation":
        # Conda packages are typically for CPython
        # Include dep unconditionally for CPython, skip for other implementations
        if op == "==":
            if value.lower() == "cpython":
                return "__CPYTHON_ALWAYS"  # Special marker to include unconditionally
            else:
                return "__SKIP_DEP"  # Skip deps for PyPy, etc.
        elif op == "!=":
            if value.lower() == "cpython":
                return "__SKIP_DEP"  # Skip if NOT CPython
            else:
                return "__CPYTHON_ALWAYS"  # Include if NOT other implementations

    if variable == "implementation_name":
        # Similar to platform_python_implementation
        if op == "==":
            if value.lower() == "cpython":
                return "__CPYTHON_ALWAYS"
            else:
                return "__SKIP_DEP"
        elif op == "!=":
            if value.lower() == "cpython":
                return "__SKIP_DEP"
            else:
                return "__CPYTHON_ALWAYS"

    raise DependencyConversionError(
        f"Cannot convert marker variable '{variable}': unsupported"
    )


def _convert_marker_tree(tree: list) -> str:
    """Recursively convert a marker tree to conda condition string."""
    converted_items = []
    platform_flags = []
    platform_version_idx = None
    platform_version_value = None
    has_skip_dep = False
    has_cpython_always = False

    for item in tree:
        if isinstance(item, list):
            sub_result = _convert_marker_tree(item)
            # Propagate special markers from sub-expressions
            if sub_result == "__SKIP_DEP":
                has_skip_dep = True
            elif sub_result == "__CPYTHON_ALWAYS":
                has_cpython_always = True
            else:
                converted_items.append(("part", f"({sub_result})"))
        elif isinstance(item, str):
            converted_items.append(("op", item))
        elif isinstance(item, tuple) and len(item) == 3:
            var_obj, op_obj, val_obj = item
            variable = str(var_obj)
            op = str(op_obj)
            value = str(val_obj)

            converted = _convert_marker_atom(variable, op, value)

            if converted == "__SKIP_DEP":
                has_skip_dep = True
            elif converted == "__CPYTHON_ALWAYS":
                has_cpython_always = True
            elif converted in ("__win", "__linux", "__osx", "__unix"):
                platform_flags.append(len(converted_items))
                converted_items.append(("platform", converted))
            elif converted.startswith("__PLATFORM_VERSION__"):
                platform_version_idx = len(converted_items)
                platform_version_value = converted.replace("__PLATFORM_VERSION__", "")
                converted_items.append(("version", converted))
            else:
                converted_items.append(("part", converted))
        else:
            converted_items.append(("part", str(item)))

    # Handle special CPython markers
    # If we have an "and" with __SKIP_DEP, skip the whole thing
    # If we have an "and" with __CPYTHON_ALWAYS, just return the other conditions
    # If we only have the special marker, return it directly
    if has_skip_dep:
        # Any "and" with skip means skip; "or" would need more complex handling
        # For simplicity, if skip_dep appears at all, we skip
        return "__SKIP_DEP"
    if has_cpython_always and len(converted_items) == 0:
        # Only the CPython marker, no other conditions
        return "__CPYTHON_ALWAYS"

    if platform_version_value is not None:
        if len(platform_flags) == 1:
            flag_idx = platform_flags[0]
            flag_value = converted_items[flag_idx][1]
            combined = f"{flag_value} {platform_version_value}"

            result_parts = []
            skip_indices = {flag_idx, platform_version_idx}

            min_idx = min(flag_idx, platform_version_idx)
            max_idx = max(flag_idx, platform_version_idx)
            for i in range(min_idx + 1, max_idx):
                if converted_items[i][0] == "op":
                    skip_indices.add(i)
                    break

            for i, (_item_type, item_value) in enumerate(converted_items):
                if i in skip_indices:
                    if i == flag_idx:
                        result_parts.append(combined)
                    continue
                result_parts.append(item_value)

            return " ".join(result_parts)
        elif len(platform_flags) == 0:
            raise DependencyConversionError(
                "platform_version requires a platform marker"
            )

    result_parts = []
    for item_type, item_value in converted_items:
        if item_type == "version":
            raise DependencyConversionError(
                "platform_version requires a platform marker"
            )
        result_parts.append(item_value)

    return " ".join(result_parts)


def _marker_to_condition(marker: Marker) -> str:
    """Convert a PEP 508 environment marker to a conda condition string."""
    return _convert_marker_tree(marker._markers)


def _extract_extra_from_marker(marker: Marker) -> tuple[str | None, str | None]:
    """Extract the extra name and any remaining condition from a marker."""
    marker_str = str(marker)

    pure_match = re.fullmatch(r"extra\s*==\s*['\"]([^'\"]+)['\"]", marker_str.strip())
    if pure_match:
        return (pure_match.group(1), None)

    and_match = re.match(
        r"extra\s*==\s*['\"]([^'\"]+)['\"]\s+and\s+(.+)",
        marker_str.strip(),
    )
    if and_match:
        return (and_match.group(1), and_match.group(2))

    and_match_reverse = re.match(
        r"(.+)\s+and\s+extra\s*==\s*['\"]([^'\"]+)['\"]",
        marker_str.strip(),
    )
    if and_match_reverse:
        return (and_match_reverse.group(2), and_match_reverse.group(1))

    return (None, None)


def _requirement_to_conda_dep(req: Requirement, condition: str | None = None) -> str:
    """Convert a packaging Requirement to a conda dependency string."""
    name = req.name.lower().replace("_", "-")
    dep = name

    if req.extras:
        extras_list = ",".join(sorted(req.extras))
        dep = f"{dep}[extras=[{extras_list}]]"

    if req.specifier:
        dep = f"{dep} {req.specifier}"

    if condition:
        dep = f"{dep}; if {condition}"

    return dep


def _create_tar_zst(file_dict: dict[str, bytes]) -> bytes:
    """Create a zstd-compressed tarball from a dict of {path: content}."""
    import tarfile

    tar_buffer = io.BytesIO()
    with tarfile.open(fileobj=tar_buffer, mode="w") as tar:
        for name, content in file_dict.items():
            info = tarfile.TarInfo(name=name)
            info.size = len(content)
            tar.addfile(info, io.BytesIO(content))

    cctx = zstd.ZstdCompressor(level=19)
    return cctx.compress(tar_buffer.getvalue())


def _convert_dependencies(
    dependencies: list[str],
    requires_python: str | None,
) -> tuple[list[str], dict[str, list[str]]]:
    """Convert wheel dependencies to conda format.

    Returns:
        Tuple of (main_dependencies, extra_depends)
    """
    conda_deps = []
    extras: dict[str, list[str]] = {}

    # Add python dependency
    if requires_python:
        conda_deps.append(f"python {requires_python.replace(' ', '')}")
    else:
        conda_deps.append("python")

    for req_str in dependencies:
        req = Requirement(req_str)

        if req.marker:
            extra_name, remaining_marker = _extract_extra_from_marker(req.marker)
            if extra_name:
                condition = None
                if remaining_marker:
                    remaining_marker_obj = Marker(remaining_marker)
                    condition = _marker_to_condition(remaining_marker_obj)
                    # Handle special markers for CPython
                    if condition == "__SKIP_DEP":
                        continue  # Skip this dependency
                    elif condition == "__CPYTHON_ALWAYS":
                        condition = None  # Include unconditionally

                conda_dep = _requirement_to_conda_dep(req, condition)
                if extra_name not in extras:
                    extras[extra_name] = []
                extras[extra_name].append(conda_dep)
                continue

            condition = _marker_to_condition(req.marker)
            # Handle special markers for CPython
            if condition == "__SKIP_DEP":
                continue  # Skip this dependency
            elif condition == "__CPYTHON_ALWAYS":
                condition = None  # Include unconditionally
            conda_deps.append(_requirement_to_conda_dep(req, condition))
        else:
            conda_deps.append(_requirement_to_conda_dep(req))

    return conda_deps, extras


def _iter_file(path: Path, chunk_size: int = 65536) -> Iterable[bytes]:
    """Yield chunks from a file."""
    with open(path, "rb") as f:
        while chunk := f.read(chunk_size):
            yield chunk


class _ChunksAsFile:
    """Wrap an iterator of bytes chunks as a file-like object."""

    def __init__(self, chunks: Iterable[bytes]):
        self._chunks = iter(chunks)
        self._buffer = b""

    def read(self, size: int = -1) -> bytes:
        if size < 0:
            result = self._buffer + b"".join(self._chunks)
            self._buffer = b""
            return result

        while len(self._buffer) < size:
            try:
                self._buffer += next(self._chunks)
            except StopIteration:
                break

        result = self._buffer[:size]
        self._buffer = self._buffer[size:]
        return result


def convert_wheel(
    source: Path | Iterable[bytes],
    output_dir: Path,
    filename: str | None = None,
) -> ConversionResult:
    """Convert a wheel to conda package in single streaming pass.

    Args:
        source: Path to wheel file or iterable of bytes chunks.
        output_dir: Directory to write the .conda file to.
        filename: Original wheel filename (required if source is an iterable).

    Returns:
        ConversionResult with path and metadata of created package.
    """
    from stream_unzip import stream_unzip

    output_dir.mkdir(parents=True, exist_ok=True)

    # Determine filename and source chunks
    if isinstance(source, Path):
        wheel_filename = source.name
        chunks = _iter_file(source)
    else:
        if filename is None:
            raise ValueError("filename is required when source is an iterable")
        wheel_filename = filename
        chunks = source

    # Use temp file for pkg archive (handles large wheels)
    # Create temp file and close immediately (Windows keeps files locked)
    pkg_tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".tar.zst")
    pkg_tmp_path = Path(pkg_tmp.name)
    pkg_tmp.close()

    try:
        buffered_metadata: dict[str, bytes] = {}
        file_metadata: list[FileMetadata] = []
        dist_info_prefix: str | None = None

        # Stream through wheel, writing to pkg archive
        with open(pkg_tmp_path, "wb") as pkg_file:
            with StreamingTarZstWriter(pkg_file) as pkg_writer:
                # stream_unzip yields (filename_bytes, size, file_chunks) tuples
                for file_name_bytes, file_size, file_chunks in stream_unzip(chunks):
                    # Decode filename (ZIP filenames are UTF-8 or CP437)
                    file_name = file_name_bytes.decode("utf-8")

                    # Skip directories
                    if file_name.endswith("/"):
                        continue

                    # Detect dist-info prefix from first .dist-info file
                    if dist_info_prefix is None and ".dist-info/" in file_name:
                        dist_info_prefix = file_name.split("/")[0]

                    # Skip INSTALLER - we add our own
                    if file_name.endswith("/INSTALLER"):
                        # Must consume chunks even if skipping
                        for _ in file_chunks:
                            pass
                        continue

                    dest_path = f"site-packages/{file_name}"

                    # Check if this is a metadata file we need to buffer
                    is_metadata = dist_info_prefix is not None and file_name in {
                        f"{dist_info_prefix}/METADATA",
                        f"{dist_info_prefix}/WHEEL",
                        f"{dist_info_prefix}/entry_points.txt",
                    }

                    if is_metadata:
                        # Buffer small metadata files
                        content = b"".join(file_chunks)
                        buffered_metadata[file_name] = content
                        pkg_writer.add_file(dest_path, content)
                    elif file_size is not None:
                        # Size known, can stream directly
                        pkg_writer.add_stream(
                            dest_path, _ChunksAsFile(file_chunks), file_size
                        )
                    else:
                        # Size unknown, must buffer
                        content = b"".join(file_chunks)
                        pkg_writer.add_file(dest_path, content)

                if dist_info_prefix is None:
                    raise ValueError("No .dist-info directory found in wheel")

                # Add INSTALLER file
                installer_path = f"site-packages/{dist_info_prefix}/INSTALLER"
                pkg_writer.add_file(installer_path, b"conda\n")

                file_metadata = pkg_writer.get_file_metadata()

            # Parse metadata from buffered content
            metadata_content = buffered_metadata.get(f"{dist_info_prefix}/METADATA")
            wheel_content = buffered_metadata.get(f"{dist_info_prefix}/WHEEL")
            entry_points_content = buffered_metadata.get(
                f"{dist_info_prefix}/entry_points.txt"
            )

            if not metadata_content or not wheel_content:
                raise ValueError("Missing required METADATA or WHEEL file")

            wheel_metadata = parse_wheel_metadata(
                metadata_content=metadata_content,
                wheel_content=wheel_content,
                entry_points_content=entry_points_content,
                filename=wheel_filename,
            )

            # Convert dependencies
            dependencies, extra_depends = _convert_dependencies(
                wheel_metadata.dependencies,
                wheel_metadata.requires_python,
            )

            # Build info archive
            info_files: dict[str, bytes] = {}

            # index.json
            index_json: dict = {
                "name": wheel_metadata.conda_name,
                "version": wheel_metadata.version,
                "build": "py_0",
                "build_number": 0,
                "depends": dependencies,
                "subdir": wheel_metadata.conda_subdir,
            }

            if extra_depends:
                index_json["extra_depends"] = extra_depends

            if wheel_metadata.license:
                index_json["license"] = wheel_metadata.license

            if wheel_metadata.is_pure_python:
                index_json["noarch"] = "python"

            info_files["info/index.json"] = json.dumps(index_json, indent=2).encode()

            # paths.json
            paths_json = {
                "paths": [
                    {
                        "_path": fm.path,
                        "path_type": "hardlink",
                        "sha256": fm.sha256,
                        "size_in_bytes": fm.size,
                    }
                    for fm in file_metadata
                ],
                "paths_version": 1,
            }
            info_files["info/paths.json"] = json.dumps(paths_json, indent=2).encode()

            # files list
            files_content = "\n".join(fm.path for fm in file_metadata)
            info_files["info/files"] = files_content.encode()

            # about.json
            about_json: dict = {}
            if wheel_metadata.summary:
                about_json["summary"] = wheel_metadata.summary
            if wheel_metadata.description:
                about_json["description"] = wheel_metadata.description
            if wheel_metadata.home_url:
                about_json["home"] = wheel_metadata.home_url
            if wheel_metadata.doc_url:
                about_json["doc_url"] = wheel_metadata.doc_url
            if wheel_metadata.dev_url:
                about_json["dev_url"] = wheel_metadata.dev_url
            if wheel_metadata.source_url:
                about_json["source_url"] = wheel_metadata.source_url
            info_files["info/about.json"] = json.dumps(about_json, indent=2).encode()

            # link.json
            if wheel_metadata.is_pure_python:
                noarch_data: dict = {"type": "python"}
                entry_points = (
                    wheel_metadata.console_scripts + wheel_metadata.gui_scripts
                )
                if entry_points:
                    noarch_data["entry_points"] = entry_points
                link_json = {
                    "noarch": noarch_data,
                    "package_metadata_version": 1,
                }
                info_files["info/link.json"] = json.dumps(link_json, indent=2).encode()

            info_tar_zst = _create_tar_zst(info_files)

            # Write final .conda file
            conda_name = (
                f"{wheel_metadata.conda_name}-{wheel_metadata.version}-py_0.conda"
            )
            conda_path = output_dir / conda_name

            with zipfile.ZipFile(
                conda_path, "w", compression=zipfile.ZIP_STORED
            ) as conda_zip:
                conda_zip.writestr(
                    "metadata.json", json.dumps({"conda_pkg_format_version": 2})
                )

                info_name = f"info-{wheel_metadata.conda_name}-{wheel_metadata.version}-py_0.tar.zst"
                conda_zip.writestr(info_name, info_tar_zst)

                pkg_name = f"pkg-{wheel_metadata.conda_name}-{wheel_metadata.version}-py_0.tar.zst"
                conda_zip.write(pkg_tmp_path, pkg_name)

            entry_points = wheel_metadata.console_scripts + wheel_metadata.gui_scripts

        return ConversionResult(
            path=conda_path,
            name=wheel_metadata.conda_name,
            version=wheel_metadata.version,
            dependencies=dependencies,
            extra_depends=extra_depends,
            entry_points=entry_points,
            subdir=wheel_metadata.conda_subdir,
            original_requirements=wheel_metadata.dependencies,
        )
    finally:
        # Cleanup temp file
        pkg_tmp_path.unlink(missing_ok=True)
