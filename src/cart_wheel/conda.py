"""Conda package building utilities."""

import hashlib
import io
import json
import re
import tarfile
from pathlib import Path
from zipfile import ZipFile, ZIP_STORED

import zstandard as zstd
from packaging.markers import Marker
from packaging.requirements import Requirement

from .wheel import WheelMetadata


class DependencyConversionError(Exception):
    """Raised when a wheel dependency cannot be converted to conda format."""

    pass


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
    """Convert a single marker comparison to conda condition.

    Args:
        variable: The marker variable (e.g., 'python_version', 'sys_platform')
        op: The operator (e.g., '==', '>=', '<')
        value: The comparison value

    Returns:
        Conda condition string for this atom

    Raises:
        DependencyConversionError: If the marker variable is unsupported
    """
    if variable == "python_version":
        return f"python {op}{value}"

    if variable == "sys_platform":
        if op == "==":
            if value in _PLATFORM_MAP:
                return _PLATFORM_MAP[value]
            raise DependencyConversionError(
                f"Unknown sys_platform value: {value}"
            )
        elif op == "!=":
            if value in _PLATFORM_MAP:
                return f"not {_PLATFORM_MAP[value]}"
            raise DependencyConversionError(
                f"Unknown sys_platform value: {value}"
            )

    if variable == "platform_system":
        if op == "==":
            if value in _PLATFORM_SYSTEM_MAP:
                return _PLATFORM_SYSTEM_MAP[value]
            raise DependencyConversionError(
                f"Unknown platform_system value: {value}"
            )
        elif op == "!=":
            if value in _PLATFORM_SYSTEM_MAP:
                return f"not {_PLATFORM_SYSTEM_MAP[value]}"
            raise DependencyConversionError(
                f"Unknown platform_system value: {value}"
            )

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
        # platform_version alone cannot be converted - needs platform context
        # Return a placeholder that will be combined with platform flag later
        return f"__PLATFORM_VERSION__{op}{value}"

    # Unsupported marker variable
    raise DependencyConversionError(
        f"Cannot convert marker variable '{variable}': unsupported"
    )


def _convert_marker_tree(tree: list) -> str:
    """Recursively convert a marker tree to conda condition string.

    Args:
        tree: The marker tree (list of atoms, operators, and nested lists)

    Returns:
        Conda condition string
    """
    # First pass: convert all items
    converted_items = []  # List of (type, value) tuples
    platform_flags = []  # Track indices of platform flags
    platform_version_idx = None
    platform_version_value = None

    for item in tree:
        if isinstance(item, list):
            # Nested expression
            converted_items.append(("part", f"({_convert_marker_tree(item)})"))
        elif isinstance(item, str):
            # Boolean operator ('and' or 'or')
            converted_items.append(("op", item))
        elif isinstance(item, tuple) and len(item) == 3:
            # Comparison: (Variable, Op, Value)
            var_obj, op_obj, val_obj = item
            variable = str(var_obj)
            op = str(op_obj)
            value = str(val_obj)

            converted = _convert_marker_atom(variable, op, value)

            if converted in ("__win", "__linux", "__osx", "__unix"):
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

    # Handle platform_version combination
    if platform_version_value is not None:
        if len(platform_flags) == 1:
            # Combine single platform flag with version
            flag_idx = platform_flags[0]
            flag_value = converted_items[flag_idx][1]
            combined = f"{flag_value} {platform_version_value}"

            # Build result, skipping the separate platform and version entries
            # and the 'and' operator between them
            result_parts = []
            skip_indices = {flag_idx, platform_version_idx}

            # Also skip the operator between platform and version
            min_idx = min(flag_idx, platform_version_idx)
            max_idx = max(flag_idx, platform_version_idx)
            for i in range(min_idx + 1, max_idx):
                if converted_items[i][0] == "op":
                    skip_indices.add(i)
                    break

            for i, (item_type, item_value) in enumerate(converted_items):
                if i in skip_indices:
                    if i == flag_idx:
                        # Insert the combined value at the platform flag position
                        result_parts.append(combined)
                    continue
                result_parts.append(item_value)

            return " ".join(result_parts)
        elif len(platform_flags) == 0:
            raise DependencyConversionError(
                "platform_version requires a platform marker (sys_platform, platform_system, or os_name)"
            )
        # Multiple platform flags with version - unusual, just include everything

    # Normal case: just build the result
    result_parts = []
    for item_type, item_value in converted_items:
        if item_type == "version":
            raise DependencyConversionError(
                "platform_version requires a platform marker (sys_platform, platform_system, or os_name)"
            )
        result_parts.append(item_value)

    return " ".join(result_parts)


def _marker_to_condition(marker: Marker) -> str:
    """Convert a PEP 508 environment marker to a conda condition string.

    Args:
        marker: A packaging.markers.Marker object

    Returns:
        A conda condition string (without the leading '; if ')

    Raises:
        DependencyConversionError: If the marker cannot be converted
    """
    # Access the internal marker tree structure
    # _markers is a list containing tuples (Variable, Op, Value) and strings ('and', 'or')
    tree = marker._markers

    return _convert_marker_tree(tree)


def _extract_extra_from_marker(marker: Marker) -> tuple[str | None, str | None]:
    """Extract the extra name and any remaining condition from a marker.

    Args:
        marker: A packaging.markers.Marker object

    Returns:
        A tuple of (extra_name, remaining_condition).
        - If marker is purely `extra == 'name'`: ("name", None)
        - If marker is `extra == 'name' and <condition>`: ("name", "<condition>")
        - If marker doesn't involve extras: (None, None)
    """
    marker_str = str(marker)

    # Match patterns like: extra == 'dev' or extra == "dev" (pure extra)
    pure_match = re.fullmatch(r"extra\s*==\s*['\"]([^'\"]+)['\"]", marker_str.strip())
    if pure_match:
        return (pure_match.group(1), None)

    # Match patterns like: extra == 'dev' and <other conditions>
    # The marker could be: extra == 'dev' and python_version < '3.11'
    and_match = re.match(
        r"extra\s*==\s*['\"]([^'\"]+)['\"]\s+and\s+(.+)",
        marker_str.strip(),
    )
    if and_match:
        extra_name = and_match.group(1)
        remaining = and_match.group(2)
        return (extra_name, remaining)

    # Also check for: <other conditions> and extra == 'dev'
    and_match_reverse = re.match(
        r"(.+)\s+and\s+extra\s*==\s*['\"]([^'\"]+)['\"]",
        marker_str.strip(),
    )
    if and_match_reverse:
        remaining = and_match_reverse.group(1)
        extra_name = and_match_reverse.group(2)
        return (extra_name, remaining)

    return (None, None)


def _create_tar_zst(file_dict: dict[str, bytes]) -> bytes:
    """Create a zstd-compressed tarball from a dict of {path: content}."""
    tar_buffer = io.BytesIO()
    with tarfile.open(fileobj=tar_buffer, mode="w") as tar:
        for name, content in file_dict.items():
            info = tarfile.TarInfo(name=name)
            info.size = len(content)
            tar.addfile(info, io.BytesIO(content))

    tar_bytes = tar_buffer.getvalue()

    # Compress with zstd
    cctx = zstd.ZstdCompressor(level=19)
    return cctx.compress(tar_bytes)


def _requirement_to_conda_dep(req: Requirement, condition: str | None = None) -> str:
    """Convert a packaging Requirement to a conda dependency string.

    Args:
        req: A packaging.requirements.Requirement object
        condition: Optional conda condition string to append (e.g., "__win")

    Returns:
        A conda dependency string, optionally with extras and condition suffix

    Note:
        Extras in the requirement (e.g., requests[security]) are converted to
        conda format: requests[extras=[security]]
    """
    # Normalize name: lowercase, underscores to hyphens
    name = req.name.lower().replace("_", "-")

    # Build the dependency string
    dep = name

    # Add extras if present (e.g., requests[security] -> requests[extras=[security]])
    if req.extras:
        extras_list = ",".join(sorted(req.extras))
        dep = f"{dep}[extras=[{extras_list}]]"

    # Add version specifier if present
    if req.specifier:
        dep = f"{dep} {req.specifier}"

    # Add condition if provided
    if condition:
        dep = f"{dep}; if {condition}"

    return dep


def build_conda_package(
    metadata: WheelMetadata,
    output_dir: Path,
) -> Path:
    """Build a .conda package from wheel metadata.

    Args:
        metadata: Parsed wheel metadata
        output_dir: Directory to write the .conda file to

    Returns:
        Path to the created .conda file
    """
    if not metadata.wheel_path:
        raise ValueError("WheelMetadata must have wheel_path set")

    output_dir.mkdir(parents=True, exist_ok=True)

    # Collect files from wheel
    pkg_files: dict[str, bytes] = {}
    info_files: dict[str, bytes] = {}

    # Track file metadata for paths.json
    paths_data = []

    with ZipFile(metadata.wheel_path, "r") as whl:
        for zip_info in whl.infolist():
            if zip_info.is_dir():
                continue

            # Skip INSTALLER file - we'll add our own
            if zip_info.filename.endswith("/INSTALLER"):
                continue

            content = whl.read(zip_info.filename)

            # Determine destination path
            # Wheel files go into site-packages
            dest_path = f"site-packages/{zip_info.filename}"

            pkg_files[dest_path] = content

            # Calculate SHA256
            sha256 = hashlib.sha256(content).hexdigest()

            paths_data.append({
                "_path": dest_path,
                "path_type": "hardlink",
                "sha256": sha256,
                "size_in_bytes": len(content),
            })

    # Add INSTALLER file marking this as conda-installed
    dist_info_name = f"{metadata.name.replace('-', '_')}-{metadata.version}.dist-info"
    installer_path = f"site-packages/{dist_info_name}/INSTALLER"
    installer_content = b"conda\n"
    pkg_files[installer_path] = installer_content
    paths_data.append({
        "_path": installer_path,
        "path_type": "hardlink",
        "sha256": hashlib.sha256(installer_content).hexdigest(),
        "size_in_bytes": len(installer_content),
    })

    # Build info files
    # index.json - main metadata
    dependencies = []

    # Add python dependency
    if metadata.requires_python:
        dependencies.append(f"python {metadata.requires_python.replace(' ', '')}")
    else:
        dependencies.append("python")

    # Convert wheel dependencies to conda format
    # Also collect optional dependencies (extras)
    extras: dict[str, list[str]] = {}

    for req_str in metadata.dependencies:
        req = Requirement(req_str)

        if req.marker:
            # Check if marker involves an extra (optional dependency)
            extra_name, remaining_marker = _extract_extra_from_marker(req.marker)
            if extra_name:
                # This is an optional dependency for an extra
                # If there's a remaining condition, convert it
                condition = None
                if remaining_marker:
                    # Create a Marker object from the remaining string to convert it
                    remaining_marker_obj = Marker(remaining_marker)
                    condition = _marker_to_condition(remaining_marker_obj)

                conda_dep = _requirement_to_conda_dep(req, condition)
                if extra_name not in extras:
                    extras[extra_name] = []
                extras[extra_name].append(conda_dep)
                continue

            # Convert environment marker to conda condition
            condition = _marker_to_condition(req.marker)
            dependencies.append(_requirement_to_conda_dep(req, condition))
        else:
            dependencies.append(_requirement_to_conda_dep(req))

    index_json = {
        "name": metadata.conda_name,
        "version": metadata.version,
        "build": "py_0",  # Simple build string for wheel conversions
        "build_number": 0,
        "depends": dependencies,
        "subdir": metadata.conda_subdir,
    }

    if extras:
        index_json["extra_depends"] = extras

    if metadata.license:
        index_json["license"] = metadata.license

    if metadata.is_pure_python:
        index_json["noarch"] = "python"

    info_files["info/index.json"] = json.dumps(index_json, indent=2).encode("utf-8")

    # paths.json
    paths_json = {
        "paths": paths_data,
        "paths_version": 1,
    }
    info_files["info/paths.json"] = json.dumps(paths_json, indent=2).encode("utf-8")

    # files - simple list
    files_content = "\n".join(p["_path"] for p in paths_data)
    info_files["info/files"] = files_content.encode("utf-8")

    # about.json
    about_json = {}
    if metadata.summary:
        about_json["summary"] = metadata.summary
    if metadata.description:
        about_json["description"] = metadata.description
    if metadata.home_url:
        about_json["home"] = metadata.home_url
    if metadata.doc_url:
        about_json["doc_url"] = metadata.doc_url
    if metadata.dev_url:
        about_json["dev_url"] = metadata.dev_url
    if metadata.source_url:
        about_json["source_url"] = metadata.source_url
    info_files["info/about.json"] = json.dumps(about_json, indent=2).encode("utf-8")

    # link.json - for noarch python packages
    if metadata.is_pure_python:
        noarch_data: dict = {
            "type": "python",
        }
        if metadata.console_scripts or metadata.gui_scripts:
            noarch_data["entry_points"] = metadata.console_scripts + metadata.gui_scripts
        link_json = {
            "noarch": noarch_data,
            "package_metadata_version": 1,
        }
        info_files["info/link.json"] = json.dumps(link_json, indent=2).encode("utf-8")

    # Create the .conda file (ZIP with two tar.zst files inside)
    conda_name = f"{metadata.conda_name}-{metadata.version}-py_0.conda"
    conda_path = output_dir / conda_name

    # Create compressed archives
    info_tar_zst = _create_tar_zst(info_files)
    pkg_tar_zst = _create_tar_zst(pkg_files)

    # Write the .conda file
    with ZipFile(conda_path, "w", compression=ZIP_STORED) as conda_zip:
        # metadata.json at the root (required by .conda format)
        conda_metadata = {
            "conda_pkg_format_version": 2,
        }
        conda_zip.writestr("metadata.json", json.dumps(conda_metadata))

        # Info archive
        info_name = f"info-{metadata.conda_name}-{metadata.version}-py_0.tar.zst"
        conda_zip.writestr(info_name, info_tar_zst)

        # Package archive
        pkg_name = f"pkg-{metadata.conda_name}-{metadata.version}-py_0.tar.zst"
        conda_zip.writestr(pkg_name, pkg_tar_zst)

    return conda_path
