"""Command-line interface for cart-wheel."""

from __future__ import annotations

import argparse
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn

from .channel import index_channel
from .conda import DependencyConversionError, convert_wheel
from .pypi import PyPIError, download_wheel, get_matching_versions, select_best_wheel
from .state import (
    Dependencies,
    WheelState,
    list_packages,
    load_state,
    save_state,
    validate_all_dependencies,
)
from .sync import check_for_updates, sync_all, sync_package

console = Console()

# Default paths (relative to CWD)
DEFAULT_PACKAGES_DIR = Path("packages")
DEFAULT_STATE_DIR = Path("state")
DEFAULT_OUTPUT_DIR = Path("output")

# Conda mapping API
CONDA_MAPPING_URL = "https://conda-mapping.prefix.dev/pypi-to-conda-v1/conda-forge"


def lookup_conda_mapping(pypi_name: str) -> str | None:
    """Look up the conda-forge package name for a PyPI package.

    Uses the prefix.dev mapping API to find the corresponding conda package.

    Args:
        pypi_name: PyPI package name (normalized)

    Returns:
        Conda-forge package name if found, None otherwise
    """
    from .http import get_cached_client

    normalized = pypi_name.lower().replace("_", "-")
    url = f"{CONDA_MAPPING_URL}/{normalized}.json"

    try:
        client = get_cached_client()
        response = client.get(url)

        if response.status_code == 404:
            return None

        if response.status_code != 200:
            return None

        data = response.json()
        conda_versions = data.get("conda_versions", {})

        # Get any conda package name from the versions
        for version_packages in conda_versions.values():
            if version_packages:
                return version_packages[0]

        return None
    except Exception:
        # Silently fail - we'll fall back to prompting the user
        return None


def cmd_convert(args: argparse.Namespace) -> int:
    """Convert a wheel to conda package."""
    wheel_path: Path = args.wheel
    output_dir: Path = args.output_dir

    if not wheel_path.exists():
        print(f"Error: Wheel file not found: {wheel_path}", file=sys.stderr)
        return 1

    if not wheel_path.suffix == ".whl":
        print(
            f"Error: File does not appear to be a wheel: {wheel_path}", file=sys.stderr
        )
        return 1

    try:
        if args.verbose:
            print(f"Converting wheel: {wheel_path}")

        result = convert_wheel(wheel_path, output_dir)

        if args.verbose:
            print(f"  Name: {result.name}")
            print(f"  Version: {result.version}")
            print(f"  Subdir: {result.subdir}")
            print(f"  Dependencies: {len(result.dependencies)}")
            if result.entry_points:
                print(f"  Entry points: {len(result.entry_points)}")

        print(f"Created: {result.path}")
        return 0

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


def cmd_sync(args: argparse.Namespace) -> int:
    """Sync all packages."""
    packages_dir = args.packages_dir
    state_dir = args.state_dir
    output_dir = args.output_dir
    dry_run = args.dry_run
    quiet = args.quiet

    if not packages_dir.exists():
        print(f"Error: Packages directory not found: {packages_dir}", file=sys.stderr)
        return 1

    results = sync_all(
        packages_dir,
        state_dir,
        output_dir,
        dry_run=dry_run,
        show_progress=not quiet,
    )

    total_failed = sum(len(r.failed) for r in results)
    return 1 if total_failed > 0 else 0


def cmd_sync_package(args: argparse.Namespace) -> int:
    """Sync a single package."""
    package = args.package
    packages_dir = args.packages_dir
    state_dir = args.state_dir
    output_dir = args.output_dir
    dry_run = args.dry_run
    quiet = args.quiet

    result = sync_package(
        package,
        packages_dir,
        state_dir,
        output_dir,
        dry_run=dry_run,
        show_progress=not quiet,
    )

    return 1 if result.failed else 0


def cmd_check(args: argparse.Namespace) -> int:
    """Check for new versions on PyPI."""
    packages_dir = args.packages_dir
    state_dir = args.state_dir

    if not packages_dir.exists():
        print(f"Error: Packages directory not found: {packages_dir}", file=sys.stderr)
        return 1

    updates = check_for_updates(packages_dir, state_dir)

    if not updates:
        print("All packages are up to date.")
        return 0

    print("New versions available:")
    for package, versions in updates.items():
        print(f"  {package}: {', '.join(versions)}")

    return 0


def cmd_validate(args: argparse.Namespace) -> int:
    """Validate dependencies for all packages."""
    packages_dir = args.packages_dir
    state_dir = args.state_dir

    if not packages_dir.exists():
        print(f"Error: Packages directory not found: {packages_dir}", file=sys.stderr)
        return 1

    missing = validate_all_dependencies(state_dir, packages_dir)

    if not missing:
        print("All dependencies are satisfied.")
        return 0

    print("Missing dependencies:")
    for package, deps in missing.items():
        print(f"  {package}: {', '.join(deps)}")

    return 1


def cmd_index(args: argparse.Namespace) -> int:
    """Generate repodata.json for the channel."""
    output_dir = args.output_dir

    if not output_dir.exists():
        print(f"Error: Output directory not found: {output_dir}", file=sys.stderr)
        return 1

    index_channel(output_dir)
    print(f"Indexed channel: {output_dir}")
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    """Show status of all packages."""
    packages_dir = args.packages_dir
    state_dir = args.state_dir

    if not packages_dir.exists():
        print("No packages configured.")
        return 0

    packages = list_packages(packages_dir)

    if not packages:
        print("No packages configured.")
        return 0

    for package in packages:
        state = load_state(state_dir, package)
        converted = sum(1 for s in state.values() if s.status == "converted")
        failed = sum(1 for s in state.values() if s.status == "failed")
        pending = sum(1 for s in state.values() if s.status == "pending")

        status_parts = []
        if converted:
            status_parts.append(f"{converted} converted")
        if failed:
            status_parts.append(f"{failed} failed")
        if pending:
            status_parts.append(f"{pending} pending")

        status = ", ".join(status_parts) if status_parts else "no state"
        print(f"{package}: {status}")

    return 0


def _extract_dep_name(dep: str) -> str:
    """Extract package name from a dependency string."""
    from packaging.requirements import Requirement

    req = Requirement(dep)
    return req.name.lower().replace("_", "-")


def _is_required_dep(dep: str) -> bool:
    """Check if a dependency is required (not an extra/optional dependency)."""
    from packaging.requirements import Requirement
    import re

    req = Requirement(dep)
    if not req.marker:
        return True

    # Check if marker contains 'extra ==' which indicates optional dependency
    marker_str = str(req.marker)
    if re.search(r"extra\s*==\s*['\"]", marker_str):
        return False

    return True


@dataclass
class PackageInfo:
    """Information about a package to be added."""

    name: str  # Normalized name
    original_name: str  # Original PyPI name
    constraint: str
    wheels: list[tuple]  # List of (release, wheel) tuples
    wheel_dependencies: dict[str, list[str]]  # filename -> deps list
    required_deps: set[str]
    optional_deps: set[str]
    error: str | None = None
    conda_forge: str | None = None  # Conda-forge package name if mapped


def _fetch_package_info(
    package: str,
    constraint: str,
    max_versions: int,
    packages_dir: Path,
    force: bool,
    console: Console,
    indent: int = 0,
) -> PackageInfo | None:
    """Fetch and validate package info from PyPI without writing any files.

    Returns PackageInfo if successful, None if package already exists (and not force).
    Raises exception or returns PackageInfo with error field set on failure.
    """
    from .pypi import fetch_wheel_metadata
    from .wheel import parse_dependencies_from_metadata

    prefix = "  " * indent
    normalized_name = package.lower().replace("_", "-")

    # Check if package already exists
    config_path = packages_dir / f"{normalized_name}.toml"
    if config_path.exists() and not force:
        console.print(f"{prefix}[dim]Skipping {normalized_name} (already exists)[/]")
        return None

    console.print(f"{prefix}[bold]Checking:[/] {package}")

    # Fetch from PyPI
    try:
        releases = list(
            get_matching_versions(package, constraint or ">=0", max_versions=max_versions)
        )
    except PyPIError as e:
        return PackageInfo(
            name=normalized_name,
            original_name=package,
            constraint=constraint,
            wheels=[],
            wheel_dependencies={},
            required_deps=set(),
            optional_deps=set(),
            error=str(e),
        )

    if not releases:
        return PackageInfo(
            name=normalized_name,
            original_name=package,
            constraint=constraint,
            wheels=[],
            wheel_dependencies={},
            required_deps=set(),
            optional_deps=set(),
            error=f"No releases found matching constraint '{constraint or '>=0'}'",
        )

    console.print(f"{prefix}  Found [cyan]{len(releases)}[/] version(s)")

    # Select pure Python wheels only
    wheels_to_add = []
    skipped_versions = []
    for release in releases:
        pure_wheel = None
        for wheel in release.wheels:
            if "py3-none-any" in wheel.filename or "py2.py3-none-any" in wheel.filename:
                pure_wheel = wheel
                break

        if pure_wheel:
            wheels_to_add.append((release, pure_wheel))
        else:
            skipped_versions.append(release.version)

    if skipped_versions:
        console.print(
            f"{prefix}  [yellow]Warning:[/] Skipping {len(skipped_versions)} version(s) without pure Python wheel: "
            f"{', '.join(skipped_versions[:3])}{'...' if len(skipped_versions) > 3 else ''}"
        )

    if not wheels_to_add:
        return PackageInfo(
            name=normalized_name,
            original_name=package,
            constraint=constraint,
            wheels=[],
            wheel_dependencies={},
            required_deps=set(),
            optional_deps=set(),
            error="No pure Python wheels found (only platform-specific wheels available)",
        )

    # Fetch dependencies using PEP 658 metadata
    required_deps: set[str] = set()
    optional_deps: set[str] = set()
    wheel_dependencies: dict[str, list[str]] = {}

    for release, wheel in wheels_to_add:
        metadata = fetch_wheel_metadata(wheel.url)
        if metadata:
            deps = parse_dependencies_from_metadata(metadata)
            wheel_dependencies[wheel.filename] = deps
            for dep in deps:
                dep_name = _extract_dep_name(dep)
                if dep_name != "python":
                    if _is_required_dep(dep):
                        required_deps.add(dep_name)
                    else:
                        optional_deps.add(dep_name)
            console.print(f"{prefix}  [dim]Fetched metadata for {wheel.filename}[/]")
        else:
            console.print(f"{prefix}  [yellow]Warning:[/] No PEP 658 metadata for {wheel.filename}")

    return PackageInfo(
        name=normalized_name,
        original_name=package,
        constraint=constraint,
        wheels=wheels_to_add,
        wheel_dependencies=wheel_dependencies,
        required_deps=required_deps,
        optional_deps=optional_deps,
    )


def _write_package_files(
    info: PackageInfo,
    packages_dir: Path,
    state_dir: Path,
    console: Console,
) -> None:
    """Write package config and state files."""
    packages_dir.mkdir(parents=True, exist_ok=True)
    state_dir.mkdir(parents=True, exist_ok=True)

    # Build config content
    config_lines = [f"# Package configuration for {info.name}"]

    # Add conda_forge mapping if present
    if info.conda_forge:
        config_lines.append(f'conda_forge = "{info.conda_forge}"')

    # Add wheels section if we have wheels
    if info.wheels:
        config_lines.append(f'version_constraint = "{info.constraint}"')
        config_lines.append("skip_versions = []")
        config_lines.append("")
        config_lines.append("wheels = [")
        for release, wheel in info.wheels:
            config_lines.append(f'  {{ filename = "{wheel.filename}" }},')
        config_lines.append("]")

    config_content = "\n".join(config_lines) + "\n"

    config_path = packages_dir / f"{info.name}.toml"
    config_path.write_text(config_content)

    # Create state/<name>.json with pending wheels (only if we have wheels)
    if info.wheels:
        state: dict[str, WheelState] = {}

        for release, wheel in info.wheels:
            deps = None
            original_reqs = None
            if wheel.filename in info.wheel_dependencies:
                original_reqs = info.wheel_dependencies[wheel.filename]
                req_deps = []
                for d in original_reqs:
                    if _is_required_dep(d):
                        dep_name = _extract_dep_name(d)
                        if dep_name != "python":
                            req_deps.append(dep_name)
                deps = Dependencies(required=req_deps, optional={})

            state[wheel.filename] = WheelState(
                status="pending",
                sha256=wheel.sha256,
                dependencies=deps,
                original_requirements=original_reqs,
            )

        save_state(state_dir, info.name, state)
        wheel_info = f"{len(info.wheels)} wheel(s) pending"
    else:
        wheel_info = "conda-forge only"

    conda_info = f", maps to {info.conda_forge}" if info.conda_forge and info.wheels else ""
    console.print(f"  [green]Created[/] {info.name} ({wheel_info}{conda_info})")


@dataclass
class MappingResult:
    """Result of the dependency resolution prompt."""

    action: str  # "map", "empty_package", "abort"
    value: str | None = None  # conda name or None


def _prompt_for_dependency_deferred(
    package: str,
    error: str,
    required_by: str | None,
    console: Console,
) -> MappingResult:
    """Prompt user to resolve a failed dependency (deferred file writing).

    Options:
    - Map to conda-forge package
    - Add as package with no wheels
    - Abort

    Args:
        package: Package name that failed
        error: Error message
        required_by: Parent package that requires this one
        console: Rich console for output

    Returns:
        MappingResult with action and optional value (no files written)
    """
    from InquirerPy import inquirer
    from InquirerPy.base.control import Choice

    console.print()
    if required_by:
        console.print(f"[red]Error:[/] {package} (required by {required_by}): {error}")
    else:
        console.print(f"[red]Error:[/] {package}: {error}")
    console.print()

    try:
        action = inquirer.select(
            message="What would you like to do?",
            choices=[
                Choice(value="map", name=f"Map to conda-forge package (default: {package})"),
                Choice(value="empty_package", name="Add as package with no wheels"),
                Choice(value="abort", name="Abort"),
            ],
            default="map",
            pointer=">",
            show_cursor=False,
        ).execute()

        if action == "abort":
            return MappingResult(action="abort")

        if action == "empty_package":
            return MappingResult(action="empty_package", value=package)

        # Prompt for conda package name with default
        conda_name = inquirer.text(
            message="Conda-forge package name",
            default=package,
        ).execute()

        if not conda_name:
            conda_name = package

        return MappingResult(action="map", value=conda_name)

    except (EOFError, KeyboardInterrupt):
        return MappingResult(action="abort")


def _write_empty_package(
    package: str,
    packages_dir: Path,
    state_dir: Path,
    console: Console,
) -> None:
    """Create a package config with no wheels."""
    normalized = package.lower().replace("_", "-")

    packages_dir.mkdir(parents=True, exist_ok=True)
    state_dir.mkdir(parents=True, exist_ok=True)

    # Create packages/<name>.toml with empty wheels
    config_content = f'''# Package configuration for {normalized}
# No pure Python wheels available - platform-specific only
version_constraint = ""
skip_versions = []

wheels = []
'''

    config_path = packages_dir / f"{normalized}.toml"
    config_path.write_text(config_content)

    # Create empty state file
    state_path = state_dir / f"{normalized}.json"
    state_path.write_text("{}\n")

    console.print(f"[cyan]Created empty package:[/] {normalized} (no wheels)")


def _write_conda_forge_package(
    package: str,
    conda_name: str,
    packages_dir: Path,
    console: Console,
) -> None:
    """Create a package config that references a conda-forge package."""
    normalized = package.lower().replace("_", "-")

    packages_dir.mkdir(parents=True, exist_ok=True)

    # Create packages/<name>.toml with conda_forge reference
    config_content = f'''# {normalized} - available on conda-forge
conda_forge = "{conda_name}"
'''

    config_path = packages_dir / f"{normalized}.toml"
    config_path.write_text(config_content)

    console.print(f"[green]Created conda-forge reference:[/] {normalized} -> {conda_name}")


def cmd_add(args: argparse.Namespace) -> int:
    """Add a new package to the channel.

    This command uses a two-phase approach:
    1. Phase 1: Recursively fetch and validate ALL packages (no files written)
    2. Phase 2: If all packages are valid, write all files

    If a package fails validation (e.g., no pure Python wheels), the user is
    prompted to either create a conda-forge reference or abort.
    """
    package = args.package
    constraint = args.constraint or ""
    max_versions = args.versions
    packages_dir = args.packages_dir
    state_dir = args.state_dir
    dry_run = args.dry_run

    # Auto-detect non-interactive mode if stdin is not a TTY
    non_interactive = args.non_interactive or not sys.stdin.isatty()

    console.print("[bold]Phase 1:[/] Validating packages and dependencies...")
    console.print()

    # Track packages: (name, constraint, required_by)
    pending: list[tuple[str, str, str | None]] = [(package, constraint, None)]
    seen: set[str] = set()  # Normalized names we've processed
    packages_to_add: list[PackageInfo] = []  # Packages with wheels to convert
    empty_packages_to_add: list[str] = []  # Packages with no wheels

    while pending:
        current_pkg, current_constraint, required_by = pending.pop(0)
        normalized = current_pkg.lower().replace("_", "-")

        # Skip if already processed
        if normalized in seen:
            continue
        seen.add(normalized)

        # Check if already exists as a package (includes conda-forge references)
        existing_packages = list_packages(packages_dir)
        if normalized in existing_packages and not args.force:
            console.print(f"[dim]Skipping {normalized} (already exists)[/]")
            continue

        # Check conda-forge mapping (stored as metadata in the package file)
        conda_name = lookup_conda_mapping(normalized)

        # Fetch package info from PyPI
        info = _fetch_package_info(
            package=current_pkg,
            constraint=current_constraint,
            max_versions=max_versions,
            packages_dir=packages_dir,
            force=args.force,
            console=console,
            indent=1 if required_by else 0,
        )

        # None means package already exists (and not force)
        if info is None:
            continue

        # Check for errors - use conda mapping if available, otherwise prompt or abort
        if info.error:
            # If we have a conda-forge mapping, use it instead of prompting
            if conda_name:
                info.conda_forge = conda_name
                packages_to_add.append(info)  # Will be written as conda-forge only
                continue

            # No conda-forge mapping - prompt user or abort
            if non_interactive:
                console.print()
                if required_by:
                    console.print(f"[red]Error:[/] {normalized} (required by {required_by}): {info.error}")
                else:
                    console.print(f"[red]Error:[/] {normalized}: {info.error}")
                console.print()
                console.print("[yellow]No files were written.[/]")
                console.print("Tip: Create a conda-forge reference package if it exists on conda-forge.")
                return 1

            result = _prompt_for_dependency_deferred(
                normalized, info.error, required_by, console
            )
            if result.action == "abort":
                console.print()
                console.print("[yellow]Aborted. No files were written.[/]")
                return 1

            # Track for Phase 2
            if result.action == "map":
                info.conda_forge = result.value or normalized
                packages_to_add.append(info)  # Will be written as conda-forge only
            elif result.action == "empty_package":
                empty_packages_to_add.append(normalized)
            continue

        # Success - store conda mapping if found
        info.conda_forge = conda_name
        packages_to_add.append(info)

        # Queue dependencies for processing
        all_deps = info.required_deps | info.optional_deps
        for dep in sorted(all_deps):
            dep_normalized = dep.lower().replace("_", "-")
            if dep_normalized not in seen:
                pending.append((dep, "", info.name))

    console.print()

    total_to_add = len(packages_to_add) + len(empty_packages_to_add)
    if total_to_add == 0:
        console.print("[yellow]No new packages to add.[/]")
        return 0

    # Phase 2: Write all files
    if dry_run:
        console.print("[bold]Phase 2:[/] Dry run - no files written")
        console.print()
        console.print(f"Would add {total_to_add} package(s):")
        for info in packages_to_add:
            wheel_info = f"{len(info.wheels)} wheel(s)" if info.wheels else "conda-forge only"
            conda_info = f", conda-forge: {info.conda_forge}" if info.conda_forge else ""
            console.print(f"  [dim]-[/] {info.name} ({wheel_info}{conda_info})")
        for pkg_name in empty_packages_to_add:
            console.print(f"  [dim]-[/] {pkg_name} (empty package)")
        return 0

    console.print("[bold]Phase 2:[/] Writing package files...")
    console.print()

    for info in packages_to_add:
        _write_package_files(info, packages_dir, state_dir, console)

    for pkg_name in empty_packages_to_add:
        _write_empty_package(pkg_name, packages_dir, state_dir, console)

    console.print()
    console.print(f"[bold green]Successfully added {total_to_add} package(s):[/]")
    for info in packages_to_add:
        suffix = ""
        if info.conda_forge:
            suffix = " (conda-forge)" if not info.wheels else f" (conda-forge: {info.conda_forge})"
        console.print(f"  [dim]-[/] {info.name}{suffix}")
    for pkg_name in empty_packages_to_add:
        console.print(f"  [dim]-[/] {pkg_name} (empty)")

    return 0


def main(argv: list[str] | None = None) -> int:
    """Main entry point for cart-wheel CLI."""
    parser = argparse.ArgumentParser(
        prog="cart-wheel",
        description="Convert Python wheels to conda packages",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # convert subcommand
    convert_parser = subparsers.add_parser(
        "convert",
        help="Convert a wheel to conda package",
    )
    convert_parser.add_argument(
        "wheel",
        type=Path,
        help="Path to the .whl file to convert",
    )
    convert_parser.add_argument(
        "-o",
        "--output-dir",
        type=Path,
        default=Path("."),
        help="Output directory for the .conda file (default: current directory)",
    )
    convert_parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Print verbose output",
    )
    convert_parser.set_defaults(func=cmd_convert)

    # sync subcommand
    sync_parser = subparsers.add_parser(
        "sync",
        help="Sync all packages from PyPI",
    )
    sync_parser.add_argument(
        "--packages-dir",
        type=Path,
        default=DEFAULT_PACKAGES_DIR,
        help=f"Packages directory (default: {DEFAULT_PACKAGES_DIR})",
    )
    sync_parser.add_argument(
        "--state-dir",
        type=Path,
        default=DEFAULT_STATE_DIR,
        help=f"State directory (default: {DEFAULT_STATE_DIR})",
    )
    sync_parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Output directory (default: {DEFAULT_OUTPUT_DIR})",
    )
    sync_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be converted without actually converting",
    )
    sync_parser.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="Suppress progress output",
    )
    sync_parser.set_defaults(func=cmd_sync)

    # sync-package subcommand
    sync_pkg_parser = subparsers.add_parser(
        "sync-package",
        help="Sync a single package from PyPI",
    )
    sync_pkg_parser.add_argument(
        "package",
        help="Package name to sync",
    )
    sync_pkg_parser.add_argument(
        "--packages-dir",
        type=Path,
        default=DEFAULT_PACKAGES_DIR,
        help=f"Packages directory (default: {DEFAULT_PACKAGES_DIR})",
    )
    sync_pkg_parser.add_argument(
        "--state-dir",
        type=Path,
        default=DEFAULT_STATE_DIR,
        help=f"State directory (default: {DEFAULT_STATE_DIR})",
    )
    sync_pkg_parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Output directory (default: {DEFAULT_OUTPUT_DIR})",
    )
    sync_pkg_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be converted without actually converting",
    )
    sync_pkg_parser.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="Suppress progress output",
    )
    sync_pkg_parser.set_defaults(func=cmd_sync_package)

    # check subcommand
    check_parser = subparsers.add_parser(
        "check",
        help="Check for new versions on PyPI",
    )
    check_parser.add_argument(
        "--packages-dir",
        type=Path,
        default=DEFAULT_PACKAGES_DIR,
        help=f"Packages directory (default: {DEFAULT_PACKAGES_DIR})",
    )
    check_parser.add_argument(
        "--state-dir",
        type=Path,
        default=DEFAULT_STATE_DIR,
        help=f"State directory (default: {DEFAULT_STATE_DIR})",
    )
    check_parser.set_defaults(func=cmd_check)

    # validate subcommand
    validate_parser = subparsers.add_parser(
        "validate",
        help="Validate dependencies for all packages",
    )
    validate_parser.add_argument(
        "--packages-dir",
        type=Path,
        default=DEFAULT_PACKAGES_DIR,
        help=f"Packages directory (default: {DEFAULT_PACKAGES_DIR})",
    )
    validate_parser.add_argument(
        "--state-dir",
        type=Path,
        default=DEFAULT_STATE_DIR,
        help=f"State directory (default: {DEFAULT_STATE_DIR})",
    )
    validate_parser.set_defaults(func=cmd_validate)

    # index subcommand
    index_parser = subparsers.add_parser(
        "index",
        help="Generate repodata.json for the channel",
    )
    index_parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Output directory (default: {DEFAULT_OUTPUT_DIR})",
    )
    index_parser.set_defaults(func=cmd_index)

    # status subcommand
    status_parser = subparsers.add_parser(
        "status",
        help="Show status of all packages",
    )
    status_parser.add_argument(
        "--packages-dir",
        type=Path,
        default=DEFAULT_PACKAGES_DIR,
        help=f"Packages directory (default: {DEFAULT_PACKAGES_DIR})",
    )
    status_parser.add_argument(
        "--state-dir",
        type=Path,
        default=DEFAULT_STATE_DIR,
        help=f"State directory (default: {DEFAULT_STATE_DIR})",
    )
    status_parser.set_defaults(func=cmd_status)

    # add subcommand
    add_parser = subparsers.add_parser(
        "add",
        help="Add a new package to the channel",
    )
    add_parser.add_argument(
        "package",
        help="PyPI package name to add",
    )
    add_parser.add_argument(
        "-c",
        "--constraint",
        default="",
        help="PEP 440 version constraint (e.g., '>=2.0', '>=1.0,<3.0')",
    )
    add_parser.add_argument(
        "-n",
        "--versions",
        type=int,
        default=5,
        help="Maximum number of versions to include (default: 5)",
    )
    add_parser.add_argument(
        "--packages-dir",
        type=Path,
        default=DEFAULT_PACKAGES_DIR,
        help=f"Packages directory (default: {DEFAULT_PACKAGES_DIR})",
    )
    add_parser.add_argument(
        "--state-dir",
        type=Path,
        default=DEFAULT_STATE_DIR,
        help=f"State directory (default: {DEFAULT_STATE_DIR})",
    )
    add_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate packages without writing files",
    )
    add_parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing packages",
    )
    add_parser.add_argument(
        "--non-interactive",
        action="store_true",
        help="Abort on errors instead of prompting (auto-detected when stdin is not a TTY)",
    )
    add_parser.set_defaults(func=cmd_add)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
