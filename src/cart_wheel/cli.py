"""Command-line interface for cart-wheel."""

from __future__ import annotations

import argparse
import asyncio
import sys
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from rich.console import Console
from rich.live import Live
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.prompt import Prompt
from rich.table import Table

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

if TYPE_CHECKING:
    from hishel.httpx import AsyncCacheClient

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
    from .sync import sync_all_async

    packages_dir = args.packages_dir
    state_dir = args.state_dir
    output_dir = args.output_dir
    dry_run = args.dry_run
    quiet = args.quiet

    if not packages_dir.exists():
        print(f"Error: Packages directory not found: {packages_dir}", file=sys.stderr)
        return 1

    if quiet:
        # Use original sync_all for quiet mode
        results = sync_all(
            packages_dir,
            state_dir,
            output_dir,
            dry_run=dry_run,
            show_progress=False,
        )
    else:
        # Use async version with Rich Live display
        console.print("[bold]Syncing packages...[/]")
        console.print()

        results = asyncio.run(
            sync_all_async(
                packages_dir,
                state_dir,
                output_dir,
                live_console=console,
                dry_run=dry_run,
            )
        )

        # Print summary
        console.print()
        total_converted = sum(len(r.converted) for r in results)
        total_failed = sum(len(r.failed) for r in results)

        console.print("[bold]Summary:[/]")
        if total_converted:
            console.print(f"  [green]✓[/] {total_converted} wheel(s) converted")
        if total_failed:
            console.print(f"  [red]✗[/] {total_failed} wheel(s) failed")
        if not total_converted and not total_failed:
            console.print("  [dim]No wheels to process[/]")

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
    warnings: list[str] = field(default_factory=list)  # PyPI warnings
    required_by: str | None = None  # Parent package that requires this


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


async def _lookup_conda_mapping_async(
    pypi_name: str,
    client: AsyncCacheClient,
) -> str | None:
    """Look up conda-forge mapping asynchronously."""
    normalized = pypi_name.lower().replace("_", "-")
    url = f"{CONDA_MAPPING_URL}/{normalized}.json"

    try:
        response = await client.get(url)
        if response.status_code != 200:
            return None

        data = response.json()
        conda_versions = data.get("conda_versions", {})

        for version_packages in conda_versions.values():
            if version_packages:
                return version_packages[0]
        return None
    except Exception:
        return None


async def _fetch_package_info_async(
    package: str,
    constraint: str,
    max_versions: int,
    client: AsyncCacheClient,
    required_by: str | None = None,
) -> PackageInfo:
    """Fetch and validate package info from PyPI asynchronously."""
    from .pypi import (
        PyPIError,
        fetch_wheel_metadata_async,
        get_matching_versions_async,
    )
    from .wheel import parse_dependencies_from_metadata

    normalized_name = package.lower().replace("_", "-")

    # Fetch from PyPI
    try:
        releases, pypi_warnings = await get_matching_versions_async(
            package, constraint or ">=0", client, max_versions=max_versions
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
            required_by=required_by,
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
            warnings=pypi_warnings,
            required_by=required_by,
        )

    # Select pure Python wheels only
    wheels_to_add = []
    for release in releases:
        for wheel in release.wheels:
            if "py3-none-any" in wheel.filename or "py2.py3-none-any" in wheel.filename:
                wheels_to_add.append((release, wheel))
                break

    if not wheels_to_add:
        return PackageInfo(
            name=normalized_name,
            original_name=package,
            constraint=constraint,
            wheels=[],
            wheel_dependencies={},
            required_deps=set(),
            optional_deps=set(),
            error="No pure Python wheels found",
            warnings=pypi_warnings,
            required_by=required_by,
        )

    # Fetch dependencies using PEP 658 metadata (concurrently)
    required_deps: set[str] = set()
    optional_deps: set[str] = set()
    wheel_dependencies: dict[str, list[str]] = {}

    async def fetch_metadata(wheel):
        metadata = await fetch_wheel_metadata_async(wheel.url, client)
        return wheel.filename, metadata

    metadata_tasks = [fetch_metadata(w) for _, w in wheels_to_add]
    metadata_results = await asyncio.gather(*metadata_tasks, return_exceptions=True)

    for result in metadata_results:
        if isinstance(result, Exception):
            continue
        filename, metadata = result
        if metadata:
            deps = parse_dependencies_from_metadata(metadata)
            wheel_dependencies[filename] = deps
            for dep in deps:
                dep_name = _extract_dep_name(dep)
                if dep_name != "python":
                    if _is_required_dep(dep):
                        required_deps.add(dep_name)
                    else:
                        optional_deps.add(dep_name)

    return PackageInfo(
        name=normalized_name,
        original_name=package,
        constraint=constraint,
        wheels=wheels_to_add,
        wheel_dependencies=wheel_dependencies,
        required_deps=required_deps,
        optional_deps=optional_deps,
        warnings=pypi_warnings,
        required_by=required_by,
    )


@dataclass
class _FetchResult:
    """Result of fetching a single package."""

    info: PackageInfo
    conda_name: str | None


async def _fetch_single_package(
    package: str,
    constraint: str,
    max_versions: int,
    client: AsyncCacheClient,
    required_by: str | None = None,
) -> _FetchResult:
    """Fetch conda mapping and PyPI info concurrently."""
    conda_task = _lookup_conda_mapping_async(package, client)
    pypi_task = _fetch_package_info_async(package, constraint, max_versions, client, required_by)

    conda_name, info = await asyncio.gather(conda_task, pypi_task)
    return _FetchResult(info=info, conda_name=conda_name)


def _make_progress_table(
    total: int,
    completed: int,
    succeeded: int,
    conda_forge: int,
    need_input: int,
    in_flight: list[str],
) -> Table:
    """Create a progress table for Rich Live display."""
    table = Table.grid(padding=(0, 2))
    table.add_column()
    table.add_column()

    # Progress row
    progress_text = f"Progress: {completed}/{total}"
    stats_text = (
        f"[green]✓ {succeeded}[/] ok  "
        f"[cyan]⚡ {conda_forge}[/] conda-forge  "
        f"[yellow]? {need_input}[/] need input"
    )
    table.add_row(progress_text, stats_text)

    # In-flight row
    if in_flight:
        pkg_list = ", ".join(in_flight[:8])
        if len(in_flight) > 8:
            pkg_list += f" (+{len(in_flight) - 8} more)"
        table.add_row("Fetching:", f"[dim]{pkg_list}[/]")

    return table


async def _add_packages_async(
    package: str,
    constraint: str,
    max_versions: int,
    packages_dir: Path,
    force: bool,
    live_console: Console,
) -> tuple[list[PackageInfo], list[PackageInfo], bool]:
    """Fetch packages concurrently with live progress display.

    Returns:
        Tuple of (packages_to_add, needs_input, aborted)
    """
    from .http import get_async_client

    packages_to_add: list[PackageInfo] = []
    needs_input: list[PackageInfo] = []

    # Track state
    seen: set[str] = set()
    existing_packages = set(list_packages(packages_dir))
    pending: list[tuple[str, str, str | None]] = [(package, constraint, None)]

    # Stats for display
    total = 0
    completed = 0
    succeeded = 0
    conda_forge_count = 0
    need_input_count = 0
    in_flight: list[str] = []

    # Semaphore to limit concurrent requests
    semaphore = asyncio.Semaphore(50)

    async with get_async_client() as client:
        active_tasks: dict[asyncio.Task, str] = {}

        def update_display(live: Live) -> None:
            table = _make_progress_table(
                total, completed, succeeded, conda_forge_count, need_input_count, in_flight
            )
            live.update(table)

        with Live(
            _make_progress_table(0, 0, 0, 0, 0, []),
            console=live_console,
            refresh_per_second=4,
        ) as live:

            async def fetch_wrapper(
                pkg: str, cons: str, req_by: str | None
            ) -> tuple[_FetchResult, str, str | None]:
                async with semaphore:
                    result = await _fetch_single_package(pkg, cons, max_versions, client, req_by)
                    return result, pkg, req_by

            def start_fetch(pkg: str, cons: str, req_by: str | None) -> asyncio.Task | None:
                nonlocal total
                normalized = pkg.lower().replace("_", "-")

                if normalized in seen:
                    return None
                seen.add(normalized)

                if normalized in existing_packages and not force:
                    live.console.print(f"[dim]Skipping {normalized} (exists)[/]")
                    return None

                total += 1
                in_flight.append(normalized)
                update_display(live)

                task = asyncio.create_task(fetch_wrapper(pkg, cons, req_by))
                active_tasks[task] = normalized
                return task

            # Start initial package
            start_fetch(package, constraint, None)

            while active_tasks or pending:
                # Start any pending fetches
                while pending:
                    pkg, cons, req_by = pending.pop(0)
                    start_fetch(pkg, cons, req_by)

                if not active_tasks:
                    break

                # Wait for at least one task to complete
                done, _ = await asyncio.wait(
                    active_tasks.keys(), return_when=asyncio.FIRST_COMPLETED
                )

                for task in done:
                    normalized = active_tasks.pop(task)
                    if normalized in in_flight:
                        in_flight.remove(normalized)
                    completed += 1

                    try:
                        result, pkg_name, req_by = task.result()
                    except Exception as e:
                        live.console.print(f"[red]Error fetching {normalized}: {e}[/]")
                        update_display(live)
                        continue

                    info = result.info

                    # Print warnings
                    for warning in info.warnings:
                        live.console.print(f"[dim yellow]⚠ {warning}[/]")

                    if info.error:
                        if result.conda_name:
                            # Has conda-forge mapping - use it
                            info.conda_forge = result.conda_name
                            packages_to_add.append(info)
                            succeeded += 1
                            conda_forge_count += 1
                            live.console.print(
                                f"[green]✓[/] {info.name} → conda-forge: {result.conda_name}"
                            )
                        else:
                            # Needs user input
                            needs_input.append(info)
                            need_input_count += 1
                            err_short = info.error[:50] + "..." if len(info.error) > 50 else info.error
                            live.console.print(f"[yellow]?[/] {info.name}: {err_short}")
                    else:
                        # Success
                        info.conda_forge = result.conda_name
                        packages_to_add.append(info)
                        succeeded += 1
                        if result.conda_name:
                            conda_forge_count += 1

                        wheel_count = len(info.wheels)
                        conda_info = f" [dim](cf: {result.conda_name})[/]" if result.conda_name else ""
                        live.console.print(f"[green]✓[/] {info.name}: {wheel_count} wheels{conda_info}")

                        # Queue dependencies
                        all_deps = info.required_deps | info.optional_deps
                        for dep in sorted(all_deps):
                            dep_normalized = dep.lower().replace("_", "-")
                            if dep_normalized not in seen:
                                pending.append((dep, "", info.name))

                    update_display(live)

    return packages_to_add, needs_input, False


def _prompt_for_package(info: PackageInfo, console: Console) -> tuple[str, str | None]:
    """Prompt user to resolve a package that needs input.

    Returns:
        Tuple of (action, value) where action is "map", "empty", or "abort"
    """
    console.print()
    if info.required_by:
        console.print(f"[red]Error:[/] {info.name} (required by {info.required_by}): {info.error}")
    else:
        console.print(f"[red]Error:[/] {info.name}: {info.error}")
    console.print()
    console.print("Options:")
    console.print("  [bold]1[/] - Map to conda-forge package")
    console.print("  [bold]2[/] - Add as empty package (no wheels)")
    console.print("  [bold]3[/] - Abort")
    console.print()

    try:
        choice = Prompt.ask("Choice", choices=["1", "2", "3"], default="1")

        if choice == "3":
            return "abort", None
        elif choice == "2":
            return "empty", info.name
        else:
            conda_name = Prompt.ask("Conda-forge package name", default=info.name)
            return "map", conda_name or info.name

    except (EOFError, KeyboardInterrupt):
        return "abort", None


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

    Uses concurrent fetching with Rich Live progress display.
    Packages needing user input are prompted after fetching completes.
    """
    package = args.package
    constraint = args.constraint or ""
    max_versions = args.versions
    packages_dir = args.packages_dir
    state_dir = args.state_dir
    dry_run = args.dry_run

    # Auto-detect non-interactive mode if stdin is not a TTY
    non_interactive = args.non_interactive or not sys.stdin.isatty()

    console.print("[bold]Fetching packages and dependencies...[/]")
    console.print()

    # Run async fetching
    packages_to_add, needs_input, _ = asyncio.run(
        _add_packages_async(
            package=package,
            constraint=constraint,
            max_versions=max_versions,
            packages_dir=packages_dir,
            force=args.force,
            live_console=console,
        )
    )

    console.print()
    console.print(f"[bold]Fetching complete.[/] {len(packages_to_add)} ready, {len(needs_input)} need input.")
    console.print()

    # Handle packages that need user input
    empty_packages_to_add: list[str] = []
    aborted = False

    if needs_input:
        if non_interactive:
            console.print(f"[red]Error:[/] {len(needs_input)} package(s) need user input (non-interactive mode):")
            for info in needs_input[:10]:  # Show first 10
                req_info = f" (required by {info.required_by})" if info.required_by else ""
                console.print(f"  - {info.name}{req_info}: {info.error}")
            if len(needs_input) > 10:
                console.print(f"  ... and {len(needs_input) - 10} more")
            console.print()
            console.print("[yellow]No files were written.[/]")
            console.print("[dim]Tip: Run interactively to resolve these packages, or pre-create conda-forge references.[/]")
            return 1

        console.print(f"[yellow]{len(needs_input)} package(s) need your input:[/]")
        console.print()

        for info in needs_input:
            action, value = _prompt_for_package(info, console)

            if action == "abort":
                aborted = True
                break
            elif action == "map":
                info.conda_forge = value
                packages_to_add.append(info)
            elif action == "empty":
                empty_packages_to_add.append(info.name)

    if aborted:
        console.print()
        console.print("[yellow]Aborted. No files were written.[/]")
        return 1

    total_to_add = len(packages_to_add) + len(empty_packages_to_add)
    if total_to_add == 0:
        console.print("[yellow]No new packages to add.[/]")
        return 0

    # Write files
    if dry_run:
        console.print()
        console.print("[bold]Dry run - no files written[/]")
        console.print()
        console.print(f"Would add {total_to_add} package(s):")
        for info in packages_to_add:
            wheel_info = f"{len(info.wheels)} wheel(s)" if info.wheels else "conda-forge only"
            conda_info = f", conda-forge: {info.conda_forge}" if info.conda_forge else ""
            console.print(f"  [dim]-[/] {info.name} ({wheel_info}{conda_info})")
        for pkg_name in empty_packages_to_add:
            console.print(f"  [dim]-[/] {pkg_name} (empty package)")
        return 0

    console.print()
    console.print("[bold]Writing package files...[/]")
    console.print()

    for info in packages_to_add:
        _write_package_files(info, packages_dir, state_dir, console)

    for pkg_name in empty_packages_to_add:
        _write_empty_package(pkg_name, packages_dir, state_dir, console)

    console.print()
    console.print(f"[bold green]Successfully added {total_to_add} package(s)[/]")

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
