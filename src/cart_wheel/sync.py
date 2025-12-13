"""Sync orchestration - coordinates PyPI fetch, conversion, and state update."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)

from .conda import ConversionResult, DependencyConversionError, convert_wheel
from .pypi import (
    PyPIError,
    download_wheel,
    get_matching_versions,
    select_best_wheel,
)
from .state import (
    Dependencies,
    WheelConfig,
    WheelState,
    get_pending_wheels,
    list_packages,
    load_package_config,
    load_state,
    save_state,
)

if TYPE_CHECKING:
    pass

console = Console()


@dataclass
class SyncResult:
    """Result of syncing a package."""

    package: str
    converted: list[str] = field(default_factory=list)
    failed: list[tuple[str, str]] = field(default_factory=list)  # (version, error)
    skipped: list[str] = field(default_factory=list)


class SyncError(Exception):
    """Error during sync operation."""


def _extract_dependencies(result: ConversionResult) -> Dependencies:
    """Extract dependencies from conversion result."""
    # The dependencies list contains conda-format dependencies
    # We need to extract just the package names for validation
    required = []
    for dep in result.dependencies:
        # Strip conda conditions and version specifiers
        # Format is like "package >=1.0" or "package __win"
        parts = dep.split()
        if parts:
            name = parts[0]
            # Skip python itself
            if name != "python":
                required.append(name)

    optional: dict[str, list[str]] = {}
    for extra_name, extra_deps in result.extra_depends.items():
        extra_required = []
        for dep in extra_deps:
            parts = dep.split()
            if parts:
                name = parts[0]
                if name != "python":
                    extra_required.append(name)
        if extra_required:
            optional[extra_name] = extra_required

    return Dependencies(required=required, optional=optional)


def sync_package(
    package: str,
    packages_dir: Path,
    state_dir: Path,
    output_dir: Path,
    *,
    dry_run: bool = False,
    show_progress: bool = False,
    progress: Progress | None = None,
) -> SyncResult:
    """Sync a single package - convert pending wheels.

    Args:
        package: Package name
        packages_dir: Directory containing package configs
        state_dir: Directory containing state files
        output_dir: Directory for conda packages
        dry_run: If True, don't actually convert or update state
        show_progress: If True, show rich progress output
        progress: Optional existing Progress instance to use

    Returns:
        SyncResult with conversion outcomes
    """
    result = SyncResult(package=package)

    # Load config and state
    config = load_package_config(packages_dir, package)
    state = load_state(state_dir, package)

    # Get wheels that need conversion
    pending = get_pending_wheels(config, state)

    if not pending:
        return result

    # Setup progress tracking
    own_progress = False
    if show_progress and progress is None:
        progress = Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            console=console,
        )
        progress.start()
        own_progress = True

    task_id = None
    if progress is not None:
        task_id = progress.add_task(f"[cyan]{package}", total=len(pending))

    try:
        for wheel_config in pending:
            filename = wheel_config.filename
            start_time = time.perf_counter()

            if progress is not None:
                progress.update(
                    task_id,
                    description=f"[cyan]{package}[/] → {filename}",
                )

            if dry_run:
                result.converted.append(wheel_config.version)
                if progress is not None:
                    progress.advance(task_id)
                continue

            # Find the wheel URL from PyPI
            try:
                wheel_url = _find_wheel_url(package, wheel_config)
            except PyPIError as e:
                elapsed = time.perf_counter() - start_time
                result.failed.append((wheel_config.version, str(e)))
                _update_failed_state(state, filename, str(e))
                save_state(state_dir, package, state)
                if progress is not None:
                    progress.advance(task_id)
                    console.print(
                        f"  [red]✗[/] {filename} [dim]({elapsed:.1f}s)[/]: {e}",
                        highlight=False,
                    )
                continue

            # Download and convert
            try:
                wheel_bytes = download_wheel(wheel_url)
                subdir_path = output_dir / "noarch"  # Pure Python wheels go to noarch
                subdir_path.mkdir(parents=True, exist_ok=True)

                conversion_result = convert_wheel(
                    wheel_bytes,
                    subdir_path,
                    filename=filename,
                )

                # Update state with success
                deps = _extract_dependencies(conversion_result)
                state[filename] = WheelState(
                    status="converted",
                    converted_at=datetime.now(UTC),
                    conda_file=conversion_result.path.name,
                    subdir=conversion_result.subdir,
                    dependencies=deps,
                    original_requirements=conversion_result.original_requirements,
                )
                save_state(state_dir, package, state)

                elapsed = time.perf_counter() - start_time
                result.converted.append(wheel_config.version)
                if progress is not None:
                    progress.advance(task_id)
                    console.print(
                        f"  [green]✓[/] {filename} → {conversion_result.path.name} [dim]({elapsed:.1f}s)[/]",
                        highlight=False,
                    )

            except DependencyConversionError as e:
                elapsed = time.perf_counter() - start_time
                error_msg = str(e)
                result.failed.append((wheel_config.version, error_msg))
                _update_failed_state(state, filename, error_msg)
                save_state(state_dir, package, state)
                if progress is not None:
                    progress.advance(task_id)
                    console.print(
                        f"  [red]✗[/] {filename} [dim]({elapsed:.1f}s)[/]: {error_msg}",
                        highlight=False,
                    )

            except Exception as e:
                elapsed = time.perf_counter() - start_time
                error_msg = f"{type(e).__name__}: {e}"
                result.failed.append((wheel_config.version, error_msg))
                _update_failed_state(state, filename, error_msg)
                save_state(state_dir, package, state)
                if progress is not None:
                    progress.advance(task_id)
                    console.print(
                        f"  [red]✗[/] {filename} [dim]({elapsed:.1f}s)[/]: {error_msg}",
                        highlight=False,
                    )
    finally:
        if own_progress and progress is not None:
            progress.stop()

    return result


def _find_wheel_url(package: str, wheel_config: WheelConfig) -> str:
    """Find the URL for a specific wheel from PyPI.

    We look for the wheel by matching filename.
    """
    for release in get_matching_versions(package, f"=={wheel_config.version}"):
        for wheel in release.wheels:
            if wheel.filename == wheel_config.filename:
                return wheel.url

    raise PyPIError(f"Wheel not found on PyPI: {wheel_config.filename}")


def _update_failed_state(
    state: dict[str, WheelState],
    filename: str,
    error: str,
) -> None:
    """Update state for a failed conversion."""
    existing = state.get(filename)
    retry_count = (existing.retry_count + 1) if existing else 1

    state[filename] = WheelState(
        status="failed" if retry_count < 3 else "skipped",
        error=error,
        retry_count=retry_count,
    )


def sync_all(
    packages_dir: Path,
    state_dir: Path,
    output_dir: Path,
    *,
    dry_run: bool = False,
    show_progress: bool = False,
) -> list[SyncResult]:
    """Sync all packages.

    Args:
        packages_dir: Directory containing package configs
        state_dir: Directory containing state files
        output_dir: Directory for conda packages
        dry_run: If True, don't actually convert or update state
        show_progress: If True, show rich progress output

    Returns:
        List of SyncResult for each package
    """
    results = []
    packages = list_packages(packages_dir)

    if not packages:
        return results

    if show_progress:
        console.print(f"[bold]Syncing {len(packages)} package(s)...[/]")
        console.print()

        with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            for package in packages:
                result = sync_package(
                    package,
                    packages_dir,
                    state_dir,
                    output_dir,
                    dry_run=dry_run,
                    show_progress=False,
                    progress=progress,
                )
                results.append(result)

        # Print summary
        console.print()
        total_converted = sum(len(r.converted) for r in results)
        total_failed = sum(len(r.failed) for r in results)
        total_skipped = sum(len(r.skipped) for r in results)

        console.print("[bold]Summary:[/]")
        if total_converted:
            console.print(f"  [green]✓[/] {total_converted} wheel(s) converted")
        if total_failed:
            console.print(f"  [red]✗[/] {total_failed} wheel(s) failed")
        if total_skipped:
            console.print(f"  [yellow]○[/] {total_skipped} wheel(s) skipped")
        if not total_converted and not total_failed and not total_skipped:
            console.print("  [dim]No wheels to process[/]")
    else:
        for package in packages:
            result = sync_package(
                package,
                packages_dir,
                state_dir,
                output_dir,
                dry_run=dry_run,
            )
            results.append(result)

    return results


def check_for_updates(
    packages_dir: Path,
    state_dir: Path,
) -> dict[str, list[str]]:
    """Check PyPI for new versions of tracked packages.

    Args:
        packages_dir: Directory containing package configs
        state_dir: Directory containing state files

    Returns:
        Dict mapping package name to list of new versions available
    """
    updates: dict[str, list[str]] = {}
    packages = list_packages(packages_dir)

    for package in packages:
        config = load_package_config(packages_dir, package)

        # Get current wheel filenames in config
        known_filenames = {w.filename for w in config.wheels}

        # Check PyPI for new versions matching constraint
        try:
            for release in get_matching_versions(
                package,
                config.version_constraint or ">=0",
            ):
                # Find best wheel for this release
                wheel = select_best_wheel(release.wheels)
                if wheel and wheel.filename not in known_filenames:
                    if package not in updates:
                        updates[package] = []
                    updates[package].append(release.version)
        except PyPIError:
            # Skip packages we can't fetch
            continue

    return updates
