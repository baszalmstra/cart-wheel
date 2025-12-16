"""Sync orchestration - coordinates PyPI fetch, conversion, and state update."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from rich.console import Console
from rich.live import Live
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Table

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
    from hishel.httpx import AsyncCacheClient

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


# =============================================================================
# Async concurrent sync implementation
# =============================================================================


@dataclass
class _WheelTask:
    """A wheel to be synced."""

    package: str
    wheel_config: WheelConfig
    wheel_url: str | None = None


@dataclass
class _WheelResult:
    """Result of syncing a single wheel."""

    package: str
    filename: str
    version: str
    success: bool
    error: str | None = None
    conda_file: str | None = None
    elapsed: float = 0.0
    conversion_result: ConversionResult | None = None


async def _find_wheel_url_async(
    package: str,
    wheel_config: WheelConfig,
    client: AsyncCacheClient,
) -> str:
    """Find the URL for a specific wheel from PyPI asynchronously."""
    from .pypi import get_matching_versions_async

    releases, _ = await get_matching_versions_async(
        package, f"=={wheel_config.version}", client
    )

    for release in releases:
        for wheel in release.wheels:
            if wheel.filename == wheel_config.filename:
                return wheel.url

    raise PyPIError(f"Wheel not found on PyPI: {wheel_config.filename}")


def _download_and_convert_wheel(
    url: str,
    filename: str,
    output_dir: Path,
) -> ConversionResult:
    """Download and convert wheel in a single streaming pass.

    Runs in thread pool for true parallelism (network + CPU).
    """
    from .pypi import download_wheel_streaming

    subdir_path = output_dir / "noarch"
    subdir_path.mkdir(parents=True, exist_ok=True)

    # Stream download directly into conversion
    chunks = download_wheel_streaming(url)
    return convert_wheel(chunks, subdir_path, filename=filename)


async def _sync_single_wheel(
    task: _WheelTask,
    output_dir: Path,
    client: AsyncCacheClient,
    dry_run: bool = False,
) -> _WheelResult:
    """Download and convert a single wheel."""
    start_time = time.perf_counter()
    filename = task.wheel_config.filename
    version = task.wheel_config.version

    try:
        # Find wheel URL (async, uses cached client)
        if task.wheel_url is None:
            task.wheel_url = await _find_wheel_url_async(task.package, task.wheel_config, client)

        if dry_run:
            elapsed = time.perf_counter() - start_time
            return _WheelResult(
                package=task.package,
                filename=filename,
                version=version,
                success=True,
                elapsed=elapsed,
            )

        # Run download+convert in thread pool (true parallelism)
        loop = asyncio.get_event_loop()
        conversion_result = await loop.run_in_executor(
            None,
            _download_and_convert_wheel,
            task.wheel_url,
            filename,
            output_dir,
        )

        elapsed = time.perf_counter() - start_time
        return _WheelResult(
            package=task.package,
            filename=filename,
            version=version,
            success=True,
            conda_file=conversion_result.path.name,
            elapsed=elapsed,
            conversion_result=conversion_result,
        )

    except DependencyConversionError as e:
        elapsed = time.perf_counter() - start_time
        return _WheelResult(
            package=task.package,
            filename=filename,
            version=version,
            success=False,
            error=str(e),
            elapsed=elapsed,
        )
    except PyPIError as e:
        elapsed = time.perf_counter() - start_time
        return _WheelResult(
            package=task.package,
            filename=filename,
            version=version,
            success=False,
            error=str(e),
            elapsed=elapsed,
        )
    except Exception as e:
        elapsed = time.perf_counter() - start_time
        return _WheelResult(
            package=task.package,
            filename=filename,
            version=version,
            success=False,
            error=f"{type(e).__name__}: {e}",
            elapsed=elapsed,
        )


def _make_sync_progress_table(
    total: int,
    completed: int,
    converted: int,
    failed: int,
    in_flight: list[str],
) -> Table:
    """Create a progress table for Rich Live display."""
    table = Table.grid(padding=(0, 2))
    table.add_column()
    table.add_column()

    progress_text = f"Progress: {completed}/{total}"
    stats_text = f"[green]✓ {converted}[/] converted  [red]✗ {failed}[/] failed"
    table.add_row(progress_text, stats_text)

    if in_flight:
        pkg_list = ", ".join(in_flight[:8])
        if len(in_flight) > 8:
            pkg_list += f" (+{len(in_flight) - 8} more)"
        table.add_row("Converting:", f"[dim]{pkg_list}[/]")

    return table


async def sync_all_async(
    packages_dir: Path,
    state_dir: Path,
    output_dir: Path,
    live_console: Console,
    *,
    dry_run: bool = False,
) -> list[SyncResult]:
    """Sync all packages concurrently with live progress display.

    Args:
        packages_dir: Directory containing package configs
        state_dir: Directory containing state files
        output_dir: Directory for conda packages
        live_console: Console for output
        dry_run: If True, don't actually convert or update state

    Returns:
        List of SyncResult for each package
    """
    from .http import get_async_client

    packages = list_packages(packages_dir)
    if not packages:
        return []

    # Collect all pending wheels across all packages
    all_tasks: list[_WheelTask] = []
    package_states: dict[str, dict[str, WheelState]] = {}

    for package in packages:
        config = load_package_config(packages_dir, package)
        state = load_state(state_dir, package)
        package_states[package] = state

        pending = get_pending_wheels(config, state)
        for wheel_config in pending:
            all_tasks.append(_WheelTask(package=package, wheel_config=wheel_config))

    if not all_tasks:
        live_console.print("[dim]No wheels to process[/]")
        return [SyncResult(package=p) for p in packages]

    # Stats for display
    total = len(all_tasks)
    completed = 0
    converted_count = 0
    failed_count = 0
    in_flight: list[str] = []

    # Results tracking
    results_by_package: dict[str, SyncResult] = {p: SyncResult(package=p) for p in packages}

    # Semaphore to limit concurrent downloads
    semaphore = asyncio.Semaphore(10)

    async with get_async_client() as client:
        active_tasks: dict[asyncio.Task, _WheelTask] = {}

        def update_display(live: Live) -> None:
            table = _make_sync_progress_table(
                total, completed, converted_count, failed_count, in_flight
            )
            live.update(table)

        with Live(
            _make_sync_progress_table(total, 0, 0, 0, []),
            console=live_console,
            refresh_per_second=4,
        ) as live:

            async def sync_wrapper(task: _WheelTask) -> _WheelResult:
                async with semaphore:
                    return await _sync_single_wheel(task, output_dir, client, dry_run)

            # Start all tasks
            for task in all_tasks:
                short_name = f"{task.package}/{task.wheel_config.version}"
                in_flight.append(short_name)
                update_display(live)

                async_task = asyncio.create_task(sync_wrapper(task))
                active_tasks[async_task] = task

            # Process completions
            while active_tasks:
                done, _ = await asyncio.wait(
                    active_tasks.keys(), return_when=asyncio.FIRST_COMPLETED
                )

                for async_task in done:
                    task = active_tasks.pop(async_task)
                    short_name = f"{task.package}/{task.wheel_config.version}"
                    if short_name in in_flight:
                        in_flight.remove(short_name)
                    completed += 1

                    try:
                        result = async_task.result()
                    except Exception as e:
                        live.console.print(f"[red]Error:[/] {task.wheel_config.filename}: {e}")
                        failed_count += 1
                        update_display(live)
                        continue

                    pkg_result = results_by_package[result.package]
                    state = package_states[result.package]

                    if result.success:
                        converted_count += 1
                        pkg_result.converted.append(result.version)

                        if not dry_run and result.conversion_result:
                            # Update state
                            deps = _extract_dependencies(result.conversion_result)
                            state[result.filename] = WheelState(
                                status="converted",
                                converted_at=datetime.now(UTC),
                                conda_file=result.conda_file,
                                subdir=result.conversion_result.subdir,
                                dependencies=deps,
                                original_requirements=result.conversion_result.original_requirements,
                            )
                            save_state(state_dir, result.package, state)

                        if result.conda_file:
                            live.console.print(
                                f"[green]✓[/] {result.filename} → {result.conda_file} "
                                f"[dim]({result.elapsed:.1f}s)[/]"
                            )
                        else:
                            live.console.print(
                                f"[green]✓[/] {result.filename} [dim]({result.elapsed:.1f}s)[/]"
                            )
                    else:
                        failed_count += 1
                        pkg_result.failed.append((result.version, result.error or "Unknown error"))

                        if not dry_run:
                            # Update state with failure
                            _update_failed_state(state, result.filename, result.error or "Unknown")
                            save_state(state_dir, result.package, state)

                        live.console.print(
                            f"[red]✗[/] {result.filename} [dim]({result.elapsed:.1f}s)[/]: {result.error}"
                        )

                    update_display(live)

    return list(results_by_package.values())
