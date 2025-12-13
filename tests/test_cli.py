"""Tests for CLI functionality."""

from pathlib import Path

import pytest

from cart_wheel.cli import main

# Basic CLI tests


def test_cli_convert_wheel_success(sample_wheel: Path, tmp_path: Path, capsys):
    """Successful conversion returns exit code 0."""
    output_dir = tmp_path / "output"

    result = main(["convert", str(sample_wheel), "-o", str(output_dir)])

    assert result == 0
    captured = capsys.readouterr()
    assert "Created:" in captured.out


def test_cli_convert_wheel_creates_output(sample_wheel: Path, tmp_path: Path):
    """Conversion creates .conda file with correct name."""
    output_dir = tmp_path / "output"

    main(["convert", str(sample_wheel), "-o", str(output_dir)])

    conda_files = list(output_dir.glob("*.conda"))
    assert len(conda_files) == 1
    assert "sample-package" in conda_files[0].name


def test_cli_default_output_directory(sample_wheel: Path, tmp_path: Path, monkeypatch):
    """Output defaults to current directory."""
    monkeypatch.chdir(tmp_path)

    result = main(["convert", str(sample_wheel)])

    assert result == 0
    conda_files = list(tmp_path.glob("*.conda"))
    assert len(conda_files) == 1


def test_cli_verbose_output(sample_wheel: Path, tmp_path: Path, capsys):
    """Verbose flag shows conversion details."""
    output_dir = tmp_path / "output"

    main(["convert", str(sample_wheel), "-o", str(output_dir), "-v"])

    captured = capsys.readouterr()
    assert "Converting wheel:" in captured.out
    assert "Name:" in captured.out
    assert "Version:" in captured.out
    assert "Subdir:" in captured.out
    assert "Dependencies:" in captured.out


def test_cli_output_path_in_message(sample_wheel: Path, tmp_path: Path, capsys):
    """Output message includes path to created file."""
    output_dir = tmp_path / "output"

    main(["convert", str(sample_wheel), "-o", str(output_dir)])

    captured = capsys.readouterr()
    assert ".conda" in captured.out


def test_cli_help_option(capsys):
    """Help option shows usage information."""
    with pytest.raises(SystemExit) as exc_info:
        main(["--help"])

    assert exc_info.value.code == 0
    captured = capsys.readouterr()
    assert "cart-wheel" in captured.out
    assert "Convert Python wheels to conda packages" in captured.out


# Error handling tests


def test_cli_nonexistent_wheel_error(tmp_path: Path, capsys):
    """Missing wheel file returns error."""
    fake_wheel = tmp_path / "nonexistent.whl"

    result = main(["convert", str(fake_wheel)])

    assert result == 1
    captured = capsys.readouterr()
    assert "Error:" in captured.err
    assert "not found" in captured.err


def test_cli_non_wheel_file_error(tmp_path: Path, capsys):
    """Non-.whl file returns error."""
    not_a_wheel = tmp_path / "file.txt"
    not_a_wheel.write_text("not a wheel")

    result = main(["convert", str(not_a_wheel)])

    assert result == 1
    captured = capsys.readouterr()
    assert "Error:" in captured.err
    assert "wheel" in captured.err.lower()


def test_cli_converts_platform_specific_wheel(tmp_wheel, tmp_path: Path, capsys):
    """Platform-specific wheels are converted with correct subdir."""
    wheel_path = tmp_wheel(
        name="native_pkg",
        version="1.0.0",
        python_tag="cp311",
        abi_tag="cp311",
        platform_tag="win_amd64",
    )
    output_dir = tmp_path / "output"

    result = main(["convert", str(wheel_path), "-o", str(output_dir), "-v"])

    assert result == 0
    captured = capsys.readouterr()
    assert "Subdir: win-64" in captured.out
    conda_files = list(output_dir.glob("*.conda"))
    assert len(conda_files) == 1


# Edge case tests


def test_cli_wheel_with_spaces_in_path(tmp_wheel, tmp_path: Path):
    """Paths with spaces are handled correctly."""
    space_dir = tmp_path / "path with spaces"
    space_dir.mkdir()

    wheel_path = tmp_wheel(name="test_pkg", version="1.0.0")
    new_path = space_dir / wheel_path.name
    wheel_path.rename(new_path)

    output_dir = tmp_path / "output"
    result = main(["convert", str(new_path), "-o", str(output_dir)])

    assert result == 0


def test_cli_output_directory_created(sample_wheel: Path, tmp_path: Path):
    """Nested output directories are created automatically."""
    output_dir = tmp_path / "nested" / "dirs" / "output"

    result = main(["convert", str(sample_wheel), "-o", str(output_dir)])

    assert result == 0
    assert output_dir.exists()


def test_cli_minimal_wheel_conversion(minimal_wheel: Path, tmp_path: Path):
    """Minimal wheels with sparse metadata convert successfully."""
    output_dir = tmp_path / "output"

    result = main(["convert", str(minimal_wheel), "-o", str(output_dir)])

    assert result == 0
    conda_files = list(output_dir.glob("*.conda"))
    assert len(conda_files) == 1


def test_cli_converts_dependencies_with_extras(tmp_wheel, tmp_path: Path):
    """Dependencies with extras are converted to conda format."""
    wheel_path = tmp_wheel(
        name="extras_pkg",
        version="1.0.0",
        requires_dist=["requests[security]>=2.0"],
    )
    output_dir = tmp_path / "output"

    result = main(["convert", str(wheel_path), "-o", str(output_dir)])

    assert result == 0
    conda_files = list(output_dir.glob("*.conda"))
    assert len(conda_files) == 1


def test_cli_converts_environment_markers(tmp_wheel, tmp_path: Path):
    """Environment markers are converted to conda conditions."""
    wheel_path = tmp_wheel(
        name="marker_pkg",
        version="1.0.0",
        requires_dist=["typing-extensions; python_version < '3.11'"],
    )
    output_dir = tmp_path / "output"

    result = main(["convert", str(wheel_path), "-o", str(output_dir)])

    assert result == 0
    conda_files = list(output_dir.glob("*.conda"))
    assert len(conda_files) == 1


def test_cli_unsupported_marker_error(tmp_wheel, tmp_path: Path, capsys):
    """Unsupported markers show clear error."""
    wheel_path = tmp_wheel(
        name="bad_marker_pkg",
        version="1.0.0",
        requires_dist=["foo; platform_machine == 'x86_64'"],
    )
    output_dir = tmp_path / "output"

    result = main(["convert", str(wheel_path), "-o", str(output_dir)])

    assert result == 1
    captured = capsys.readouterr()
    assert "Error:" in captured.err
    assert "platform_machine" in captured.err
