"""Command-line interface for cart-wheel."""

import argparse
import sys
from pathlib import Path

from .conda import build_conda_package
from .wheel import parse_wheel


def main(argv: list[str] | None = None) -> int:
    """Main entry point for cart-wheel CLI."""
    parser = argparse.ArgumentParser(
        prog="cart-wheel",
        description="Convert Python wheels to conda packages",
    )
    parser.add_argument(
        "wheel",
        type=Path,
        help="Path to the .whl file to convert",
    )
    parser.add_argument(
        "-o", "--output-dir",
        type=Path,
        default=Path("."),
        help="Output directory for the .conda file (default: current directory)",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Print verbose output",
    )

    args = parser.parse_args(argv)

    wheel_path: Path = args.wheel
    output_dir: Path = args.output_dir

    if not wheel_path.exists():
        print(f"Error: Wheel file not found: {wheel_path}", file=sys.stderr)
        return 1

    if not wheel_path.suffix == ".whl":
        print(f"Error: File does not appear to be a wheel: {wheel_path}", file=sys.stderr)
        return 1

    try:
        if args.verbose:
            print(f"Parsing wheel: {wheel_path}")

        metadata = parse_wheel(wheel_path)

        if not metadata.is_pure_python:
            print(
                f"Error: Only pure Python wheels are supported. "
                f"This wheel has platform tag: {metadata.platform_tag}",
                file=sys.stderr,
            )
            return 1

        if args.verbose:
            print(f"  Name: {metadata.name}")
            print(f"  Version: {metadata.version}")
            print(f"  Pure Python: {metadata.is_pure_python}")
            print(f"  Platform: {metadata.platform_tag}")
            print(f"  Dependencies: {len(metadata.dependencies)}")

        if args.verbose:
            print(f"Building conda package...")

        conda_path = build_conda_package(metadata, output_dir)

        print(f"Created: {conda_path}")
        return 0

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
