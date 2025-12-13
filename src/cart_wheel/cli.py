"""Command-line interface for cart-wheel."""

import argparse
import sys
from pathlib import Path

from .conda import convert_wheel


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

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
