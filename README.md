# cart-wheel

Convert Python wheels to conda packages.

## Installation

```bash
pixi install
```

## Usage

```bash
pixi convert path/to/package.whl -o output_dir
```

Options:
- `-o, --output-dir`: Output directory (default: current directory)
- `-v, --verbose`: Show detailed parsing information

## Features

- Converts pure Python wheels to `.conda` format
- Transforms PEP 508 environment markers to conda conditions
- Supports extras and conditional dependencies
- Preserves package metadata (license, URLs, description)

## Development

```bash
pixi run test
```
