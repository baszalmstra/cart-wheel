"""Pytest fixtures for cart-wheel tests."""

import zipfile
from pathlib import Path

import pytest


@pytest.fixture
def tmp_wheel(tmp_path: Path):
    """Factory fixture to create test wheel files."""

    def _create_wheel(
        name: str = "test_package",
        version: str = "1.0.0",
        python_tag: str = "py3",
        abi_tag: str = "none",
        platform_tag: str = "any",
        summary: str = "A test package",
        description: str = "Long description",
        license: str = "MIT",
        requires_python: str = ">=3.8",
        requires_dist: list[str] | None = None,
        home_page: str | None = None,
        project_urls: list[str] | None = None,
    ) -> Path:
        """Create a minimal wheel file for testing."""
        wheel_name = f"{name}-{version}-{python_tag}-{abi_tag}-{platform_tag}.whl"
        wheel_path = tmp_path / wheel_name

        dist_info = f"{name}-{version}.dist-info"

        # Build METADATA content
        metadata_lines = [
            "Metadata-Version: 2.1",
            f"Name: {name}",
            f"Version: {version}",
            f"Summary: {summary}",
        ]
        if license:
            metadata_lines.append(f"License: {license}")
        if requires_python:
            metadata_lines.append(f"Requires-Python: {requires_python}")
        if home_page:
            metadata_lines.append(f"Home-page: {home_page}")
        if project_urls:
            for url in project_urls:
                metadata_lines.append(f"Project-URL: {url}")
        if requires_dist:
            for req in requires_dist:
                metadata_lines.append(f"Requires-Dist: {req}")
        metadata_lines.append("")
        metadata_lines.append(description)

        metadata_content = "\n".join(metadata_lines)

        # Build WHEEL content
        wheel_content = f"""Wheel-Version: 1.0
Generator: test
Root-Is-Purelib: true
Tag: {python_tag}-{abi_tag}-{platform_tag}
"""

        # Build RECORD content (simplified)
        record_content = f"""{dist_info}/METADATA,sha256=abc,123
{dist_info}/WHEEL,sha256=def,456
{dist_info}/RECORD,,
{name}/__init__.py,sha256=ghi,789
"""

        # Create the wheel (zip file)
        with zipfile.ZipFile(wheel_path, "w") as whl:
            whl.writestr(f"{dist_info}/METADATA", metadata_content)
            whl.writestr(f"{dist_info}/WHEEL", wheel_content)
            whl.writestr(f"{dist_info}/RECORD", record_content)
            whl.writestr(f"{dist_info}/INSTALLER", "pip\n")
            whl.writestr(f"{name}/__init__.py", '"""Test package."""\n')

        return wheel_path

    return _create_wheel


@pytest.fixture
def sample_wheel(tmp_wheel) -> Path:
    """Create a sample wheel with typical metadata."""
    return tmp_wheel(
        name="sample_package",
        version="2.0.0",
        summary="A sample package for testing",
        description="# Sample Package\n\nThis is a sample package.",
        license="Apache-2.0",
        requires_python=">=3.10",
        requires_dist=[
            "requests>=2.0",
            "click>=8.0",
            "typing-extensions; extra == 'dev'",
        ],
        home_page="https://example.com",
        project_urls=[
            "Documentation, https://docs.example.com",
            "Source, https://github.com/example/sample",
            "Repository, https://github.com/example/sample",
        ],
    )


@pytest.fixture
def minimal_wheel(tmp_wheel) -> Path:
    """Create a minimal wheel with only required metadata."""
    return tmp_wheel(
        name="minimal",
        version="0.1.0",
        summary="",
        description="",
        license="",
        requires_python="",
        requires_dist=None,
        home_page=None,
        project_urls=None,
    )
