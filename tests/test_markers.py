"""Tests for marker conversion functionality."""

import pytest
from packaging.markers import Marker

from cart_wheel.conda import (
    DependencyConversionError,
    _extract_extra_from_marker,
    _marker_to_condition,
)

# _marker_to_condition tests


def test_marker_to_condition_python_version_less_than():
    """Python version less than is converted."""
    marker = Marker("python_version < '3.11'")
    result = _marker_to_condition(marker)
    assert result == "python <3.11"


def test_marker_to_condition_python_version_greater_equal():
    """Python version greater or equal is converted."""
    marker = Marker("python_version >= '3.8'")
    result = _marker_to_condition(marker)
    assert result == "python >=3.8"


def test_marker_to_condition_sys_platform_win32():
    """sys_platform win32 becomes __win."""
    marker = Marker("sys_platform == 'win32'")
    result = _marker_to_condition(marker)
    assert result == "__win"


def test_marker_to_condition_sys_platform_linux():
    """sys_platform linux becomes __linux."""
    marker = Marker("sys_platform == 'linux'")
    result = _marker_to_condition(marker)
    assert result == "__linux"


def test_marker_to_condition_sys_platform_darwin():
    """sys_platform darwin becomes __osx."""
    marker = Marker("sys_platform == 'darwin'")
    result = _marker_to_condition(marker)
    assert result == "__osx"


def test_marker_to_condition_platform_system_windows():
    """platform_system Windows becomes __win."""
    marker = Marker("platform_system == 'Windows'")
    result = _marker_to_condition(marker)
    assert result == "__win"


def test_marker_to_condition_os_name_nt():
    """os_name nt becomes __win."""
    marker = Marker("os_name == 'nt'")
    result = _marker_to_condition(marker)
    assert result == "__win"


def test_marker_to_condition_os_name_posix():
    """os_name posix becomes __unix."""
    marker = Marker("os_name == 'posix'")
    result = _marker_to_condition(marker)
    assert result == "__unix"


def test_marker_to_condition_complex_and():
    """Complex marker with 'and' is converted."""
    marker = Marker("python_version >= '3.8' and sys_platform == 'win32'")
    result = _marker_to_condition(marker)
    assert "python >=3.8" in result
    assert "__win" in result
    assert "and" in result


def test_marker_to_condition_complex_or():
    """Complex marker with 'or' is converted."""
    # win32 or linux -> __win or __linux
    marker = Marker("sys_platform == 'win32' or sys_platform == 'linux'")
    result = _marker_to_condition(marker)
    assert "__win" in result
    assert "__linux" in result
    assert "or" in result


def test_marker_to_condition_duplicate_platform():
    """Duplicate platform values are both included."""
    # win32 and cygwin both map to __win
    marker = Marker("sys_platform == 'win32' or sys_platform == 'cygwin'")
    result = _marker_to_condition(marker)
    assert "__win" in result


def test_marker_to_condition_platform_version_with_win():
    """platform_version combined with Windows platform marker."""
    marker = Marker("sys_platform == 'win32' and platform_version >= '10.0'")
    result = _marker_to_condition(marker)
    assert result == "__win >=10.0"


def test_marker_to_condition_platform_version_with_osx():
    """platform_version combined with macOS platform marker."""
    marker = Marker("sys_platform == 'darwin' and platform_version >= '21.0'")
    result = _marker_to_condition(marker)
    assert result == "__osx >=21.0"


def test_marker_to_condition_platform_version_first():
    """platform_version before platform marker is also handled."""
    marker = Marker("platform_version >= '5.4' and sys_platform == 'linux'")
    result = _marker_to_condition(marker)
    assert result == "__linux >=5.4"


def test_marker_to_condition_platform_version_alone_raises():
    """platform_version without platform marker raises error."""
    marker = Marker("platform_version >= '10.0'")
    with pytest.raises(DependencyConversionError) as exc_info:
        _marker_to_condition(marker)
    assert "platform_version" in str(exc_info.value)


def test_marker_to_condition_unsupported_raises():
    """Unsupported marker variables raise DependencyConversionError."""
    marker = Marker("platform_machine == 'x86_64'")
    with pytest.raises(DependencyConversionError) as exc_info:
        _marker_to_condition(marker)
    assert "platform_machine" in str(exc_info.value)


# _extract_extra_from_marker tests


def test_extract_extra_from_marker_simple():
    """Simple extra marker returns extra name with no remaining condition."""
    marker = Marker("extra == 'dev'")
    extra_name, remaining = _extract_extra_from_marker(marker)
    assert extra_name == "dev"
    assert remaining is None


def test_extract_extra_from_marker_double_quotes():
    """Extra marker with double quotes works."""
    marker = Marker('extra == "test"')
    extra_name, remaining = _extract_extra_from_marker(marker)
    assert extra_name == "test"
    assert remaining is None


def test_extract_extra_from_marker_not_extra():
    """Non-extra marker returns None for both values."""
    marker = Marker("python_version < '3.11'")
    extra_name, remaining = _extract_extra_from_marker(marker)
    assert extra_name is None
    assert remaining is None


def test_extract_extra_from_marker_with_condition():
    """Extra marker with additional condition returns both."""
    marker = Marker("extra == 'dev' and python_version >= '3.8'")
    extra_name, remaining = _extract_extra_from_marker(marker)
    assert extra_name == "dev"
    # Quote style may vary, check content
    assert "python_version" in remaining
    assert "3.8" in remaining


def test_extract_extra_from_marker_condition_first():
    """Condition before extra is also handled."""
    marker = Marker("sys_platform == 'win32' and extra == 'dev'")
    extra_name, remaining = _extract_extra_from_marker(marker)
    assert extra_name == "dev"
    # Quote style may vary, check content
    assert "sys_platform" in remaining
    assert "win32" in remaining
