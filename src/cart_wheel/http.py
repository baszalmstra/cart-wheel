"""HTTP client with disk caching for PyPI requests."""

from __future__ import annotations

from pathlib import Path

from hishel import SyncSqliteStorage
from hishel.httpx import SyncCacheClient

# Default cache directory
_CACHE_DIR = Path.home() / ".cache" / "cart-wheel" / "http"


def get_client(cache_dir: Path | None = None) -> SyncCacheClient:
    """Get an HTTP client with SQLite-based caching.

    Uses hishel for HTTP caching which respects cache headers
    and stores responses in a SQLite database.

    Args:
        cache_dir: Directory for cache storage. Defaults to ~/.cache/cart-wheel/http

    Returns:
        A SyncCacheClient with caching enabled.
    """
    cache_path = cache_dir or _CACHE_DIR
    cache_path.mkdir(parents=True, exist_ok=True)

    storage = SyncSqliteStorage(database_path=cache_path / "cache.db")
    return SyncCacheClient(storage=storage, timeout=30.0)


# Module-level cached client (lazily initialized)
_client: SyncCacheClient | None = None


def get_cached_client() -> SyncCacheClient:
    """Get a shared cached HTTP client.

    This returns a module-level client that is reused across calls,
    avoiding the overhead of creating new connections.
    """
    global _client
    if _client is None:
        _client = get_client()
    return _client


def clear_cache(cache_dir: Path | None = None) -> None:
    """Clear the HTTP cache.

    Args:
        cache_dir: Cache directory to clear. Defaults to ~/.cache/cart-wheel/http
    """
    import shutil

    cache_path = cache_dir or _CACHE_DIR
    if cache_path.exists():
        shutil.rmtree(cache_path)
