"""Streaming tar.zst archive creation with hash computation."""

import hashlib
import io
import tarfile
from dataclasses import dataclass
from typing import BinaryIO

import zstandard as zstd


@dataclass
class FileMetadata:
    """Metadata collected while streaming a file."""

    path: str
    sha256: str
    size: int


class HashingReader:
    """File-like wrapper that computes hash while reading."""

    def __init__(self, source: BinaryIO, hasher: "hashlib._Hash"):
        self._source = source
        self._hasher = hasher

    def read(self, size: int = -1) -> bytes:
        data = self._source.read(size)
        self._hasher.update(data)
        return data


class StreamingTarZstWriter:
    """Write a tar.zst archive with streaming support.

    Streams file content directly to the tar archive while computing SHA256 hashes.
    Uses tarfile streaming mode and zstd stream compression.
    """

    def __init__(self, output: BinaryIO, compression_level: int = 19):
        """Initialize the streaming writer.

        Args:
            output: File-like object to write compressed data to.
            compression_level: Zstandard compression level (1-22, default 19).
        """
        self._cctx = zstd.ZstdCompressor(level=compression_level)
        self._compressor = self._cctx.stream_writer(output)
        self._tar = tarfile.open(fileobj=self._compressor, mode="w|")
        self._files: list[FileMetadata] = []

    def add_file(self, dest_path: str, content: bytes) -> FileMetadata:
        """Add a file from bytes content.

        Args:
            dest_path: Path within the archive.
            content: File content as bytes.

        Returns:
            FileMetadata with path, hash, and size.
        """
        sha256 = hashlib.sha256(content).hexdigest()

        info = tarfile.TarInfo(name=dest_path)
        info.size = len(content)
        self._tar.addfile(info, io.BytesIO(content))

        meta = FileMetadata(path=dest_path, sha256=sha256, size=len(content))
        self._files.append(meta)
        return meta

    def add_stream(self, dest_path: str, source: BinaryIO, size: int) -> FileMetadata:
        """Add a file by streaming from source, computing hash during copy.

        Args:
            dest_path: Path within the archive.
            source: File-like object to read from.
            size: Size of the source file (required for tar header).

        Returns:
            FileMetadata with computed SHA256 hash.
        """
        hasher = hashlib.sha256()

        info = tarfile.TarInfo(name=dest_path)
        info.size = size

        hashing_reader = HashingReader(source, hasher)
        self._tar.addfile(info, hashing_reader)

        sha256 = hasher.hexdigest()
        meta = FileMetadata(path=dest_path, sha256=sha256, size=size)
        self._files.append(meta)
        return meta

    def get_file_metadata(self) -> list[FileMetadata]:
        """Return metadata for all files added."""
        return self._files.copy()

    def close(self) -> None:
        """Close the tar and compressor."""
        self._tar.close()
        self._compressor.close()

    def __enter__(self) -> "StreamingTarZstWriter":
        return self

    def __exit__(self, *args) -> None:
        self.close()
