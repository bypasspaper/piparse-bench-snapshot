"""Force cold file reads by evicting a file's pages from the OS page cache.

`posix_fadvise(POSIX_FADV_DONTNEED)` drops the clean cached pages of a single
file without root — the same mechanism the handover's rung-1 cold baseline used.
We additionally measure page residency via `mincore(2)` before/after so a run can
PROVE the read was actually cold: DONTNEED is best-effort and silently keeps
pages that are dirty, locked, or under memory pressure, so a blind fadvise can
leave a run partially warm without any error.

Pure stdlib (ctypes + mmap) — no helper binary, no root.
"""

from __future__ import annotations

import ctypes
import ctypes.util
import os
from dataclasses import dataclass
from pathlib import Path

_PAGE = os.sysconf("SC_PAGE_SIZE")

_libc = ctypes.CDLL(ctypes.util.find_library("c") or "libc.so.6", use_errno=True)
_libc.mmap.restype = ctypes.c_void_p
_libc.mmap.argtypes = [
    ctypes.c_void_p, ctypes.c_size_t, ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_long
]
_libc.munmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
_libc.mincore.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_char_p]

_PROT_READ = 0x1
_MAP_SHARED = 0x1
_MAP_FAILED = ctypes.c_void_p(-1).value


@dataclass
class EvictResult:
    path: str
    total_pages: int
    resident_before: int
    resident_after: int

    @property
    def total_mb(self) -> float:
        return self.total_pages * _PAGE / (1024 * 1024)

    @property
    def resident_before_mb(self) -> float:
        return self.resident_before * _PAGE / (1024 * 1024)

    @property
    def resident_after_mb(self) -> float:
        return self.resident_after * _PAGE / (1024 * 1024)

    @property
    def fully_cold(self) -> bool:
        """True if the page cache is effectively empty for this file. A tiny
        residual is tolerated (readahead races / inode metadata): the larger of
        1% of the file or 1 MiB worth of pages."""
        return self.resident_after <= max(self.total_pages // 100, 256)

    @property
    def fully_warm(self) -> bool:
        """True if essentially the whole file is resident in the page cache
        (>=99% of pages) — the inverse check for a deliberately-warmed run."""
        return self.resident_after >= (self.total_pages * 99) // 100


def _resident_pages(fd: int, size: int) -> int:
    """Count file pages currently resident in the page cache via mincore(2)."""
    if size == 0:
        return 0
    addr = _libc.mmap(None, size, _PROT_READ, _MAP_SHARED, fd, 0)
    if addr is None or addr == _MAP_FAILED:
        raise OSError(ctypes.get_errno(), "mmap failed for residency probe")
    try:
        n_pages = (size + _PAGE - 1) // _PAGE
        vec = ctypes.create_string_buffer(n_pages)
        if _libc.mincore(ctypes.c_void_p(addr), size, vec) != 0:
            raise OSError(ctypes.get_errno(), "mincore failed")
        # Low bit of each vec byte = page resident.
        return sum(b & 1 for b in vec.raw)
    finally:
        _libc.munmap(ctypes.c_void_p(addr), size)


def evict_file(path: str | Path, measure: bool = True) -> EvictResult:
    """Drop `path` from the page cache (POSIX_FADV_DONTNEED) so the next read is
    cold. When `measure`, probe resident pages before+after (mincore) to verify
    the eviction actually took — see EvictResult.fully_cold."""
    p = Path(path)
    size = p.stat().st_size
    fd = os.open(str(p), os.O_RDONLY)
    try:
        before = _resident_pages(fd, size) if measure else -1
        # DONTNEED only evicts CLEAN pages; flush any dirty ones first so the
        # eviction is complete (no-op for a static read-only data file).
        try:
            os.fsync(fd)
        except OSError:
            pass
        os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
        after = _resident_pages(fd, size) if measure else -1
        total = (size + _PAGE - 1) // _PAGE
        return EvictResult(str(p), total, before, after)
    finally:
        os.close(fd)


def warm_file(path: str | Path, measure: bool = True) -> EvictResult:
    """Pull `path` fully into the page cache so the next read is a cache hit (the
    WARM arm of a warm-vs-cold A/B). Sequentially reads the whole file; when
    `measure`, probe residency before+after to verify the cache is now full —
    see EvictResult.fully_warm."""
    p = Path(path)
    size = p.stat().st_size
    fd = os.open(str(p), os.O_RDONLY)
    try:
        before = _resident_pages(fd, size) if measure else -1
        os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_WILLNEED)
        # Actually touch every byte: WILLNEED is only an async hint, a real read
        # guarantees the pages are resident before the query starts.
        chunk = 8 * 1024 * 1024
        while os.read(fd, chunk):
            pass
        after = _resident_pages(fd, size) if measure else -1
        total = (size + _PAGE - 1) // _PAGE
        return EvictResult(str(p), total, before, after)
    finally:
        os.close(fd)
