import typing as tp

from rwlocks.base import RWLock, AsyncRWLock
import collections
import contextlib


class _CleanupMixin:
    def __init__(self) -> None:
        self._locks: tp.MutableMapping[str, tp.Any] = {}

    def _cleanup(self, key: str) -> None:
        if key not in self._locks:
            return

        lock = self._locks[key]
        if lock._writer or lock._readers or lock._pending_writers:
            return

        # If lock is empty remove it
        self._locks.pop(key, None)
        return

    def _get_lock(self, key: str) -> tp.Union[RWLock, AsyncRWLock]:
        raise NotImplementedError()


class KeyRWLock(_CleanupMixin):
    def __init__(self) -> None:
        self._locks: tp.MutableMapping[str, RWLock] = collections.defaultdict(RWLock)

    @contextlib.contextmanager
    def read_lock(self, key: str) -> tp.Generator["RWLock", None, None]:
        lock = self._locks[key]
        with lock.read_lock() as locked:
            yield locked
        self._cleanup(key)

    @contextlib.contextmanager
    def write_lock(self, key: str) -> tp.Generator["RWLock", None, None]:
        lock = self._locks[key]
        with lock.write_lock() as locked:
            yield locked
        self._cleanup(key)


class KeyAsyncRWLock(_CleanupMixin):
    def __init__(self) -> None:
        self._locks: tp.MutableMapping[str, AsyncRWLock] = collections.defaultdict(
            AsyncRWLock
        )

    @contextlib.asynccontextmanager
    async def read_lock(self, key: str) -> tp.AsyncGenerator["AsyncRWLock", None]:
        lock = self._locks[key]
        async with lock.read_lock() as locked:
            yield locked
        self._cleanup(key)

    @contextlib.asynccontextmanager
    async def write_lock(self, key: str) -> tp.AsyncGenerator["AsyncRWLock", None]:
        lock = self._locks[key]
        async with lock.write_lock() as locked:
            yield locked
        self._cleanup(key)
