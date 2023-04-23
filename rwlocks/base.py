import typing as tp

import threading
import collections
import contextlib
import anyio


class RWLock:
    def __init__(self) -> None:
        self._cond = threading.Condition()
        self._readers: tp.Dict[threading.Thread, int] = {}
        self._writer: tp.Optional[threading.Thread] = None
        self._pending_writers: tp.Deque[threading.Thread] = collections.deque()

    @contextlib.contextmanager
    def read_lock(self) -> tp.Generator["RWLock", None, None]:
        me = threading.current_thread()
        if me in self._pending_writers:
            raise RuntimeError(
                f"'{me}' cannot acquire read lock while waiting for write lock."
            )

        with self._cond:
            # wait until:
            # - No writer
            # - No pending writers
            while self._writer is not None or self._pending_writers:
                self._cond.wait()

            # Add reader
            self._readers[me] = self._readers.get(me, 0) + 1

        try:
            yield self
        finally:
            with self._cond:
                nb_readers = self._readers.get(me, 0)
                if nb_readers > 1:
                    self._readers[me] -= 1
                else:
                    self._readers.pop(me, None)
                    # Notify when all readers have finished
                    self._cond.notify_all()

    @contextlib.contextmanager
    def write_lock(self) -> tp.Generator["RWLock", None, None]:
        me = threading.current_thread()
        if me is self._writer:
            yield self
            return

        if me in self._readers:
            raise RuntimeError(
                f"'{me}' cannot acquire write lock while holding read lock."
            )

        with self._cond:
            self._pending_writers.append(me)
            # wait until:
            # - no writer
            # - no readers
            # - me is next in line
            while (
                self._writer is not None
                or self._readers
                or self._pending_writers[0] is not me
            ):
                self._cond.wait()

            # Add writer
            self._writer = self._pending_writers.popleft()

        try:
            yield self
        finally:
            with self._cond:
                self._writer = None
                self._cond.notify_all()


class AsyncRWLock:
    def __init__(self) -> None:
        self._cond = anyio.Condition()
        self._readers: tp.Dict[anyio.TaskInfo, int] = {}
        self._writer: tp.Optional[anyio.TaskInfo] = None
        self._pending_writers: tp.Deque[anyio.TaskInfo] = collections.deque()

    @contextlib.asynccontextmanager
    async def read_lock(self) -> tp.AsyncGenerator["AsyncRWLock", None]:
        me = anyio.get_current_task()
        if me in self._pending_writers:
            raise RuntimeError(
                f"'{me}' cannot acquire read lock while waiting for write lock."
            )

        async with self._cond:
            # wait until:
            # - No writer
            # - No pending writers
            while self._writer is not None or self._pending_writers:
                await self._cond.wait()

            # Add reader
            self._readers[me] = self._readers.get(me, 0) + 1

        try:
            yield self
        finally:
            async with self._cond:
                nb_readers = self._readers.get(me, 0)
                if nb_readers > 1:
                    self._readers[me] -= 1
                else:
                    self._readers.pop(me, None)
                    # Notify when all readers have finished
                    self._cond.notify_all()

    @contextlib.asynccontextmanager
    async def write_lock(self) -> tp.AsyncGenerator["AsyncRWLock", None]:
        me = anyio.get_current_task()
        if me is self._writer:
            yield self
            return

        if me in self._readers:
            raise RuntimeError(
                f"'{me}' cannot acquire write lock while holding read lock."
            )

        async with self._cond:
            self._pending_writers.append(me)
            # wait until:
            # - no writer
            # - no readers
            # - me is next in line
            while (
                self._writer is not None
                or self._readers
                or self._pending_writers[0] is not me
            ):
                await self._cond.wait()

            # Add writer
            self._writer = self._pending_writers.popleft()

        try:
            yield self
        finally:
            async with self._cond:
                self._writer = None
                self._cond.notify_all()
