import collections
import threading
import time
import typing as tp
from concurrent.futures import ThreadPoolExecutor

import anyio
import pytest

import rwlocks

pytestmark = pytest.mark.anyio


def test_RWLock() -> None:
    lock = rwlocks.RWLock()
    active_threads: tp.Deque[threading.Thread] = collections.deque()
    duplicate_threads: tp.Deque[threading.Thread] = collections.deque()
    N = 50

    def worker(i: int, read: bool = True) -> None:
        lock_func = lock.read_lock if read else lock.write_lock
        me = threading.current_thread()
        with lock_func() as _l:
            if read:
                assert _l._writer is None
                assert not _l._pending_writers
            else:
                assert _l._writer is not None
                assert not _l._readers
                if active_threads:
                    duplicate_threads.append(me)
                    duplicate_threads.extend(active_threads)

            active_threads.append(me)
            try:
                time.sleep(i / (N * 10))
            finally:
                active_threads.remove(me)

    with ThreadPoolExecutor(max_workers=N) as executor:
        for i in range(N):
            executor.submit(worker, i, read=True)
            executor.submit(worker, i, read=False)

    assert not active_threads
    assert not duplicate_threads


async def test_AsyncRWLock() -> None:
    lock = rwlocks.AsyncRWLock()
    active_threads: tp.Deque[threading.Thread] = collections.deque()
    duplicate_threads: tp.Deque[threading.Thread] = collections.deque()
    N = 50

    async def worker(i: int, read: bool = True) -> None:
        lock_func = lock.read_lock if read else lock.write_lock
        me = threading.current_thread()
        async with lock_func() as _l:
            if read:
                assert _l._writer is None
                assert not _l._pending_writers
            else:
                assert _l._writer is not None
                assert not _l._readers
                if active_threads:
                    duplicate_threads.append(me)
                    duplicate_threads.extend(active_threads)

            active_threads.append(me)
            try:
                await anyio.sleep(i / (N * 10))
            finally:
                active_threads.remove(me)

    async with anyio.create_task_group() as tg:
        for i in range(N):
            tg.start_soon(worker, i, True)
            tg.start_soon(worker, i, False)

    assert not active_threads
    assert not duplicate_threads
