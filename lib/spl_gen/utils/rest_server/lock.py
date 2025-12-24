import logging
import sys
import os
import errno

from splunk.clilib.bundle_paths import make_splunkhome_path

sys.path.append(make_splunkhome_path(["etc", "apps", "Splunk_AI_Assistant_Cloud", "lib"]))

import portalocker
from threading import Lock, current_thread, main_thread
from typing import List, Dict, Optional
from contextlib import contextmanager
from ...constants import FILE_LOCK_DIRECTORY, WINDOWS_SYS_PLATFORM
from os import makedirs, remove, getpid
from splunk.clilib.bundle_paths import make_splunkhome_path
from hashlib import blake2b
from ..profiler import Profile

is_windows = sys.platform in WINDOWS_SYS_PLATFORM

if not is_windows:
    import signal


logger = logging.getLogger(__name__)


def make_lock_directory(namespace):
    makedirs(get_lock_directory(namespace), exist_ok=True)


def get_lock_directory(namespace):
    # mission_control pattern matches the pattern of the folder created by ingestion helpers
    # using it here to avoid creating multiple mc var/run/splunk folders
    return make_splunkhome_path(["var", "run", "splunk", "Splunk_AI_Assistant_Cloud", FILE_LOCK_DIRECTORY, namespace])


@contextmanager
def os_time_out(seconds: int):
    """
    Needed to support timeouts on os file operations
    Example:
    with os_time_out(1):
        f = open("test.lck", "w")
        try:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        except OSError as e:
            if e.errno != errno.EINTR:
                raise e
            print "Lock timed out"
    """
    if is_windows or current_thread() is not main_thread():
        # Windows-specific implementation using threading.Timer
        import threading

        def interrupt():
            raise InterruptedError(f"Timeout occurred after {seconds} seconds")

        timer = threading.Timer(seconds, interrupt)
        timer.start()
        try:
            yield
        finally:
            timer.cancel()
    else:
        # Unix-like implementation using signals
        def timeout_handler(signum, frame):
            raise InterruptedError(f"Timeout occurred after {seconds} seconds")

        original_handler = signal.signal(signal.SIGALRM, timeout_handler)
        try:
            signal.alarm(seconds)
            yield
        finally:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, original_handler)


def hash_object_id(object_id: str, max_locks: Optional[int]):
    if max_locks and max_locks > 0:
        return str(int(blake2b(object_id.encode()).hexdigest(), 16) % max_locks) # nosemgrep

    return object_id


class FileLock:
    def __init__(
        self, namespace: str, object_id: str, fail_without_lock: bool, dir_initialized: bool = False, timeout: int = -1
    ):
        self.object_id = object_id
        if not namespace:
            ValueError("namespace must be defined and nonempty")
        self.namespace = namespace
        self.timeout = timeout
        if not dir_initialized:
            make_lock_directory(self.namespace)
        self.locking_file_name = f"{object_id}.flock"
        self.locking_file_location = os.path.join(get_lock_directory(self.namespace), self.locking_file_name)
        self._locking_file = None
        # To make file locks in-memory exclusive, moving to take the file lock should be locked
        self._lock = Lock()
        self._file_open = False
        self._fail_without_lock = fail_without_lock

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, type, value, traceback):
        self.release()

    def acquire(self, blocking: bool = True):
        with self._lock:
            if self.timeout > 0:
                with os_time_out(self.timeout):
                    return self._acquire(blocking)
            else:
                return self._acquire(blocking)

    def _acquire(self, blocking: bool):
        if not self._file_open:
            # pylint: disable=consider-using-with
            self._locking_file = open(self.locking_file_location, 'w')
            self._file_open = True
        else:
            # should only happen if acquisition timed out but failure was not set to raise
            logger.warning(
                "Attempted to open a file when it was already open",
                extra={"metadata": {"locking_file": self.locking_file_name, "namespace": self.namespace}},
            )
        try:
            if not blocking:
                portalocker.lock(self._locking_file, portalocker.LOCK_EX | portalocker.LOCK_NB)
            else:
                portalocker.lock(self._locking_file, portalocker.LOCK_EX)
            return True
        except portalocker.exceptions.LockException as le:
            # Failed to acquire lock (Timeout or Nonblocking condition)
            logger.exception(
                "Failed to acquire lock",
                extra={
                    "metadata": {
                        "timeout": self.timeout,
                        "locking_file": self.locking_file_name,
                        "namespace": self.namespace,
                    }
                },
            )
            if self._fail_without_lock:
                raise RuntimeError("Failed to acquire lock") from le
            return False

    def release(self):
        with self._lock:
            if self._file_open:
                try:
                    portalocker.unlock(self._locking_file)
                    self._locking_file.close()
                    self._file_open = False
                except OSError as ose:
                    if ose.errno == errno.EBADF:
                        logger.warning(
                            "Attempted to unlock a file that was not locked",
                            extra={"metadata": {"locking_file": self.locking_file_name, "namespace": self.namespace}},
                        )
                        # Do not raise
                        # this can happen if lock was ignored due to timeout and unlocked by the overriding process
                        return
                    raise ose

    def __del__(self):
        self.release()

    def clean(self):
        self.release()
        remove(self.locking_file_location)


class ObjectLock:
    def __init__(
        self,
        object_id: str,
        use_file_lock: bool,
        fail_without_lock: bool = False,
        file_namespace: Optional[str] = None,
        dir_initialized: bool = False,
        max_locks: Optional[int] = None,
        wait_time: int = -1,
    ):
        self.object_id = object_id
        self.wait_list: List[int] = list()
        self.add_to_wait()
        self._lock = Lock()
        self._use_file_lock = use_file_lock
        self.wait_time = wait_time
        if self._use_file_lock:
            self._file_lock = FileLock(
                file_namespace,
                self.object_id,
                fail_without_lock,
                dir_initialized=dir_initialized,
                timeout=self.wait_time,
            )
        self._fail_without_lock = fail_without_lock

    def add_to_wait(self):
        self.wait_list.append(getpid())

    def remove_from_wait(self):
        try:
            self.wait_list.remove(getpid())
        except ValueError:
            pass

    def acquire(self):
        with Profile("acquire_time") as profiler:
            with Profile("in_memory_acquire_time", profiler=profiler, previous_time="acquire_time"):
                # pylint: disable=consider-using-with
                acquired = self._lock.acquire(timeout=self.wait_time)
            if not acquired:
                if self._fail_without_lock:
                    raise RuntimeError(
                        f"Failed to acquire in-memory lock for {self.object_id} after {self.wait_time} seconds!"
                    )
            if self._use_file_lock:
                with Profile("acquire_file_time", profiler=profiler):
                    # Will raise RuntimeException if set to fail without lock
                    try:
                        acquired = self._file_lock.acquire()
                    except RuntimeError as re:
                        # Release in memory lock as file lock failed
                        self._lock.release()
                        if self._fail_without_lock:
                            raise re
        log_metadata = profiler.metadata()
        log_metadata["metadata"]["wait_list"] = len(self.wait_list)
        log_metadata["metadata"]["acquired"] = bool(acquired)
        logger.info("Lock acquired", extra=log_metadata)

    def release(self):
        self._lock.release()
        if self._use_file_lock:
            self._file_lock.release()

    def clean(self):
        if self._use_file_lock:
            self._file_lock.clean()


class KvObjectLocker:
    dir_initialized = False

    def __init__(
        self,
        wait_time: int = -1,
        fail_without_lock: bool = False,
        use_file_locks: bool = False,
        file_namespace: Optional[str] = None,
        max_locks: Optional[int] = None,
    ):
        self._pool_lock = Lock()
        self._lock_pool: Dict[str, ObjectLock] = dict()
        self.wait_time = wait_time
        self.fail_without_lock = fail_without_lock
        # File locks are required for locking between parallel python processes
        self.use_file_locks = use_file_locks
        self.file_namespace = file_namespace
        self.max_locks = max_locks
        if self.use_file_locks:
            if not self.file_namespace:
                raise ValueError("file_namespace must be defined to use file locks")
            make_lock_directory(self.file_namespace)
            self.dir_initialized = True

    def acquire_locks(self, object_ids):
        # Sort object ids to ensure locks are acquired in a consistent order to prevent deadlocks
        sorted_object_ids = sorted(set(self.hash_object_ids(object_ids)))

        # Get locks and add object id to wait list
        locks: List[ObjectLock] = list()
        with self._pool_lock:
            for object_id in sorted_object_ids:
                if object_id in self._lock_pool:
                    self._lock_pool[object_id].add_to_wait()
                else:
                    self._lock_pool[object_id] = ObjectLock(
                        object_id=object_id,
                        use_file_lock=self.use_file_locks,
                        file_namespace=self.file_namespace,
                        dir_initialized=self.dir_initialized,
                        wait_time=self.wait_time,
                        fail_without_lock=self.fail_without_lock,
                        max_locks=self.max_locks,
                    )
                locks.append(self._lock_pool[object_id])

        # Acquire locks in ascending order to prevent deadlocks
        for lock in locks:
            lock.acquire()
        return locks

    def release_locks(self, object_ids: List[str]):
        # Release locks in descending order to prevent deadlocks
        sorted_object_ids = sorted(set(self.hash_object_ids(object_ids)), reverse=True)
        with self._pool_lock:
            for object_id in sorted_object_ids:
                self._lock_pool[object_id].release()
                self._lock_pool[object_id].remove_from_wait()
                if len(self._lock_pool[object_id].wait_list) == 0:
                    # Last thread finished, delete the lock reference
                    del self._lock_pool[object_id]

    def hash_object_ids(self, object_ids):
        if self.max_locks:
            return [hash_object_id(object_id, self.max_locks) for object_id in object_ids]

        return object_ids


class GroupLock:
    """
    Use by leveraging a with statement:

        with GroupLock(["a", "b", "c"], object_locker):
            <your locked code>

    object_locker must be a common reference between threads you want to lock.

    # Creates a locker with infinite blocking time
    object_locker = KvObjectLocker()
    # Creates a locker with 5 second blocking time before relinquishing the lock
    object_locker = KvObjectLocker(5)

    Requests using the persistent rest server have a common object locker in cache pool:

        with GroupLock(["a", "b", "c"], self.request_context.cache_pool.object_locker):
            <fetch an object>
            <manipulate object>
            <save object>
    """

    def __init__(self, object_ids: List[str], object_locker: Optional[KvObjectLocker]):
        self.object_ids: List[str] = object_ids
        self._locker: Optional[KvObjectLocker] = object_locker

    def __enter__(self):
        if self._locker:
            # Lock the objects
            return self._locker.acquire_locks(self.object_ids)
        return None

    def __exit__(self, type, value, traceback):
        if self._locker:
            # Unlock the objects
            self._locker.release_locks(self.object_ids)
