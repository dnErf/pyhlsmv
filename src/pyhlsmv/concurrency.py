import time
import threading
from typing import Any, Dict, Set, Optional, Tuple
from enum import Enum
from queue import Queue

class TrueTime:
    def __init__(self, epsilon:float = 0.001):
        self.epsilon = epsilon
        self.last_ts = 0

    def now(self) -> Tuple[int, int]:
        current = int(time.time() * 1_000_000)
        earliest = current
        latest = current + int(self.epsilon * 1_000_000)
        self.last_ts = latest
        return earliest, latest
    
    def wait_until(self, ts:int) -> None:
        current = int(time.time() * 1_000_000)
        if current < ts:
            time.sleep((ts - current) / 1_000_000)

    def is_after(self, earliest:int, latest:int) -> bool:
        current_earliest, _ = self.now()
        return current_earliest > latest
    
    def overlap(self, tl_start:int, t1_end:int, t2_start:int, t2_end:int) -> bool:
        return t1_end <= t2_end and t2_start <= t1_end
    
class LockMode(Enum):
    READ = "READ"
    WRITE = "WRITE"
    EXCLUSIVE = "EXCLUSIVE"

class LockWaiter:
    def __init__(self, txn_id:int, mode:LockMode):
        self.txn_id = txn_id
        self.mode = mode
        self.acquired = threading.Event()

class Lock:
    def __init__(self, key):
        self.key = key
        self.readers = set()
        self.writer = None
        self.waiters: list[LockWaiter] = []
        self.cv = threading.Condition(threading.RLock())

    def can_acquire(self, txn_id:int, mode:LockMode) -> bool:
        match mode:
            case LockMode.READ:
                return self.writer is None or self.writer == txn_id
            case LockMode.WRITE:
                return len(self.readers) == 0 and (self.writer is None or self.writer == txn_id)
            case LockMode.EXCLUSIVE:
                return len(self.readers) == 0 and self.writer is None
        return None

    def acquire(self, txn_id:int, mode:LockMode, timeout:float = 5.0) -> bool:
        with self.cv:
            start = time.time()
            waiter = LockWaiter(txn_id, mode)
            self.waiters.append(waiter)

            while not self.can_aquire(txn_id, mode):
                elapsed = time.time() - start
                if elapsed > timeout:
                    self.waiters.remove(waiter)
                    return False
                
                remaining = timeout - elapsed
                self.cv.wait(remaining)

            self.waiters.remove(waiter)

            if mode == LockMode.READ:
                self.readers.add(txn_id)
            elif mode in (LockMode.WRITE, LockMode.EXCLUSIVE):
                self.writer = txn_id

            return True
    
    def release(self, txn_id:int) -> None:
        with self.cv:
            if txn_id in self.readers:
                self.readers.discard(txn_id)
            elif self.writer == txn_id:
                self.writer = None

            self.cv.notify_all()

    def is_locked(self) -> bool:
        with self.cv:
            return len(self.readers) > 0 or self.writer is not None
    
class SpannerLockManager:
    def __init__(self):
        self.txn_locks: Dict[int, Set[Tuple[Any, LockMode]]] = {}
        self.nex_txn_id = 0
        self.txn_id_lock = threading.Lock()
        self.lock_table = threading.RLock()
        self.locks = {}
    
    def begin_txn(self) -> int:
        with self.txn_id_lock:
            txn_id = self.next_txn_id
            self.next_txn_id += 1
        self.txn_locks[txn_id] = set()
        return txn_id
    
    def acquire_lock(self, txn_id:int, key:Any, mode:LockMode) -> bool:
        with self.lock_table:
            if key not in self.locks:
                self.locks[key] = Lock(key)

            lock = self.locks[key]
            if lock.acquire(txn_id, mode):
                self.txn_locks[txn_id].add((key, mode))
                return True
            return False
        
    def release_lock(self, txn_id:int, key:Any) -> None:
        with self.lock_table:
            if key in self.locks:
                self.locks[key].release(txn_id)
            self.txn_locks[txn_id].discard((key, LockMode.READ))
            self.txn_locks[txn_id].discard((key, LockMode.WRITE))
            self.txn_locks[txn_id].discard((key, LockMode.EXCLUSIVE))

    def release_all(self, txn_id:int) -> None:
        with self.lock_table:
            for key, _ in list(self.txn_locks.get(txn_id, set())):
                if key in self.locks:
                    self.locks[key].release(txn_id)
            self.txn_locks.pop(txn_id, None)

    def commit(self, txn_id:int) -> None:
        self.release_all(txn_id)

class MVCCVersion:
    def __init__(self, version_id:int, timestamp:Tuple[int,int]):
        self.version_id = version_id
        self.timestamp = timestamp
        self.data = {}

class NonBlockingConcurrencyControl:
    def __init__(self):
        self.lock_manager = SpannerLockManager()
        self.truetime = TrueTime()
        self.write_queue = Queue()
        self.version_lock = threading.Lock()
        self.current_version = 0
        self.versions = {}

    def write(self, key, value) -> int:
        txn_id = self.lock_manager.begin_txn()

        if not self.lock_manager.acquire_lock(txn_id, key, LockMode.WRITE):
            self.lock_manager.release_all(txn_id)
            raise TimeoutError("write lock not acquired")
        
        try:
            earliest, latest = self.truetime.now()
            self.write_queue.put((key, value, (earliest, latest)))

            with self.version_lock:
                self.current_version += 1
                version = MVCCVersion(self.current_version, (earliest, latest))
                version.data[key] = value
                self.versions[self.current_version] = version

            self.lock_manager.commit(txn_id)
            return self.current_version
        except Exception as e:
            self.lock_manager.release_all(txn_id)
            raise

    def read(self, key) -> tuple[Optional[Any], int]:
        version_id = self.current_version
        version = self.versions.get(version_id)
        if version:
            return version.data.get(key), version_id
        return None, version_id
    
    def snapshot_read(self, key, version_id:int) -> Optional[Any]:
        version = self.versions.get(version_id)
        if version:
            return version.data.get(key)
        return None
    
    def scan_snapsnot(self, min_key = None, max_key = None) -> Dict[Any, Any]:
        version_id = self.current_version
        version = self.versions.get(version_id)
        if not version:
            return {}
        
        result = {}
        for k, v in version.data.items():
            if (min_key is None or k >= min_key) and (max_key is None or k < max_key):
                result[k] = v

        return result
