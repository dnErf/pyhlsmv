import threading
from collections import OrderedDict
from typing import Any, Optional, Dict

class LRUCache:
    def __init__(self, capacity:int = 10000):
        self.access_order = OrderedDict()
        self.lock = threading.RLock()
        self.cache:Dict[Any, Any] = {}
        self.capacity = capacity

    def get(self, key:Any) -> Optional[Any]:
        with self.lock:
            if key in self.cache:
                self.access_order.move_to_end(key)
                return self.cache[key]
            return None
        
    def put(self, key:Any, value:Any) -> None:
        with self.lock:
            if key in self.cache:
                self.access_order.move_to_end(key)
                self.cache[key] = value
            else:
                if len(self.cache) >= self.capacity:
                    lru_key = next(iter(self.access_order))
                    del self.cache[lru_key]
                    del self.access_order[lru_key]
                self.cache[key] = value
                self.access_order[key] = None

    def invalidate(self, key:Any) -> None:
        with self.lock:
            if key in self.cache:
                del self.cache[key]
                del self.access_order[key]

    def clear(self) -> None:
        with self.lock:
            self.cache.clear()
            self.access_order.clear()

    def size(self) -> int:
        with self.lock:
            return len(self.cache)
        
    def __contains__(self, key:Any) -> bool:
        with self.lock:
            return key in self.cache
