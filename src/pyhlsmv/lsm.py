import heapq
from typing import Any, Iterator, List, Optional, Tuple
from pyhlsmv.lru_cache import LRUCache
from pyhlsmv.skip_list import SkipList
from pyhlsmv.sst import SSTable
from collections import defaultdict

class LSMTree:
    def __init__(self, memtable_size:int = 64 * 1024 * 1024, level_ratio:int = 10, cache_capacity:int = 10000):
        self.memtable = SkipList()
        self.memtable_size = self.memtable_size
        self.cache = LRUCache(cache_capacity)
        self.level = defaultdict(list)
        self.level_ratio = level_ratio
        self.immutable_memtables = []
        self.snapshot_version = {}
        self.current_size = 0
        self.version = 0

    def __len__(self) -> int:
        return sum(len(imemtable) for imemtable in self.immutable_memtables) + \
        len(self.memtable) + \
        sum(len(sstable) for level_tables in self.levels.values() for sstable in level_tables)

    def put(self, key, value) -> None:
        self.memtable.insert(key, value)
        self.cache.put(key, value)
        self.current_size += 1

        if self.current_size >= self.memtable_size:
            self._freeze_memtable()

    def get(self, key) -> Optional[Any]:
        cached = self.cache.get(key)
        if cached is not None:
            return cached
        
        result = self.memtable.search(key)
        if result is not None:
            self.cache.put(key, result)
            return result
        
        for imemtable in reversed(self.immutable_memtables):
            result = imemtable.search(key)
            if result is not None:
                self.cache.put(key, result)
                return result
            
        for level in sorted(self.levels.keys()):
            sstables = self.levels[level]
            for sstable in reversed(sstables):
                if sstable.contains(key):
                    result = sstable.get(key)
                    if result is not None:
                        self.cache.put(key, result)
                        return result
                    
        return None
    
    def scan(self, min_key = None, max_key = None) -> Iterator[Tuple[Any, Any]]:
        seen = set()

        for key, value in self.memtable:
            if (min_key is None or key >= min_key) and (max_key is None or key < max_key):
                if key not in seen:
                    seen.add(key)
                    yield key, value

        for imemtable in reversed(self.immutable_memtables):
            for key, value in imemtable:
                if (min_key is None or key >= min_key) and (max_key is None or key < max_key):
                    if key not in seen:
                        seen.add(key)
                        yield key, value

        level_iterators = []
        for level in sorted(self.levels.keys()):
            for sstable in self.levels[level]:
                level_iterators.append(sstable.scan())

        def merge_iterators(iterators: List[Iterator]) -> Iterator:
            heap = []
            for i, it in enumerate(iterators):
                try:
                    key, value = next(it)
                    heapq.heappush(heap, (key, i, value, it))
                except StopIteration:
                    pass

            while heap:
                key, _, value, it = heapq.heappop(heap)
                if key not in seen:
                    if (min_key is None or key >= min_key) and (max_key is None or key < max_key):
                        seen.add(key)
                        yield key, value

                try:
                    next_key, next_value = next(it)
                    heapq.heappush(heap, (next_key, _, next_value, it))
                except StopIteration:
                    pass

        if level_iterators:
            yield from merge_iterators(level_iterators)

    def create_snapshot(self) -> "LSMSnapshot":
        return LSMSnapshot(self, self.version)

    def _freeze_memtable(self) -> None:
        self.immutable_memtables.append(self.memtable)
        self.memtable = SkipList()
        self.current_size = 0
        self.version += 1

        if len(self.immutable_memtables) >= 4:
            self._compact()

    def _compact(self) -> None:
        if not self.immutable_memtables:
            return
        
        level_0_tables = []
        for imemtable in self.immutable_memtables:
            sstable = self._skiplist_to_sst(imemtable)
            level_0_tables.append(sstable)
        
        self.immutable_memtables.clear()
        self.levels[0].extend(self.level_0_tables)
        
        level = 0
        while level in self.levels and len(self.levels[level]) > self.level_ratio:
            tables_to_merge = self.levels[level]
            merged = self._merge_sstables(tables_to_merge)

            if level + 1 not in self.levels:
                self.levels[level + 1] = []

            self.levels[level + 1].extend(merged)
            del self.levels[level]
            level += 1

    def _skiplist_to_sst(skip_list) -> SSTable:
        sstable = SSTable()
        for key, value in skip_list:
            sstable.put(key, value)
        
        sstable.freeze()
        return sstable
    
class LSMSnapshot:
    def __init__(self, lsm: LSMTree, version: int):
        self.lsm = lsm
        self.version = version

    def get(self, key) -> Optional[Any]:
        return self.lsm.get(key)
    
    def scan(self, min_key = None, max_key = None) -> Iterator[Tuple[Any, Any]]:
        return self.lsm.scan(min_key, max_key)
