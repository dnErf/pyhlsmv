from typing import Any, Dict, List, Iterator, Optional, Tuple
import hashlib
import struct
import pyarrow as pa
import pyarrow.ipc as ipc

class BloomFilter:
    def __init__(self, size:int = 10000, hash_count:int = 1):
        self.size = size
        self.hash_count = hash_count
        self.bits = bytearray((size + 7) // 8)

    def _hash(self, item, seed:int) -> int:
        h = hashlib.sha256(f"{item}:{seed}".encode("utf-8")).digest()
        return struct.unpack("<I", h[:4])[0] % self.size
    
    def add(self, item:Any) -> None:
        for i in range(self.hash_count):
            idx = self._hash(item, i)
            byte_idx = idx // 8
            bit_idx = idx % 8
            self.bits[byte_idx] |= (1 << bit_idx)

    def contains(self, item:Any) -> bool:
        for i in range(self.hash_count):
            idx = self._hash(item, i)
            byte_idx = idx // 8
            bit_idx = idx % 8

            if not (self.bits[byte_idx] & (1 << bit_idx)):
                return False
            
        return True
    
class SSTIndex:
    def __init__(self):
        self.entries:Dict[Any, int] = {}
        self.keys_sorted:List[Any] = []

    def add_entry(self, key, offset:int) -> None:
        self.entries[key] = offset
        if key not in self.keys_sorted:
            self.keys_sorted.append(key)
            self.keys_sorted.sort()

    def find_range(self, min_key, max_key) -> List[Any]:
        return [k for k in self.keys_sorted if min_key <= k < max_key]
    
class SSTable:
    def __init__(self, level:int = 0, compression:str = "snappy"):
        self.index = SSTIndex()
        self.bloom_filter = BloomFilter()
        self.level = level
        self.frozen = False
        self.compression = compression
        self.data = {}
        self.min_key:Optional[Any] = None
        self.max_key:Optional[Any] = None

    def contains(self, key) -> bool:
        return self.bloom_filter.contains(key) and key in self.data
    
    def get(self, key) -> Optional[Any]:
        if not self.bloom_filter.contains(key):
            return None
        
        return self.data.get(key)
    
    def put(self, key, value) -> None:
        if self.frozen:
            raise ValueError("")
        
        self.data[key] = value
        self.bloom_filter.add(key)
        if self.min_key is None or key < self.min_key:
            self.min_key = key
        if self.max_key is None or key > self.max_key:
            self.max_key = key

    def freeze(self) -> None:
        if not self.frozen:
            sorted_keys = sorted(self.data.keys())
            for i, key in enumerate(sorted_keys):
                self.index.add_entry(key, i)
            self.frozen = True

    def range_query(self, min_key, max_key) -> Iterator[Tuple[Any, Any]]:
        if not self.frozen:
            self.freeze()

        relevant_keys = self.index.find_range(min_key, max_key)
        for key in relevant_keys:
            yield key, self.data[key]

    def scan(self) -> Iterator[Tuple[Any, Any]]:
        if not self.frozen:
            self.freeze()

        for key in sorted(self.data.keys()):
            yield key, self.data[key]

    def to_arrow_table(self) -> pa.Table:
        if not self.frozen:
            self.freeze()

        keys = []
        values = []
        for k, v in self.scan():
            keys.append(k)
            values.append(str(v) if not isinstance(v, (int, float, str)) else v)

        return pa.table({
            "key": pa.array(keys),
            "values": pa.array(values),
        })
    
    def to_ipc_bytes(self) -> bytes:
        arrow_table = self.to_arrow_table()
        sink = pa.BufferOutputStream()
        with ipc.new_stream(sink, arrow_table.schema) as writer:
            writer.write_table(arrow_table)
        return sink.getValue().to_pybytes()
    
    def __len__(self) -> int:
        return len(self.data)
    
    def __iter__(self) -> Iterator[Tuple[Any, Any]]:
        return self.scan()
    
    @staticmethod
    def from_ipc_bytes(data:bytes) -> "SSTable":
        reader = ipc.open_stream(data)
        arrow_table = reader.read_all()

        sstable = SSTable()
        keys = arrow_table.column("key").to_pylist()
        values = arrow_table.column("value").to_pylist()

        for k, v in zip(keys, values):
            sstable.put(k, v)
        
        sstable.freeze()
        return sstable
