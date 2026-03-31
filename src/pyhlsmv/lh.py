import time
import threading
from typing import Any, Dict, List, Iterator, Optional, Tuple
from dataclasses import dataclass
from pyhlsmv.concurrency import NonBlockingConcurrencyControl, TrueTime
from pyhlsmv.storage import HStorage, TableSchema
from pyhlsmv.lsm import LSMTree, LSMSnapshot
from pyhlsmv.sst import SSTable

@dataclass
class WriteResult:
    success: bool
    timestamp: Optional[Tuple[int, int]] = None
    version: Optional[int] = None
    message: str = ""
    
class LH:
    def __init__(self, base_path:str = "./lhs", memtable_size:int = 64 * 1024 * 1024, compaction_interal:float = 5.0):
        self.base_path = base_path
        self.memtable_size = memtable_size
        self.compaction_interval = compaction_interal
        self.truetime = TrueTime()
        self.storage = HStorage()
        self.concurrency = NonBlockingConcurrencyControl()
        self.tables:Dict[str, LSMTree] = {}
        self.table_schemas:Dict[str, List[TableSchema]] = {}
        self.table_versions:Dict[str, int] = {}
        self.write_lock = threading.RLock()
        self.compaction_thread:Optional[threading.Thread] = None
        self.running = False
        self.start_compaction_worker()

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def _compaction_loop(self) -> None:
        while self.running:
            try:
                time.sleep(self.compaction_interval)

                for table_key, lsm in self.tables.items():
                    if len(lsm.immutable_memtables) > 0:
                        lsm._compact()
                        schema_name, table_name = table_key.split(".")
                        self.persist_table(schema_name, table_name, "auto-compact")
            except Exception as exc:
                print("compaction error")

    def start_compaction_worker(self) -> None:
        self.running = True
        self.compaction_thread = threading.Thread(target = self._compaction_loop, daemon = True)
        self.compaction_thread.start()

    def stop (self) -> None:
        self.running = False
        if self.compaction_thread:
            self.compaction_thread.join(timeout = 5)

    def create_schema(self, schema_name:str) -> None:
        self.storage.create_schema(schema_name)

    def create_table(self, schema_name:str, table_name:str, columns:List[TableSchema]) -> None:
        table_key = f"{schema_name}.{table_name}"
        self.storage.create_table(schema_name, table_name, columns)
        self.tables[table_key] = LSMTree(memtable_size = self.memtable_size)
        self.table_schemas[table_key] = columns
        self.table_versions[table_key] = 0

    def write(self, schema_name:str, table_name:str, key, value) -> WriteResult:
        table_key = f"{schema_name}.{table_name}"
        if table_key not in self.tables:
            return WriteResult(False, message = "table not found")
        
        try:
            with self.write_lock:
                earliest, latest = self.truetime.now()
                version = self.concurrency.write(key, value)

                self.tables[table_key].put(key, value)
                self.table_versions[table_key] = version

                return WriteResult(
                    success = True,
                    timestamp = (earliest, latest),
                    version = version,
                    message = "write committed"
                )
        except Exception as exc:
            return WriteResult(False, message = "write failed")
        
    def read(self, schema_name:str, table_name:str, key) -> Optional[Any]:
        table_key = f"{schema_name}.{table_name}"

        if table_key not in self.tables:
            return None
        
        return self.tables[table_key].get(key)
    
    def scan(self, schema_name:str, table_name:str, min_key = None, max_key = None) -> Iterator[Tuple[Any, Any]]:
        table_key = f"{schema_name}.{table_name}"

        if table_key not in self.tables:
            return
        
        return self.tables[table_key].scan(min_key, max_key)
    
    def create_snapshot(self, schema_name:str, table_name:str) -> LSMSnapshot:
        table_key = f"{schema_name}.{table_name}"

        if table_key not in self.tables:
            raise ValueError("table not found")
        
        return self.tables[table_key].create_snapshot()
    
    def persist_table(self, schema_name:str, table_name:str, commit_message:str = "") -> bool:
        table_key = f"{schema_name}.{table_name}"
        if table_key not in self.tables:
            return False
        
        try:
            lsm = self.tables[table_key]
            earliest, latest = self.truetime.now()
            version = self.table_versions[table_key]

            sstable = SSTable()
            for key, value in lsm.scan():
                sstable.put(key, value)
            sstable.freeze()

            self.storage.write_sstable(
                schema_name, 
                table_name, 
                sstable, 
                timestamp = (earliest, latest), 
                version = version,
                commit_message = commit_message,
            )

            return True
        except Exception as exc:
            print("persistence failed")
            return False
        
    def get_stats(self, schema_name:str, table_name:str) -> Dict[str, Any]:
        table_key = f"{schema_name}.{table_name}"

        if table_key not in self.tables:
            return {}
        
        lsm = self.tables[table_key]
        return {
            "table_name": table_name,
            "schema_name": schema_name,
            "total_records": len(lsm),
            "memtable_size": lsm.current_size,
            "immutable_memtables": len(lsm.immutable_memtables),
            "lsm_levels": { level: len(tables) for level, tables in lsm.levels.items() },
            "version": self.table_versions.get(table_key, 0),
        }
