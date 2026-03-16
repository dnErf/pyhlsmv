import json
import pyarrow as pa
import pyarrow.ipc as ipc
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone
from pathlib import Path
from pyhlsmv.sst import SSTable

@dataclass
class TableSchema:
    name: str
    dtype: str
    nullable: bool = True

@dataclass
class SchemaMetadata:
    name: str
    created_at: str
    tables: List[str]
    integrity_hash: str

@dataclass
class TableMetadata:
    schema_name: str
    table_name: str
    timestamp: tuple
    version: int
    num_records: int
    file_size: int
    commit_message: str

class HStorage:
    """
    storage layer with
    - schema folder
    - table organization
    - metadata, index, data arrow ipc files
    - timeline tracking
    """
    def __init__(self, base_path:str = "./lhs"):
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        self.schemas: Dict[str, SchemaMetadata] = {}
        self.tables = {}
        self._load_metadata()

    def _load_metadata(self) -> None:
        if not self.base_path.exists():
            return
        
        for schema_dir in self.base_path.iterdir():
            if schema_dir.is_dir():
                schema_meta_file = schema_dir / "schema.arrow"
                if schema_meta_file.exists():
                    self.schemas[schema_dir.name] = SchemaMetadata(
                        name = schema_dir.name,
                        tables = [],
                        created_at = datetime.now(timezone.utc).isoformat(),
                        integrity_hash = ""
                    )

                    for table_dir in schema_dir.iterdir():
                        if table_dir.is_dir() and table_dir.name not in ["metadata", "index", "data"]:
                            self.schemas[schema_dir.name].tables.append(table_dir.name)

    def _save_schema_metadata(self, schema_name:str, metadata:SchemaMetadata) -> None:
        schema_path = self.base_path / schema_name
        metadata_file = schema_path / "schema.arrow"

        metadata_dict = asdict(metadata)
        table = pa.table({
            "key": pa.array(list(metadata_dict.keys())),
            "value": pa.array([str(v) for v in metadata_dict.values()])
        })
        self._save_arrow(metadata_file, table)

    def _save_arrow(self, path:Path, table: pa.Table) -> None:
        with open(path, "wb") as f:
            writer = ipc.new_stream(f, table.schema)
            writer.write_table(table)

    def _load_arrow(self, path:Path) -> Optional[pa.Table]:
        if path.exists():
            with open(path, "rb") as f:
                reader = ipc.open_stream(f)
                return reader.read_all()
        return None

    def create_schema(self, schema_name: str) -> None:
        schema_path = self.base_path / schema_name
        schema_path.mkdir(parents = True, exists_ok = True)

        metadata = SchemaMetadata(
            name = schema_name,
            tables = [],
            created_at = datetime.now(timezone.utc).isoformat(),
            integrity_hash = ""
        )
        self.schemas[schema_name] = metadata
        self._save_schema_metadata(schema_name, metadata)

    def create_table(self, schema_name:str, table_name:str, columns:List[TableSchema]) -> None:
        if schema_name not in self.schemas:
            self.create_schema(schema_name)

        table_path = self.base_path / schema_name / table_name
        table_path.mkdir(parents = True, exist_ok = True)

        (table_path / "metadata").mkdir(exist_ok = True)
        (table_path / "index").mkdir(exist_ok = True)
        (table_path / "data").mkdir(exist_ok = True)

        schema_data = {
            "columns": [asdict(col) for col in columns],
            "created_at": datetime.now(timezone.utc).isoformat()
        }

        self.tables[f"{schema_name}.{table_name}"] = {
            "schema": schema_data,
            "metadata": [],
            "sstables": []
        }

        schema_file = table_path / "schema.arrow"
        self._save_arrow(schema_file, pa.table({"schema": pa.array([json.dumps(schema_data)])}))

    def write_sstable(self, schema_name:str, table_name:str, sstable:SSTable, timestamp:tuple, version:int, commit_message:str = "str") -> None:
        table_key = f"{schema_name}.{table_name}"
        table_path = self.base_path / schema_name / table_name

        arrow_table = sstable.to_arrow_table()
        data_file = table_path / "data" / f"v{version:06d}.arrow"
        self._save_arrow(data_file, arrow_table)

        index_data = {
            "min_key": str(sstable.min_key),
            "max_key": str(sstable.max_key),
            "row_count": len(sstable)
        }

        index_file = table_path / "index" / f"v{version:06d}.arrow"
        self._save_arrow(index_file, pa.table({
            "key": pa.array([index_data["min_key"], index_data["max_key"]]),
            "value": pa.array([index_data["min_key"], index_data["max_key"]])
        }))

        metadata = TableMetadata(
            schema_name = schema_name,
            table_name = table_name,
            version = version,
            timestamp = timestamp,
            num_records = len(sstable),
            file_size = len(sstable.to_ipc_bytes()),
            commit_message = commit_message
        )

        metadata_dict = asdict(metadata)
        metadata_file = table_path / "metadata" / f"v{version:06d}.arrow"
        self._save_arrow(metadata_file, pa.table({
            "key": pa.array(list(metadata_dict.keys())),
            "value": pa.array([str(v) for v in metadata_dict.values()])
        }))

        if table_key not in self.tables:
            self.tables[table_key] = {"schema": {}, "metadata": [], "sstables": []}

        self.tables[table_key]["metadata"].append(asdict(metadata))
        self.tables[table_key]["sstables"].append({
            "version": version,
            "timestamp": timestamp,
            "data_file": str(data_file),
            "index_file": str(index_file),
            "metadata_file": str(metadata_file)
        })

    def read_table_data(self, schema_name:str, table_name:str, version:Optional[int] = None) -> List[Dict[str, Any]]:
        table_path = self.base_path / schema_name / table_name / "data"

        if not table_path.exists():
            return []
        
        arrow_files = sorted(table_path.glob("v*.arrow"))
        if not arrow_files:
            return []
        
        if version is None:
            target_file = arrow_files[-1]
        else:
            target_file = table_path / f"v{version:06d}.arrow"

        if not target_file.exists():
            return []
    
        table = self._load_arrow(target_file)
        if table is None:
            return []
        
        records = []
        for i in range(len(table)):
            record = {}
            for col in table.column_names:
                record[col] = table[col][i].as_py()
            records.append(record)

        return records
    
    def list_schemas(self) -> List[str]:
        return list(self.schemas.keys())
    
    def list_tables(self, schema_name:str) -> List[str]:
        if schema_name in self.schemas:
            return self.schemas[schema_name].tables
        return []
    
    def get_table_metadata(self, schema_name:str, table_name:str) -> Optional[Dict[str, Any]]:
        table_key = f"{schema_name}.{table_name}"
        return self.tables.get(table_key)
