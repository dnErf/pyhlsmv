from typing import Any, Dict, Optional
from litestar import Litestar, get, post
from litestar.config.cors import CORSConfig
from ..lh import LH
from pyhlsmv.storage import TableSchema

_lh:Optional[LH] = None

def get_lh(bp=None) -> LH:
    global _lh
    if _lh is None:
        _lh = LH(bp)
    return _lh


@get("/{schema_name:str}")
async def get_schema_info(schema_name:str) -> Dict[str, Any]:
    lh = get_lh()
    tables = lh.storage.list_tables(schema_name)
    return {
        "schema_name": schema_name,
        "tables": tables,
        "table_count": len(tables)
    }


@post("/{schema_name:str}")
async def create_schema(schema_name:str) -> Dict[str, str]:
    lh = get_lh()
    try:
        lh.create_schema(schema_name)
        return {
            "message": f"schema {schema_name} created successfully",
            "schema_name": schema_name
        }
    except Exception as exc:
        return { "error": str(exc) }
    

@get("/{schema_name:str}/tables")
async def list_tables(schema_name:str) -> Dict[str, Any]:
    lh = get_lh()
    tables = lh.storage.list_tables(schema_name)
    return {
        "schema_name": schema_name,
        "tables": tables,
        "count": len(tables)
    }


@post("/{schema_name:str}/tables/{table_name:str}")
async def create_table(schema_name:str, table_name:str, data:Dict[str, Any]) -> Dict[str, Any]:
    lh = get_lh()
    try:
        columns_data = data.get("columns", [])
        columns = [
            TableSchema(name = col["name"], dtype = col["dtype"], nullable = col.get("nullable", True)) for col in columns_data
        ]
        lh.create_table(schema_name, table_name, columns)
        return {
            "message": "table created",
            "schema_name": schema_name,
            "table_name": table_name,
            "columns": len(columns)
        }
    except Exception as exc:
        return { "error": str(exc) }


@get("/{schema_name:str}/{table_name:str}")
async def list_records(schema_name:str, table_name:str, min_key:Optional[Any] = None, max_key:Optional[Any] = None) -> Dict[str, Any]:
    lh = get_lh()
    try:
        records = []
        for key, value in lh.scan(schema_name, table_name, min_key, max_key):
            records.append({ "key": key, "value": value })

        return {
            "schema_name": schema_name,
            "table_name": table_name,
            "records": records,
            "count": len(records)
        }
    except Exception as exc:
        return { "error": str(exc) }
    

@get("/{schema_name:str}/{table_name:str}/{key:str}")
async def read_record(schema_name:str, table_name:str, key:str) -> Dict[str, Any]:
    lh = get_lh()
    try:
        value = lh.read(schema_name, table_name, key)
        if value is None:
            return { "error": "record not found", "key": key }
        return {
            "schema_name": schema_name,
            "table_name": table_name,
            "key": key,
            "value": value
        }
    except Exception as exc:
        return { "error": str(exc) }


@post("/{schema_name:str}/{table_name:str}")
async def write_record(schema_name:str, table_name:str, data:Dict[str, Any]) -> Dict[str, Any]:
    lh = get_lh()
    try:
        result = lh.write(schema_name, table_name, data.get("key"), data.get("value"))
        return {
            "success": result.success,
            "timestamp": result.timestamp,
            "version": result.version,
            "message": result.message
        }
    except Exception as exc:
        return { "success": False, "message": str(exc) }



@get("/health")
async def health_check() -> dict:
    return { "status": "ok" }


app = Litestar(debug = True, cors_config = CORSConfig(allow_origins = ["*"]), route_handlers = [
    health_check,
    create_schema,
    get_schema_info,
    create_table,
    list_tables,
    list_records,
    read_record,
    write_record,
])
