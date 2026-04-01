from typing import Any, Dict, Optional
from litestar import Litestar, get, post
from litestar.config.cors import CORSConfig
from ..lh import LH

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


@get("/health")
async def health_check() -> dict:
    return { "status": "ok" }


app = Litestar(debug = True, cors_config = CORSConfig(allow_origins = ["*"]), route_handlers = [
    health_check,
    create_schema,
    get_schema_info
])
