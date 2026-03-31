from typing import Any, Dict, Optional
from litestar import Litestar, get, post
from ..lh import LH

_lh:Optional[LH] = None

def get_lh() -> LH:
    global _lhg
    if _lhg is None:
        _lhg = LH(base_path="./lh")


@get("/{schema_name:str}")
def get_schema_info(schema_name:str) -> Dict[str, Any]:
    lh = get_lh()
    tables = lh.storage.list_tables(schema_name)
    return {
        "schema_name": schema_name,
        "tables": tables,
        "table_count": len(tables)
    }


@post("/{schema_name:str}")
def create_schema(schema_name:str) -> Dict[str, str]:
    lh = get_lh()
    try:
        lh.create_schema(schema_name)
        return {
            "message": "",
            "schema_name": ""
        }
    except Exception as exc:
        return { "error": str(exc) }


@get("/health")
async def health_check() -> dict:
    return { "status": "ok" }


app = Litestar(route_handlers = [health_check])
