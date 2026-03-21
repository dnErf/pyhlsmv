from litestar import Litestar, get

@get("/health")
async def health_check() -> dict:
    return { "status": "ok" }

app = Litestar(route_handlers = [health_check])
