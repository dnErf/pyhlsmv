from litestar import Litestar, get

@get("/")
async def get_hello_world() -> dict:
    return { "message": "hello world" }

app = Litestar(
    route_handlers=[get_hello_world]
)
