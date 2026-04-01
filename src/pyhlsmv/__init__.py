# from .api import app
from daphne.server import Server
from .endpoints import app

def main() -> None:
    server = Server(app, endpoints=["tcp:8000:interface=0.0.0.0"])
    server.run()

if __name__ == "__main__":
    main()
