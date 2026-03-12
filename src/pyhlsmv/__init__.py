from .api import app
from daphne.server import Server

def main() -> None:
    print("Hello from pyhlsmv!")
    server = Server(app, endpoints=["tcp:8000:interface=0.0.0.0"])
    server.run()

if __name__ == "__main__":
    main()
