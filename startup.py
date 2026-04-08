import os

from waitress import serve

from app import app


def main() -> None:
    port = int(os.environ.get("HTTP_PLATFORM_PORT", "5000"))
    serve(app, host="127.0.0.1", port=port)


if __name__ == "__main__":
    main()
