import os

from waitress import serve

from app import app


def main() -> None:
    port = int(
        os.environ.get("ASPNETCORE_PORT")
        or os.environ.get("HTTP_PLATFORM_PORT")
        or "5000"
    )
    print(f"Starting server on port {port}")
    serve(app, host="0.0.0.0", port=port)


if __name__ == "__main__":
    main()
