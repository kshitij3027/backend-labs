"""Entry point for running the package: python -m src"""

import sys


def main():
    from src.config import load_config
    from src.web.app import create_app

    config = load_config()
    app = create_app(config)

    host = config.host
    port = config.port

    if len(sys.argv) > 1 and sys.argv[1] == "serve":
        # Support: python -m src serve --host X --port Y
        for i, arg in enumerate(sys.argv):
            if arg == "--host" and i + 1 < len(sys.argv):
                host = sys.argv[i + 1]
            if arg == "--port" and i + 1 < len(sys.argv):
                port = int(sys.argv[i + 1])

    app.run(host=host, port=port, debug=config.debug)


if __name__ == "__main__":
    main()
