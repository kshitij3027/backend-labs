"""Web query entry point — starts the Flask app."""

from shared.config_loader import load_yaml
from query.src.config import QueryConfig
from query.src.web import create_app


def main() -> None:
    cfg = QueryConfig.from_dict(load_yaml()["query"])
    app = create_app(cfg.storage_dir)
    app.run(host="0.0.0.0", port=8000)


if __name__ == "__main__":
    main()
