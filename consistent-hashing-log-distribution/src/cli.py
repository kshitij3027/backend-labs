"""CLI interface for the consistent hashing log distribution system."""

import click


@click.group()
def cli():
    """Consistent Hashing Log Distribution System."""
    pass


@cli.command()
@click.option("--host", default="0.0.0.0", help="Dashboard host")
@click.option("--port", default=5000, type=int, help="Dashboard port")
@click.option("--config", "config_path", default=None, help="Path to cluster config YAML")
def serve(host, port, config_path):
    """Start the web dashboard and API server."""
    from src.config import load_config, load_config_from_env

    if config_path:
        config = load_config(config_path)
    else:
        try:
            config = load_config()
        except FileNotFoundError:
            config = load_config_from_env()

    config.dashboard_host = host
    config.dashboard_port = port

    # Import here to avoid circular imports
    from src.app import create_app

    app = create_app(config)
    app.run(host=host, port=port, debug=False)


@cli.command()
@click.option("--count", default=10000, type=int, help="Number of logs to generate")
@click.option("--nodes", default=3, type=int, help="Number of initial nodes")
def demo(count, nodes):
    """Run benchmark demo with success criteria validation."""
    import importlib.util
    import os

    # Load scripts/demo.py directly
    demo_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "scripts",
        "demo.py",
    )
    spec = importlib.util.spec_from_file_location("demo_script", demo_path)
    demo_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(demo_module)

    demo_module.run_demo(count=count, num_nodes=nodes)
