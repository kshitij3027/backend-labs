"""Main entry point for a Raft node."""

import asyncio
import signal
import structlog
from src.config import load_config
from src.node import RaftNode
from src.election import ElectionManager
from src.heartbeat import HeartbeatManager
from src.rpc_server import RaftRpcServer
from src.rpc_client import RpcClient

logger = structlog.get_logger()


async def run_node():
    """Start and run a Raft node."""
    config = load_config()

    logger.info(
        "node_starting",
        node_id=config.node_id,
        port=config.port,
        peers=config.peers,
        cluster_size=config.cluster_size,
        majority=config.majority,
    )

    # Create components
    node = RaftNode(config)
    rpc_client = RpcClient(rpc_timeout=0.1)
    election_manager = ElectionManager(node, config, rpc_client)
    heartbeat_manager = HeartbeatManager(node, config, rpc_client)

    # Wire up the heartbeat callback to reset election timer
    rpc_server = RaftRpcServer(
        node=node,
        host=config.host,
        port=config.port,
        on_heartbeat_received=election_manager.reset_election_timer,
        rpc_client=rpc_client,
    )

    # Start the gRPC server
    await rpc_server.start()
    logger.info("grpc_server_started", node_id=config.node_id, port=config.port)

    # Handle shutdown signals
    shutdown_event = asyncio.Event()

    def handle_signal():
        logger.info("shutdown_signal_received", node_id=config.node_id)
        shutdown_event.set()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, handle_signal)

    try:
        # Run all coroutines concurrently
        await asyncio.gather(
            election_manager.run_election_timer(),
            heartbeat_manager.run_heartbeat_loop(),
            shutdown_event.wait(),
        )
    except asyncio.CancelledError:
        pass
    finally:
        logger.info("node_shutting_down", node_id=config.node_id)
        await election_manager.stop()
        await heartbeat_manager.stop()
        await rpc_server.stop()
        await rpc_client.close()
        logger.info("node_stopped", node_id=config.node_id)


def main():
    asyncio.run(run_node())


if __name__ == "__main__":
    main()
