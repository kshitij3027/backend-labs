"""External-client adapters (Redis, …) for the Log Recommendation Engine.

Each module here wraps one piece of infrastructure the app talks to. Clients are
built as lazy per-process singletons from :func:`src.config.get_settings` and are
written to degrade gracefully (never crash the request flow when the backing
service is unavailable).
"""
