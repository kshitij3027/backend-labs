"""Plain HTTP routes that sit beside the GraphQL surface.

Everything here is deliberately *not* GraphQL: liveness (``/health``) and, from C9, Prometheus
exposition (``/metrics``). Both are consumed by infrastructure — a container HEALTHCHECK, a
compose dependency gate, a Prometheus scraper — none of which speaks GraphQL, and none of which
should have to POST a query document to find out whether a process is up.
"""
