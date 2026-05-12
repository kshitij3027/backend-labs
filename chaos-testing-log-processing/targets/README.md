# Target Stack

The "distributed log processing system" the chaos framework attacks.

```
log-producer  ──LPUSH──▶  redis:7-alpine  ──BLPOP──▶  log-consumer
   :9001 host                 :6379                       :9002 host
```

Both services are small FastAPI apps using the async `redis-py` client.

## Why they run as root

The producer and consumer containers intentionally **run as root** (no `USER`
directive in their Dockerfiles). Later commits (C7-C9) `docker exec` into
these containers with the `NET_ADMIN` cap to manipulate qdiscs via `tc netem`
and apply CPU/memory pressure via `stress-ng`. Keeping them non-root would
require extra `setcap` setup on every binary in the toolchain.

The fault-injection toolchain (`iproute2`, `stress-ng`, `iptables`, plus
`ca-certificates` and `curl` for the HEALTHCHECK) is pre-installed in each
image so that `docker exec` calls from C7+ work out of the box.

## Bring up just the target stack

```bash
docker compose up -d redis log-producer log-consumer
```

## Smoke test (after ~8 s)

```bash
curl localhost:9001/sent_count   # producer: monotonically increasing
curl localhost:9002/counter      # consumer: should track producer
```

The `chaos.target=true` label on each service is how the framework's
`DockerClient.list_chaos_targets()` discovers them (C5+).
