# Migrator multi-source test

Two Redpanda Migrator pipelines replicate clusters A and B into one destination. A Kroxylicious filter keeps their consumer groups apart by prefixing the group IDs. The task spec is in `../docs/MIGRATOR_MULTI_SOURCE_TEST_PLAN.md`.

## Step 1: consumer-group API inventory

Deliverables:
- `docs/migrator-consumer-group-api-calls.md`
- `docs/findings.md`

```sh
MIGRATOR_SRC=/path/to/connect-v4.100.0 make step1-verify
```

This checks that every key file:line reference in the inventory still matches the migrator source (`MIGRATOR_SRC`, a checkout of github.com/redpanda-data/connect at tag `v4.100.0`; required) and the franz-go module cache (`GOMODCACHE`). The reference list is `scripts/step1-refs.txt`.

## Step 2: ConsumerGroupPrefix filter and proxy test

The Kroxylicious filter is in `kroxylicious-filter/`, built on Kroxylicious 0.24.0 (the version `kroxy-linking-demo` uses). It adds `prefix` to every consumer group ID on requests and strips it from responses; topics are untouched. Config:

```yaml
filterDefinitions:
  - name: cg-prefix-a
    type: demo.cgprefix.ConsumerGroupPrefix
    config:
      prefix: "a_"            # required
      stripOnResponse: true   # default true (false breaks franz-go; debugging only)
      filterListGroups: true  # default true: ListGroups only shows prefixed groups, stripped
```

```sh
make filter-test   # unit tests only (Maven in Docker; no local Maven/JDK needed)
make step2         # unit tests + proxy integration test on a fresh stack, then tear down
```

`make step2` builds the proxy image (`kroxylicious-filter/Dockerfile`: the filter JAR in `quay.io/kroxylicious/proxy:0.24.0`'s `classpath-plugins`) and starts `compose/docker-compose.step2.yaml`: Redpanda `v26.2.2` (`redpanda-dest`, direct on `localhost:19092`) and the proxy (virtual cluster `dest-a`, `localhost:9192`, Prometheus on `localhost:9190/metrics`). It then runs `tests/step2_proxy_test.go` from the host and `scripts/step2-check-proxy-logs.sh`, which checks the proxy's DEBUG log for a rewrite of every destination-side API from Step 1, in both legacy and batched wire shapes.

To iterate on a running stack:

```sh
make step2-up
make step2-test    # or: cd tests && go test -tags step2 -run TestStep2 -v ./...
make step2-down
```

Rewrite logging is controlled with `CG_PREFIX_LOG_LEVEL` on the proxy container (default `DEBUG`). Log lines look like `api=OFFSET_FETCH version=8 request group 'app-group' -> 'a_app-group'`. The counter `kroxylicious_consumer_group_prefix_rewrites_total{api,direction,prefix}` is on the Prometheus endpoint.

## Step 3: two sources into one destination

```sh
make step3            # brokers + proxies -> seed -> migrators -> assertions + log checks -> tear down
make step3-negative   # same, but both migrators write straight to redpanda-dest (no proxies)
make step3-known-issues  # requirements the migrator doesn't meet yet; EXPECTED TO FAIL (see docs/findings.md)
```

`compose/docker-compose.step3.yaml` runs:
- `redpanda-a`, `redpanda-b`, `redpanda-dest` (Redpanda `v26.2.2`; host ports: Kafka 19092 / 29092 / 39092, Schema Registry 18081 / 28081 / 38081)
- one proxy per source: `kroxylicious-a` (prefix `a_`, `proxy/config-step3-a.yaml`, metrics on 19190) and `kroxylicious-b` (prefix `b_`, 29190)
- `migrator-a` / `migrator-b` (profile `migrators`)

The migrators use the published image `docker.redpanda.com/redpandadata/connect:4.100.0` with `migrator/migrator-{a,b}.yaml`. Each output's `seed_brokers` defaults to its own proxy; `DEST_BROKERS` overrides it for the negative control. Validate the configs with:

```sh
docker run --rm -v "$PWD/migrator:/cfg:ro,z" docker.redpanda.com/redpandadata/connect:4.100.0 lint /cfg/migrator-a.yaml
```

`tests/step3_e2e_test.go` (build tag `step3`):
- `TestStep3Seed` seeds both sources before the migrators start.
- `TestStep3Replicated`, `TestStep3LiveSync` and `TestStep3ActiveConsumer` (groups with live members on the source) assert against the brokers directly, bypassing the proxies.
- `TestStep3NegativeControl` records the collision.
- `TestStep3KnownIssueCorrectionAfterStop` states a requirement Connect 4.100.0 doesn't meet. It is **expected to fail** and runs only via `make step3-known-issues`.

`scripts/step3-check-logs.sh` checks the migrator and proxy logs. Individual stages: `make step3-up`, `step3-seed`, `step3-migrators`, `step3-test`, `step3-down`.

`migrator/Dockerfile` builds Connect from the v4.100.0 source instead. It is untested and not used; see `docs/findings.md`.

## Runbook: manual migration with live traffic

`runbook/` walks through a full migration as eleven independent scripts, run by hand: clusters, then producers and consumers on the sources, then proxies and migrators, checks on data and offset translation, stopping source traffic, and finally moving the consumers to the destination and checking where they resume. See [`runbook/README.md`](runbook/README.md).
