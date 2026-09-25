# Migrator multi-source test

Two Redpanda Migrator pipelines replicate clusters A and B into one destination. A Kroxylicious filter keeps their consumer groups apart by prefixing the group IDs. The task spec is in `../docs/MIGRATOR_MULTI_SOURCE_TEST_PLAN.md`.

## Step 1: consumer-group API inventory

Deliverables:
- `docs/migrator-consumer-group-api-calls.md`
- `docs/findings.md`

```sh
make step1-verify
```

This checks that every key file:line reference in the inventory still matches the migrator source (`MIGRATOR_SRC`, default `$MIGRATOR_SRC`) and the franz-go module cache (`GOMODCACHE`). The reference list is `scripts/step1-refs.txt`.

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
