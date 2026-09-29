# TLS + SASL/SCRAM runbook: findings

Results of running `runbook-tls-scram/` end to end, and the decisions and deviations behind it. The plaintext project's findings are in `../docs/findings.md`. Its "Known issues" (migrator offset translation, Redpanda group quirks) apply here unchanged, because TLS and SCRAM don't affect them.

## Result

The full runbook (steps 00–11) passes with TLS + SCRAM-SHA-256 on every Kafka connection and HTTPS + basic auth on every Schema Registry. The results match the plaintext runbook:
- **Replication:** every `a_*` / `b_*` topic caught up. `a_orders-value` / `b_orders-value` were copied to the destination Schema Registry with translated IDs.
- **Offsets:** translated positions converged to `exact` in step 10.
- **Cutover:** in step 11 both consumers resumed exactly at the source group's final offset on every partition, and every source record was consumed **exactly once** across source and destination.
- **Auth checks:** every check passed, on each cluster (step 1) and through each proxy (step 4).
- **Existing log checks:** `../scripts/step3-check-logs.sh` (run against `compose/docker-compose.tls-scram.yaml`) finds 26 request rewrites per proxy, and none cross over (`a_` on `kroxylicious-b` or the reverse).
  - Its migrator check reports one WARN per migrator: `Kafka broker read failed … err="context canceled" request=Fetch` on the input.
  - That line is logged when step 10 stops the migrators. It isn't a TLS or auth error; `make step3` never sees it because it checks before stopping anything.

## Security findings

1. **SASL passthrough through Kroxylicious 0.24.0 works for SCRAM with the ConsumerGroupPrefix filter in the chain.**
   - **Setup:** no SASL filter is configured, so the proxy forwards `SaslHandshake` / `SaslAuthenticate` unchanged (Kroxylicious docs at `v0.24.0`, "SASL Passthrough", the default mode).
   - **Evidence:** `migrator-a` / `migrator-b` authenticate as themselves on `redpanda-dest`, and a wrong password through the proxy fails with the broker's own `SASL_AUTHENTICATION_FAILED`.
   - **No filter change:** the filter only overrides group APIs, so the SASL frames never reach its code.

2. **TLS is ended and started again at the proxy.**
   - **Setup:** the gateway presents `kroxylicious-{a,b}.crt` (PEM `privateKeyFile` / `certificateFile`), and `targetCluster.tls.trust` trusts the demo CA (`storeType: PEM`).
   - **Why SCRAM still works:** SCRAM isn't channel-bound (Kafka has no `-PLUS` variants), so it works across the two TLS connections.
   - **What it means:** the proxy sees plaintext Kafka frames, which it must in order to rewrite group IDs. So the proxy host is inside the trust boundary.
   - **Evidence:**
     - Plaintext clients get `NotSslRecordException` in the proxy log.
     - Clients that don't trust the demo CA fail with `x509: certificate signed by unknown authority`.
   - **Misleading startup log:** the proxy logs `Gateway configuration {downstream=… (TLS: -) …, upstream=… (TLS: -)}` even with TLS enabled on both sides, so that line doesn't tell you whether TLS is on.

3. **"TLS, no SASL" through a proxy fails with a client timeout, not a connection close.**
   - **Directly against a broker:** Redpanda closes the connection at once (`broker closed the connection immediately after a request was issued … is SASL missing?`).
   - **Through a proxy:** `redpanda-dest` rejects the request just the same (`Unexpected auth request 3 expected handshake`, 3 = Metadata) and closes its connection to the proxy. But the client only sees `i/o timeout` / `context deadline exceeded`: rpk after about 15 s, rbtool after its 10 s limit.
   - **Access is still denied**, but a misconfigured client behind the proxy gets a less useful error, later. `check-auth` accepts any error for this case.
   - **Not investigated further:** whether the delay comes from Kroxylicious's handling of an upstream close before authentication.

4. **The Schema Registry path is covered.**
   - **Setup:** the migrator's `schema_registry.tls` + `basic_auth` work on the input (source) and the output (destination).
   - **Schemas:** step 2 registers `orders-value` on each source, since the plaintext runbook registers none. Step 6 checks that both prefixed subjects arrive.
   - **Basic auth uses the SCRAM users:** Redpanda's `authentication_method: http_basic` on the Schema Registry listener checks the same SCRAM credentials as Kafka.

## Decisions and deviations

- **Superusers only, no ACLs** (the user's choice).
  - **Users:**

    | Clusters | Users |
    |---|---|
    | Sources | `admin`, `app`, `migrator` |
    | Destination | `admin`, `app`, `migrator-a`, `migrator-b` |

  - **Not demonstrated:** with every user a superuser, the runbook doesn't show that the broker authorizes the *rewritten* group ID. For example, a prefixed ACL `a_*` for `migrator-a` would isolate the sources at the broker. That's a natural next step.
- **Admin API stays plaintext and unauthenticated** (`:9644`). It keeps the Compose healthcheck and `rpk security user create` in step 1 simple, and it's the default in dev-container mode. Its host ports (19645/29645/39645) are published on all host interfaces, like every port in the plaintext stack. Anyone who can reach them can create users. Protecting it (`admin_api_require_auth`, TLS on admin) would need the healthcheck and user creation to authenticate too.
- **Broker listeners come from a template, not `--kafka-addr` flags.**
  - `rpk redpanda start --kafka-addr` replaces the whole `kafka_api` list, which drops the per-listener `authentication_method`. So the compose entrypoint renders `redpanda/tls-scram/redpanda.yaml.tmpl` into `/etc/redpanda/redpanda.yaml` and starts with only `--mode=dev-container --smp=1`.
  - Dev-container mode then adds its usual settings. Its "Unknown property … for node config store" warnings are normal and appear in the plaintext stack too.
- **Cluster properties go in `/etc/redpanda/.bootstrap.yaml`:** `enable_sasl: true`, `sasl_mechanisms: [SCRAM]` and `superusers` are applied on first start. Users are created afterwards through the Admin API. On a re-run of step 1 their passwords are reset from `.state/secrets.env`.
- **Certificates:**
  - `certs/gen-certs.sh` runs inside the Redpanda image, which ships openssl, so the host needs no openssl.
  - One RSA-2048 server certificate per broker and per proxy, each with SANs for its service name, `localhost` and `127.0.0.1`. No client certificates (no mTLS).
  - **Demo shortcut:** keys are unencrypted and world-readable (0644), so the container users (redpanda uid 101, kroxylicious uid 185) can read the bind-mounted files.
- **Passwords are random per setup:** `.state/secrets.env`, mode 0600. The compose file reads them with `${VAR:-}`, so compose commands like `logs` / `down` work without them, but migrators started without them fail to authenticate.
- **Migrator configs** read `${SRC_SASL_PASSWORD}` / `${DEST_SASL_PASSWORD}`. `connect lint` fails when these aren't set (`required environment variables were not set`), so `00-setup.sh` lints with them set.
- **`rbtool check-auth` sends a Metadata request, not a ping.** Redpanda answers `ApiVersions` before SASL, so a ping (or any check that stops at `ApiVersions`) passes without credentials.
- **The proxies' bootstrap ports are published** (19192 / 29192) so the host can run the step 4 checks through them. Only the bootstrap connection is used. The addresses the proxies advertise (`kroxylicious-a:9193`) resolve only inside the compose network, which is enough for a single Metadata request.
- **Separate stack:** compose project `cgprefix-tls-scram`, its own host ports and `.state/`. It ran at the same time as the plaintext runbook's clusters without conflicts (see "Verification").

## Verification

| Run | Result |
|---|---|
| Fresh `00-setup.sh` → `11-cutover-consumers.sh` → `99-teardown.sh` | all steps `OK` (numbers above) |
| Second full pass: `00` → `01` → `01` again → … → `11`, while the plaintext stack's `redpanda-b` and `redpanda-dest` were running | all steps `OK`; resumed exactly and every record exactly once on all 8 partitions |
| `create_user` with a changed password (throwaway user `probe`) | old password refused, new one accepted |

- **Coexistence:** in the second pass the plaintext `redpanda-a` couldn't start. An unrelated container on the test machine held ports 19092/19644. So running both stacks side by side was tested with two of the three plaintext clusters, and there were no conflicts.
- **Step 1 twice:** running step 1 again recreates each user with the same password, and Redpanda accepts that. With a different password Redpanda answers `User already exists`, and `create_user` resets the password instead.
