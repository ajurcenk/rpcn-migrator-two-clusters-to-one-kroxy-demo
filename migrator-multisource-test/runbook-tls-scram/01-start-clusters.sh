#!/usr/bin/env bash
# Step 1: start the source clusters (redpanda-a, redpanda-b) and the destination (redpanda-dest)
# with TLS + SASL/SCRAM on every Kafka listener and HTTPS + basic auth on Schema Registry. Then
# create the SCRAM users (all superusers, see redpanda/tls-scram/bootstrap-*.yaml) and prove each
# cluster refuses plaintext, missing SASL, wrong passwords and clients that don't trust the CA.
source "$(dirname "$0")/lib.sh"
step 1 "start clusters (TLS + SASL/SCRAM)"

ensure_certs
ensure_secrets
ensure_rbtool
"${COMPOSE[@]}" up -d --wait redpanda-a redpanda-b redpanda-dest

info "creating SCRAM-SHA-256 users (via the Admin API) ..."
for svc in redpanda-a redpanda-b; do
  create_user "$svc" admin "$ADMIN_PASSWORD"
  create_user "$svc" app "$APP_PASSWORD"
  create_user "$svc" migrator "$MIGRATOR_PASSWORD"
done
create_user redpanda-dest admin "$ADMIN_PASSWORD"
create_user redpanda-dest app "$APP_PASSWORD"
create_user redpanda-dest migrator-a "$MIGRATOR_A_PASSWORD"
create_user redpanda-dest migrator-b "$MIGRATOR_B_PASSWORD"

info ""
info "checking every cluster enforces TLS + SCRAM ..."
retry 30 rbtool check-auth -endpoints "source-A=$SRC_A_ADDR,source-B=$SRC_B_ADDR,destination=$DEST_ADDR" | sed 's/^/  /' \
  || fail "a cluster does not enforce TLS + SCRAM as expected"

info ""
info "checking Schema Registry requires HTTPS + basic auth ..."
for sr in "source-A localhost:18082" "source-B localhost:28082" "destination localhost:38082"; do
  read -r name addr <<<"$sr"
  ok_code="$(curl -s -o /dev/null -w '%{http_code}' --cacert "$TLS_DIR/ca.crt" -u "admin:$ADMIN_PASSWORD" "https://$addr/subjects")"
  noauth_code="$(curl -s -o /dev/null -w '%{http_code}' --cacert "$TLS_DIR/ca.crt" "https://$addr/subjects")"
  plain_code="$(curl -s -o /dev/null -w '%{http_code}' "http://$addr/subjects" || true)"
  [[ "$ok_code" == 200 && "$noauth_code" == 401 && "$plain_code" != 200 ]] \
    || fail "$name schema registry: with credentials $ok_code (want 200), without $noauth_code (want 401), plain HTTP $plain_code (want no 200)"
  info "  $name https://$addr: admin 200, no credentials 401, plain HTTP refused"
done

info ""
info "source A:    $SRC_A_ADDR   (schema registry https://localhost:18082)"
info "source B:    $SRC_B_ADDR   (schema registry https://localhost:28082)"
info "destination: $DEST_ADDR   (schema registry https://localhost:38082)"
info "CA certificate: .state/tls/ca.crt, passwords: .state/secrets.env"
next "./02-start-producers.sh"
