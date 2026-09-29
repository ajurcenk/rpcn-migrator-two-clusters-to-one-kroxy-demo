#!/usr/bin/env bash
# Generates the demo PKI into OUT_DIR (default /out): one private CA, a server certificate per
# Redpanda cluster and one per Kroxylicious proxy. Runs inside the Redpanda image (it ships
# openssl), so the host needs nothing extra; lib.sh's ensure_certs calls it.
#
# SANs: the compose service name (what the migrators and proxies dial), plus localhost and
# 127.0.0.1 (the host-side listeners and rpk inside the container).
#
# Demo only: keys are unencrypted and world-readable so the redpanda (uid 101) and kroxylicious
# (uid 185) container users can read the bind-mounted files.
set -euo pipefail

OUT_DIR="${OUT_DIR:-/out}"
DAYS=825
cd "$OUT_DIR"

openssl req -x509 -newkey rsa:2048 -nodes -days "$DAYS" -sha256 \
  -subj "/CN=cgprefix-tls-scram demo CA" \
  -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" \
  -keyout ca.key -out ca.crt 2>/dev/null

for name in redpanda-a redpanda-b redpanda-dest kroxylicious-a kroxylicious-b; do
  openssl req -newkey rsa:2048 -nodes -subj "/CN=$name" -keyout "$name.key" -out "$name.csr" 2>/dev/null
  openssl x509 -req -in "$name.csr" -CA ca.crt -CAkey ca.key -CAcreateserial -days "$DAYS" -sha256 \
    -extfile <(printf 'subjectAltName=DNS:%s,DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\nkeyUsage=critical,digitalSignature,keyEncipherment\n' "$name") \
    -out "$name.crt" 2>/dev/null
  rm -f "$name.csr"
done
rm -f ca.srl
chmod 0644 ./*.crt ./*.key
echo "certificates written: $(ls | tr '\n' ' ')"
