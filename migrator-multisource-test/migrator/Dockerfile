# UNTESTED: kept for a possible patched migrator build. Step 3 uses the published image
# docker.redpanda.com/redpandadata/connect:4.100.0 instead (see docs/findings.md).
# Redpanda Connect (with redpanda_migrator) built from source. The Connect module is passed in as
# the "connect" build context, e.g.:
#
#   docker build -t rpcn-migrator:v4.100.0 \
#     --build-context connect=$MIGRATOR_SRC/connect \
#     migrator
#
# The repo's own resources/docker/Dockerfile only packages a goreleaser-built binary, so it can't be
# used to build from source directly.
FROM golang:1.26.5 AS build
WORKDIR /src
COPY --from=connect go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY --from=connect . .
ARG VERSION=v4.100.0
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath \
      -ldflags "-s -w -X main.Version=${VERSION} -X main.DateBuilt=source" \
      -o /out/redpanda-connect ./cmd/redpanda-connect

FROM debian:12-slim AS certs
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates

FROM busybox
COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /out/redpanda-connect /redpanda-connect
ENTRYPOINT ["/redpanda-connect"]
