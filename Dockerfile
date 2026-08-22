# syntax=docker/dockerfile:1.7

FROM ubuntu:24.04 AS release

ARG TARGETARCH
ARG SAKUIN_VERSION=latest
ARG SAKUIN_RELEASE_BASE_URL=https://github.com/BrandenXia/sakuin/releases
ARG SAKUIN_BINARY_URL=""

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /tmp/sakuin-release
RUN set -eux; \
    case "${TARGETARCH}" in amd64|arm64) ;; *) echo "Unsupported architecture: ${TARGETARCH}" >&2; exit 1 ;; esac; \
    asset="sakuin-linux-${TARGETARCH}.tar.gz"; \
    if [ -n "${SAKUIN_BINARY_URL}" ]; then \
      url="${SAKUIN_BINARY_URL}"; \
    elif [ "${SAKUIN_VERSION}" = "latest" ]; then \
      url="${SAKUIN_RELEASE_BASE_URL}/latest/download/${asset}"; \
    else \
      url="${SAKUIN_RELEASE_BASE_URL}/download/${SAKUIN_VERSION}/${asset}"; \
    fi; \
    curl --fail --location --retry 3 --output "${asset}" "${url}"; \
    curl --fail --location --retry 3 --output "${asset}.sha256" "${url}.sha256"; \
    sha256sum --check "${asset}.sha256"; \
    mkdir -p /opt/sakuin; \
    tar --extract --gzip --file "${asset}" --directory /opt/sakuin; \
    test -x /opt/sakuin/bin/sakuin; \
    test -x /opt/sakuin/bin/sakuin-api-key

FROM ubuntu:24.04

LABEL org.opencontainers.image.source="https://github.com/BrandenXia/sakuin" \
      org.opencontainers.image.description="Self-hosted BitTorrent DHT indexer"

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 sakuin \
    && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin sakuin \
    && install -d -o sakuin -g sakuin -m 0750 /var/lib/sakuin

COPY --from=release --chown=root:root /opt/sakuin /opt/sakuin

ENV LD_LIBRARY_PATH=/opt/sakuin/lib
ENV PATH=/opt/sakuin/bin:$PATH

USER 10001:10001
WORKDIR /var/lib/sakuin

VOLUME ["/var/lib/sakuin"]
EXPOSE 8080/tcp 6881/udp
STOPSIGNAL SIGTERM

ENTRYPOINT ["sakuin"]
CMD ["--config=/etc/sakuin/sakuin.toml"]
