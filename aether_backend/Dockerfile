# Aether server image.
#
#   docker build -t aether ./aether_backend
#   docker run -p 9000:9000 -v aether-data:/data \
#     -e AETHER_S3__ACCESS_KEY=admin -e AETHER_S3__SECRET_KEY=change-me-please aether
#
# Configuration comes from AETHER_* environment variables, or mount a config
# file and pass `--config /etc/aether/aether.toml` as the command.

FROM rust:1.98-slim-bookworm AS build
WORKDIR /src
COPY . .
# Cache mounts keep the crate registry and compiled dependencies between builds.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked --bin aether \
 && cp target/release/aether /usr/local/bin/aether \
 && mkdir /data

# glibc, CA certificates and nothing else; runs as uid 65532.
FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build /usr/local/bin/aether /usr/local/bin/aether
COPY --from=build --chown=65532:65532 /data /data

# Inside a container both APIs must listen on all interfaces to be published;
# publish the admin port (9001) only where it should be reachable.
ENV AETHER_STORAGE__DATA_DIR=/data \
    AETHER_S3__LISTEN=0.0.0.0:9000 \
    AETHER_ADMIN__LISTEN=0.0.0.0:9001
VOLUME ["/data"]
EXPOSE 9000 9001
USER 65532:65532

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD ["/usr/local/bin/aether", "health"]
ENTRYPOINT ["/usr/local/bin/aether"]
CMD ["serve"]
