FROM rust:1.98.1-slim-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends build-essential cmake pkg-config && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN cargo build --release -p verdandi

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl && rm -rf /var/lib/apt/lists/* \
    && useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin verdandi \
    && mkdir /data && chown 10001:10001 /data
COPY --from=build /src/target/release/verdandi /usr/local/bin/verdandi
WORKDIR /
USER 10001
VOLUME /data
EXPOSE 8080 7946
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/verdandi"]
