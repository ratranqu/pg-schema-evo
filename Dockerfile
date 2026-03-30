FROM swift:6.2 AS builder

WORKDIR /build
COPY . .

RUN swift build -c release --static-swift-stdlib

# Runtime image with PostgreSQL client tools
FROM ubuntu:24.04

RUN apt-get update && \
    apt-get install -y --no-install-recommends postgresql-client ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/.build/release/pg-schema-evo /usr/local/bin/pg-schema-evo

ENTRYPOINT ["pg-schema-evo"]
