# syntax=docker/dockerfile:1.6
FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.work go.work.sum* ./
COPY pkg ./pkg
COPY services/ingestion ./services/ingestion
COPY services/query ./services/query
COPY tests/stress ./tests/stress

RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    cd services/query && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/query ./cmd/query

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/query /query
EXPOSE 8081
USER nonroot:nonroot
ENTRYPOINT ["/query"]
