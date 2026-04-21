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
    cd tests/stress && \
    CGO_ENABLED=0 go build -trimpath -o /out/stress ./cmd/stress

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/stress /stress
USER nonroot:nonroot
ENTRYPOINT ["/stress"]
