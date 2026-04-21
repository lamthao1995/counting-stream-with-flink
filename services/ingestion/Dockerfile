# syntax=docker/dockerfile:1.6
# Build from the repo root so the build context includes pkg/ and services/.
FROM golang:1.22-alpine AS build
WORKDIR /src

# Copy the workspace + relevant modules.
COPY go.work go.work.sum* ./
COPY pkg ./pkg
COPY services/ingestion ./services/ingestion
COPY services/query ./services/query
COPY tests/stress ./tests/stress

RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    cd services/ingestion && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ingestion ./cmd/ingestion

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ingestion /ingestion
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/ingestion"]
