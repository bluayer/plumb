# syntax=docker/dockerfile:1
FROM --platform=$BUILDPLATFORM golang:1.25 AS build
ARG TARGETOS TARGETARCH VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY api/ api/
COPY cmd/ cmd/
COPY internal/ internal/
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" -o /out/plumb ./cmd/plumb

FROM gcr.io/distroless/static:nonroot
LABEL org.opencontainers.image.source=https://github.com/bluayer/plumb \
      org.opencontainers.image.licenses=Apache-2.0 \
      org.opencontainers.image.description="Multi-cluster adaptive capacity for Kubernetes inference workloads"
COPY --from=build /out/plumb /plumb
USER 65532:65532
ENTRYPOINT ["/plumb"]
