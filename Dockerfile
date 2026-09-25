FROM golang:1.25 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY api/ api/
COPY cmd/ cmd/
COPY internal/ internal/
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/plumb ./cmd/plumb

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/plumb /plumb
USER 65532:65532
ENTRYPOINT ["/plumb"]
