# Build the manager image:
#
#   podman build -t ghcr.io/llm-d/llm-d-operators:dev .
#   podman push ghcr.io/llm-d/llm-d-operators:dev
#
# The binary is built static (CGO_ENABLED=0) so the distroless base
# has no libc requirement.
FROM golang:1.26 AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/ cmd/
COPY api/ api/
COPY internal/ internal/
RUN CGO_ENABLED=0 go build -o /manager ./cmd

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /
COPY --from=builder /manager /manager
USER nonroot:nonroot
ENTRYPOINT ["/manager"]
