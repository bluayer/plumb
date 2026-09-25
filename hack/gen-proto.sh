#!/usr/bin/env bash
# Regenerates Go and Python gRPC code. Needs: python -m pip install grpcio-tools,
# go install google.golang.org/protobuf/cmd/protoc-gen-go@latest google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
set -euo pipefail
cd "$(dirname "$0")/.."
export PATH="$PATH:$(go env GOPATH)/bin"
MOD=github.com/bluayer/agent-inference-scheduler

python3 -m grpc_tools.protoc -I proto \
  --go_out=. --go_opt=module=$MOD \
  --go-grpc_out=. --go-grpc_opt=module=$MOD \
  proto/plumb/decision/v1/decision.proto proto/externalscaler/externalscaler.proto

OUT=decision-service/plumb_decision/gen
mkdir -p "$OUT"
python3 -m grpc_tools.protoc -I proto --python_out="$OUT" --grpc_python_out="$OUT" proto/plumb/decision/v1/decision.proto
touch "$OUT/__init__.py" "$OUT/plumb/__init__.py" "$OUT/plumb/decision/__init__.py" "$OUT/plumb/decision/v1/__init__.py"
