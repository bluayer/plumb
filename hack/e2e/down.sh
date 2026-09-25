#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
PROVIDER=${PROVIDER:-kind}
PREFIX=${PREFIX:-plumb-e2e}
for n in home remote; do
  cluster="${PREFIX}-${n}"
  case "$PROVIDER" in
    kind) kind delete cluster --name "$cluster" || true ;;
    minikube) minikube delete -p "$cluster" || true ;;
    kwok) kwokctl delete cluster --name "$cluster" || true ;;
  esac
done
rm -rf .e2e
