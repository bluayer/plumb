#!/usr/bin/env bash
# Copyright The Plumb Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# hack/e2e.sh up|down
#
# up creates disposable clusters for the e2e suite ("home" and "remote", with an
# optional third member) and
# makes KWOK manage fake nodes in them, so tests can create GPU nodes without GPUs.
#
#   PROVIDER=kind     hack/e2e.sh up   # default; needs docker + kind
#   PROVIDER=minikube hack/e2e.sh up   # needs minikube
#   PROVIDER=kwok     hack/e2e.sh up   # kwokctl; no containers at all (fastest)
#
# The real kube-scheduler places pods in every mode; KWOK only simulates the kubelet
# of nodes annotated kwok.x-k8s.io/node=fake. Kubeconfigs land in .e2e/.
# REMOTE=0 skips the second cluster (multi-region tests are then skipped).
# THIRD=1 adds a third cluster for three-way traffic tests.
set -euo pipefail
cd "$(dirname "$0")/.."

PROVIDER=${PROVIDER:-kind}
REMOTE=${REMOTE:-1}
THIRD=${THIRD:-0}
KWOK_VERSION=${KWOK_VERSION:-$(go list -modfile=hack/tools/go.mod -m -f '{{.Version}}' sigs.k8s.io/kwok)} # Dependabot bumps it there
KIND_IMAGE=${KIND_IMAGE:-}          # e.g. kindest/node:v1.33.7
PREFIX=${PREFIX:-plumb-e2e}
OUT=.e2e

# kwokctl from PATH, else the version pinned in hack/tools/go.mod.
command -v kwokctl >/dev/null || kwokctl() { go tool -modfile=hack/tools/go.mod kwokctl "$@"; }

if [[ "${1:-}" == "down" ]]; then
  for n in home remote third; do
    case "$PROVIDER" in
      kind) kind delete cluster --name "${PREFIX}-${n}" || true ;;
      minikube) minikube delete -p "${PREFIX}-${n}" || true ;;
      kwok) kwokctl delete cluster --name "${PREFIX}-${n}" || true ;;
    esac
  done
  rm -rf "$OUT"
  exit 0
fi
[[ "${1:-}" == "up" ]] || { echo "usage: $0 up|down" >&2; exit 2; }
mkdir -p "$OUT"

names=(home)
[[ "$REMOTE" == "1" ]] && names+=(remote)
[[ "$THIRD" == "1" ]] && names+=(third)
if [[ "$THIRD" == "1" && "$REMOTE" != "1" ]]; then
  echo "THIRD=1 requires REMOTE=1" >&2
  exit 2
fi

# KWOK in-cluster manifests, rendered from the pinned Go module (no GitHub download).
kwok_manifests() {
  local dir
  dir=$(go mod download -json "sigs.k8s.io/kwok@${KWOK_VERSION}" | sed -n 's/.*"Dir": "\(.*\)".*/\1/p')
  kubectl kustomize "$dir/kustomize/kwok" |
    sed "s#image: registry.k8s.io/kwok/kwok\$#image: registry.k8s.io/kwok/kwok:${KWOK_VERSION}#"
  echo "---"
  kubectl kustomize "$dir/kustomize/stage/fast"
}

install_kwok() {
  local kc=$1
  kwok_manifests | kubectl --kubeconfig "$kc" apply --server-side -f - >/dev/null
  kubectl --kubeconfig "$kc" -n kube-system rollout status deploy/kwok-controller --timeout=180s
}

for n in "${names[@]}"; do
  cluster="${PREFIX}-${n}"
  kc="$OUT/$n.kubeconfig"
  case "$PROVIDER" in
    kind)
      kind get clusters 2>/dev/null | grep -qx "$cluster" ||
        kind create cluster --name "$cluster" ${KIND_IMAGE:+--image "$KIND_IMAGE"} --wait 120s
      kind get kubeconfig --name "$cluster" >"$kc"
      install_kwok "$kc"
      ;;
    minikube)
      minikube status -p "$cluster" >/dev/null 2>&1 || minikube start -p "$cluster" --nodes 1 --memory 2048
      # minikube names the context after the profile.
      kubectl config view --minify --flatten --context "$cluster" >"$kc"
      install_kwok "$kc"
      ;;
    kwok)
      read -ra extra <<<"${KWOKCTL_ARGS:-}" # e.g. "--etcd-binary /path/etcd"
      kwokctl get clusters 2>/dev/null | grep -qx "$cluster" ||
        kwokctl create cluster --name "$cluster" --runtime "${KWOK_RUNTIME:-binary}" ${extra[@]+"${extra[@]}"} --wait 120s
      kwokctl get kubeconfig --name "$cluster" >"$kc"
      ;;
    *)
      echo "unknown PROVIDER=$PROVIDER (kind|minikube|kwok)" >&2
      exit 1
      ;;
  esac
  echo "cluster $cluster ready: $kc"
done

remote_config=""
third_config=""
[[ "$REMOTE" == "1" ]] && remote_config=" PLUMB_E2E_REMOTE_KUBECONFIG=$OUT/remote.kubeconfig"
[[ "$THIRD" == "1" ]] && third_config=" PLUMB_E2E_THIRD_KUBECONFIG=$OUT/third.kubeconfig"
cat <<MSG

Run the suite:
  PLUMB_E2E_KUBECONFIG=$OUT/home.kubeconfig${remote_config}${third_config} make e2e
MSG
