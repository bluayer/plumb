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

# hack/chart-smoke.sh
#
# Installs charts/plumb in a disposable kind cluster as docs/installation.md does, with an
# image built from this checkout, and checks what the e2e suite (which runs Plumb
# in-process) cannot: the chart's pods start with its arguments, RBAC and probes. Both
# Deployments become ready; the agent reports on a policy and, leading a fleet of one,
# holds the hub Lease and writes the fleet status; nothing restarts and no request is
# denied; helm uninstall removes the pods.
#
#   hack/chart-smoke.sh               # needs docker; kind and helm come from hack/tools
#   KEEP=1 hack/chart-smoke.sh        # leave the cluster up (kind delete cluster --name plumb-chart)
#   KIND_CONFIG=f.yaml hack/chart-smoke.sh   # extra kind config, e.g. failCgroupV1: false on a cgroup v1 host
set -euo pipefail
cd "$(dirname "$0")/.."

CLUSTER=${CLUSTER:-plumb-chart}
KIND_CONFIG=${KIND_CONFIG:-}
KEEP=${KEEP:-0}
IMAGE=plumb:chart-smoke
NS=plumb
mkdir -p .e2e
export KUBECONFIG=$PWD/.e2e/chart.kubeconfig

kind() { go tool -modfile=hack/tools/go.mod kind "$@"; }
helm() { go tool -modfile=hack/tools/go.mod helm "$@"; }

finish() {
  local rc=$?
  if [[ $rc -ne 0 ]]; then
    echo "--- FAILED; cluster state:" >&2
    kubectl get pods,leases -n "$NS" -o wide >&2 || true
    kubectl describe pods -n "$NS" >&2 || true
    kubectl logs -n "$NS" -l app.kubernetes.io/name=plumb-agent --all-containers --tail=100 --prefix >&2 || true
    kubectl logs -n "$NS" -l app.kubernetes.io/name=plumb-scaler --all-containers --tail=100 --prefix >&2 || true
    kubectl get adaptivepolicy -A -o yaml >&2 || true
  fi
  [[ "$KEEP" == "1" ]] || kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
  exit "$rc"
}

# until DESCRIPTION SECONDS COMMAND...: retries COMMAND every 2s.
until_ok() {
  local what=$1 deadline=$((SECONDS + $2))
  shift 2
  until "$@" >/dev/null 2>&1; do
    if ((SECONDS > deadline)); then
      echo "timed out waiting for $what" >&2
      return 1
    fi
    sleep 2
  done
  echo "ok: $what"
}

nonempty() { [[ -n "$(kubectl "$@")" ]]; }
no_pods() { [[ -z "$(kubectl get pods -n "$NS" -o name)" ]]; }

kind create cluster --name "$CLUSTER" --kubeconfig "$KUBECONFIG" ${KIND_CONFIG:+--config "$KIND_CONFIG"} --wait 2m
trap finish EXIT

docker build -t "$IMAGE" --build-arg VERSION=chart-smoke .
kind load docker-image "$IMAGE" --name "$CLUSTER"

# The one prerequisite without which the agent cannot start (docs/installation.md).
kubectl apply -f test/e2e/testdata/crds/multicluster.x-k8s.io_clusterprofiles.yaml

helm install plumb charts/plumb -n "$NS" --create-namespace --wait --timeout 3m \
  --set clusterName=smoke \
  --set image.repository="${IMAGE%%:*}" --set image.tag="${IMAGE##*:}" --set image.pullPolicy=Never

kubectl create namespace inference
kubectl create deployment llm -n inference --image=registry.k8s.io/pause:3.10
kubectl apply -f - <<'EOF'
apiVersion: plumb-k8s.github.io/v1alpha1
kind: AdaptivePolicy
metadata:
  name: llm
  namespace: inference
spec:
  workload: {name: llm}
  clusters:
    - {name: smoke, maxReplicas: 4}
EOF

until_ok "the agent's report" 120 nonempty get adaptivepolicy llm -n inference -o 'jsonpath={.status.report.time}'
until_ok "the hub Lease" 120 nonempty get lease plumb-hub -n "$NS" -o 'jsonpath={.spec.holderIdentity}'
until_ok "the fleet status" 120 nonempty get adaptivepolicy llm -n inference -o 'jsonpath={.status.fleet.phase}'

restarts=$(kubectl get pods -n "$NS" -o 'jsonpath={range .items[*]}{.metadata.name}={.status.containerStatuses[*].restartCount}{"\n"}{end}' | grep -v '=0$' || true)
if [[ -n "$restarts" ]]; then
  echo "restarted: $restarts" >&2
  exit 1
fi
echo "ok: no restarts"

# A denied request is logged, not fatal: find the RBAC the chart is missing.
denied=$(kubectl logs -n "$NS" -l 'app.kubernetes.io/name in (plumb-agent,plumb-scaler)' --all-containers --prefix --tail=-1 | grep -i forbidden || true)
if [[ -n "$denied" ]]; then
  echo "$denied" >&2
  exit 1
fi
echo "ok: no request denied"

helm uninstall plumb -n "$NS" --wait --timeout 2m
until_ok "no Plumb pods after uninstall" 60 no_pods
