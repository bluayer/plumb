# Every tool version is pinned in hack/tools/go.mod (actionlint in its own module, its
# dependencies clash with the others), where Dependabot keeps it current.
TOOL := go tool -modfile=$(CURDIR)/hack/tools/go.mod

.PHONY: all test generate proto build lint chart chart-smoke verify verify-generate verify-mod

all: generate lint test build

# DeepCopy, CRD and the agent ClusterRole, written straight into the chart.
generate:
	$(TOOL) controller-gen object:headerFile=hack/boilerplate.go.txt paths=./api/...
	$(TOOL) controller-gen rbac:roleName=plumb-agent crd paths=./... \
		output:crd:artifacts:config=charts/plumb/crds output:rbac:artifacts:config=charts/plumb/templates

# KEDA's external scaler gRPC code. Needs protoc; the plugins come from hack/tools.
MODULE := github.com/bluayer/plumb
proto:
	protoc --plugin=protoc-gen-go=$$($(TOOL) -n protoc-gen-go) --plugin=protoc-gen-go-grpc=$$($(TOOL) -n protoc-gen-go-grpc) \
		--go_opt=module=$(MODULE) --go-grpc_opt=module=$(MODULE) --go_out=. --go-grpc_out=. \
		-I internal/scaler/externalscaler internal/scaler/externalscaler/externalscaler.proto

build:
	go build -o bin/plumb ./cmd/plumb

test:
	go vet ./...
	go test -race ./...

lint:
	$(TOOL) golangci-lint run ./...
	go tool -modfile=$(CURDIR)/hack/tools/actionlint/go.mod actionlint

chart:
	$(TOOL) helm lint charts/plumb --strict --set clusterName=ci
	$(TOOL) helm template plumb charts/plumb --set clusterName=ci --set serviceMonitor.enabled=true --set scaler.tlsSecretName=tls >/dev/null

# Installs the chart in a disposable kind cluster and checks that it runs (needs docker).
chart-smoke:
	./hack/chart-smoke.sh

# Fails when generated code is stale (as in CI and the pre-commit codegen hook).
verify-generate: generate
	git diff --exit-code -- api/ charts/plumb/crds/ charts/plumb/templates/role.yaml

# go.mod and go.sum match the imports, in the module and in both tool modules.
verify-mod:
	go mod tidy -diff
	cd hack/tools && go mod tidy -diff
	cd hack/tools/actionlint && go mod tidy -diff

verify: verify-generate proto
	git diff --exit-code -- '*.pb.go'

# E2E scheduling tests against disposable clusters with KWOK fake GPU nodes.
# PROVIDER=kind|minikube|kwok (see hack/e2e.sh).
.PHONY: e2e-up e2e e2e-down
e2e-up:
	./hack/e2e.sh up

e2e:
	PLUMB_E2E_KUBECONFIG=$${PLUMB_E2E_KUBECONFIG:-$(CURDIR)/.e2e/home.kubeconfig} \
	PLUMB_E2E_REMOTE_KUBECONFIG=$${PLUMB_E2E_REMOTE_KUBECONFIG:-$$(test -f .e2e/remote.kubeconfig && echo $(CURDIR)/.e2e/remote.kubeconfig)} \
	PLUMB_E2E_THIRD_KUBECONFIG=$${PLUMB_E2E_THIRD_KUBECONFIG:-$$(test -f .e2e/third.kubeconfig && echo $(CURDIR)/.e2e/third.kubeconfig)} \
	go test -tags e2e -count=1 -timeout 15m -v ./test/e2e/...

e2e-down:
	./hack/e2e.sh down
