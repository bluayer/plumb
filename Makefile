CONTROLLER_GEN ?= $(CURDIR)/bin/controller-gen

.PHONY: all test generate proto build lint verify verify-generate

all: generate lint test build

$(CONTROLLER_GEN):
	GOBIN=$(CURDIR)/bin go install sigs.k8s.io/controller-tools/cmd/controller-gen@v0.20.0

# DeepCopy, CRD and the agent ClusterRole, written straight into the chart.
generate: $(CONTROLLER_GEN)
	$(CONTROLLER_GEN) object:headerFile=hack/boilerplate.go.txt paths=./api/...
	$(CONTROLLER_GEN) rbac:roleName=plumb-agent crd paths=./... \
		output:crd:artifacts:config=charts/plumb/crds output:rbac:artifacts:config=charts/plumb/templates

# KEDA's external scaler gRPC code. Needs protoc, protoc-gen-go, protoc-gen-go-grpc.
MODULE := github.com/bluayer/plumb
proto:
	PATH="$$PATH:$$(go env GOPATH)/bin" protoc --go_opt=module=$(MODULE) --go-grpc_opt=module=$(MODULE) --go_out=. --go-grpc_out=. \
		-I internal/scaler/externalscaler internal/scaler/externalscaler/externalscaler.proto

build:
	go build -o bin/plumb ./cmd/plumb

test:
	go vet ./...
	go test -race ./...

GOLANGCI_LINT ?= go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.14.0
lint:
	$(GOLANGCI_LINT) run ./...

# Fails when generated code is stale (as in CI and the pre-commit codegen hook).
verify-generate: generate
	git diff --exit-code -- api/ charts/plumb/crds/ charts/plumb/templates/role.yaml

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
	go test -tags e2e -count=1 -timeout 15m -v ./test/e2e/...

e2e-down:
	./hack/e2e.sh down
