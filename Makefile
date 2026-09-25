CONTROLLER_GEN ?= $(CURDIR)/bin/controller-gen

.PHONY: all test test-go test-py generate proto build lint

all: generate test build

$(CONTROLLER_GEN):
	GOBIN=$(CURDIR)/bin go install sigs.k8s.io/controller-tools/cmd/controller-gen@v0.20.0

# DeepCopy, CRD and the agent ClusterRole, written straight into the chart.
generate: $(CONTROLLER_GEN)
	$(CONTROLLER_GEN) object paths=./api/...
	$(CONTROLLER_GEN) rbac:roleName=plumb-agent crd paths=./... \
		output:crd:artifacts:config=charts/plumb/crds output:rbac:artifacts:config=charts/plumb/templates

# Go and Python gRPC code next to each .proto. Needs grpcio-tools, protoc-gen-go, protoc-gen-go-grpc.
MODULE := github.com/bluayer/agent-inference-scheduler
PROTOC := python3 -m grpc_tools.protoc --go_opt=module=$(MODULE) --go-grpc_opt=module=$(MODULE) --go_out=. --go-grpc_out=.
proto:
	PATH="$$PATH:$$(go env GOPATH)/bin" $(PROTOC) -I decision-service decision-service/plumb_decision/decision.proto
	PATH="$$PATH:$$(go env GOPATH)/bin" $(PROTOC) -I internal/scaler/externalscaler internal/scaler/externalscaler/externalscaler.proto
	cd decision-service && python3 -m grpc_tools.protoc -I . --python_out=. --grpc_python_out=. plumb_decision/decision.proto

build:
	go build -o bin/plumb ./cmd/plumb

test: test-go test-py

test-go:
	go vet ./...
	go test -race ./...

test-py:
	cd decision-service && python3 -m pytest -q

lint:
	gofmt -l . | grep -v '^bin/' | (! grep .)

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
