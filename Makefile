CONTROLLER_GEN ?= $(CURDIR)/bin/controller-gen

.PHONY: all test test-go test-py generate manifests proto build lint

all: generate test build

$(CONTROLLER_GEN):
	GOBIN=$(CURDIR)/bin go install sigs.k8s.io/controller-tools/cmd/controller-gen@v0.20.0

generate: $(CONTROLLER_GEN)
	$(CONTROLLER_GEN) object:headerFile=hack/boilerplate.go.txt paths=./api/...

manifests: $(CONTROLLER_GEN)
	$(CONTROLLER_GEN) rbac:roleName=plumb-agent crd paths=./... \
		output:crd:artifacts:config=config/crd output:rbac:artifacts:config=config/rbac
	cp config/crd/*.yaml charts/plumb/crds/
	sed "1{/^---$$/d}" config/rbac/role.yaml > charts/plumb/files/agent-role.yaml

proto:
	./hack/gen-proto.sh

build:
	go build -o bin/plumb-agent ./cmd/plumb-agent
	go build -o bin/plumb-scaler ./cmd/plumb-scaler

test: test-go test-py

test-go:
	go vet ./...
	go test -race ./...

test-py:
	cd decision-service && python3 -m pytest -q

lint:
	gofmt -l . | grep -v '^bin/' | (! grep .)

# E2E scheduling tests against disposable clusters with KWOK fake GPU nodes.
# PROVIDER=kind|minikube|kwok (see hack/e2e/up.sh).
.PHONY: e2e-up e2e e2e-down
e2e-up:
	./hack/e2e/up.sh

e2e:
	PLUMB_E2E_KUBECONFIG=$${PLUMB_E2E_KUBECONFIG:-$(CURDIR)/.e2e/home.kubeconfig} \
	PLUMB_E2E_REMOTE_KUBECONFIG=$${PLUMB_E2E_REMOTE_KUBECONFIG:-$$(test -f .e2e/remote.kubeconfig && echo $(CURDIR)/.e2e/remote.kubeconfig)} \
	go test -tags e2e -count=1 -timeout 15m -v ./test/e2e/...

e2e-down:
	./hack/e2e/down.sh
