# Image URL to use all building/pushing image targets
IMG ?= ghcr.io/llm-d/llm-d-operators:dev

# controller-tools pinned for reproducible codegen
CONTROLLER_GEN := go run sigs.k8s.io/controller-tools/cmd/controller-gen@v0.17.3

.PHONY: all build test manifests generate fmt vet install-crd install-rbac clean

all: build

## build: build the manager binary
build:
	go build -o bin/manager ./cmd

## test: run unit tests (fake client; no envtest)
test:
	go test ./...

## manifests: generate CRD manifests and RBAC into config/
manifests:
	$(CONTROLLER_GEN) crd:allowDangerousTypes=true paths=./api/... output:crd:artifacts:config=config/crd/bases
	$(CONTROLLER_GEN) rbac:roleName=manager-role paths=./cmd/... paths=./internal/controller/... output:rbac:artifacts:config=config/rbac

## generate: generate deepcopy code
generate:
	$(CONTROLLER_GEN) object paths=./api/...

## fmt: go fmt
fmt:
	go fmt ./...

## vet: go vet
vet:
	go vet ./...

## install-crd: apply generated CRDs
install-crd:
	kubectl apply -f config/crd/bases

## install-rbac: apply generated RBAC
install-rbac:
	kubectl apply -f config/rbac

clean:
	rm -rf bin
