default: build

GO_TOOLS_BIN_PATH := $(shell pwd)/.tools/bin
PATH := $(GO_TOOLS_BIN_PATH):$(PATH)
SHELL := env PATH='$(PATH)' GOBIN='$(GO_TOOLS_BIN_PATH)' $(shell which bash)

install-tools:
	@mkdir -p $(GO_TOOLS_BIN_PATH)
	@(which golangci-lint && golangci-lint version | grep '1.49') >/dev/null 2>&1 || curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(GO_TOOLS_BIN_PATH) v1.49.0
	@grep '_' toolset/toolset.go | sed 's/"//g' | awk '{print $$2}' | xargs go install

ALL_PKG := github.com/tatris-io/tatris
PACKAGES := $(shell go list ./...) 
PACKAGE_DIRECTORIES := $(subst $(ALL_PKG)/,,$(PACKAGES))
PACKAGES_WITHOUT_TOOLSET := $(shell go list ./... | sed '/^github.com\/tatris-io\/tatris\/toolset/d')
PACKAGE_DIRECTORIES_WITHOUT_TOOLSET := $(subst $(ALL_PKG)/,,$(PACKAGES_WITHOUT_TOOLSET))

check: install-tools
	@ echo "check license ..."
	@ make check-license
	@ echo "gofmt ..."
	@ echo "$(PACKAGE_DIRECTORIES)"
	@ gofmt -s -l -w $(PACKAGE_DIRECTORIES)
	@ echo "golangci-lint ..."
	@ golangci-lint run $(PACKAGE_DIRECTORIES_WITHOUT_TOOLSET)
	@ echo "revive ..."
	@ revive -formatter friendly -config revive.toml $(PACKAGES_WITHOUT_TOOLSET)

test: install-tools
	@ echo "go test ..."
	@ go test -timeout 5m -race -cover $(PACKAGES_WITHOUT_TOOLSET)

check-license:
	@ sh ./scripts/check-license.sh

build: check
	@ mkdir -p ./bin
	@ go build -o ./bin/tatris-meta ./cmd/meta/...
	@ go build -o ./bin/tatris-indexer ./cmd/indexer/...
	@ go build -o ./bin/tatris ./cmd/standalone/...

clean:
	@ rm -f ./bin/tatris-meta
