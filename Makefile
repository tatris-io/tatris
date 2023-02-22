default: build

GO_TOOLS_BIN_PATH := $(shell pwd)/.tools/bin
PATH := $(GO_TOOLS_BIN_PATH):$(PATH)
SHELL := env PATH='$(PATH)' GOBIN='$(GO_TOOLS_BIN_PATH)' $(shell which bash)

install-tools:
	@ echo "install-tools ..."
	@ mkdir -p $(GO_TOOLS_BIN_PATH)
	@ (which golangci-lint && golangci-lint version | grep '1.49') >/dev/null 2>&1 || curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(GO_TOOLS_BIN_PATH) v1.49.0
	@ grep '_' toolset/toolset.go | sed 's/"//g' | awk '{print $$2}' | xargs go install

ALL_PKG := github.com/tatris-io/tatris
PACKAGES := $(shell go list ./...) 
PACKAGE_DIRECTORIES := $(subst $(ALL_PKG)/,,$(PACKAGES))
PACKAGES_WITHOUT_TOOLSET := $(shell go list ./... | sed '/^github.com\/tatris-io\/tatris\/toolset/d')

REVISION := $(shell git rev-parse --short HEAD 2>/dev/null)
REVISION_DATE := $(shell git log -1 --pretty=format:'%ad' --date short 2>/dev/null)
BUILD_TIME=$(shell date +%Y-%m-%d-%H:%M:%S)
VERSION_PKG := github.com/tatris-io/tatris/internal/common/consts
LDFLAGS = -s -w
ifneq ($(strip $(REVISION)),)
    LDFLAGS += -X $(VERSION_PKG).revision=$(REVISION) \
           -X $(VERSION_PKG).revisionDate=$(REVISION_DATE) \
           -X $(VERSION_PKG).buildTime=$(BUILD_TIME)
endif

DOCKER_BUILD_ARGS := -f docker/Dockerfile --build-arg GOLANG_LDFLAGS="$(LDFLAGS)" .
ifdef TARGETPLATFORM
DOCKER_BUILD_ARGS := --platform $(TARGETPLATFORM) $(DOCKER_BUILD_ARGS)
endif
ifdef TAG
DOCKER_BUILD_ARGS := -t $(TAG)  $(DOCKER_BUILD_ARGS)
endif

check: install-tools
	@ echo "do checks ..."
	@ make check-license
	@ echo "gofmt ..."
	@ gofmt -s -l -w $(PACKAGE_DIRECTORIES)
	@ echo "golines ..."
	@ golines --max-len=100 --shorten-comments -w internal cmd test
	@ echo "golangci-lint ..."
	@ golangci-lint cache clean
	@ golangci-lint run -c golangci-lint.yml ./internal/... ./cmd/...
	@ echo "revive ..."
	@ revive -formatter friendly -config revive.toml $(PACKAGES_WITHOUT_TOOLSET)

test: install-tools
	@ echo "unit test ..."
	@ go test -timeout 5m -race -cover $(PACKAGES_WITHOUT_TOOLSET)

check-license:
	@ echo "check-license ..."
	@ sh ./scripts/check-license.sh

add-license:
	@ echo "add-license ..."
	@ sh ./scripts/add-license.sh

build: check fast-build

fast-build:
	@ echo "building ..."
	@ mkdir -p ./bin
	@ go build -ldflags="$(LDFLAGS)" -o ./bin/tatris-meta ./cmd/meta/...
	@ go build -ldflags="$(LDFLAGS)" -o ./bin/tatris-server ./cmd/server/...

docker-image:
	@ echo "building docker image, args: $(DOCKER_BUILD_ARGS)"
	@ docker buildx build $(DOCKER_BUILD_ARGS)

clean:
	@ echo "clean ..."
	@ rm -f ./bin/tatris-meta
	@ rm -f ./bin/tatris-server
