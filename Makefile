SHELL := /bin/bash
VERSION_PACKAGE = github.com/replicatedhq/pvmigrate/pkg/version
VERSION ?=`git describe --tags --dirty`
DATE=`date -u +"%Y-%m-%dT%H:%M:%SZ"`

GIT_TREE = $(shell git rev-parse --is-inside-work-tree 2>/dev/null)
ifneq "$(GIT_TREE)" ""
define GIT_UPDATE_INDEX_CMD
git update-index --assume-unchanged
endef
define GIT_SHA
`git rev-parse HEAD`
endef
else
define GIT_UPDATE_INDEX_CMD
echo "Not a git repo, skipping git update-index"
endef
define GIT_SHA
""
endef
endif

define LDFLAGS
-ldflags "\
	-X ${VERSION_PACKAGE}.version=${VERSION} \
	-X ${VERSION_PACKAGE}.gitSHA=${GIT_SHA} \
	-X ${VERSION_PACKAGE}.buildTime=${DATE} \
"
endef

.PHONY: clean
clean:
	rm -rf ./bin

.PHONY: deps
deps:
	go get golang.org/x/lint/golint

.PHONY: lint
lint: deps
	golint -set_exit_status ./... ./cmd/...

.PHONY: vet
vet:
	go vet ./... ./cmd/...

.PHONY: test
test: lint vet
	go test -coverprofile=coverage.out ./... ./cmd/...

.PHONY: build
build: bin/pvmigrate

bin/pvmigrate: cmd/main.go pkg/migrate/migrate.go pkg/version/version.go
	go build ${LDFLAGS} -o bin/pvmigrate cmd/main.go
