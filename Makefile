GO       ?= go
GOFLAGS  ?=
BIN      := bin/syzy
PKG      := ./sqlite/cmd/syzy

# Release builds stamp a version and strip; dev builds do neither, and
# report "devel+<revision>" from the VCS data the toolchain embeds.
#
#	make build                  → devel build
#	make build build-pg ext VERSION=v0.1.0 → release build
VERSION ?=
LDFLAGS ?= $(if $(VERSION),-s -w -X github.com/wjordan/syzy/internal/buildinfo.version=$(VERSION),)

PG_BIN := bin/syzy-pg
PG_PKG := ./cmd/syzy-pg

.PHONY: all build build-pg test test-pg test-ext ext vet fmt clean

all: build

build:
	@mkdir -p $(dir $(BIN))
	$(GO) build $(GOFLAGS) -ldflags "$(LDFLAGS)" -o $(BIN) $(PKG)

# The Postgres sidecar is a separate nested module (pg/) so the SQLite
# product never carries the Postgres driver dependencies.
build-pg:
	@mkdir -p $(dir $(PG_BIN))
	cd pg && $(GO) build $(GOFLAGS) -ldflags "$(LDFLAGS)" -o ../$(PG_BIN) $(PG_PKG)

test:
	$(GO) test -race -count=1 ./...

test-pg:
	cd pg && $(GO) test -race -count=1 ./...

# Extension tests that load the built ext/*.so (they skip without it),
# so the artifacts are built first. Not part of `test`: the c-shared
# link costs more than the tests it enables.
test-ext: ext
	$(GO) test -count=1 ./sqlite/cmd/syzy-ext/...

ext:
	$(MAKE) -C ext LDFLAGS='$(LDFLAGS)'

# The extension reads its knobs through internal/syzyenv, which is
# libc-backed in that build; os.Getenv there returns "" for variables a
# host process's setproctitle overwrote. Nothing else may read them.
vet:
	$(GO) vet ./...
	cd pg && $(GO) vet ./...
	@! git grep -ln --untracked 'os\.Getenv("SYZY_' -- '*.go' ':!*_test.go' ':!internal/syzyenv/*' \
		|| { echo "read SYZY_* through internal/syzyenv, not os.Getenv"; exit 1; }

fmt:
	$(GO) fmt ./...
	cd pg && $(GO) fmt ./...

clean:
	rm -rf bin
	$(MAKE) -C ext clean
