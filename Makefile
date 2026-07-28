GO       ?= go
GOFLAGS  ?=
BIN      := bin/syzy
PKG      := ./sqlite/cmd/syzy

# Release builds stamp a version and strip; dev builds do neither, and
# report "devel+<revision>" from the VCS data the toolchain embeds.
#
#	make build                  → devel build
#	make build ext VERSION=v0.1.0 → release build
VERSION ?=
LDFLAGS ?= $(if $(VERSION),-s -w -X github.com/wjordan/syzy/internal/buildinfo.version=$(VERSION),)

.PHONY: all build test ext vet fmt clean

all: build

build:
	@mkdir -p $(dir $(BIN))
	$(GO) build $(GOFLAGS) -ldflags "$(LDFLAGS)" -o $(BIN) $(PKG)

test:
	$(GO) test -race -count=1 ./...

ext:
	$(MAKE) -C ext LDFLAGS='$(LDFLAGS)'

vet:
	$(GO) vet ./...

fmt:
	$(GO) fmt ./...

clean:
	rm -rf bin
	$(MAKE) -C ext clean
