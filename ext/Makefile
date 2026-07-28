GO    ?= go
LDFLAGS ?=
CC    ?= cc
TAGS  ?= syzy_extension
OUT   ?= syzy.so
SHIM  ?= syzy-shim.so
PKG   := ./cmd/syzy-ext
ROOT  := $(abspath $(dir $(lastword $(MAKEFILE_LIST)))/..)
SQLITE := $(ROOT)/sqlite

.PHONY: all engine shim clean

# The shim is an LD_PRELOAD interposer, which is a Linux mechanism; it
# also links -ldl, a library darwin folds into libSystem and does not
# provide. Nothing about it would work there, so don't build it.
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
all: engine
else
all: engine shim
endif

engine:
	cd $(SQLITE) && CGO_ENABLED=1 $(GO) build -tags $(TAGS) -ldflags "$(LDFLAGS)" -buildmode=c-shared -o ../ext/$(OUT) $(PKG)

# Standalone pure-C preload: same interposer source, no Go linked.
# dlopen's the engine (ext/syzy.so) on the first SYZY_DB-matching open.
shim:
	$(CC) -shared -fPIC -O2 -Wall -DSYZY_LAZY_SHIM -DSYZY_EXTENSION=1 \
		-I$(ROOT)/third_party/sqlite -I$(ROOT)/sqlitebridge \
		-o $(ROOT)/ext/$(SHIM) \
		$(SQLITE)/cmd/syzy-ext/autoload_shim.c \
		$(SQLITE)/cmd/syzy-ext/app_vfs.c -ldl -lpthread

clean:
	rm -f $(OUT) $(SHIM) syzy.h
