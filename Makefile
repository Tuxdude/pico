# Set the shell to bash.
SHELL := /bin/bash -e -o pipefail

# Enable a verbose output from the makesystem.
VERBOSE ?= no

# Disable the colorized output from make if either
# explicitly overridden or if no tty is attached.
DISABLE_COLORS ?= $(shell [ -t 0 ] && echo no)

# Silence echoing the commands being invoked unless
# overridden to be verbose.
ifneq ($(VERBOSE),yes)
    silent := @
else
    silent :=
endif

# Configure colors.
ifeq ($(DISABLE_COLORS),no)
    COLOR_BLUE    := \x1b[1;34m
    COLOR_RESET   := \x1b[0m
else
    COLOR_BLUE    :=
    COLOR_RESET   :=
endif

# Common utilities.
ECHO := echo -e

# go and related binaries.
GO_CMD          := go
GO_IMPORTS_CMD  := goimports
GO_FMT_CMD      := gofmt
GO_LINT_CMD     := golint
GO_CI_LINT_CMD  := golangci-lint

# Commands invoked from rules.
GOBUILD         := $(GO_CMD) build
GOSTRIPPEDBUILD := CGO_ENABLED=0 GOOS=linux $(GO_CMD) build -a -ldflags "-s -w" -installsuffix cgo
GOCLEAN         := $(GO_CMD) clean
GOGET           := $(GO_CMD) get -u
GOLIST          := $(GO_CMD) list
GOMOD           := $(GO_CMD) mod
GOTEST          := $(GO_CMD) test -v
GOCOVERAGE      := $(GO_CMD) test -v -race -coverprofile coverage.out -covermode atomic
GOVET           := $(GO_CMD) vet
GOIMPORTS       := $(GO_IMPORTS_CMD) -w
GOFMT           := $(GO_FMT_CMD) -s -w
GOLINT          := $(GO_LINT_CMD) -set_exit_status -min_confidence 0.200001
GOLINTAGG       := $(GO_LINT_CMD) -set_exit_status -min_confidence 0
GOLANGCILINT    := $(GO_CI_LINT_CMD) run
BUILDCMD        := if [ -d cmd ]; then $(MAKE) -C cmd; else echo "No cmd directory found, ignoring ..."; fi

# Alternative for running golangci-lint, using docker instead:
# docker run \
#   --rm \
#   --tty \
#   --volume $$(pwd):/go-src:ro \
#   --workdir /go-src \
#   golangci/golangci-lint:v1.44.0 \
#   golangci-lint run

# Helpful functions
# ExecWithMsg
# $(1) - Message
# $(2) - Command to be executed
define ExecWithMsg
    $(silent)$(ECHO) "\n===  $(COLOR_BLUE)$(1)$(COLOR_RESET)  ==="
    $(silent)$(2)
endef

# List of packages in the current directory.
PKGS ?= $(shell $(GO_CMD) list ./... | grep -v /vendor/)
# Define tags.
TAGS ?=

DEP_PKGS := $(shell $(GO_CMD) list -f '{{ join .Imports "\n" }}' | grep tuxdude || true)
ifeq ($(DEP_PKGS),)
    DEP_PKGS_TEXT := None
else
    DEP_PKGS_TEXT := $(DEP_PKGS)
    DEP_PKGS := $(addsuffix @master,$(DEP_PKGS))
endif

all: fiximports fmt lint vet build buildcmd test

clean:
	$(call ExecWithMsg,Cleaning,$(GOCLEAN) ./...)

fiximports:
	$(call ExecWithMsg,Fixing imports,$(GOIMPORTS) .)

fmt:
	$(call ExecWithMsg,Fixing formatting,$(GOFMT) .)

lint: tidy
	$(call ExecWithMsg,Linting,$(GOLINT) . && $(GOLANGCILINT))

lintagg: tidy
	$(call ExecWithMsg,Aggressive Linting,$(GOLINTAGG) . && $(GOLANGCILINT))

vet: tidy
	$(call ExecWithMsg,Vetting,$(GOVET) ./...)

tidy:
	$(call ExecWithMsg,Tidying module,$(GOMOD) tidy)

deps_update:
	$(call ExecWithMsg,Updating to the latest version of dependencies for \"$(DEP_PKGS_TEXT)\",$(GOGET) $(DEP_PKGS))

build: tidy
	$(call ExecWithMsg,Building,$(GOBUILD) ./...)

buildcmd: build
	$(call ExecWithMsg,Building recursively in cmd ...,$(BUILDCMD))

buildstripped: tidy
	$(call ExecWithMsg,Building Stripped,$(GOSTRIPPEDBUILD) ./...)

test: tidy
	$(call ExecWithMsg,Testing,$(GOTEST) ./...)

coverage: tidy
	$(call ExecWithMsg,Testing with Coverage generation,$(GOCOVERAGE) ./...)

.PHONY: all clean fiximports fmt lint lintagg vet tidy deps_update build buildcmd buildstripped test
