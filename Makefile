APP_NAME := log2sql
DIST_DIR := dist
PKG := ./cmd/log2sql
VERSION ?= dev

GO ?= go
HOST_GOOS := $(shell $(GO) env GOOS)
HOST_GOARCH := $(shell $(GO) env GOARCH)

BUILD_NAME = $(APP_NAME)-$(VERSION)-$(1)-$(2)$(3)

.PHONY: help build test clean build-all macos linux windows

help:
	@echo "Targets:"
	@echo "  make build      Build the current platform binary into $(DIST_DIR)/"
	@echo "  make build-all  Build release binaries for macOS, Linux, and Windows"
	@echo "  make macos      Build macOS binaries (amd64, arm64)"
	@echo "  make linux      Build Linux binaries (amd64, arm64)"
	@echo "  make windows    Build Windows binaries (amd64, arm64)"
	@echo "  make test       Run go test ./..."
	@echo "  make clean      Remove $(DIST_DIR)/"
	@echo ""
	@echo "Variables:"
	@echo "  VERSION=...     Release version used in file names (default: $(VERSION))"
	@echo ""
	@echo "Examples:"
	@echo "  make build-all VERSION=v0.1.0"
	@echo "  make windows VERSION=v0.1.0"

build:
	@mkdir -p $(DIST_DIR)
	$(GO) build -o $(DIST_DIR)/$(call BUILD_NAME,$(HOST_GOOS),$(HOST_GOARCH),$(if $(filter windows,$(HOST_GOOS)),.exe,)) $(PKG)

test:
	$(GO) test ./...

clean:
	rm -rf $(DIST_DIR)

build-all: macos linux windows

macos:
	@mkdir -p $(DIST_DIR)
	GOOS=darwin GOARCH=amd64 $(GO) build -o $(DIST_DIR)/$(call BUILD_NAME,darwin,amd64,) $(PKG)
	GOOS=darwin GOARCH=arm64 $(GO) build -o $(DIST_DIR)/$(call BUILD_NAME,darwin,arm64,) $(PKG)

linux:
	@mkdir -p $(DIST_DIR)
	GOOS=linux GOARCH=amd64 $(GO) build -o $(DIST_DIR)/$(call BUILD_NAME,linux,amd64,) $(PKG)
	GOOS=linux GOARCH=arm64 $(GO) build -o $(DIST_DIR)/$(call BUILD_NAME,linux,arm64,) $(PKG)

windows:
	@mkdir -p $(DIST_DIR)
	GOOS=windows GOARCH=amd64 $(GO) build -o $(DIST_DIR)/$(call BUILD_NAME,windows,amd64,.exe) $(PKG)
	GOOS=windows GOARCH=arm64 $(GO) build -o $(DIST_DIR)/$(call BUILD_NAME,windows,arm64,.exe) $(PKG)
