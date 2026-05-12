APP_NAME := log2sql
DIST_DIR := dist
PKG := ./cmd/log2sql

GO ?= go

.PHONY: help build test clean build-all macos linux windows

help:
	@echo "Targets:"
	@echo "  make build      Build the current platform binary into $(DIST_DIR)/"
	@echo "  make build-all  Build binaries for macOS, Linux, and Windows"
	@echo "  make macos      Build macOS binaries (amd64, arm64)"
	@echo "  make linux      Build Linux binaries (amd64, arm64)"
	@echo "  make windows    Build Windows binaries (amd64, arm64)"
	@echo "  make test       Run go test ./..."
	@echo "  make clean      Remove $(DIST_DIR)/"

build:
	@mkdir -p $(DIST_DIR)
	$(GO) build -o $(DIST_DIR)/$(APP_NAME) $(PKG)

test:
	$(GO) test ./...

clean:
	rm -rf $(DIST_DIR)

build-all: macos linux windows

macos:
	@mkdir -p $(DIST_DIR)
	GOOS=darwin GOARCH=amd64 $(GO) build -o $(DIST_DIR)/$(APP_NAME)_darwin_amd64 $(PKG)
	GOOS=darwin GOARCH=arm64 $(GO) build -o $(DIST_DIR)/$(APP_NAME)_darwin_arm64 $(PKG)

linux:
	@mkdir -p $(DIST_DIR)
	GOOS=linux GOARCH=amd64 $(GO) build -o $(DIST_DIR)/$(APP_NAME)_linux_amd64 $(PKG)
	GOOS=linux GOARCH=arm64 $(GO) build -o $(DIST_DIR)/$(APP_NAME)_linux_arm64 $(PKG)

windows:
	@mkdir -p $(DIST_DIR)
	GOOS=windows GOARCH=amd64 $(GO) build -o $(DIST_DIR)/$(APP_NAME)_windows_amd64.exe $(PKG)
	GOOS=windows GOARCH=arm64 $(GO) build -o $(DIST_DIR)/$(APP_NAME)_windows_arm64.exe $(PKG)
