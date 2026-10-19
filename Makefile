# Keep Go responsible for dependency resolution and compilation.
GO ?= go

.DEFAULT_GOAL := build
.PHONY: build clean test

build:
	@test ! -L build || { echo "Refusing a symlinked build directory" >&2; exit 1; }
	@mkdir -p build
	@test ! -L build/rasdaemon-exporter || { echo "Refusing a symlinked output binary" >&2; exit 1; }
	CGO_ENABLED=1 $(GO) build -o build/rasdaemon-exporter .

# Remove only the owned binary, not arbitrary files or Go's shared caches.
clean:
	@test ! -L build || { echo "Refusing a symlinked build directory" >&2; exit 1; }
	rm -f -- build/rasdaemon-exporter

test:
	CGO_ENABLED=1 $(GO) test ./...
