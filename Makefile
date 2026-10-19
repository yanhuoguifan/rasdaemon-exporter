# Keep Go responsible for dependency resolution and compilation.
GO ?= go

.DEFAULT_GOAL := build
.PHONY: build clean test install

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

install:
	@test -x build/rasdaemon-exporter || { echo "Missing executable build/rasdaemon-exporter; run make build first" >&2; exit 1; }
	install -Dm0755 build/rasdaemon-exporter /usr/local/bin/rasdaemon-exporter
	install -Dm0644 rasdaemon-exporter.service /etc/systemd/system/rasdaemon-exporter.service
