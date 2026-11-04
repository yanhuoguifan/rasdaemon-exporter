# Keep Go responsible for dependency resolution and compilation.
GO ?= go
VERSION = 0.1.0
DIST_NAME = rasdaemon-exporter-$(VERSION)-linux-amd64

.DEFAULT_GOAL := build
.PHONY: build build-static dist clean test install

build:
	@test ! -L build || { echo "Refusing a symlinked build directory" >&2; exit 1; }
	@mkdir -p build
	@test ! -L build/rasdaemon-exporter || { echo "Refusing a symlinked output binary" >&2; exit 1; }
	CGO_ENABLED=1 $(GO) build -ldflags "-X main.version=$(VERSION)" -o build/rasdaemon-exporter .

build-static:
	@test ! -L build || { echo "Refusing a symlinked build directory" >&2; exit 1; }
	@mkdir -p build
	@test ! -L build/rasdaemon-exporter || { echo "Refusing a symlinked output binary" >&2; exit 1; }
	CGO_ENABLED=1 CC=musl-gcc GOOS=linux GOARCH=amd64 $(GO) build -tags 'netgo osusergo sqlite_omit_load_extension' -ldflags "-linkmode=external -extldflags=-static -X main.version=$(VERSION)" -o build/rasdaemon-exporter .

dist: build-static
	@test ! -L dist || { echo "Refusing a symlinked dist directory" >&2; exit 1; }
	@mkdir -p dist
	@test ! -L "dist/$(DIST_NAME).tar.gz" || { echo "Refusing a symlinked archive" >&2; exit 1; }
	@test ! -L "dist/SHA256SUMS-$(VERSION)" || { echo "Refusing a symlinked checksum file" >&2; exit 1; }
	tar --transform='s,^,$(DIST_NAME)/,' -czf "dist/$(DIST_NAME).tar.gz" \
		-C build rasdaemon-exporter \
		-C "$(CURDIR)" rasdaemon-exporter.service LICENSE README.md METRICS.md
	cd dist && sha256sum "$(DIST_NAME).tar.gz" > "SHA256SUMS-$(VERSION)"

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
