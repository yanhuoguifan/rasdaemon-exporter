# rasdaemon-exporter

`rasdaemon-exporter` reads RAS event records from rasdaemon's SQLite database
and writes cumulative Prometheus metrics for node_exporter's textfile
collector. It does not expose an HTTP endpoint.

The current collector supports `devlink_event`, `mc_event`, `aer_event`,
`disk_errors`, `mce_record`, `extlog_event`, `non_standard_event`, `arm_event`,
`hip08_pcie_local_event_v2`, `hisi_common_section_v2`,
`hip08_oem_type1_event_v2`, and `hip08_oem_type2_event_v2`.
Other rasdaemon event tables are not exported yet. The collector is designed against the
upstream rasdaemon v0.8.4 schema; other versions are best-effort when their
database fields match.

```text
kernel RAS events → rasdaemon → SQLite → rasdaemon-exporter → .prom file → node_exporter → Prometheus
```

## Requirements

- Linux to run the exporter.
- Go 1.23 or newer, GNU Make, CGO, and a C compiler to build it.
- rasdaemon writing an existing SQLite database with at least one supported table.
- node_exporter with its textfile collector enabled.
- Optional local `pci.ids` at `/usr/share/hwdata/pci.ids` or
  `/usr/share/misc/pci.ids` for AER and HiSilicon common PCI device names;
  unavailable names are `unknown`.

The exporter opens the database read-only. It does not configure rasdaemon,
enable event recording, create the database, or remove its records.
Enable SQLite recording in rasdaemon (`-r` in its service command) using your
distribution's service configuration; do not launch a second rasdaemon process.

## Build and test

```sh
make build
make test
./build/rasdaemon-exporter --version
```

Use `make build-static` instead of `make build` for a static Linux amd64 build
(requires `musl-gcc`). Both targets write `build/rasdaemon-exporter`.
`make clean` removes that binary.
The release version is defined in the Makefile; a direct `go build` uses `dev`.
`make dist` runs the static build and creates
`dist/rasdaemon-exporter-<VERSION>-linux-amd64.tar.gz` and
`dist/SHA256SUMS-<VERSION>`.
`make install` installs a previously built binary and the systemd unit; it does
not build, enable, or start the service:

```sh
sudo make install
```

## Install from a release

Use the archive and matching checksum from `make dist` or the same release.
When upgrading, stop the service first and retain the database, state, and textfiles.

```sh
(
  set -eu
  release_version=0.1.0 # Set to the package version.
  sha256sum -c "SHA256SUMS-${release_version}"
  tar -xzf "rasdaemon-exporter-${release_version}-linux-amd64.tar.gz"
  cd "rasdaemon-exporter-${release_version}-linux-amd64"
  sudo install -Dm0755 rasdaemon-exporter /usr/local/bin/rasdaemon-exporter
  sudo install -Dm0644 rasdaemon-exporter.service /etc/systemd/system/rasdaemon-exporter.service
)
```

Then follow [systemd service](#systemd-service) to start the exporter.

## Run

The default output directory must already exist. Create it if needed; do not
change permissions on existing node_exporter directories:

```sh
sudo mkdir -p /var/lib/node_exporter/textfile_collector
sudo ./build/rasdaemon-exporter
```

The process collects immediately, then waits the configured interval after
each completed round. It updates the snapshot when it consumes new records; an
idle collector leaves the event snapshot and state unchanged while refreshing
the separate health file. Use either a
foreground process or the systemd service, not both with the same state
directory.

## Options

| Option                  | Default                                                      | Description                                                  |
| ----------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| `--db PATH`           | `/var/lib/rasdaemon/ras-mc_event.db`                       | Existing rasdaemon SQLite database; read-only.               |
| `--state-dir PATH`    | `/var/lib/rasdaemon-exporter`                              | Private collector state directory.                           |
| `--output PATH`       | `/var/lib/node_exporter/textfile_collector/rasdaemon.prom` | Prometheus textfile output. Its parent directory must exist. |
| `--interval DURATION` | `5s`                                                       | Delay after a completed collection round; must be positive.  |
| `--version`           | —                                                           | Print the version and exit.                                  |

Health metrics default to
`/var/lib/node_exporter/textfile_collector/rasdaemon.health.prom`.
The path is derived from `--output`: a trailing `.prom` is replaced with
`.health.prom`; otherwise `.health.prom` is appended. No extra option is needed.

Run `rasdaemon-exporter --help` for the command-line summary. For custom paths
or intervals, pass the corresponding options to the executable and update the
systemd unit's `ExecStart` if using the service.

## Use with node_exporter and Prometheus

Configure the existing node_exporter service to include this flag, keeping its
other arguments:

```text
--collector.textfile.directory=/var/lib/node_exporter/textfile_collector
```

Prometheus should scrape node_exporter as usual. Example checks (adapt labels
to your scrape configuration):

```promql
rasdaemon_devlink_health_events_total
node_textfile_scrape_error
```

`node_textfile_scrape_error` should be `0`. An event counter represents
records rasdaemon stored; it does not by itself mean the reported condition is
still present. Because idle rounds do not rewrite files, the `.prom` file's
modification time is not a collector heartbeat.

## systemd service

The supplied service runs as root to read rasdaemon's private database directory.
After installing from source or a release:

```sh
sudo mkdir -p /var/lib/node_exporter/textfile_collector
sudo systemctl daemon-reload
sudo systemctl enable --now rasdaemon-exporter.service
sudo systemctl status rasdaemon-exporter.service --no-pager
sudo journalctl -u rasdaemon-exporter.service -n 30 --no-pager
```

Stop a foreground instance before starting the service. Stop the service before
replacing its binary. Do not delete the state files to troubleshoot or reset
the collector; they preserve restart continuity.

## Documentation

- [METRICS.md](METRICS.md): metric names, labels, and counting semantics.
- [TESTING.md](TESTING.md): test and verification guidance.
- [SOURCE_LAYOUT.md](SOURCE_LAYOUT.md): overview of the source tree.
- [DESIGN.md](DESIGN.md): stable design principles and maintenance boundaries.

## Upstream projects

- [rasdaemon](https://github.com/mchehab/rasdaemon)
- [node_exporter textfile collector](https://github.com/prometheus/node_exporter#textfile-collector)
