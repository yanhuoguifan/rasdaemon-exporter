# rasdaemon-exporter

`rasdaemon-exporter` reads RAS event records from rasdaemon's SQLite database
and writes cumulative Prometheus metrics for node_exporter's textfile
collector. It does not expose an HTTP endpoint.

The current collector supports `devlink_event`, `mc_event`, and `aer_event`.
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
  `/usr/share/misc/pci.ids` for AER device names; unavailable names are `unknown`.

The exporter opens the database read-only. It does not configure rasdaemon,
enable event recording, create the database, or remove its records.
Enable SQLite recording in rasdaemon (`-r` in its service command) using your
distribution's service configuration; do not launch a second rasdaemon process.

## Build and test

```sh
make build
make test
```

The executable is `build/rasdaemon-exporter`. `make clean` removes that binary.
`make install` installs a previously built binary and the systemd unit; it does
not build, enable, or start the service:

```sh
sudo make install
```

## Run

The default output directory must already exist. Create it if needed; do not
change permissions on existing node_exporter directories:

```sh
sudo mkdir -p /var/lib/node_exporter/textfile_collector
sudo ./build/rasdaemon-exporter
```

The process collects immediately, then waits the configured interval after
each completed round. It updates the snapshot when it consumes new records; an
idle collector leaves the `.prom` and state files unchanged. Use either a
foreground process or the systemd service, not both with the same state
directory.

## Options

| Option                  | Default                                                      | Description                                                  |
| ----------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| `--db PATH`           | `/var/lib/rasdaemon/ras-mc_event.db`                       | Existing rasdaemon SQLite database; read-only.               |
| `--state-dir PATH`    | `/var/lib/rasdaemon-exporter`                              | Private collector state directory.                           |
| `--output PATH`       | `/var/lib/node_exporter/textfile_collector/rasdaemon.prom` | Prometheus textfile output. Its parent directory must exist. |
| `--interval DURATION` | `5s`                                                       | Delay after a completed collection round; must be positive.  |

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
To install and start it after building:

```sh
sudo mkdir -p /var/lib/node_exporter/textfile_collector
sudo make install
sudo systemctl daemon-reload
sudo systemctl enable --now rasdaemon-exporter.service
sudo systemctl status rasdaemon-exporter.service --no-pager
sudo journalctl -u rasdaemon-exporter.service -n 30 --no-pager
```

Stop a foreground instance before starting the service. Stop the service before
replacing its binary. Do not delete the state files to troubleshoot or reset
the collector; they preserve restart continuity.

## Roadmap

The following sources and collector metrics are planned, but are not currently
implemented. No implementation order or release date is committed.

- `mce_record`: Machine Check records.
- `arm_event`: ARM processor events.
- `extlog_event`: firmware-reported memory events.
- `disk_errors`: block I/O errors.
- `non_standard_event`: non-standard firmware error sections.
- `hip08_oem_type1_event_v2`, `hip08_oem_type2_event_v2`,
  `hip08_pcie_local_event_v2`, and `hisi_common_section_v2`: HiSilicon-specific
  records.
- Collector health and operational metrics, including polling status and
  duration, backlog, database size, and exported series count.

See [METRICS.md](METRICS.md) for the planned metric names and mappings.

## Documentation

- [METRICS.md](METRICS.md): metric names, labels, and counting semantics.
- [TESTING.md](TESTING.md): test and verification guidance.
- [SOURCE_LAYOUT.md](SOURCE_LAYOUT.md): overview of the source tree.
- [DESIGN.md](DESIGN.md): stable design principles and maintenance boundaries.

## Upstream projects

- [rasdaemon](https://github.com/mchehab/rasdaemon)
- [node_exporter textfile collector](https://github.com/prometheus/node_exporter#textfile-collector)
