# Log2SQL

Log2SQL imports Graylog-style application logs into SQLite and serves a local web UI for browsing rows and running read-only SQL queries.

## What a regular user does

1. Build or download the `log2sql` executable for their platform.
2. Run `log2sql`.
3. Use the browser home screen to import a log file or open an existing DB.
4. Explore the data or run SQL in the built-in UI.

## Build

Build the current platform:

```bash
make build
```

Build macOS, Linux, and Windows binaries:

```bash
make build-all
```

The binaries are written to `dist/`.

## Basic usage

Start the web UI home screen:

```bash
./dist/log2sql
```

Import a CSV or plain-text log file directly from the CLI and immediately open the web UI:

```bash
./dist/log2sql ingest --input ./logs.csv --db ./logs.sqlite
```

During import, the CLI shows live progress with processed rows and file percentage.

Import only, without starting the browser UI:

```bash
./dist/log2sql ingest --input ./service.log --format plain --db ./logs.sqlite --no-serve
```

Open an existing database immediately:

```bash
./dist/log2sql serve --db ./logs.sqlite
```

By default the UI starts at `http://127.0.0.1:8090`.

Managed databases created from the browser upload flow are stored under:

```text
./log2sql-data/databases/
```

## Built-in help

General help:

```bash
./dist/log2sql help
```

Command-specific help:

```bash
./dist/log2sql help ingest
./dist/log2sql help serve
```

## Input formats

- CSV with a mandatory `message` column
- Plain-text logs with Graylog-style prefixes

If both the CSV row and the parsed message contain the same field such as `request_id`, the CSV value wins.

## Web UI flows

- `Import log file`: upload a CSV or plain-text log file, choose a database name, and store it under `./log2sql-data/databases/`
- `Open existing DB`: choose a known managed DB or type the path to any existing SQLite file

## SQL tables

- `logs`: parsed log rows
- `imports`: import history
- `schema_columns`: discovered dynamic columns
