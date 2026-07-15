# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo is

A single bash script (`demo.sh`) that stands up a full Confluent Platform for
Flink demo on GKE: GKE cluster → Kafka/Schema Registry/Control Center (CP) →
cert-manager → Flink Kubernetes Operator → Confluent Manager for Apache Flink
(CMF) → Flink environments/catalog/compute pools → a self-contained
streaming SQL pipeline. There is no application code, build system, or test
suite — this is infrastructure automation glued together with `bash`,
`kubectl`, `helm`, the `confluent` CLI, `curl`, and `jq`.

There is no lint/build/test step. "Running" this project means invoking
`demo.sh` subcommands against a real GCP project and GKE cluster — see
README.md for the full command reference, troubleshooting table, and REST/CLI
cheatsheet, which are not duplicated here.

## Repo layout

- `demo.sh` — all logic, dispatched via subcommand (`up`, `down`, `cluster`,
  `kafka`, `statement`, `generate-data`, `status`, etc.). Each `cmd_*`
  function is also runnable standalone.
- `config.sh` — every tunable default (project/zone/cluster, chart versions,
  container image versions, ports, resource names). Sourced automatically by
  `demo.sh`; every value is a `: "${VAR:=default}"` so it can be overridden
  by exporting the env var before invoking `demo.sh`. This is the only place
  version numbers should be bumped — `cp/cp.yaml` has `${..._VERSION}`
  placeholders substituted in by `render_kafka_manifest()` (sed, not
  envsubst), so never hardcode a version directly into `cp/cp.yaml`.
- `cp/cp.yaml` — CP custom resources (KRaftController, Kafka, SchemaRegistry,
  ControlCenter, Prometheus, AlertManager) applied by `cmd_kafka`.
- `flink/*.json` — CMF resource bodies (`catalogv2.json`, `databasev2.json`,
  `compute-pool.json` DEDICATED, `compute-pool-shared.json` SHARED), POSTed
  via the `confluent` CLI or, for updates, direct REST calls (the CLI has no
  `compute-pool update`).
- `sql/*.sql` — the four statements of the demo pipeline, run in strict order
  by `cmd_statement` (create `demo_events` → create `demo_aggregated` → seed
  data → continuous windowed aggregation). CMF accepts one statement per
  submission, so these can never be merged into one script.
- `cpf_basic_app.json`, `cpf3.json` — standalone `FlinkApplication` CRDs
  deployed via `cmd_application` (unrelated to the SQL pipeline).

## Architecture notes worth knowing before editing `demo.sh`

- **Two compute pools, two purposes.** `pool` (DEDICATED) is for bounded/
  one-shot jobs; `shared-pool` (SHARED) is for DDL, ad hoc queries, and the
  long-running streaming job. This split exists because of an operator bug:
  bounded statements submitted to a SHARED pool actually finish but the
  statement resource gets stuck reporting a non-terminal phase forever
  instead of `COMPLETED`. Any new one-shot/bounded logic must go on the
  DEDICATED pool (or accept unverifiable completion, as `cmd_generate_data`
  does when defaulting to `shared-pool` — see its `sleep 10` + grace-period
  branch).
- **Idempotency pattern.** Every subcommand is safe to re-run: cluster/
  namespace/helm-release creation checks-then-skips, `CREATE TABLE` uses
  `IF NOT EXISTS`, and `create_or_update_compute_pool` falls back from CLI
  `create` to a raw `PUT` against the CMF REST API when the resource already
  exists (with a warning if that also fails, e.g. because a SHARED pool has
  active statements and can't be resized until they're stopped).
- **Port-forwards are tracked via PID files** (`.demo-cmf-port-forward.pid`,
  `.demo-c3-port-forward.pid`, both gitignored) so `demo.sh` can detect a
  live forward, reuse an already-listening port started outside the script,
  or clean up a stale PID left by a dead process. `cmd_down` always stops
  these first.
- **`wait_for_statement_phase`** is the polling primitive used everywhere a
  statement's lifecycle matters (`create_events` → `COMPLETED`, streaming job
  → `RUNNING`, etc.) — it polls `.status.phase` via `confluent ... describe -o
  json`, since the CLI's own `--wait` flag is unreliable (see README
  Troubleshooting).
- **`cmd_up` is the composition root**: it just calls the other `cmd_*`
  functions in sequence and runs `cmd_verify` per `FLINK_ENVIRONMENTS`. When
  adding a new install step, add both a `cmd_<name>` function, a `case`
  branch in the dispatch table at the bottom of `demo.sh`, and (if it belongs
  in the full flow) a call from `cmd_up` — plus an entry in `cmd_help` and
  the README command-reference table.
