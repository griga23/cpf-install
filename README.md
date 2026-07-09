# Confluent Platform for Flink — GKE Demo Environment

`demo.sh` automates a full Confluent Platform + Confluent Manager for Apache
Flink (CMF) demo on GKE: cluster creation, Kafka, CMF, Flink compute pools,
and a self-contained stream-processing pipeline (create tables → seed data →
continuous windowed aggregation).

## Prerequisites

- `gcloud` authenticated: `gcloud auth login`
- `kubectl`, `helm`, `confluent` CLI, `curl`, `jq` installed
- Access to a GCP project (default `sales-engineering-206314`, override with `PROJECT=...`)

## Quick start

```sh
./demo.sh up          # cluster, Kafka, CMF, Flink operator, environments, catalog, compute pools
./demo.sh statement    # create tables, seed data, start the streaming aggregation job (prod)
./demo.sh status       # check pod health and list Flink environments
./demo.sh down         # stop port-forwards and delete the GKE cluster
```

Run `./demo.sh help` any time for the full subcommand list.

## Configuration

All defaults (cluster settings, ports, resource names, `cp/cp.yaml` container
versions) live in `config.sh` (sourced automatically by `demo.sh`) — edit
that file directly, or override any value with an environment variable:

```sh
PROJECT=my-proj ZONE=us-central1-a CLUSTER_NAME=my-demo ./demo.sh cluster
```

## Command reference

Every step is also runnable standalone — e.g. after editing
`flink/compute-pool.json`, just run `./demo.sh compute-pool`.

| Subcommand | What it does |
|---|---|
| `up` | Runs everything below, in order |
| `cluster` | Creates the GKE cluster, fetches `kubectl` credentials |
| `helm-repo` | Adds/updates the `confluentinc` helm repo |
| `namespace` | Creates the `confluent` namespace, sets it as default context |
| `operator` | Installs the `confluent-for-kubernetes` operator |
| `kafka` | Applies `cp/cp.yaml` (Kafka, Schema Registry, Control Center) |
| `cert-manager` | Installs cert-manager (required by the Flink Kubernetes Operator) |
| `flink-operator` | Creates `prod`/`test` namespaces, installs the Flink Kubernetes Operator |
| `cmf` | Installs Confluent Manager for Apache Flink |
| `port-forward` | Background port-forward `cmf-service` → `localhost:8080` |
| `stop-port-forward` | Stops all background port-forwards started by this script |
| `flink-environments` | Creates the `prod`/`test` CMF environments |
| `catalog` | Creates/updates the `kafka-cat` catalog and `kafka-db` database, with DDL permissions |
| `compute-pool` | Creates/updates the `pool` (DEDICATED) and `shared-pool` (SHARED) compute pools |
| `verify [env]` | Sanity-checks the environment via a disposable table (default `prod`) |
| `statement [env]` | Runs the full stream-processing demo pipeline (default `prod`) |
| `generate-data [env] [count] [pool]` | Inserts more random rows into `demo_events` on demand (default `prod`, `20` rows, `shared-pool`) |
| `application [env] [file]` | Deploys a raw `FlinkApplication` (default `prod`, `cpf_basic_app.json`) |
| `c3-forward` | Background port-forward for Control Center → `localhost:9021` |
| `status` | Shows pod health, Flink environments, and port-forward status |
| `down [--yes]` | Stops port-forwards, deletes the GKE cluster |

## The demo pipeline

Two compute pools, used for different purposes:

| Pool | Type | Used for |
|---|---|---|
| `pool` | DEDICATED | One-shot bounded jobs (seed data insert, ad hoc bounded `SELECT`s) |
| `shared-pool` | SHARED | DDL, ad hoc queries, and the continuous streaming job |

(Bounded jobs never report as finished on `shared-pool` — see
[Troubleshooting](#troubleshooting) — so anything bounded runs on `pool`
instead.)

`./demo.sh statement [env]` runs four SQL statements in sequence against a
self-contained schema (CMF only accepts one statement per submission, so
these can't be combined into a single script):

| # | SQL file | Pool | Result |
|---|---|---|---|
| 1 | `sql/create_demo_events.sql` | `shared-pool` | `demo_events` table, watermarked on `event_time` |
| 2 | `sql/create_demo_aggregated.sql` | `shared-pool` | `demo_aggregated` table for windowed results |
| 3 | `sql/insert_demo_data.sql` | `pool` | 10 seed rows, timestamped relative to `CURRENT_TIMESTAMP` (always "fresh") |
| 4 | `sql/streaming_aggregation.sql` | `shared-pool` | Continuous 30s tumbling-window aggregation, left `RUNNING` as `flink-statement` |

Safe to re-run: `CREATE TABLE` uses `IF NOT EXISTS`, the seed insert just adds
another batch of rows, and step 4 is skipped with a warning if
`flink-statement` already exists.

`./demo.sh catalog` sets `spec.ddlEnvironments: ["prod", "test"]` on
`kafka-db` (`flink/databasev2.json`), which is what lets `CREATE TABLE`/
`DROP TABLE` work in both environments from the start.

Once `flink-statement` is running, feed it more data any time with
`./demo.sh generate-data [env] [count]` — see [Common tasks](#common-tasks).

## Common tasks

**Generate more demo data.** Trigger this any time the streaming job needs
fresh input — it inserts random rows (values 1-50, category `A`/`B`, spread
over the last 90s), then cleans up the one-shot statement it used:

```sh
./demo.sh generate-data prod            # 20 rows (default) on shared-pool - fast, ~10s
./demo.sh generate-data prod 100        # specify a row count
./demo.sh generate-data prod 20 pool    # use the DEDICATED pool instead - slower (~1-2 min
                                         # cold start), but confirms COMPLETED; shared-pool
                                         # can't (see Troubleshooting), so this just fires
                                         # the insert and moves on after a short grace period
```

**Check the aggregation results.** A plain `SELECT * FROM demo_aggregated`
is unbounded and will hang, since it's a live Kafka topic — bound it:

```sh
export CONFLUENT_CMF_URL=http://localhost:8080

confluent --environment prod flink statement create check-agg \
  --catalog kafka-cat --database kafka-db --compute-pool pool \
  --sql "SELECT * FROM demo_aggregated /*+ OPTIONS('scan.bounded.mode'='latest-offset') */;"

# poll until COMPLETED (can take 1-2 minutes - DEDICATED pool cold start):
confluent --environment prod flink statement describe check-agg -o json | jq '.status.phase'

# then fetch the rows:
curl -s "$CONFLUENT_CMF_URL/cmf/api/v1/environments/prod/statements/check-agg/results" | jq

confluent --environment prod flink statement delete check-agg --force
```

**Open Control Center:**

```sh
./demo.sh c3-forward
open http://localhost:9021/home
```

**Open a Flink SQL shell:**

```sh
confluent flink shell --environment prod --compute-pool shared-pool
```

**Manage the running streaming statement:**

```sh
confluent --environment prod flink statement list
confluent --environment prod flink statement describe flink-statement
confluent --environment prod flink statement web-ui-forward flink-statement
confluent --environment prod flink statement stop flink-statement
confluent --environment prod flink statement delete flink-statement --force
```

**Deploy the sample FlinkApplication** (`StateMachineExample.jar`):

```sh
./demo.sh application prod
./demo.sh application test cpf3.json    # or point at a different resource file
```

**Resize a compute pool:** edit `flink/compute-pool.json` or
`flink/compute-pool-shared.json`, then:

```sh
./demo.sh compute-pool
```

(A `SHARED` pool can't resize while it has active statements — stop/delete
them first: `confluent --environment <env> flink statement stop <name>`.)

**Restart a dead port-forward:**

```sh
./demo.sh port-forward      # CMF, localhost:8080
./demo.sh c3-forward        # Control Center, localhost:9021
```

**Watch pods come up during install:**

```sh
watch kubectl get pods -n confluent
```

## CLI cheatsheet

Full reference: https://docs.confluent.io/cp-flink/current/clients-api/cli.html

```sh
# -o json / -o yaml work on every list/describe command
confluent flink environment list -o json | jq
confluent flink compute-pool describe --environment prod pool -o json

# FlinkApplication CRDs
confluent flink application list --environment prod
confluent flink application describe <name> --environment prod -o json
confluent flink application instance list --application <name> --environment prod
confluent flink application event list --application <name> --environment prod

# Statement lifecycle
confluent flink statement rescale <name> --environment prod --parallelism 2
confluent flink statement resume <name> --environment prod
confluent flink statement exception list <name> --environment prod

# Savepoints, secrets, system info
confluent flink savepoint list --application <name> --environment prod
confluent flink savepoint create --application <name> --environment prod
confluent flink secret list
confluent flink secret-mapping list --environment prod
confluent flink system-info

# Catalog database management
confluent flink catalog database create flink/databasev2.json --catalog kafka-cat
confluent flink catalog database update flink/databasev2.json --catalog kafka-cat
confluent flink catalog database describe kafka-db --catalog kafka-cat -o json
```

> `confluent flink catalog list`/`describe` always show a blank "Databases"
> column — that's a CLI display quirk. Check via REST instead (below).

## REST API

`demo.sh` exports `CONFLUENT_CMF_URL=http://localhost:8080` for any
subcommand that needs it; the same calls work directly:

```sh
curl -s $CONFLUENT_CMF_URL/cmf/api/v1/environments | jq

curl -s -H "Content-Type: application/json" -X POST \
  -d @flink/catalogv2.json \
  $CONFLUENT_CMF_URL/cmf/api/v1/catalogs/kafka

curl -s -X DELETE $CONFLUENT_CMF_URL/cmf/api/v1/catalogs/kafka/kafka-cat/databases/kafka-db

curl -s -H "Content-Type: application/json" -X POST \
  -d @flink/compute-pool.json \
  $CONFLUENT_CMF_URL/cmf/api/v1/environments/test/compute-pools

curl -s -X PUT -H "Content-Type: application/json" \
  -d @flink/compute-pool.json \
  $CONFLUENT_CMF_URL/cmf/api/v1/environments/prod/compute-pools/pool
```

In-cluster (no port-forward needed):
`http://confluent-manager-for-apache-flink.confluent.svc.cluster.local:8080`

**mTLS** (optional, not automated by `demo.sh` — generate the certs first):

```sh
confluent flink environment list \
  --url https://confluent-manager-for-apache-flink.confluent.svc.cluster.local:8080 \
  --client-cert-path generated/server.pem \
  --client-key-path generated/server-key.pem \
  --certificate-authority-path generated/ca.pem
```

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `up` hangs on a wait step | Best-effort readiness wait timed out (10 min Kafka, 3 min others) | `kubectl -n confluent get pods`, re-run the specific subcommand once healthy |
| `confluent flink ...` fails with a connection error | CMF port-forward died | `./demo.sh port-forward` |
| `CREATE TABLE`/DDL fails, `SELECT`/`INSERT` work fine | `ddlEnvironments` missing the environment | `./demo.sh catalog` re-applies it; check with `curl -s $CONFLUENT_CMF_URL/cmf/api/v1/catalogs/kafka/kafka-cat/databases/kafka-db \| jq .spec.ddlEnvironments` |
| Pod stuck `Pending` (`Insufficient cpu`) but `kubectl top nodes` shows low usage | Kubernetes schedules on CPU *requests*, not actual usage | Lower `cpu` in `flink/compute-pool*.json` / the `FlinkApplication` JSON (already `0.5`), or bump `NUM_NODES`/`MACHINE_TYPE`. Trade-off: lower `cpu` also means slower pod cold-start/execution (a DEDICATED pool job can take 1-2 minutes instead of ~60s) |
| Job crash-loops: `transaction timeout is larger than the maximum value allowed by the broker` | Kafka sink's default transaction timeout exceeds the broker max | Add `/*+ OPTIONS('properties.transaction.timeout.ms'='300000') */` after `INSERT INTO` (already in `sql/insert_demo_data.sql`, `sql/streaming_aggregation.sql`) |
| Bounded `INSERT`/`SELECT` on `shared-pool` stuck `PENDING`/`RUNNING` forever | Operator bug — the `FlinkSessionJob` gets stuck `RECONCILING` (`UpgradeFailureException: Latest checkpoint not externally addressable`) even after the job finishes | Run bounded/one-shot work on the DEDICATED `pool` instead |
| `describe -o json` / `--wait` missing `.result.results.data`, or `--wait` errors `retry failed due to timeout of 1m0s` | CLI `--wait` has a hard ~60s timeout; inline results are unreliable for real `SELECT`/`INSERT` | Poll `.status.phase` manually; fetch rows from `.../statements/<name>/results` |
| `demo.sh` exits silently mid-pipeline, no error printed | `set -euo pipefail` + `var=$(cmd \| jq ...)`: if `cmd` fails, the pipeline's exit status is non-zero (due to `pipefail`), so the assignment itself trips `set -e` and exits *before* any `\|\| die`/`warn` handling runs, even ones written to catch exactly this. Every such assignment in `demo.sh` (including inside `wait_for_statement_phase`'s polling loop) ends in `\|\| true` so the intended check always runs — apply the same pattern to any new one you add | N/A — already fixed everywhere in this script |
| `SHARED` pool won't resize (`409`) | Active statements in a non-updatable phase | `confluent --environment <env> flink statement stop <name>`, then retry |
| Compute pool `PUT` returns 200 but pods still show old resources | Operator redeploys asynchronously | Wait a few seconds; `kubectl -n <env> get pods -w` |

## Teardown

```sh
./demo.sh down          # prompts for confirmation
./demo.sh down --yes    # skip the prompt
```

Also stops any background port-forwards started by this script.
