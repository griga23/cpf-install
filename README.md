# Confluent Platform for Flink — GKE/EKS Demo Environment

`demo.sh` automates a full Confluent Platform + Confluent Manager for Apache
Flink (CMF) demo on GKE or EKS: cluster creation, Kafka, CMF, Flink compute
pools, and a self-contained stream-processing pipeline (create tables → seed
data → continuous windowed aggregation). Every subcommand requires picking a
cloud via `--gcp` or `--aws` (see below) — the rest of the pipeline (Kafka,
CMF, Flink, the SQL demo) is identical on both.

## Prerequisites

Common to both clouds:
- `kubectl`, `helm`, `confluent` CLI, `curl`, `jq` installed

GCP (`--gcp`):
- `gcloud` authenticated: `gcloud auth login`
- Access to a GCP project (default `sales-engineering-206314`, override with `PROJECT=...`)

AWS (`--aws`):
- `eksctl` and `aws` CLI v2 installed
- Assume the correct AWS profile yourself before running `demo.sh` — e.g.
  `export AWS_PROFILE=...` — and confirm it works with `aws sts get-caller-identity`.
  `demo.sh` verifies your identity before touching AWS, but never manages
  credentials or assumes roles on your behalf.

## Quick start

```sh
# GCP
./demo.sh --gcp up          # cluster, Kafka, CMF, Flink operator, environments, catalog, compute pools
./demo.sh --gcp statement    # create tables, seed data, start the streaming aggregation job (prod)
./demo.sh --gcp status       # check pod health and list Flink environments
./demo.sh --gcp down         # stop port-forwards and delete the GKE cluster

# AWS (--user tags every AWS resource created: cflt_managed_id=<name>, and is appended to the EKS cluster name)
./demo.sh --aws --user myusername up
./demo.sh --aws --user myusername statement
./demo.sh --aws --user myusername status
./demo.sh --aws --user myusername down
```

`--gcp`/`--aws` and `--user` may appear anywhere on the command line (before
or after the subcommand). They're required for every subcommand except
`help`/`-h`/`--help`.

Run `./demo.sh help` any time for the full subcommand list.

## Configuration

All defaults (cluster settings, ports, resource names, `cp/cp.yaml` container
versions) live in `config.sh` (sourced automatically by `demo.sh`) — edit
that file directly, or override any value with an environment variable:

```sh
PROJECT=my-proj ZONE=us-central1-a CLUSTER_NAME=my-demo ./demo.sh --gcp cluster
EKS_REGION=us-east-1 EKS_CLUSTER_NAME_PREFIX=my-eks-demo ./demo.sh --aws --user myusername cluster
```

AWS-specific defaults: `EKS_REGION` (`eu-west-1`), `EKS_CLUSTER_NAME_PREFIX`
(`cpf-eks-demo`), `EKS_NUM_NODES` (`3`), `EKS_NODE_TYPE` (`m5.xlarge`). The
actual EKS cluster name is always `<EKS_CLUSTER_NAME_PREFIX>-<user>` (e.g.
`cpf-eks-demo-myusername`), so multiple people sharing the same AWS
account/region automatically get their own cluster without needing to
override anything themselves.

## Command reference

Every step is also runnable standalone — e.g. after editing
`flink/compute-pool.json`, just run `./demo.sh --gcp compute-pool` (or
`--aws --user <name>`). All subcommands require `--gcp` or `--aws` (see
Quick start); omitted below for brevity.

| Subcommand | What it does |
|---|---|
| `up` | Runs everything below, in order |
| `cluster` | Creates the GKE/EKS cluster, fetches `kubectl` credentials. On AWS, also installs the EBS CSI driver add-on + a default `gp3` `StorageClass` (GKE ships with a default StorageClass already) |
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
| `down [--yes]` | Stops port-forwards, deletes the GKE/EKS cluster. On AWS, PVCs are marked for deletion first, then their EBS volumes actually release as `eksctl` drains the nodegroup; a final check warns about any leftover volume |

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

The examples below use `--gcp`; swap in `--aws --user <name>` for EKS.

**Generate more demo data.** Trigger this any time the streaming job needs
fresh input — it inserts random rows (values 1-50, category `A`/`B`, spread
over the last 90s), then cleans up the one-shot statement it used:

```sh
./demo.sh --gcp generate-data prod            # 20 rows (default) on shared-pool - fast, ~10s
./demo.sh --gcp generate-data prod 100        # specify a row count
./demo.sh --gcp generate-data prod 20 pool    # use the DEDICATED pool instead - slower (~1-2 min
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
./demo.sh --gcp c3-forward
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
./demo.sh --gcp application prod
./demo.sh --gcp application test cpf3.json    # or point at a different resource file
```

**Resize a compute pool:** edit `flink/compute-pool.json` or
`flink/compute-pool-shared.json`, then:

```sh
./demo.sh --gcp compute-pool
```

(A `SHARED` pool can't resize while it has active statements — stop/delete
them first: `confluent --environment <env> flink statement stop <name>`.)

**Restart a dead port-forward:**

```sh
./demo.sh --gcp port-forward      # CMF, localhost:8080
./demo.sh --gcp c3-forward        # Control Center, localhost:9021
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

| Symptom | Fix |
|---|---|
| `up` hangs on a wait step | Check `kubectl -n confluent get pods`, re-run the subcommand once healthy |
| `confluent flink ...` connection error | CMF port-forward died — run `./demo.sh --gcp port-forward` (or `--aws --user <name>`) |
| DDL fails but `SELECT`/`INSERT` work | `ddlEnvironments` missing the environment — re-run `./demo.sh --gcp catalog` (or `--aws --user <name>`) |
| Pod stuck `Pending` (`Insufficient cpu`) despite low `kubectl top nodes` usage | K8s schedules on CPU *requests*, not usage — lower `cpu` in the pool/app JSON, or bump `NUM_NODES`/`MACHINE_TYPE` |
| Job crash-loops with a Kafka transaction timeout error | Add `/*+ OPTIONS('properties.transaction.timeout.ms'='300000') */` after `INSERT INTO` |
| Bounded `INSERT`/`SELECT` on `shared-pool` stuck `PENDING`/`RUNNING` forever | Operator bug — run bounded/one-shot work on the DEDICATED `pool` instead |
| `--wait` times out or is missing result data | Don't use `--wait`; poll `.status.phase` and fetch rows from `.../statements/<name>/results` |
| `SHARED` pool won't resize (`409`) | Stop active statements first: `confluent --environment <env> flink statement stop <name>` |
| Compute pool `PUT` returns 200 but pods unchanged | Wait a few seconds — the operator redeploys asynchronously |
| EKS pods stuck `Pending` with unbound PVCs (Kafka/SR/Control Center) | The EBS CSI driver/default `gp3` `StorageClass` didn't finish setting up — check `kubectl -n kube-system get pods -l app=ebs-csi-controller` and `kubectl get storageclass` (expect `gp3` marked `(default)`); re-run `./demo.sh --aws --user <name> cluster` to retry (idempotent) |
| `aws`/`eksctl` commands fail with an auth error | Your assumed AWS profile expired or isn't set — re-run `export AWS_PROFILE=...` and confirm with `aws sts get-caller-identity` before retrying |
| `confluent flink ...` fails with `Error: not logged in`, even though `confluent context list` shows a current context | That context's session token is stale/corrupted. This is unrelated to which cloud/CMF instance you're using — `confluent flink ...` commands talk to CMF purely via `CONFLUENT_CMF_URL`/`--url`, never through the login's own target. Fix: `confluent context list`, delete the broken context(s) with `confluent context delete <name>`, then `confluent login --save` (any login works — Confluent Cloud or Platform) and retry |

## Teardown

```sh
./demo.sh --gcp down                         # prompts for confirmation
./demo.sh --gcp down --yes                   # skip the prompt

./demo.sh --aws --user myusername down          # prompts for confirmation
./demo.sh --aws --user myusername down --yes    # skip the prompt
```

On AWS, `down` marks PVCs for deletion first, then deletes the cluster; their
EBS volumes actually release while `eksctl` gracefully drains the nodegroup
(pods must stop before their attached volumes can be reclaimed), and a final
check afterward warns if any volume was somehow left behind. This takes
materially longer than the GCP path — typically 10-15 minutes for the full
CloudFormation stack deletion.

Also stops any background port-forwards started by this script...
