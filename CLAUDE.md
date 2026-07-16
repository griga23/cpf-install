# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo is

A bash-driven automation of a Confluent Platform + Confluent Manager for Apache Flink (CMF) demo
environment on GKE or EKS. There is no application code to build/compile/test — everything is
orchestrated by `demo.sh`, configured via `config.sh`, and expressed as Kubernetes/CMF resource
manifests (YAML/JSON) and Flink SQL files. Changes here are almost always edits to shell logic,
manifest JSON/YAML, or SQL.

## Commands

There is no build, lint, or test suite. Validate changes by running the relevant subcommand against a
live GKE/EKS cluster (or reading through the bash logic carefully, since there's no CI/dry-run mode).
Every subcommand requires a cloud flag, `--gcp` or `--aws --user <cflt-username>` (exempt only for
`help`/`-h`/`--help`); flags may appear anywhere in argv, before or after the subcommand.

```sh
./demo.sh --gcp up                                     # full install: cluster -> kafka -> cmf -> flink operator -> pools
./demo.sh --gcp statement [env]                        # run the 4-step demo pipeline (default env: prod)
./demo.sh --gcp generate-data [env] [count] [pool]     # feed more rows into the running pipeline
./demo.sh --gcp verify [env]                            # sanity-check DDL/SELECT work in an environment
./demo.sh --gcp status                                  # pod health + Flink environments + port-forward status
./demo.sh --gcp down [--yes]                            # stop port-forwards, delete the cluster
./demo.sh help                                          # full subcommand list (no cloud flag needed)

# Same subcommands work against EKS by swapping the cloud flag:
./demo.sh --aws --user <cflt-username> up
```

Every step in `up` is independently re-runnable (e.g. `./demo.sh --gcp compute-pool` after editing
`flink/compute-pool.json`). Config defaults live in `config.sh` and can be overridden per-invocation
via environment variables, e.g. `PROJECT=my-proj ZONE=us-central1-a ./demo.sh --gcp cluster`.

When editing `demo.sh`, keep `bash -n demo.sh` (syntax check) and `shellcheck demo.sh` (if available)
clean — the script runs under `set -euo pipefail`. Note the argument-parsing loop at the bottom of
`demo.sh` (before the dispatch `case`) explicitly length-checks arrays before expanding them (`bash
3.2`, macOS's default `/bin/bash`, throws `unbound variable` expanding an empty array under `set -u`).

## Architecture

**`demo.sh`** is a single dispatcher script: `cmd_<subcommand>` functions implement each step, and a
`case` statement at the bottom maps CLI subcommands to them. `cmd_up` just calls the other `cmd_*`
functions in order. Shared logic (port-forward lifecycle, statement polling, pipeline execution) is
factored into helper functions above the `cmd_*` definitions — read those before adding a new subcommand
that needs to wait on a Flink statement or manage a port-forward.

**`config.sh`** is sourced by `demo.sh` and defines every tunable (cluster name/zone, image versions,
resource names, file paths) using `: "${VAR:=default}"`, so any value can be overridden by an env var
without touching the file. GCP vars (`PROJECT`, `ZONE`, `CLUSTER_NAME`, `NUM_NODES`, `MACHINE_TYPE`)
and AWS vars (`EKS_REGION`, `EKS_CLUSTER_NAME_PREFIX`, `EKS_NUM_NODES`, `EKS_NODE_TYPE`) coexist unconditionally
in the same file — they never collide, so `config.sh` doesn't need to know which cloud is active.

**Cloud selection is the only cloud-specific layer.** `cmd_cluster`/`cmd_down` are thin dispatchers
that branch on the required `--gcp`/`--aws` flag (parsed near the bottom of `demo.sh`, into `$CLOUD`/
`$CFLT_USER`) to `cmd_cluster_gcp`/`cmd_cluster_aws` and `cmd_down_gcp`/`cmd_down_aws`. Every other
`cmd_*` subcommand is cloud-agnostic — it operates purely via `kubectl`/`helm`/`confluent` against
whatever cluster the active kubeconfig context points at, so it needs no cloud branching at all.
`cmd_cluster_aws` uses `eksctl` (VPC/IAM/managed node group in one command) and, since EKS has no
default `StorageClass` out of the box (unlike GKE), also calls `setup_ebs_csi_driver` to install the
EBS CSI driver add-on (IRSA-backed IAM role + `eksctl create addon`) and apply a default `gp3`
`StorageClass` — this is what lets the Kafka/SR/Control Center PVCs (`dataVolumeCapacity` in
`cp/cp.yaml`) bind on EKS. Don't try to "fix" this by editing `cmd_kafka` or reintroducing it as a
manual step — it's owned by `cmd_cluster_aws`. `cmd_down_aws` marks PVCs across the `confluent`/
`prod`/`test` namespaces for deletion (`kubectl delete pvc --all --wait=false`) *before* calling
`eksctl delete cluster` — non-blocking, deliberately: the `pvc-protection` finalizer can't clear
until the pod using each PVC stops, and nothing stops those pods until `eksctl` gracefully drains
the nodegroup as part of cluster deletion, so waiting synchronously at that earlier point would just
block for no reason. The actual EBS volume release happens *during* that drain (confirmed
empirically: deleting the cluster right after marking PVCs left zero orphaned volumes). `cmd_down_aws`
still does a final `aws ec2 describe-volumes` check after `eksctl delete cluster` completes and warns
if anything tagged for this user is somehow left over, since PVC-provisioned EBS volumes live outside
eksctl's CloudFormation stack and wouldn't otherwise be caught by its own cleanup. Every AWS resource
the script creates is tagged `cflt_managed_by=user` / `cflt_managed_id=<cflt-username>` (via `eksctl
--tags`, `eksctl create iamserviceaccount --tags`, and the EBS CSI addon's `extraVolumeTags` for
PVC-backed volumes — confirmed by inspecting `aws ec2 describe-volumes` after a live `kafka` install).

**Two-layer resource model**: Confluent Platform (Kafka, Schema Registry, Control Center) is deployed as
Kubernetes CRs via `cp/cp.yaml` (applied with `kubectl`), while Flink resources (environments, catalog,
compute pools, statements, applications) are managed through CMF, reached via the `confluent` CLI or its
REST API at `localhost:8080` (port-forwarded from `cmf-service`). `cmd_kafka` renders `cp/cp.yaml` through
`sed` to substitute `${..._VERSION}` placeholders with values from `config.sh` before applying it — there
is no `envsubst` dependency.

**Two Flink compute pools, deliberately split by workload type** (see README "The demo pipeline" and
"Troubleshooting" for the operator bug behind this):
- `pool` (DEDICATED, `flink/compute-pool.json`) — one-shot bounded jobs (seed inserts, ad hoc bounded
  `SELECT`s). Slower (cold-starts its own cluster) but reliably reports `COMPLETED`.
- `shared-pool` (SHARED, `flink/compute-pool-shared.json`) — DDL, ad hoc queries, and the continuous
  streaming job. Bounded jobs submitted here get stuck reporting `RECONCILING`/`PENDING` forever instead
  of `COMPLETED` (an operator quirk), so bounded work is never run there — `cmd_generate_data` special-
  cases this with a sleep-and-move-on grace period instead of polling.

**The demo pipeline** (`cmd_statement`, sql/*.sql) runs four separate statements against CMF, in order,
because CMF only accepts one SQL statement per submission:
1. `sql/create_demo_events.sql` (shared-pool) — source table, watermarked on `event_time`.
2. `sql/create_demo_aggregated.sql` (shared-pool) — sink table for windowed results.
3. `sql/insert_demo_data.sql` (pool) — 10 seed rows timestamped relative to `CURRENT_TIMESTAMP`.
4. `sql/streaming_aggregation.sql` (shared-pool) — continuous 30s tumbling-window aggregation, left
   `RUNNING` as statement `flink-statement`.

All steps are idempotent: `CREATE TABLE` uses `IF NOT EXISTS`, the seed insert just adds another batch,
and step 4 is skipped with a warning if `flink-statement` already exists. `run_pipeline_statement` and
`wait_for_statement_phase` in `demo.sh` implement the "submit, poll `.status.phase`, treat `FAILED`/
timeout as failure" pattern used throughout — reuse them rather than hand-rolling polling logic.

**Catalog/database permissions**: `./demo.sh --gcp catalog` applies `flink/databasev2.json`, which sets
`spec.ddlEnvironments: ["prod", "test"]` — this is what allows `CREATE TABLE`/`DROP TABLE` to work in
those environments. If DDL fails while `SELECT`/`INSERT` work, this is out of sync; re-run `catalog`
(it creates the database if missing, or updates it in place if it already exists, since there's
no CLI `catalog database update` shortcut for permissions alone).

**FlinkApplication manifests** (`cpf_basic_app.json`, `cpf3.json`) are standalone `FlinkApplication` CRDs
deployable via `./demo.sh --gcp application [env] [file]`, separate from the SQL-statement pipeline above —
useful for testing raw JAR-based jobs (`StateMachineExample.jar`) rather than SQL statements.

## Known operator quirks (don't try to "fix" these in demo.sh)

- Bounded `INSERT`/`SELECT` statements on the SHARED pool never report a terminal phase — this is why
  one-shot work is routed to the DEDICATED pool, and why `cmd_generate_data` uses a grace-period sleep
  instead of polling when targeting the SHARED pool.
- `confluent flink catalog list`/`describe` always show a blank "Databases" column (CLI display bug) —
  verify database state via the REST API instead.
- Compute pool `PUT` (used by `create_or_update_compute_pool` as a fallback since the CLI has no
  `compute-pool update`) can return 200 while the operator redeploys asynchronously — pods may lag behind.
- A `SHARED` pool refuses to resize (`409`) while it has active statements; stop/delete them first.

See the README's Troubleshooting table for the full list and fixes.
