# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo is

A single bash script (`demo.sh`) that stands up a full Confluent Platform for
Flink demo on **GKE or EKS**: GKE/EKS cluster → Kafka/Schema Registry/Control
Center (CP) → cert-manager → Flink Kubernetes Operator → Confluent Manager
for Apache Flink (CMF) → Flink environments/catalog/compute pools → a
self-contained streaming SQL pipeline. There is no application code, build
system, or test suite — this is infrastructure automation glued together
with `bash`, `kubectl`, `helm`, the `confluent` CLI, `gcloud`/`eksctl`/`aws`,
`curl`, and `jq`.

There is no lint/build/test step. "Running" this project means invoking
`demo.sh` subcommands against a real GCP or AWS account and a real
GKE/EKS cluster — see README.md for the full command reference,
troubleshooting table, and REST/CLI cheatsheet, which are not duplicated
here.

Every subcommand (except `help`) requires exactly one of `--gcp`/`--aws` and
a `--user <name>` — both may appear anywhere on the command line. `--user`
becomes part of the cluster name (`<prefix>-<user>`, e.g.
`cpf-gke-demo-jsvoboda`) and tags/labels every cloud resource, so multiple
people sharing a project/account each get their own isolated cluster without
any other config change.

## Repo layout

- `demo.sh` — all logic, dispatched via subcommand (`up`, `down`, `cluster`,
  `kafka`, `demo-pipeline`, `generate-data`, `status`, etc. — run
  `./demo.sh help` for the full list). Each `cmd_*` function is also runnable
  standalone. Cloud-specific behavior lives in `cmd_<name>_gcp`/`cmd_<name>_aws`
  pairs behind a `case "$CLOUD" in` in the cloud-agnostic `cmd_<name>` (see
  `cmd_cluster`, `cmd_artifacts`, `cmd_check_quotas`, `cmd_down`) — everything
  downstream of cluster creation (Kafka, CMF, Flink, the SQL demo) is
  identical on both clouds.
- `config.sh` — every tunable default (GCP project/zone/cluster, AWS
  region/cluster, chart versions, container image versions, ports, resource
  names). Sourced automatically by `demo.sh`; every value is a
  `: "${VAR:=default}"` so it can be overridden by exporting the env var
  before invoking `demo.sh`. This is the only place version numbers should
  be bumped — `cp/cp.yaml` has `${..._VERSION}` placeholders substituted in
  by `render_kafka_manifest()` (sed, not envsubst), so never hardcode a
  version directly into `cp/cp.yaml`.
  Watch the two similarly-named vars: `FLINK_NAMESPACES` (comma-separated,
  the k8s namespaces the Flink operator watches) is distinct from
  `FLINK_ENVIRONMENTS` (space-separated, the CMF environments `cmd_up`
  creates and iterates `cmd_verify` over) — different delimiters, different
  consumers.
- `cp/cp.yaml` — CP custom resources (KRaftController, Kafka, SchemaRegistry,
  ControlCenter, Prometheus, AlertManager) applied by `cmd_kafka`.
- `flink/*.json` — CMF resource bodies (`catalogv2.json`, `databasev2.json`,
  `compute-pool.json` DEDICATED, `compute-pool-shared.json` SHARED), POSTed
  via the `confluent` CLI or, for updates, direct REST calls (the CLI has no
  `compute-pool update`).
- `sql/*.sql` — the four statements of the demo pipeline, run in strict order
  by `cmd_demo_pipeline` (create `demo_events` → create `demo_aggregated` →
  seed data → continuous windowed aggregation, named `streaming-aggregation`).
  CMF accepts one statement per submission, so these can never be merged into
  one script.
- `app/*.json` — standalone `FlinkApplication` CRDs deployed via
  `cmd_application` (unrelated to the SQL pipeline): `cpf_basic_app.json`
  (default) runs as-is; `sample-app-gcp.json`/`sample-app-aws.json` are
  reference templates for a `cmf://`-artifact-based app on each cloud
  (placeholder `jarURI`/`entryClass` — fill in before use), matching exactly
  what `render_flink_storage()` generates for that cloud.

## Architecture notes worth knowing before editing `demo.sh`

- **Cloud dispatch pattern.** `CLOUD` (`gcp`/`aws`) is parsed once at the
  bottom of `demo.sh` from `--gcp`/`--aws`, along with `CFLT_USER` from
  `--user`, before the subcommand dispatch `case` runs. Cloud-agnostic
  wrapper functions (`cmd_cluster`, `cmd_artifacts`, `cmd_check_quotas`,
  `cmd_down`) just `case "$CLOUD" in gcp) ..._gcp ;; aws) ..._aws ;; esac`.
  Any new cloud-specific step should follow this same split rather than
  branching on `$CLOUD` inline throughout a function.
- **AWS quota preflighting runs first in `up`.** `cmd_check_quotas` →
  `cmd_check_quotas_aws` checks VPCs/internet-gateways/Elastic-IPs per region
  and NAT-gateways-per-AZ *before* `cmd_cluster` touches anything, since a
  shared account can be maxed out on any of these (this is a real failure
  mode, not hypothetical — a fresh `eksctl create cluster` can fail deep into
  CloudFormation with `The maximum number of VPCs has been reached`). Each
  sub-check warns and increments a `quota_exceeded` counter rather than
  aborting immediately; only at the end, if anything was exceeded, does it
  prompt to proceed (default no) — because a re-run after a partial failure
  will always look "exceeded" against resources the *first* run already
  created, and that's fine. GCP's equivalent (`cmd_check_quotas_gcp`) only
  checks that gcloud credentials are valid, then is an otherwise unimplemented stub — see its TODO comment for the likely quota codes
  before adding real checks.
- **Two compute pools, two purposes.** `pool` (DEDICATED) is for bounded/
  one-shot jobs; `shared-pool` (SHARED) is for DDL, ad hoc queries, and the
  long-running streaming job. This split exists because of an operator bug:
  bounded statements submitted to a SHARED pool actually finish but the
  statement resource gets stuck reporting a non-terminal phase forever
  instead of `COMPLETED`. Any new one-shot/bounded logic must go on the
  DEDICATED pool (or accept unverifiable completion, as `cmd_generate_data`
  does when defaulting to `shared-pool` — see its `SHARED_INSERT_GRACE_SECONDS` sleep (default 30s — must exceed startup + one 10s checkpoint or the Kafka sink never commits) + grace-period
  branch).
- **`cmd_compute_pool` waits for `shared-pool` to reach `RUNNING`** before
  returning (`wait_for_compute_pool_phase`, up to 300s) — CMF rejects
  statement submissions to a SHARED pool that's still cold-starting
  (`PENDING`/`PROVISIONING`), and `cmd_verify`/`cmd_demo_pipeline` run right
  after it in `cmd_up`. This wait is what makes a fresh `up` not race its own
  next step; don't remove it without replacing the race with something else.
- **Idempotency pattern.** Every subcommand is safe to re-run: cluster/
  namespace/helm-release creation checks-then-skips, `CREATE TABLE` uses
  `IF NOT EXISTS`, and `create_or_update_compute_pool` falls back from CLI
  `create` to a raw `PUT` against the CMF REST API when the resource already
  exists (with a warning if that also fails, e.g. because a SHARED pool has
  active statements and can't be resized until they're stopped). The very
  last line of that function is deliberately an `if/fi`, not a bare
  `[[ cond ]] && cmd` — as the function's last statement, a false condition
  would make the function return 1, and since it's called as a plain
  statement, `set -e` would silently kill the whole script (this bit us: a
  fresh `up` died right after the first pool was created, no error printed,
  every time artifacts were disabled). Keep that pattern in mind before
  writing a new function that ends in a bare `&&`.
- **Artifact storage is opt-in and cloud-symmetric.** `CMF_ARTIFACTS_ENABLED=true`
  makes `cmd_up` run `cmd_artifacts` before `cmd_cmf`: it provisions a
  per-user bucket (`cpf-artifacts-<user>`, GCS or S3) plus script-minted,
  bucket-scoped static credentials (GCP service-account JSON key or AWS IAM
  access key), stores them in the `cmf-artifacts-creds` k8s secret in the
  `confluent` namespace, replicates that secret to each Flink environment
  namespace, and wires the creds into both CMF (`cmf.artifacts.*`) and the
  Flink compute pools' own filesystem plugin — CMF does not forward its
  artifact credentials to the Flink clusters it manages, so both sides need
  their own copy. `cmd_down` tears the bucket/creds back down; on GCP, set
  `ARTIFACTS_GCS_SA=<existing-sa-email>` to reuse an SA on projects at their
  service-account quota (then `down` only deletes the minted key, not the SA).
- **CMF 2.5 feature flags** (`CMF_ENVIRONMENT_CATALOG_ENABLED`,
  `CMF_MCP_ENABLED`, `CMF_MCP_WRITE_TOOLS_ENABLED`, `CMF_STACKTRACE_LOGGING`,
  `CMF_ARTIFACTS_ENABLED`) are passed by `cmd_cmf` as helm `--set` flags,
  each mapping to an exact chart value path — CMF 2.5 validates its values
  schema, so a wrong key fails the upgrade. See the README table before
  adding a new flag.
- **Port-forwards verify actual reachability, not just PID liveness.**
  `ensure_port_forward` (used by `cmd_cmf_forward`/`cmd_c3_forward`/every step
  that talks to CMF) checks `is_port_serving` — a real HTTP round trip, not
  just "is something listening" — because a `kubectl port-forward` whose
  target pod restarted (a `helm upgrade` on `cmf`, for instance) often keeps
  the local socket open without actually forwarding anymore. If the port
  isn't serving, it reaps any tracked forward (dead or silently broken) and
  restarts it, polling up to 30s for it to actually answer before returning.
  PID files (`.demo-cmf-port-forward.pid`, `.demo-c3-port-forward.pid`, both
  gitignored) are just bookkeeping for that reaping, not the liveness check
  itself. `cmd_down` always stops both first.
- **`require_cmf_access`** is the preflight every CMF-talking subcommand runs.
  `confluent flink ...` outright refuses to run while *any* Confluent
  Cloud/Platform context is selected, and `confluent logout` alone does not
  clear that. Rather than make users delete their contexts, `demo.sh` defines a
  `confluent()` shell-function wrapper that runs the real CLI with
  `HOME=$CONFLUENT_CLI_HOME` (default `~/.cache/cpf-demo/confluent-home`), an
  isolated config dir with no contexts. Only `confluent` gets that HOME. These
  commands only ever talk to CMF via `$CONFLUENT_CMF_URL`/`--url`.
- **`wait_for_statement_phase`** and its twin **`wait_for_compute_pool_phase`**
  are the polling primitives used everywhere a statement's or compute pool's
  lifecycle matters — both poll `.status.phase` via `confluent ... describe -o
  json` and share the same loop shape (poll until an "ok" phase, `FAILED`, or
  timeout), since the CLI's own `--wait` flag is unreliable (see README
  Troubleshooting).
- **`cmd_up` is the composition root**: it just calls the other `cmd_*`
  functions in sequence (check-quotas → cluster → helm-repo → namespace →
  operator → kafka → cert-manager → flink-operator → [artifacts, if enabled]
  → cmf → cmf-forward → c3-forward → flink-environments → catalog →
  compute-pool → verify per `FLINK_ENVIRONMENTS`). `demo-pipeline` (the SQL
  demo tables/seed-data/streaming job) is deliberately **not** part of `up` —
  it's a standalone follow-up step. When adding a new install step, add both
  a `cmd_<name>` function, a `case` branch in the dispatch table at the
  bottom of `demo.sh`, and (if it belongs in the full flow) a call from
  `cmd_up` — plus an entry in `cmd_help` and the README command-reference
  table (which numbers the exact `up` order — keep that numbering in sync).
- **AWS teardown is asynchronous and slower than GCP's.** `cmd_down_aws`
  marks PVCs for deletion before deleting the cluster so their EBS volumes
  release as `eksctl` drains the nodegroup, then does a final pass to retry
  any volume left behind, plus cleaning up the per-user IAM artifact user if
  one exists. Expect 10-15 minutes for the full CloudFormation stack
  deletion, vs. GCP's much faster `gcloud container clusters delete`.
  GCP's `cmd_down_gcp` likewise records the PD disks behind the cluster's PVs
  (via an explicit `--context`, from each PV's CSI `volumeHandle`) *before*
  deleting the cluster, then deletes exactly those disks afterwards — GKE
  orphans them otherwise, and on a shared project they can't be told apart
  from other people's by label.
