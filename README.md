# Confluent Platform for Flink — GKE/EKS Demo Environment

`demo.sh` automates a full Confluent Platform + Confluent Manager for Apache
Flink (CMF) demo on GKE or EKS: cluster creation, Kafka, CMF, Flink compute
pools, and a self-contained stream-processing pipeline (create tables → seed
data → continuous windowed aggregation). Every subcommand requires picking a
cloud via `--gcp` or `--aws` **and** a `--user <name>` (see below) — the rest
of the pipeline (Kafka, CMF, Flink, the SQL demo) is identical on both.

## Prerequisites

Common to both clouds:
- `kubectl`, `helm`, `confluent` CLI, `curl`, `jq` installed

GCP (`--gcp`):
- `gcloud` authenticated: `gcloud auth login`
- Access to a GCP project (default `sales-engineering-206314`, override with `PROJECT=...`)
- A `--user <name>` (like AWS) — it names your GKE cluster (`<prefix>-<name>`)
  and labels it `cflt_managed_by=user`, `cflt_managed_id=<name>`, so people
  sharing a project each get their own cluster

AWS (`--aws`):
- `eksctl` and `aws` CLI v2 installed
- Assume the correct AWS profile yourself before running `demo.sh` — e.g.
  `export AWS_PROFILE=...` — and confirm it works with `aws sts get-caller-identity`.
  `demo.sh` verifies your identity before touching AWS, but never manages
  credentials or assumes roles on your behalf.

> ⚠️ Make sure you are not logged into Confluent Cloud or any Confluent Platform environment from your
> Confluent CLI, before proceeding: `confluent logout`

## Quick start

`--user` names your cluster (`<prefix>-<name>`) and tags/labels every cloud
resource created (`cflt_managed_by=user`, `cflt_managed_id=<name>`) on both
clouds.

```sh
# GCP
./demo.sh --gcp --user myusername up          # cluster, Kafka, CMF, Flink operator, environments, catalog, compute pools
./demo.sh --gcp --user myusername cmf-ui       # open the CMF 2.4 web UI (http://localhost:8080/)
./demo.sh --gcp --user myusername c3-forward   # port forward for Confluent Control Center (C3) UI; accessible on http://localhost:9021/home
./demo.sh --gcp --user myusername statement    # create tables, seed data, start the streaming aggregation job (prod)
./demo.sh --gcp --user myusername status       # check pod health and list Flink environments
./demo.sh --gcp --user myusername down         # stop port-forwards and delete the GKE cluster

# AWS
./demo.sh --aws --user myusername up
./demo.sh --aws --user myusername cmf-ui       # open the CMF 2.4 web UI (http://localhost:8080/)
./demo.sh --aws --user myusername c3-forward   # port forward for Confluent Control Center (C3) UI; accessible on http://localhost:9021/home
./demo.sh --aws --user myusername statement
./demo.sh --aws --user myusername status
./demo.sh --aws --user myusername down
```

**With CMF 2.4 artifact storage.** Set `CMF_ARTIFACTS_ENABLED=true` to also
provision blob storage (a per-user bucket + scoped creds) and wire it into CMF
and the Flink pools during `up`; `down` cleans it up. See
[Artifact storage](#artifact-storage-cmf-24) for details.

```sh
CMF_ARTIFACTS_ENABLED=true ./demo.sh --aws --user myusername up   # up, plus S3 bucket + IAM user/creds
# GCP shared projects are often at their service-account quota - reuse an existing SA:
CMF_ARTIFACTS_ENABLED=true ARTIFACTS_GCS_SA=<existing-sa-email> ./demo.sh --gcp --user myusername up
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
PROJECT=my-proj ZONE=us-central1-a GKE_CLUSTER_NAME_PREFIX=my-demo ./demo.sh --gcp --user myusername cluster
EKS_REGION=us-east-1 EKS_CLUSTER_NAME_PREFIX=my-demo ./demo.sh --aws --user myusername cluster
```

The actual cluster name is always `<prefix>-<user>` on both clouds — GCP uses
`GKE_CLUSTER_NAME_PREFIX` (default `cpf-gke-demo`, e.g. `cpf-gke-demo-myusername`)
and AWS uses `EKS_CLUSTER_NAME_PREFIX` (default `cpf-eks-demo`) — so multiple
people sharing the same GCP project or AWS account/region automatically get
their own cluster without overriding anything.

AWS-specific defaults: `EKS_REGION` (`eu-central-1` — the shared account's
`eu-west-1` is at its per-AZ NAT-gateway quota, which fails eksctl), `EKS_NUM_NODES` (`3`),
`EKS_NODE_TYPE` (`m5.xlarge`). GCP-specific: `PROJECT`, `ZONE`
(`europe-west1-b`), `NUM_NODES` (`3`), `MACHINE_TYPE` (`e2-standard-4`).

### CMF 2.4 feature flags

`cmd_cmf` wires the CMF 2.4 opt-in features as helm `--set` flags, controlled
from `config.sh` (each maps to an exact chart value path — CMF 2.4 validates the
values schema, so a wrong key fails the upgrade):

| Env var | Default | Chart value |
|---|---|---|
| `CMF_ENVIRONMENT_CATALOG_ENABLED` | `true` | `cmf.sql.environmentCatalog.enabled` — `CREATE FUNCTION ... USING JAR` + custom `connector`/`format` tables |
| `CMF_MCP_ENABLED` | `true` | `cmf.mcp.enabled` — MCP server at `/cmf/mcp/v1alpha1` for AI agents |
| `CMF_MCP_WRITE_TOOLS_ENABLED` | `false` | `cmf.mcp.writeTools.enabled` — MCP create/update/delete (`false` = read-only) |
| `CMF_STACKTRACE_LOGGING` | `true` | `cmf.stackTraceLogging` |
| `CMF_ARTIFACTS_ENABLED` | `false` | `cmf.artifacts.enabled` — provision blob storage + wire it in (see [Artifact storage](#artifact-storage-cmf-24)) |
| `CMF_ARTIFACTS_BASE_PATH` | _(auto)_ | `cmf.artifacts.basePath` — auto-derived as `<scheme>://cpf-artifacts-<user>/cmf`; set it only to bring your own path |

The two self-contained features (`environmentCatalog`, `mcp`) default on so a
fresh `cmf` run exercises 2.4; export the var as `false` to opt out. Managing
Flink via Control Center is deprecated in 2.4 in favor of the built-in CMF UI.

### Artifact storage (CMF 2.4)

Artifact management (upload/version Flink JARs to blob storage, reference via
`cmf://`) is **opt-in** and works on both clouds. Enable it with
`CMF_ARTIFACTS_ENABLED=true`; the `artifacts` step (run automatically by `up`
before `cmf`) provisions everything and `down` tears it back down:

- A per-user bucket **`cpf-artifacts-<user>`** (GCS on GCP, S3 on AWS).
- **Script-minted, bucket-scoped static credentials** — an AWS IAM user + access
  key, or a GCP service account + JSON key — stored in the K8s secret
  `cmf-artifacts-creds` in the `confluent` namespace and replicated to each Flink
  environment namespace (`prod`/`test`), since the Flink clusters fetch artifacts
  themselves.
- CMF is wired to those creds (`cmf.artifacts.*` + `extraEnv`/`mountedVolumes`),
  and the Flink compute pools / applications get their **own** filesystem access
  (plugin + creds) injected into their specs, since CMF does not pass its
  artifact credentials to the Flink clusters.

```sh
CMF_ARTIFACTS_ENABLED=true ./demo.sh --gcp --user <name> up          # bucket+creds created before cmf
CMF_ARTIFACTS_ENABLED=true ./demo.sh --aws --user <name> artifacts   # or provision the storage standalone
```

**Shared GCP projects:** minting a new service account can fail if the project is at
its service-account quota (common on shared projects). Set
`ARTIFACTS_GCS_SA=<an-existing-sa-email>` to reuse an existing SA instead — the script
grants it bucket access and mints a JSON key for it, and `down` deletes exactly that
key (recorded on the secret) without touching the shared SA itself.

Tunables (see `config.sh`): `ARTIFACTS_BUCKET_PREFIX` (default `cpf-artifacts`),
`ARTIFACTS_GCS_LOCATION` (derived from `ZONE`), `ARTIFACTS_MAX_UPLOAD_SIZE`
(`250MB`), `ARTIFACTS_CREDS_SECRET`, and `ARTIFACTS_S3_PLUGIN_JAR` /
`ARTIFACTS_GCS_PLUGIN_JAR` (the Flink built-in filesystem plugin enabled on each
pool/application). Left **empty by default**, which auto-derives the jar name
from that spec's own image tag (`flink-<scheme>-fs-hadoop-<tag>.jar`) — set one
explicitly only to override, e.g. if a `FlinkApplication` uses a different image
than the compute pools (verify jar names with `ls /opt/flink/opt` in the image).
Override `CMF_ARTIFACTS_BASE_PATH` to bring your own path. Minting IAM users / SA
keys may be blocked by org policy — the step fails with a clear message if so.

## Command reference

Every step is also runnable standalone — e.g. after editing
`flink/compute-pool.json`, just run `./demo.sh --gcp --user <name> compute-pool`
(or `--aws --user <name>`). All subcommands require a cloud (`--gcp`/`--aws`)
and `--user <name>` (see Quick start); both are omitted below for brevity.

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
| `artifacts` | Provisions blob storage for CMF 2.4 artifacts: a per-user bucket + scoped static creds in a K8s secret. Only runs when `CMF_ARTIFACTS_ENABLED=true` (and `up` runs it automatically before `cmf`) |
| `port-forward` | Background port-forward `cmf-service` → `localhost:8080` |
| `stop-port-forward` | Stops all background port-forwards started by this script |
| `flink-environments` | Creates the `prod`/`test` CMF environments |
| `catalog` | Creates/updates the `kafka-cat` catalog and `kafka-db` database, with DDL permissions |
| `compute-pool` | Creates/updates the `pool` (DEDICATED) and `shared-pool` (SHARED) compute pools |
| `verify [env]` | Sanity-checks the environment via a disposable table (default `prod`) |
| `statement [env] [pool]` | Runs the full stream-processing demo pipeline (default `prod`). By default every step runs on `shared-pool` (fast — no cold start); pass a pool name to force a different pool for all steps (e.g. `statement prod pool` runs the data load on the DEDICATED `pool`, which confirms COMPLETED but is slower) |
| `generate-data [env] [count] [pool]` | Inserts more random rows into `demo_events` on demand (default `prod`, `20` rows, `shared-pool`) |
| `application [env] [file]` | Deploys a raw `FlinkApplication` (default `prod`, `app/cpf_basic_app.json`) |
| `c3-forward` | Background port-forward for Control Center → `localhost:9021` |
| `cmf-ui` | Ensures the CMF port-forward and opens the CMF 2.4 web UI (root of `localhost:8080`, same port as the REST API) |
| `status` | Shows pod health, Flink environments, and port-forward status |
| `down [--yes]` | Stops port-forwards, deletes the GKE/EKS cluster. On AWS, PVCs are marked for deletion first, then their EBS volumes actually release as `eksctl` drains the nodegroup; a final check warns about any leftover volume |

## The demo pipeline

Two compute pools are available:

| Pool | Type | Used for |
|---|---|---|
| `shared-pool` | SHARED | DDL, ad hoc queries, the continuous streaming job, and (by default) the seed-data insert — fast, no per-job cold start |
| `pool` | DEDICATED | One-shot bounded jobs where you want verifiable `COMPLETED` (bounded jobs never report finished on `shared-pool` — see [Troubleshooting](#troubleshooting)) |

`./demo.sh statement [env] [pool]` runs four SQL statements in sequence against a
self-contained schema (CMF only accepts one statement per submission, so these
can't be combined into a single script). **By default every step runs on
`shared-pool`** (fastest); pass a pool name to force a different pool for all
steps — e.g. `statement prod pool` runs the bounded seed insert on the DEDICATED
`pool` so its completion is verifiable (slower, cold start):

| # | SQL file | Pool (default) | Result |
|---|---|---|---|
| 1 | `sql/create_demo_events.sql` | `shared-pool` | `demo_events` table, watermarked on `event_time` |
| 2 | `sql/create_demo_aggregated.sql` | `shared-pool` | `demo_aggregated` table for windowed results |
| 3 | `sql/insert_demo_data.sql` | `shared-pool` | 10 seed rows, timestamped relative to `CURRENT_TIMESTAMP` (always "fresh"); on `shared-pool` completion isn't verifiable, so it uses a short grace period |
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

The examples below use `--gcp` and omit the now-required `--user <name>` for
brevity — add it to each command (or use `--aws --user <name>` for EKS).

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
./demo.sh --gcp application test app/cpf_basic_app.json    # or point at a different resource file
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

> ⚠️ all Confluent CLI commands expect the env variable `CONFLUENT_CMF_URL` = `http://localhost:8080`.


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
| eksctl `up` fails with `would exceed the limit of N NAT gateways` | That region's per-AZ NAT-gateway quota is full (common on shared accounts) — retry in a different region, e.g. `EKS_REGION=<other-region> ./demo.sh --aws --user <name> up`. The default is `eu-central-1`; `eu-west-1` is known to be full on the shared account |
| EKS pods stuck `Pending` with unbound PVCs (Kafka/SR/Control Center) | The EBS CSI driver/default `gp3` `StorageClass` didn't finish setting up — check `kubectl -n kube-system get pods -l app=ebs-csi-controller` and `kubectl get storageclass` (expect `gp3` marked `(default)`); re-run `./demo.sh --aws --user <name> cluster` to retry (idempotent) |
| `aws`/`eksctl` commands fail with an auth error | Your assumed AWS profile expired or isn't set — re-run `export AWS_PROFILE=...` and confirm with `aws sts get-caller-identity` before retrying |
| `confluent flink ...` fails with `Error: not logged in`, even though `confluent context list` shows a current context | That context's session token is stale/corrupted. This is unrelated to which cloud/CMF instance you're using — `confluent flink ...` commands talk to CMF purely via `CONFLUENT_CMF_URL`/`--url`, never through the login's own target. Fix: `confluent context list`, delete the broken context(s) with `confluent context delete <name>`, then `confluent login --save` (any login works — Confluent Cloud or Platform) and retry |

## Teardown

```sh
./demo.sh --gcp --user myusername down            # prompts for confirmation
./demo.sh --gcp --user myusername down --yes      # skip the prompt

./demo.sh --aws --user myusername down          # prompts for confirmation
./demo.sh --aws --user myusername down --yes    # skip the prompt
```

On AWS, `down` marks PVCs for deletion first, then deletes the cluster; their
EBS volumes actually release while `eksctl` gracefully drains the nodegroup
(pods must stop before their attached volumes can be reclaimed), and a final
check afterward retries deleting any volume left behind. This takes
materially longer than the GCP path — typically 10-15 minutes for the full
CloudFormation stack deletion.

Also stops any background port-forwards started by this script...
