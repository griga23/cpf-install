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

> ⚠️ The `confluent flink` commands refuse to run while a Confluent Cloud or
> Confluent Platform context is selected (`Error: you must log out of Confluent
> Cloud to use this command`). `confluent logout` is **not** enough — it drops the
> credential but leaves the context selected as `current_context`. Log out **and**
> delete every context before proceeding (they only talk to CMF via
> `$CONFLUENT_CMF_URL` regardless):
>
> ```sh
> confluent logout
> confluent context list          # note the Name of each context
> confluent context delete <name>  # repeat for every context listed
> ```

## Quick start

`--user` names your cluster (`<prefix>-<name>`) and tags/labels every cloud
resource created (`cflt_managed_by=user`, `cflt_managed_id=<name>`) on both
clouds.

`up` is a composition root: it runs ~15 substeps in sequence (cluster → Kafka →
CMF → Flink operator → environments → catalog → compute pools → CMF and 
Control Center (C3) port-forwards → verify) — see the 
[Command reference](#command-reference) for the full ordered list. 

The two port-forwards are the CMF UI/API on `localhost:8080`
and Confluent Control Center on `localhost:9021`. 

Everything below `up` is an **optional follow-up**, not a step you must run 
in order; in particular, `up` has *already* started both of these port-forwards.

### GCP

```sh
# 1. Stand up everything (cluster → Kafka → CMF → Flink → pools → both port-forwards → verify)
./demo.sh --gcp --user myusername up

# Then, as needed:
./demo.sh --gcp --user myusername cmf-ui         # open the CMF 2.4 web UI in a browser (http://localhost:8080/)
./demo.sh --gcp --user myusername c3-ui          # open the Control Center (C3) web UI in a browser (http://localhost:9021/home)
./demo.sh --gcp --user myusername demo-pipeline  # run the demo pipeline: create tables, seed data, start the streaming job (prod)
./demo.sh --gcp --user myusername status         # check pod health and list Flink environments
./demo.sh --gcp --user myusername down           # stop port-forwards and delete the GKE cluster
```

### AWS

```sh
# 1. Stand up everything (cluster → Kafka → CMF → Flink → pools → both port-forwards → verify)
./demo.sh --aws --user myusername up

# Then, as needed:
./demo.sh --aws --user myusername cmf-ui         # open the CMF 2.4 web UI in a browser (http://localhost:8080/)
./demo.sh --aws --user myusername c3-ui          # open the Control Center (C3) web UI in a browser (http://localhost:9021/home)
./demo.sh --aws --user myusername demo-pipeline  # run the demo pipeline: create tables, seed data, start the streaming job (prod)
./demo.sh --aws --user myusername status         # check pod health and list Flink environments
./demo.sh --aws --user myusername down           # stop port-forwards and delete the EKS cluster
```

`--gcp`/`--aws` and `--user` may appear anywhere on the command line (before
or after the subcommand). They're required for every subcommand except
`help`/`-h`/`--help`.

Run `./demo.sh help` any time for the full subcommand list.


> **If a port-forward drops** (e.g. after a CMF or Control Center pod restart —
> `kubectl port-forward` doesn't auto-reconnect), re-run `cmf-forward` (CMF,
> `:8080`) or `c3-forward` (Control Center, `:9021`) to restart it. `up` starts
> both, but doesn't keep them alive.

> The Artifact Storage introduced with CMF 2.4 is disabled by default.
> See [Artifact storage](#artifact-storage-cmf-24) for details about enabling it.

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

### CMF 2.4 artifact storage


Set `CMF_ARTIFACTS_ENABLED=true` in the `config.sh` to enable it and also
provision blob storage (a per-user bucket + scoped creds) and wire it into CMF
and the Flink pools during `up`; `down` cleans it up.

```sh
CMF_ARTIFACTS_ENABLED=true ./demo.sh --aws --user myusername up   # up, plus S3 bucket + IAM user/creds
# GCP shared projects are often at their service-account quota - reuse an existing SA:
CMF_ARTIFACTS_ENABLED=true ARTIFACTS_GCS_SA=<existing-sa-email> ./demo.sh --gcp --user myusername up
```


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
  You can modify the prefix changing `ARTIFACTS_BUCKET_PREFIX` in `config.sh`.
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

Every subcommand is also runnable standalone — e.g. after editing
`flink/compute-pool.json`, just run `./demo.sh --gcp --user <name> compute-pool`
(or `--aws --user <name>`). All subcommands require a cloud (`--gcp`/`--aws`)
and `--user <name>` (see Quick start); both are omitted below for brevity.

### Steps run by `up`

`up` is not a step itself — it runs the steps below, **in this exact order**.
Each is also runnable standalone (e.g. to re-run or resume a partial install):


| # | Step | What it does |
|---|---|---|
| 1 | `check-quotas` | Checks cloud quotas for the resources that will be created. Warns on any exceeded quota and asks whether to proceed (default no). **AWS:** VPCs, internet gateways, and Elastic IPs per region + NAT gateways per AZ. **GCP:** not implemented yet (no-op). |
| 2 | `cluster` | Creates the GKE/EKS cluster, fetches `kubectl` credentials. On AWS, also installs the EBS CSI driver add-on + a default `gp3` `StorageClass` (GKE ships with a default StorageClass already) |
| 3 | `helm-repo` | Adds/updates the `confluentinc` helm repo |
| 4 | `namespace` | Creates the `confluent` namespace, sets it as default context |
| 5 | `operator` | Installs the `confluent-for-kubernetes` operator |
| 6 | `kafka` | Applies `cp/cp.yaml` (Kafka, Schema Registry, Control Center) |
| 7 | `cert-manager` | Installs cert-manager (required by the Flink Kubernetes Operator) |
| 8 | `flink-operator` | Creates `prod`/`test` namespaces, installs the Flink Kubernetes Operator |
| 9 | `artifacts` | Provisions blob storage for CMF 2.4 artifacts: a per-user bucket + scoped static creds in a K8s secret. **Conditional** — only runs when `CMF_ARTIFACTS_ENABLED=true` |
| 10 | `cmf` | Installs Confluent Manager for Apache Flink |
| 11 | `cmf-forward` | Background port-forward `cmf-service` → `localhost:8080` (CMF UI/API) |
| 12 | `c3-forward` | Background port-forward for Control Center → `localhost:9021` |
| 13 | `flink-environments` | Creates the `prod`/`test` CMF environments |
| 14 | `catalog` | Creates/updates the `kafka-cat` catalog and `kafka-db` database, with DDL permissions |
| 15 | `compute-pool` | Creates/updates the `pool` (DEDICATED) and `shared-pool` (SHARED) compute pools, and waits for `shared-pool` to reach `RUNNING` |
| 16 | `verify [env]` | Sanity-checks the environment by running a short sequence of Flink statements — `SHOW TABLES`, then create / describe / show-create / **drop** a throwaway `__verify_probe` table — and **deletes each statement afterward**, so nothing persists. **Repeated once per environment** in `FLINK_ENVIRONMENTS` (default `prod` and `test`). Does **not** create the demo pipeline (that's `demo-pipeline`, below) |


> ℹ️ **What `up` does and doesn't create.** During `up` you'll see Flink tables and
> statements being created and dropped — that's the `verify` step (16), which creates
> and immediately drops a throwaway `__verify_probe` table and deletes its own
> statements. `up` leaves **no** persistent tables or statements. The demo pipeline
> tables (`demo_events`, `demo_aggregated`), the seed data, and the continuous
> streaming-aggregation job are created **only** by the standalone `demo-pipeline` command
> — `up` never runs it.

> ℹ️ The `up` command is idempotent. If it stops for any reasons, run it again.

> ⚠️  If `up` was interrupted after having created cloud recources that are subhect to quotas, when you re-run `up` 
> the `check-quotas` step may show a warning because no *further* resources of a given type can be created. 
> However, these resources were created in the previous run, and `up` will not create more. In this case, you 
> can safely ignore the warning and let `up` continue.

### Standalone commands (not run by `up`)

Run these yourself, as needed, after `up`:

| Subcommand | What it does |
|---|---|
| `demo-pipeline [env] [pool]` | Runs the full stream-processing demo pipeline (default `prod`). By default every step runs on `shared-pool` (fast — no cold start); pass a pool name to force a different pool for all steps (e.g. `demo-pipeline prod pool` runs the data load on the DEDICATED `pool`, which confirms COMPLETED but is slower) |
| `generate-data [env] [count] [pool]` | Inserts more random rows into `demo_events` on demand (default `prod`, `20` rows, `shared-pool`) |
| `application [env] [file]` | Deploys a raw `FlinkApplication` (default `prod`, `app/cpf_basic_app.json`) |
| `cmf-ui` | Ensures the CMF port-forward (same as step 11) and opens the CMF 2.4 web UI in a browser (root of `localhost:8080`, same port as the REST API) |
| `c3-ui` | Ensures the Control Center port-forward (same as step 12) and opens the C3 web UI in a browser (`http://localhost:9021/home`) |
| `status` | Shows pod health, Flink environments, and port-forward status |
| `stop-port-forward` | Stops all background port-forwards started by this script (both the CMF and Control Center forwards) |
| `down [--yes]` | Stops port-forwards, deletes the GKE/EKS cluster. On AWS, PVCs are marked for deletion first, then their EBS volumes actually release as `eksctl` drains the nodegroup; a final check warns about any leftover volume |

## The demo pipeline

Two compute pools are available:

| Pool | Type | Used for |
|---|---|---|
| `shared-pool` | SHARED | DDL, ad hoc queries, the continuous streaming job, and (by default) the seed-data insert — fast, no per-job cold start |
| `pool` | DEDICATED | One-shot bounded jobs where you want verifiable `COMPLETED` (bounded jobs never report finished on `shared-pool` — see [Troubleshooting](#troubleshooting)) |

`./demo.sh demo-pipeline [env] [pool]` runs four SQL statements in sequence against a
self-contained schema (CMF only accepts one statement per submission, so these
can't be combined into a single script). **By default every step runs on
`shared-pool`** (fastest); pass a pool name to force a different pool for all
steps — e.g. `demo-pipeline prod pool` runs the bounded seed insert on the DEDICATED
`pool` so its completion is verifiable (slower, cold start):

| # | Statement name | SQL file | Produces | After it runs |
|---|---|---|---|---|
| 1 | `create-demo-events` | `sql/create_demo_events.sql` | `demo_events` table, watermarked on `event_time` | statement **deleted** (table persists) |
| 2 | `create-demo-aggregated` | `sql/create_demo_aggregated.sql` | `demo_aggregated` table for windowed results | statement **deleted** (table persists) |
| 3 | `insert-demo-data` | `sql/insert_demo_data.sql` | 10 seed rows in `demo_events`, timestamped relative to `CURRENT_TIMESTAMP` (always "fresh"); on `shared-pool` completion isn't verifiable, so it uses a short grace period | statement **deleted** (rows persist) |
| 4 | `streaming-aggregation` | `sql/streaming_aggregation.sql` | Continuous 30s tumbling-window aggregation, `demo_events` → `demo_aggregated` | **kept — stays `RUNNING`** |

Statements 1–3 are one-shot: the script runs each, waits for it to finish, then
**deletes the statement resource**, so only their effects (the two tables and the
seed rows) persist. **Only statement 4, `streaming-aggregation`, is left running** —
it's the single statement you'll see under _Flink Environment > `<env>` >
Statements_ in the CMF or C3 UI. (All steps run on `shared-pool` by default; the
2nd positional arg overrides the pool for every step, as noted above.)

Safe to re-run: `CREATE TABLE` uses `IF NOT EXISTS`, the seed insert just adds
another batch of rows, and step 4 is skipped with a warning if
`streaming-aggregation` already exists.

`./demo.sh catalog` sets `spec.ddlEnvironments: ["prod", "test"]` on
`kafka-db` (`flink/databasev2.json`), which is what lets `CREATE TABLE`/
`DROP TABLE` work in both environments from the start.

Once `streaming-aggregation` is running, feed it more data any time with
`./demo.sh generate-data [env] [count]` — see [Common tasks](#common-tasks).

## Common tasks

The examples below use `--gcp` and omit the now-required `--user <name>` for
brevity — add it to each command (or use `--aws --user <name>` for EKS).

> ℹ️ **Bare `confluent flink ...` commands need `CONFLUENT_CMF_URL`.** Unlike
> `./demo.sh` subcommands (which export it for you from `config.sh`), a raw
> `confluent` command run in your own shell has no CMF URL and fails with
> `Error: url is required`. Once per shell, make sure the CMF port-forward is up
> and export the URL:
>
> ```sh
> ./demo.sh --aws --user <name> cmf-forward     # ensure the CMF port-forward is up
> export CONFLUENT_CMF_URL=http://localhost:8080
> ```
>
> (or pass `--url http://localhost:8080` on each `confluent` command instead).

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
confluent --environment prod flink statement describe streaming-aggregation
confluent --environment prod flink statement web-ui-forward streaming-aggregation
confluent --environment prod flink statement stop streaming-aggregation
confluent --environment prod flink statement delete streaming-aggregation --force
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
./demo.sh --gcp cmf-forward       # CMF, localhost:8080
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
| `confluent flink ...` connection error | CMF port-forward died — run `./demo.sh --gcp cmf-forward` (or `--aws --user <name>`) |
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
| `demo.sh` dies with `ERROR: confluent CLI can't reach CMF` | A Confluent Cloud/Platform context is still selected — `confluent flink ...` refuses to run, and `confluent logout` alone doesn't clear it (it leaves the context as `current_context`). Log out **and** delete every context, then retry: `confluent logout`, then `confluent context list` (note each context Name) and `confluent context delete <name>` for every context listed. No re-login is needed — these commands reach CMF purely via `CONFLUENT_CMF_URL`/`--url` |
| `up` failed partway through creating resources | Fix the underlying problem and just re-run `up` — every step is idempotent, so it skips what already exists and continues. On the re-run the `check-quotas` step may now warn about a quota that was fine the first time (e.g. VPCs/Elastic IPs per region): that's expected, because the resources created by the first (partial) run already count against the quota. Answer `y` at the prompt to proceed — you're reusing those existing resources, not creating additional ones |

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
