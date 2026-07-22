#!/usr/bin/env bash
#
# Configuration for demo.sh - sourced automatically, do not run directly.
# Every value here can be overridden by setting the same-named environment
# variable before invoking demo.sh, e.g.:
#   PROJECT=my-proj ZONE=us-central1-a CLUSTER_NAME=my-demo ./demo.sh cluster
# Otherwise, edit the defaults below directly.

# --- GKE cluster (GCP) ---
: "${PROJECT:=sales-engineering-206314}"
: "${ZONE:=europe-west1-b}"
# The actual cluster name is always "<prefix>-<cflt-username>" (see demo.sh),
# mirroring EKS, so people sharing a project never collide on the same cluster.
: "${GKE_CLUSTER_NAME_PREFIX:=cpf-gke-demo}"
: "${NUM_NODES:=3}"
: "${MACHINE_TYPE:=e2-standard-4}"

# --- EKS cluster (AWS) ---
# Deliberately NOT named AWS_REGION: that's also the standard AWS CLI/SDK
# environment variable, so if it's already set in your shell (common with
# AWS SSO/profile setups) it would silently win over the default below.
: "${EKS_REGION:=eu-central-1}"
# The actual cluster name is always "<prefix>-<cflt-username>" (see demo.sh),
# so people sharing an AWS account/region never collide on the same cluster.
: "${EKS_CLUSTER_NAME_PREFIX:=cpf-eks-demo}"
: "${EKS_NUM_NODES:=3}"
: "${EKS_NODE_TYPE:=m5.xlarge}"

# --- Kubernetes namespaces / Flink environments ---
: "${CONFLUENT_NAMESPACE:=confluent}"
: "${FLINK_NAMESPACES:=confluent,test,prod}"     # watched by the Flink k8s operator
: "${FLINK_ENVIRONMENTS:=prod test}"             # CMF environments to create (space separated)

# --- Helm chart versions ---
# NOTE: CMF and the Flink k8s operator are version-locked - CMF 2.4.0 requires
# operator app version 1.15.0-cp2, which is helm CHART version 1.150.x (the chart
# and app versions differ: chart 1.150.2 -> app 1.15.0-cp2). Bump both together.
: "${CERT_MANAGER_VERSION:=v1.18.2}"
: "${FLINK_OPERATOR_VERSION:=~1.150.0}"   # chart 1.150.x = app 1.15.0-cp2 (required by CMF 2.4.0)
: "${CMF_VERSION:=~2.4.0}"

# --- CMF 2.4 feature flags (wired into cmd_cmf as helm --set) ---
# Each key below maps to an EXACT chart value path - CMF 2.4 validates the Helm
# values schema, so an unknown key fails the upgrade. Verify against:
#   helm show values confluentinc/confluent-manager-for-apache-flink --version 2.4.0
# Self-contained features default ON so a fresh `cmf` run exercises 2.4; set to
# false to opt out.
: "${CMF_ENVIRONMENT_CATALOG_ENABLED:=true}"   # cmf.sql.environmentCatalog.enabled - CREATE FUNCTION ... USING JAR + custom connectors/formats (CREATE TABLE ... WITH ('connector'=...))
: "${CMF_MCP_ENABLED:=true}"                    # cmf.mcp.enabled - MCP server at /cmf/mcp/v1alpha1 for AI agents (Claude Code, Cursor)
: "${CMF_MCP_WRITE_TOOLS_ENABLED:=false}"       # cmf.mcp.writeTools.enabled - allow MCP create/update/delete (false = read-only)
: "${CMF_STACKTRACE_LOGGING:=true}"             # cmf.stackTraceLogging - log stack traces on error (2.4 default true)
# Artifact management (upload Flink JARs to blob storage, reference via cmf://).
# OFF by default: enabling it REQUIRES a basePath or CMF refuses to start, so set
# BOTH of the following to turn it on.
: "${CMF_ARTIFACTS_ENABLED:=false}"             # cmf.artifacts.enabled - opt-in; when true, `up` also runs cmd_artifacts (creates bucket + creds) before cmf
: "${CMF_ARTIFACTS_BASE_PATH:=}"                # cmf.artifacts.basePath - leave empty to derive as <scheme>://<bucket>/cmf (see demo.sh); override to bring your own path

# --- CMF artifact storage (blob storage the script provisions per user) ---
# Bucket + credentials are only created when CMF_ARTIFACTS_ENABLED=true. The bucket
# name is always "<prefix>-<cflt-username>" (like the cluster), and the script mints
# scoped static credentials (AWS IAM user + access key / GCP service account + JSON
# key) stored in the K8s secret below. All of this is torn down by `down`.
: "${ARTIFACTS_BUCKET_PREFIX:=cpf-artifacts}"   # bucket = <prefix>-<user> (also the AWS IAM user / GCP SA id, so keep it short: SA id must be <=30 chars)
: "${ARTIFACTS_GCS_LOCATION:=${ZONE%-*}}"       # GCS bucket location - region derived from ZONE (europe-west1-b -> europe-west1)
: "${ARTIFACTS_MAX_UPLOAD_SIZE:=250MB}"         # cmf.artifacts.maxUploadSize (B/KB/MB/GB/TB)
: "${ARTIFACTS_CREDS_SECRET:=cmf-artifacts-creds}"  # K8s secret (namespace $CONFLUENT_NAMESPACE) holding the storage credentials
# GCP only: reuse an existing service account instead of minting one (e.g. when the
# project is at its SA-per-project quota). When set, cmd_artifacts_gcp grants this SA
# objectAdmin on the bucket and mints a JSON key for it rather than creating a new SA.
: "${ARTIFACTS_GCS_SA:=}"                        # existing SA email, e.g. me@<project>.iam.gserviceaccount.com
# Flink built-in filesystem plugin enabled on the Flink cluster so it can fetch cmf://
# artifacts itself (CMF does NOT pass its creds to the cluster). Leave EMPTY to derive the
# jar name from each spec's own image tag (flink-<scheme>-fs-hadoop-<tag>.jar) - important
# because a FlinkApplication may use a different image than the compute pools. Set only to
# force a specific jar (verify names with `ls /opt/flink/opt` in the image).
: "${ARTIFACTS_S3_PLUGIN_JAR:=}"
: "${ARTIFACTS_GCS_PLUGIN_JAR:=}"

# --- Container image versions for cp/cp.yaml ---
# demo.sh renders these into the manifest before applying it (see cmd_kafka),
# so bumping a version here is the only change needed - no manual editing of
# cp.yaml required.
: "${CP_SERVER_VERSION:=8.2.2}"            # confluentinc/cp-server (KRaftController + Kafka)
: "${INIT_CONTAINER_VERSION:=3.3.0}"       # confluentinc/confluent-init-container (all resources)
: "${SCHEMA_REGISTRY_VERSION:=8.2.2}"      # confluentinc/cp-schema-registry
: "${CONTROL_CENTER_VERSION:=2.5.1}"       # confluentinc/cp-enterprise-control-center-next-gen
: "${PROMETHEUS_VERSION:=2.5.1}"           # confluentinc/cp-enterprise-prometheus
: "${ALERTMANAGER_VERSION:=2.5.1}"         # confluentinc/cp-enterprise-alertmanager

# --- Local ports / CMF URL ---
: "${CMF_LOCAL_PORT:=8080}"
: "${C3_LOCAL_PORT:=9021}"
: "${CONFLUENT_CMF_URL:=http://localhost:${CMF_LOCAL_PORT}}"
export CONFLUENT_CMF_URL

# --- CMF resources: catalog, database, compute pools, demo pipeline ---
: "${CATALOG_NAME:=kafka-cat}"
: "${DATABASE_NAME:=kafka-db}"
: "${CATALOG_FILE:=flink/catalogv2.json}"
: "${DATABASE_FILE:=flink/databasev2.json}"
: "${COMPUTE_POOL_NAME:=pool}"
: "${COMPUTE_POOL_FILE:=flink/compute-pool.json}"
: "${SHARED_COMPUTE_POOL_NAME:=shared-pool}"
: "${SHARED_COMPUTE_POOL_FILE:=flink/compute-pool-shared.json}"
: "${STATEMENT_NAME:=streaming-aggregation}"   # name of the long-running streaming job (step 4 of `demo-pipeline`); stays RUNNING
: "${CREATE_EVENTS_SQL_FILE:=sql/create_demo_events.sql}"
: "${CREATE_AGGREGATED_SQL_FILE:=sql/create_demo_aggregated.sql}"
: "${INSERT_DEMO_DATA_SQL_FILE:=sql/insert_demo_data.sql}"
: "${STREAMING_SQL_FILE:=sql/streaming_aggregation.sql}"
: "${APPLICATION_FILE:=app/cpf_basic_app.json}"
: "${GENERATE_DATA_ROW_COUNT:=20}"         # default rows for ./demo.sh generate-data
: "${GENERATE_DATA_MAX_OFFSET_SECONDS:=90}" # spread new rows over the last N seconds
