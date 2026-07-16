#!/usr/bin/env bash
#
# Confluent Platform for Flink on GKE - demo environment automation.
# Run `./demo.sh help` for the list of subcommands.
#
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

# ---------------------------------------------------------------------------
# Configuration - see config.sh (override any value via environment variables)
# ---------------------------------------------------------------------------
source ./config.sh

PORT_FORWARD_PID_FILE=".demo-cmf-port-forward.pid"
C3_PORT_FORWARD_PID_FILE=".demo-c3-port-forward.pid"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
log() { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }
warn() { printf '\033[1;33m!! %s\033[0m\n' "$*" >&2; }
die() { printf '\033[1;31mERROR: %s\033[0m\n' "$*" >&2; exit 1; }

require_cmds() {
  for c in "$@"; do
    command -v "$c" >/dev/null 2>&1 || die "required command not found: $c"
  done
}

require_aws_identity() {
  require_cmds aws
  aws sts get-caller-identity >/dev/null 2>&1 \
    || die "aws CLI has no valid credentials (session expired, e.g. an SSO token, or AWS_PROFILE/credentials are wrong) - run 'aws sts get-caller-identity' to diagnose, re-authenticate (e.g. 'granted sso login ...' or re-export AWS_PROFILE), and retry"
}

# Catches a confusing failure mode: `confluent context list` can show a
# "current" context that's actually stale/corrupted, in which case every
# `confluent flink ...` call below fails with "not logged in" even though a
# context exists. This is unrelated to which cloud/CMF instance is targeted -
# the flink subcommands only talk to CMF via $CONFLUENT_CMF_URL/--url - the
# CLI just independently requires some valid session before running anything.
require_confluent_login() {
  confluent flink environment list >/dev/null 2>&1 \
    || die "confluent CLI is not logged in (or its session is stale/corrupted) - run 'confluent context list', clear any broken contexts with 'confluent context delete <name>', then 'confluent login --save' and retry. Any login works (Confluent Cloud or Platform) since these commands talk to CMF via \$CONFLUENT_CMF_URL regardless of the login's own target"
}

ensure_namespace() {
  kubectl get namespace "$1" >/dev/null 2>&1 || kubectl create namespace "$1"
}

wait_pods_ready() {
  local ns="$1" timeout="${2:-600s}"
  log "Waiting for pods in namespace '$ns' to be Ready (timeout ${timeout})"
  kubectl -n "$ns" wait --for=condition=Ready pod --all --timeout="$timeout" \
    || warn "not all pods in '$ns' reported Ready in time - check with: kubectl -n $ns get pods"
}

# Substitutes the ${..._VERSION} placeholders in cp/cp.yaml with the
# values from config.sh, so bumping a container version only requires editing
# config.sh - no envsubst dependency, just sed.
render_kafka_manifest() {
  sed \
    -e "s#\${CP_SERVER_VERSION}#${CP_SERVER_VERSION}#g" \
    -e "s#\${INIT_CONTAINER_VERSION}#${INIT_CONTAINER_VERSION}#g" \
    -e "s#\${SCHEMA_REGISTRY_VERSION}#${SCHEMA_REGISTRY_VERSION}#g" \
    -e "s#\${CONTROL_CENTER_VERSION}#${CONTROL_CENTER_VERSION}#g" \
    -e "s#\${PROMETHEUS_VERSION}#${PROMETHEUS_VERSION}#g" \
    -e "s#\${ALERTMANAGER_VERSION}#${ALERTMANAGER_VERSION}#g" \
    "$1"
}

is_port_forward_alive() {
  local pid_file="$1"
  [[ -f "$pid_file" ]] || return 1
  if kill -0 "$(cat "$pid_file")" 2>/dev/null; then
    return 0
  fi
  rm -f "$pid_file" # stale pid from a process that has since died
  return 1
}

is_port_listening() {
  curl -s -o /dev/null --max-time 1 "http://localhost:$1/" 2>/dev/null
  [[ $? -ne 7 ]] # curl exit 7 = connection refused, i.e. nothing listening
}

ensure_cmf_port_forward() {
  if is_port_forward_alive "$PORT_FORWARD_PID_FILE"; then
    return 0
  fi
  if is_port_listening "$CMF_LOCAL_PORT"; then
    warn "something is already listening on localhost:${CMF_LOCAL_PORT} (not started by this script) - reusing it"
    return 0
  fi
  log "Starting background port-forward: cmf-service -> localhost:${CMF_LOCAL_PORT}"
  kubectl -n "$CONFLUENT_NAMESPACE" port-forward service/cmf-service "${CMF_LOCAL_PORT}:80" \
    >/tmp/demo-cmf-port-forward.log 2>&1 &
  echo $! > "$PORT_FORWARD_PID_FILE"
  sleep 3
  is_port_forward_alive "$PORT_FORWARD_PID_FILE" || die "cmf port-forward failed to start, see /tmp/demo-cmf-port-forward.log"
}

ensure_c3_port_forward() {
  if is_port_forward_alive "$C3_PORT_FORWARD_PID_FILE"; then
    return 0
  fi
  if is_port_listening "$C3_LOCAL_PORT"; then
    warn "something is already listening on localhost:${C3_LOCAL_PORT} (not started by this script) - reusing it"
    return 0
  fi
  log "Starting background port-forward: controlcenter-ng-0 -> localhost:${C3_LOCAL_PORT}"
  kubectl -n "$CONFLUENT_NAMESPACE" port-forward controlcenter-ng-0 "${C3_LOCAL_PORT}:9021" \
    >/tmp/demo-c3-port-forward.log 2>&1 &
  echo $! > "$C3_PORT_FORWARD_PID_FILE"
  sleep 3
  is_port_forward_alive "$C3_PORT_FORWARD_PID_FILE" || die "control center port-forward failed to start, see /tmp/demo-c3-port-forward.log"
}

# ---------------------------------------------------------------------------
# Subcommands
# ---------------------------------------------------------------------------

cmd_cluster() {
  case "$CLOUD" in
    gcp) cmd_cluster_gcp ;;
    aws) cmd_cluster_aws ;;
  esac
}

cmd_cluster_gcp() {
  require_cmds gcloud kubectl
  log "Creating GKE cluster '$CLUSTER_NAME' in $ZONE (project $PROJECT)"
  if gcloud container clusters describe "$CLUSTER_NAME" --zone "$ZONE" --project "$PROJECT" >/dev/null 2>&1; then
    warn "cluster '$CLUSTER_NAME' already exists, skipping create"
  else
    gcloud container clusters create "$CLUSTER_NAME" \
      --zone "$ZONE" \
      --num-nodes "$NUM_NODES" \
      --machine-type "$MACHINE_TYPE" \
      --project "$PROJECT"
  fi
  gcloud container clusters get-credentials "$CLUSTER_NAME" --zone "$ZONE" --project "$PROJECT"
  kubectl cluster-info
}

cmd_cluster_aws() {
  require_cmds eksctl aws kubectl
  log "Creating EKS cluster '$EKS_CLUSTER_NAME' in $EKS_REGION"
  if aws eks describe-cluster --name "$EKS_CLUSTER_NAME" --region "$EKS_REGION" >/dev/null 2>&1; then
    warn "cluster '$EKS_CLUSTER_NAME' already exists, skipping create"
  else
    eksctl create cluster \
      --name "$EKS_CLUSTER_NAME" \
      --region "$EKS_REGION" \
      --nodes "$EKS_NUM_NODES" \
      --node-type "$EKS_NODE_TYPE" \
      --managed \
      --with-oidc \
      --tags "cflt_managed_by=user,cflt_managed_id=${CFLT_USER}"
  fi
  aws eks update-kubeconfig --name "$EKS_CLUSTER_NAME" --region "$EKS_REGION"
  kubectl cluster-info
  setup_ebs_csi_driver
}

# Installs the EBS CSI driver as an EKS add-on (in-cluster, not a local install) with an
# IRSA-backed IAM role, and applies a default gp3 StorageClass - the AWS equivalent of the
# default StorageClass GKE ships with out of the box, needed so the Kafka/SR/Control Center
# PVCs (dataVolumeCapacity in cp/cp.yaml) can bind. extraVolumeTags on the addon config tags
# every EBS volume the driver dynamically provisions, so PVC-backed disks get the same
# cflt_managed_by/cflt_managed_id tags as the rest of the cluster's resources.
setup_ebs_csi_driver() {
  local role_name="${EKS_CLUSTER_NAME}-ebs-csi-driver-role" account_id role_arn addon_config
  log "Setting up EBS CSI driver add-on (IRSA role + addon + default gp3 StorageClass)"
  account_id=$(aws sts get-caller-identity --query Account --output text)
  role_arn="arn:aws:iam::${account_id}:role/${role_name}"

  if ! aws iam get-role --role-name "$role_name" >/dev/null 2>&1; then
    eksctl create iamserviceaccount \
      --cluster "$EKS_CLUSTER_NAME" --region "$EKS_REGION" \
      --namespace kube-system --name ebs-csi-controller-sa \
      --role-name "$role_name" \
      --attach-policy-arn arn:aws:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy \
      --role-only --approve \
      --tags "cflt_managed_by=user,cflt_managed_id=${CFLT_USER}"
  fi

  # `eksctl create addon --configuration-values` isn't available as a bare CLI flag on
  # every eksctl build - a ClusterConfig file passed via --config-file works everywhere.
  addon_config=$(mktemp)
  cat > "$addon_config" <<EOF
apiVersion: eksctl.io/v1alpha5
kind: ClusterConfig
metadata:
  name: ${EKS_CLUSTER_NAME}
  region: ${EKS_REGION}
addons:
  - name: aws-ebs-csi-driver
    serviceAccountRoleARN: ${role_arn}
    configurationValues: '{"controller":{"extraVolumeTags":{"cflt_managed_by":"user","cflt_managed_id":"${CFLT_USER}"}}}'
EOF
  eksctl create addon --config-file "$addon_config" --force --wait
  rm -f "$addon_config"

  kubectl apply -f - <<'EOF'
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: gp3
  annotations:
    storageclass.kubernetes.io/is-default-class: "true"
provisioner: ebs.csi.aws.com
parameters:
  type: gp3
EOF

  kubectl -n kube-system rollout status deployment/ebs-csi-controller --timeout=180s \
    || warn "ebs-csi-controller not Ready yet - check: kubectl -n kube-system get pods -l app=ebs-csi-controller"
}

cmd_helm_repo() {
  require_cmds helm
  log "Adding/updating the confluentinc helm repo"
  helm repo add confluentinc https://packages.confluent.io/helm >/dev/null 2>&1 || true
  helm repo update
}

cmd_namespace() {
  log "Creating namespace '$CONFLUENT_NAMESPACE' and setting it as the current context default"
  ensure_namespace "$CONFLUENT_NAMESPACE"
  kubectl config set-context --current --namespace "$CONFLUENT_NAMESPACE"
}

cmd_operator() {
  log "Installing confluent-for-kubernetes operator"
  helm upgrade --install operator confluentinc/confluent-for-kubernetes \
    --namespace "$CONFLUENT_NAMESPACE"
  wait_pods_ready "$CONFLUENT_NAMESPACE" 180s
}

cmd_kafka() {
  log "Applying CP Kafka manifest (cp/cp.yaml, rendered with versions from config.sh)"
  render_kafka_manifest cp/cp.yaml | kubectl apply -n "$CONFLUENT_NAMESPACE" -f -
  wait_pods_ready "$CONFLUENT_NAMESPACE" 600s
}

cmd_c3_forward() {
  ensure_c3_port_forward
  log "Control Center reachable at http://localhost:${C3_LOCAL_PORT}/home"
}

cmd_cert_manager() {
  log "Installing cert-manager $CERT_MANAGER_VERSION"
  kubectl apply -f "https://github.com/jetstack/cert-manager/releases/download/${CERT_MANAGER_VERSION}/cert-manager.yaml"
  kubectl -n cert-manager wait --for=condition=Available deployment --all --timeout=180s \
    || warn "cert-manager deployments not Available yet - check with: kubectl -n cert-manager get pods"
}

cmd_flink_operator() {
  log "Creating Flink environment namespaces and installing the Flink Kubernetes Operator"
  ensure_namespace prod
  ensure_namespace test
  helm upgrade --install cp-flink-kubernetes-operator \
    --version "$FLINK_OPERATOR_VERSION" \
    confluentinc/flink-kubernetes-operator \
    --namespace "$CONFLUENT_NAMESPACE" \
    --set watchNamespaces="{${FLINK_NAMESPACES}}"
  wait_pods_ready "$CONFLUENT_NAMESPACE" 180s
}

cmd_cmf() {
  log "Installing Confluent Manager for Apache Flink (CMF) $CMF_VERSION"
  helm upgrade --install cmf confluentinc/confluent-manager-for-apache-flink \
    --version "$CMF_VERSION" \
    --set cmf.sql.production=false \
    --namespace "$CONFLUENT_NAMESPACE"
  wait_pods_ready "$CONFLUENT_NAMESPACE" 180s
}

cmd_port_forward() {
  ensure_cmf_port_forward
  if [[ -f "$PORT_FORWARD_PID_FILE" ]]; then
    log "CMF reachable at ${CONFLUENT_CMF_URL} (pid $(cat "$PORT_FORWARD_PID_FILE"))"
  else
    log "CMF reachable at ${CONFLUENT_CMF_URL} (via a pre-existing port-forward)"
  fi
}

cmd_stop_port_forward() {
  for f in "$PORT_FORWARD_PID_FILE" "$C3_PORT_FORWARD_PID_FILE"; do
    if is_port_forward_alive "$f"; then
      kill "$(cat "$f")"
      rm -f "$f"
    fi
  done
  log "Stopped background port-forwards"
}

cmd_flink_environments() {
  require_cmds confluent
  ensure_cmf_port_forward
  require_confluent_login
  for env in $FLINK_ENVIRONMENTS; do
    log "Creating Flink environment '$env'"
    confluent flink environment create "$env" --kubernetes-namespace "$env" \
      || warn "environment '$env' may already exist"
  done
  confluent flink environment list
}

cmd_catalog() {
  require_cmds confluent
  ensure_cmf_port_forward
  require_confluent_login
  log "Creating Kafka catalog '$CATALOG_NAME' from $CATALOG_FILE"
  confluent flink catalog create "$CATALOG_FILE" || warn "catalog may already exist"
  log "Creating database '$DATABASE_NAME' from $DATABASE_FILE (DDL permissions for: $FLINK_ENVIRONMENTS)"
  if ! confluent flink catalog database create "$DATABASE_FILE" --catalog "$CATALOG_NAME"; then
    warn "database may already exist - updating it instead so DDL permissions (ddlEnvironments) stay in sync"
    confluent flink catalog database update "$DATABASE_FILE" --catalog "$CATALOG_NAME" \
      || warn "failed to update database '$DATABASE_NAME'"
  fi
  confluent flink catalog describe "$CATALOG_NAME"
}

# The CLI has no `compute-pool update`, so fall back to the REST API directly
# (confirmed to support PUT via `curl -X OPTIONS .../compute-pools/<name>`).
# This lets re-running `compute-pool` converge sizing/config on existing pools,
# e.g. after right-sizing flink/compute-pool*.json.
create_or_update_compute_pool() {
  local env="$1" name="$2" file="$3" type="$4"
  log "Creating compute pool '$name' ($type) in environment '$env'"
  if confluent flink compute-pool create "$file" --environment "$env" 2>/dev/null; then
    return 0
  fi
  if curl -sf -X PUT -H "Content-Type: application/json" -d "@${file}" \
      "${CONFLUENT_CMF_URL}/cmf/api/v1/environments/${env}/compute-pools/${name}" >/dev/null; then
    log "Compute pool '$name' already existed in '$env' - updated it to match $file"
  else
    warn "compute pool '$name' already exists in '$env' and couldn't be updated - it likely has" \
      "active statements in a non-updatable phase (stop/delete them first with" \
      "'confluent --environment $env flink statement stop <name>')"
  fi
}

cmd_compute_pool() {
  require_cmds confluent
  ensure_cmf_port_forward
  require_confluent_login
  for env in $FLINK_ENVIRONMENTS; do
    create_or_update_compute_pool "$env" "$COMPUTE_POOL_NAME" "$COMPUTE_POOL_FILE" DEDICATED
    create_or_update_compute_pool "$env" "$SHARED_COMPUTE_POOL_NAME" "$SHARED_COMPUTE_POOL_FILE" SHARED
  done
}

delete_statement_quiet() {
  confluent --environment "$1" flink statement delete "$2" --force >/dev/null 2>&1 || true
}

# Polls a statement's phase until it matches one of the given "ok" phases (success),
# FAILED (treated as failure), or the timeout elapses. Prints the final phase seen.
wait_for_statement_phase() {
  local env="$1" name="$2" timeout="$3"; shift 3
  local waited=0 phase want
  while true; do
    phase=$(confluent --environment "$env" flink statement describe "$name" -o json 2>/dev/null | jq -r '.status.phase // "UNKNOWN"') || true
    for want in "$@"; do
      [[ "$phase" == "$want" ]] && { echo "$phase"; return 0; }
    done
    [[ "$phase" == "FAILED" ]] && { echo "$phase"; return 1; }
    (( waited >= timeout )) && { echo "$phase"; return 1; }
    sleep 3
    waited=$((waited + 3))
  done
}

# Runs a one-shot SQL file as a statement and waits for it to reach one of the ok phases.
# Deletes any previous statement of the same name first, so re-runs are safe.
run_pipeline_statement() {
  local env="$1" name="$2" pool="$3" sql_file="$4" timeout="$5"; shift 5
  [[ -f "$sql_file" ]] || die "SQL file not found: $sql_file"
  delete_statement_quiet "$env" "$name"
  confluent --environment "$env" flink statement create "$name" \
    --catalog "$CATALOG_NAME" \
    --database "$DATABASE_NAME" \
    --compute-pool "$pool" \
    --parallelism 1 \
    --sql "$(cat "$sql_file")" >/dev/null
  wait_for_statement_phase "$env" "$name" "$timeout" "$@"
}

cmd_statement() {
  require_cmds confluent jq
  ensure_cmf_port_forward
  require_confluent_login
  local env="${1:-prod}" phase

  log "Creating table 'demo_events' in '$env' (compute pool: $SHARED_COMPUTE_POOL_NAME)"
  phase=$(run_pipeline_statement "$env" create-demo-events "$SHARED_COMPUTE_POOL_NAME" "$CREATE_EVENTS_SQL_FILE" 60 COMPLETED) || true
  [[ "$phase" == "COMPLETED" ]] || die "creating demo_events failed (phase=$phase) - check: confluent --environment $env flink statement describe create-demo-events"
  delete_statement_quiet "$env" create-demo-events

  log "Creating table 'demo_aggregated' in '$env' (compute pool: $SHARED_COMPUTE_POOL_NAME)"
  phase=$(run_pipeline_statement "$env" create-demo-aggregated "$SHARED_COMPUTE_POOL_NAME" "$CREATE_AGGREGATED_SQL_FILE" 60 COMPLETED) || true
  [[ "$phase" == "COMPLETED" ]] || die "creating demo_aggregated failed (phase=$phase) - check: confluent --environment $env flink statement describe create-demo-aggregated"
  delete_statement_quiet "$env" create-demo-aggregated

  # Bounded jobs submitted to the SHARED pool get stuck reporting RECONCILING forever
  # once they finish (an operator status-reporting quirk - see README troubleshooting),
  # so the one-shot data load runs on the DEDICATED pool instead, where it reports
  # COMPLETED correctly (just slower, since it cold-starts its own cluster).
  log "Inserting demo data into 'demo_events' in '$env' (compute pool: $COMPUTE_POOL_NAME)"
  phase=$(run_pipeline_statement "$env" insert-demo-data "$COMPUTE_POOL_NAME" "$INSERT_DEMO_DATA_SQL_FILE" 150 COMPLETED) || true
  [[ "$phase" == "COMPLETED" ]] || die "inserting demo data failed (phase=$phase) - check: confluent --environment $env flink statement describe insert-demo-data"
  delete_statement_quiet "$env" insert-demo-data

  log "Starting continuous streaming aggregation '$STATEMENT_NAME' in '$env' (compute pool: $SHARED_COMPUTE_POOL_NAME)"
  if confluent --environment "$env" flink statement describe "$STATEMENT_NAME" >/dev/null 2>&1; then
    warn "statement '$STATEMENT_NAME' already exists in '$env' - leaving it as is"
    return 0
  fi
  [[ -f "$STREAMING_SQL_FILE" ]] || die "SQL file not found: $STREAMING_SQL_FILE"
  confluent --environment "$env" flink statement create "$STATEMENT_NAME" \
    --catalog "$CATALOG_NAME" \
    --database "$DATABASE_NAME" \
    --compute-pool "$SHARED_COMPUTE_POOL_NAME" \
    --parallelism 1 \
    --sql "$(cat "$STREAMING_SQL_FILE")" >/dev/null
  phase=$(wait_for_statement_phase "$env" "$STATEMENT_NAME" 60 RUNNING) || true
  [[ "$phase" == "RUNNING" ]] || warn "statement '$STATEMENT_NAME' not RUNNING yet (phase=$phase) - check with ./demo.sh status"
}

# Generates a batch of random rows into demo_events on demand, so you can add
# more data to an already-running pipeline without resetting anything.
# Defaults to shared-pool (fast, no cold start) - pass a pool name as the 3rd
# arg (e.g. $COMPUTE_POOL_NAME) to use the DEDICATED pool instead, which is
# slower but reports COMPLETED reliably (see the SHARED-pool branch below).
cmd_generate_data() {
  require_cmds confluent jq
  ensure_cmf_port_forward
  require_confluent_login
  local env="${1:-prod}" count="${2:-$GENERATE_DATA_ROW_COUNT}" pool="${3:-$SHARED_COMPUTE_POOL_NAME}"
  local categories=(A B) suffix tmpfile name phase i offset val cat sep pool_type

  [[ "$count" =~ ^[0-9]+$ ]] || die "row count must be a positive integer, got: $count"
  (( count > 0 )) || die "row count must be greater than 0, got: $count"

  pool_type=$(confluent flink compute-pool describe --environment "$env" "$pool" -o json 2>/dev/null | jq -r '.spec.type // empty') || true
  [[ -n "$pool_type" ]] || die "compute pool '$pool' not found in environment '$env'"

  suffix=$(date +%s)
  tmpfile=$(mktemp)
  {
    echo "INSERT INTO demo_events"
    echo "/*+ OPTIONS('properties.transaction.timeout.ms'='300000') */"
    echo "VALUES"
    for ((i = 0; i < count; i++)); do
      offset=$(( RANDOM % GENERATE_DATA_MAX_OFFSET_SECONDS ))
      val=$(( (RANDOM % 50) + 1 ))
      cat=${categories[$((RANDOM % ${#categories[@]}))]}
      sep=","
      (( i == count - 1 )) && sep=";"
      printf "  ('evt-%s-%d', %d, CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) - INTERVAL '%d' SECOND, '%s')%s\n" \
        "$suffix" "$i" "$val" "$offset" "$cat" "$sep"
    done
  } > "$tmpfile"

  name="generate-data-${suffix}"
  log "Generating $count demo events in '$env' (compute pool: $pool, type: $pool_type)"
  delete_statement_quiet "$env" "$name"
  confluent --environment "$env" flink statement create "$name" \
    --catalog "$CATALOG_NAME" \
    --database "$DATABASE_NAME" \
    --compute-pool "$pool" \
    --parallelism 1 \
    --sql "$(cat "$tmpfile")" >/dev/null
  rm -f "$tmpfile"

  if [[ "$pool_type" == "SHARED" ]]; then
    # Bounded jobs on a SHARED pool actually finish in a couple of seconds,
    # but the statement gets stuck reporting RECONCILING/PENDING forever
    # instead of COMPLETED (operator quirk - see README troubleshooting), so
    # there's no reliable phase to poll for. Give it a short grace period
    # instead and move on.
    sleep 10
    delete_statement_quiet "$env" "$name"
    log "Requested $count new events into 'demo_events' in '$env' (SHARED pool - completion isn't verifiable, but should already be done)"
  else
    phase=$(wait_for_statement_phase "$env" "$name" 150 COMPLETED) || true
    delete_statement_quiet "$env" "$name"
    [[ "$phase" == "COMPLETED" ]] || die "generating demo data failed (phase=$phase) - check: confluent --environment $env flink statement describe $name"
    log "Inserted $count new events into 'demo_events' in '$env'"
  fi
}

run_verify_statement() {
  local env="$1" name="$2" sql="$3" phase
  confluent --environment "$env" flink statement delete "$name" --force >/dev/null 2>&1 || true
  echo
  echo "--- $sql (environment: $env, compute pool: $SHARED_COMPUTE_POOL_NAME) ---"
  confluent --environment "$env" flink statement create "$name" \
    --catalog "$CATALOG_NAME" \
    --database "$DATABASE_NAME" \
    --compute-pool "$SHARED_COMPUTE_POOL_NAME" \
    --parallelism 1 \
    --sql "$sql" >/dev/null
  # SHOW/DESCRIBE-style metadata statements (unlike real SELECTs) embed their
  # result inline on the statement resource, so a plain describe -o json has it.
  phase=$(wait_for_statement_phase "$env" "$name" 60 COMPLETED) || true
  if [[ "$phase" == "COMPLETED" ]]; then
    confluent --environment "$env" flink statement describe "$name" -o json 2>/dev/null \
      | jq -r '.result.results.data[]?.row | join("\t")'
  else
    warn "verification statement '$name' did not complete (phase=$phase)"
  fi
  confluent --environment "$env" flink statement delete "$name" --force >/dev/null 2>&1 || true
}

cmd_verify() {
  require_cmds confluent jq
  ensure_cmf_port_forward
  require_confluent_login
  local env="${1:-prod}"
  local probe_table="__verify_probe"
  log "Running startup verification statements in environment '$env'"
  run_verify_statement "$env" verify-show-tables "SHOW TABLES;"
  # Uses its own disposable table rather than depending on any pre-existing
  # topic, so this works standalone even before ./demo.sh statement has run.
  run_verify_statement "$env" verify-create-probe \
    "CREATE TABLE IF NOT EXISTS ${probe_table} (\`id\` STRING, \`value\` INT) DISTRIBUTED INTO 1 BUCKETS;"
  run_verify_statement "$env" verify-describe-probe "DESCRIBE ${probe_table};"
  run_verify_statement "$env" verify-show-create-probe "SHOW CREATE TABLE ${probe_table};"
  run_verify_statement "$env" verify-drop-probe "DROP TABLE IF EXISTS ${probe_table};"
}

cmd_application() {
  require_cmds confluent
  ensure_cmf_port_forward
  require_confluent_login
  local env="${1:-prod}"
  local file="${2:-$APPLICATION_FILE}"
  [[ -f "$file" ]] || die "application file not found: $file"
  log "Creating Flink application from $file in environment '$env'"
  confluent flink application create "$file" --environment "$env" \
    || warn "application may already exist in '$env'"
}

cmd_status() {
  echo "--- kubectl pods ($CONFLUENT_NAMESPACE) ---"
  kubectl -n "$CONFLUENT_NAMESPACE" get pods
  echo
  echo "--- kubectl pods (prod) ---"
  kubectl -n prod get pods 2>/dev/null || true
  echo
  echo "--- kubectl pods (test) ---"
  kubectl -n test get pods 2>/dev/null || true
  if is_port_forward_alive "$PORT_FORWARD_PID_FILE" || is_port_listening "$CMF_LOCAL_PORT"; then
    echo
    echo "--- Flink environments (CMF at ${CONFLUENT_CMF_URL}) ---"
    confluent flink environment list || true
  fi
  if is_port_forward_alive "$C3_PORT_FORWARD_PID_FILE" || is_port_listening "$C3_LOCAL_PORT"; then
    echo
    echo "--- Control Center reachable at http://localhost:${C3_LOCAL_PORT}/home ---"
  fi
}

cmd_up() {
  cmd_cluster
  cmd_helm_repo
  cmd_namespace
  cmd_operator
  cmd_kafka
  cmd_cert_manager
  cmd_flink_operator
  cmd_cmf
  cmd_port_forward
  cmd_flink_environments
  cmd_catalog
  cmd_compute_pool
  for env in $FLINK_ENVIRONMENTS; do
    cmd_verify "$env"
  done
  log "Environment is up. Try: ./demo.sh statement, then ./demo.sh status"
}

cmd_down() {
  cmd_stop_port_forward || true
  case "$CLOUD" in
    gcp) cmd_down_gcp "$@" ;;
    aws) cmd_down_aws "$@" ;;
  esac
}

cmd_down_gcp() {
  require_cmds gcloud
  if [[ "${1:-}" != "--yes" ]]; then
    read -r -p "This will DELETE the GKE cluster '$CLUSTER_NAME' in $ZONE (project $PROJECT). Type 'yes' to continue: " confirm
    [[ "$confirm" == "yes" ]] || { echo "Aborted."; exit 1; }
  fi
  log "Deleting GKE cluster '$CLUSTER_NAME'"
  gcloud container clusters delete "$CLUSTER_NAME" --zone "$ZONE" --project "$PROJECT" --quiet
}

cmd_down_aws() {
  require_cmds eksctl aws
  if [[ "${1:-}" != "--yes" ]]; then
    read -r -p "This will DELETE the EKS cluster '$EKS_CLUSTER_NAME' in $EKS_REGION. Type 'yes' to continue: " confirm
    [[ "$confirm" == "yes" ]] || { echo "Aborted."; exit 1; }
  fi

  if kubectl cluster-info >/dev/null 2>&1; then
    # PVCs stay stuck Terminating as long as their pod is still running (the
    # pvc-protection finalizer blocks it) - waiting here would just block for no
    # reason, since nothing evicts those pods until eksctl drains the nodegroup
    # below. Mark them for deletion (non-blocking) so the PV's Delete reclaim
    # policy fires as soon as the drain releases each volume.
    log "Marking PVCs for deletion (their EBS volumes release once eksctl drains the nodegroup below)"
    for ns in "$CONFLUENT_NAMESPACE" $FLINK_ENVIRONMENTS; do
      kubectl -n "$ns" delete pvc --all --wait=false 2>/dev/null || true
    done
  fi

  log "Deleting EKS cluster '$EKS_CLUSTER_NAME' (region $EKS_REGION)"
  eksctl delete cluster --name "$EKS_CLUSTER_NAME" --region "$EKS_REGION" --wait

  local leftover vol attempt still_left
  leftover=$(aws ec2 describe-volumes --region "$EKS_REGION" \
    --filters "Name=tag:cflt_managed_id,Values=${CFLT_USER}" \
    --query 'Volumes[].VolumeId' --output text 2>/dev/null || true)
  if [[ -n "$leftover" ]]; then
    # These are the PVC-provisioned data volumes (Kafka/SR/Control Center) - they live
    # outside eksctl's CloudFormation stack, so eksctl's own deletion never touches them.
    # Right after the cluster disappears they can briefly still show "in-use" while AWS
    # finishes detaching them, so retry for a bit instead of giving up after one try.
    log "Deleting leftover EBS volumes tagged cflt_managed_id=${CFLT_USER}: $leftover"
    for vol in $leftover; do
      for attempt in 1 2 3 4 5 6; do
        aws ec2 delete-volume --volume-id "$vol" --region "$EKS_REGION" 2>/dev/null && break
        sleep 5
      done
    done
    still_left=$(aws ec2 describe-volumes --region "$EKS_REGION" \
      --filters "Name=tag:cflt_managed_id,Values=${CFLT_USER}" \
      --query 'Volumes[].VolumeId' --output text 2>/dev/null || true)
    if [[ -n "$still_left" ]]; then
      warn "could not delete these leftover EBS volumes automatically (likely still detaching): $still_left - delete manually with: aws ec2 delete-volume --volume-id <id> --region $EKS_REGION"
    else
      log "Leftover EBS volumes deleted"
    fi
  fi
}

cmd_help() {
  cat <<'EOF'
Usage: ./demo.sh --gcp|--aws [--user <cflt-username>] <subcommand> [args]

Cloud selection (required for every subcommand except help/-h/--help):
  --gcp                    Use GKE (existing behavior)
  --aws --user <name>      Use EKS; --user tags all AWS resources this script
                           creates (cflt_managed_by=user, cflt_managed_id=<name>)
                           - use the part of your @confluent.io email before the @
  Flags may appear anywhere on the command line, e.g.:
    ./demo.sh --gcp up
    ./demo.sh --aws --user myusername up
    ./demo.sh up --aws --user myusername

Full flow:
  up                    Run every step below in order (cluster -> ... -> compute pools)
  down [--yes]           Stop port-forwards and delete the GKE/EKS cluster

Individual steps (same order as `up`):
  cluster                Create the GKE/EKS cluster and fetch kubectl credentials
                          (EKS also installs the EBS CSI driver add-on and a
                          default gp3 StorageClass, so PVCs bind like on GKE)
  helm-repo               Add/update the confluentinc helm repo
  namespace               Create the confluent namespace and set it as default context
  operator                Install the confluent-for-kubernetes operator
  kafka                   Apply cp/cp.yaml (Kafka, Schema Registry, Control Center)
  cert-manager            Install cert-manager (required by the Flink k8s operator)
  flink-operator          Create prod/test namespaces, install the Flink Kubernetes Operator
  cmf                     Install Confluent Manager for Apache Flink (CMF)
  port-forward            Start a background port-forward to cmf-service:8080
  stop-port-forward       Stop all background port-forwards started by this script
  flink-environments      Create the prod/test Flink environments in CMF
  catalog                 Create the Kafka catalog + database (flink/catalogv2.json, databasev2.json)
  compute-pool            Create the DEDICATED pool (flink/compute-pool.json) and SHARED pool
                          (flink/compute-pool-shared.json) in prod and test
  verify [env]            Create/describe/drop a disposable table on the shared pool to
                          sanity-check the environment (default env: prod)
  statement [env]         Create demo_events/demo_aggregated, seed demo data, and start the
                          continuous windowed-aggregation job (default env: prod)
  generate-data [env] [count] [pool]  Insert more random rows into demo_events on demand
                          (default env: prod, count: 20, pool: shared-pool -
                          pass e.g. "pool" as 3rd arg to use the DEDICATED
                          pool instead, which is slower but confirms COMPLETED)
  application [env] [file] Deploy a simple FlinkApplication (default env: prod, default file: cpf_basic_app.json)

Utilities:
  c3-forward              Start a background port-forward to Control Center on :9021
  status                  Show pod status and Flink environments
  help                    Show this message

Config can be overridden via environment variables, e.g.:
  PROJECT=my-proj ZONE=us-central1-a CLUSTER_NAME=my-cluster ./demo.sh --gcp cluster
  EKS_REGION=us-east-1 EKS_CLUSTER_NAME_PREFIX=my-cluster ./demo.sh --aws --user myusername cluster
EOF
}

# ---------------------------------------------------------------------------
# Argument parsing - pulls --gcp/--aws/--user out of "$@" wherever they
# appear, leaving the rest as positionals for the subcommand dispatch below.
# ---------------------------------------------------------------------------
CLOUD=""
CFLT_USER=""
saw_gcp=0 saw_aws=0
POSITIONAL=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --gcp) CLOUD="gcp"; saw_gcp=1; shift ;;
    --aws) CLOUD="aws"; saw_aws=1; shift ;;
    --user)
      [[ $# -ge 2 ]] || die "--user requires a value, e.g. --user myusername"
      CFLT_USER="$2"; shift 2 ;;
    --user=*) CFLT_USER="${1#--user=}"; shift ;;
    *) POSITIONAL+=("$1"); shift ;;
  esac
done
(( saw_gcp && saw_aws )) && die "specify exactly one of --gcp or --aws, not both"

# bash 3.2 (macOS default) throws "unbound variable" expanding an empty array
# under set -u - always length-check before expanding.
if [[ ${#POSITIONAL[@]} -gt 0 ]]; then
  set -- "${POSITIONAL[@]}"
else
  set --
fi

# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------
sub="${1:-help}"
[[ $# -gt 0 ]] && shift

if [[ "$sub" != "help" && "$sub" != "-h" && "$sub" != "--help" ]]; then
  [[ -n "$CLOUD" ]] || die "must specify a cloud: pass exactly one of --gcp or --aws (e.g. './demo.sh --gcp up' or './demo.sh --aws --user myusername up')"
  [[ "$CLOUD" == "aws" && -z "$CFLT_USER" ]] && die "--aws requires --user <cflt-username> (the part of your @confluent.io email before the @, e.g. --user myusername)"
  # Fail fast with a clear message if the AWS session is missing/expired, rather than
  # letting some kubectl/eksctl call deep inside a subcommand fail confusingly later
  # (e.g. a stale SSO token surfacing as a kubectl port-forward or confluent CLI error).
  [[ "$CLOUD" == "aws" ]] && require_aws_identity
fi

# The real cluster name is always "<prefix>-<user>", so people sharing an AWS
# account/region never collide on the same EKS cluster.
[[ "$CLOUD" == "aws" ]] && EKS_CLUSTER_NAME="${EKS_CLUSTER_NAME_PREFIX}-${CFLT_USER}"

case "$sub" in
  up) cmd_up "$@" ;;
  down) cmd_down "$@" ;;
  cluster) cmd_cluster "$@" ;;
  helm-repo) cmd_helm_repo "$@" ;;
  namespace) cmd_namespace "$@" ;;
  operator) cmd_operator "$@" ;;
  kafka) cmd_kafka "$@" ;;
  c3-forward) cmd_c3_forward "$@" ;;
  cert-manager) cmd_cert_manager "$@" ;;
  flink-operator) cmd_flink_operator "$@" ;;
  cmf) cmd_cmf "$@" ;;
  port-forward) cmd_port_forward "$@" ;;
  stop-port-forward) cmd_stop_port_forward "$@" ;;
  flink-environments) cmd_flink_environments "$@" ;;
  catalog) cmd_catalog "$@" ;;
  compute-pool) cmd_compute_pool "$@" ;;
  statement) cmd_statement "$@" ;;
  generate-data) cmd_generate_data "$@" ;;
  verify) cmd_verify "$@" ;;
  application) cmd_application "$@" ;;
  status) cmd_status "$@" ;;
  help|-h|--help) cmd_help ;;
  *) die "unknown subcommand '$sub' (see ./demo.sh help)" ;;
esac
