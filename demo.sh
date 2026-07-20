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
      --labels "cflt_managed_by=user,cflt_managed_id=${CFLT_USER}" \
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

# Convenience wrapper around the CMF port-forward that frames it as the CMF 2.4
# web UI (served at the root of the CMF service, same port as the REST API) and
# opens it in a browser where possible.
cmd_cmf_ui() {
  ensure_cmf_port_forward
  local url="${CONFLUENT_CMF_URL}/"
  log "CMF web UI: ${url}  (REST API: ${CONFLUENT_CMF_URL}/cmf/api/v1)"
  command -v open >/dev/null 2>&1 && open "$url" >/dev/null 2>&1 || true
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

# Provisions the blob-storage bucket + scoped static credentials CMF 2.4 needs for
# artifact management, storing the creds in the K8s secret $ARTIFACTS_CREDS_SECRET.
# Idempotent: bucket/IAM principal creation checks-then-skips, and credentials are
# only minted when the secret is absent (so re-runs don't hit the AWS 2-keys limit).
cmd_artifacts() {
  if [[ "$CMF_ARTIFACTS_ENABLED" != "true" ]]; then
    warn "artifact storage is disabled (CMF_ARTIFACTS_ENABLED != true) - skipping"
    return 0
  fi
  require_cmds kubectl
  ensure_namespace "$CONFLUENT_NAMESPACE"
  case "$CLOUD" in
    gcp) cmd_artifacts_gcp ;;
    aws) cmd_artifacts_aws ;;
  esac
}

artifacts_secret_exists() {
  kubectl -n "$CONFLUENT_NAMESPACE" get secret "$ARTIFACTS_CREDS_SECRET" >/dev/null 2>&1
}

# The creds secret lives in $CONFLUENT_NAMESPACE (for CMF's own upload/download), but the
# Flink clusters that fetch cmf:// artifacts run in the environment namespaces, so the
# secret must exist there too. Copies it from the CMF namespace into each Flink env
# namespace (idempotent apply; strips instance metadata so it re-applies cleanly).
replicate_artifacts_secret() {
  require_cmds jq
  local ns
  for ns in $FLINK_ENVIRONMENTS; do
    ensure_namespace "$ns"
    kubectl -n "$CONFLUENT_NAMESPACE" get secret "$ARTIFACTS_CREDS_SECRET" -o json \
      | jq 'del(.metadata.namespace,.metadata.resourceVersion,.metadata.uid,.metadata.creationTimestamp,.metadata.ownerReferences)' \
      | kubectl -n "$ns" apply -f - >/dev/null
  done
}

cmd_artifacts_aws() {
  require_cmds aws jq
  local user="$ARTIFACTS_BUCKET" bucket="$ARTIFACTS_BUCKET" region="$EKS_REGION" akid secret
  log "Setting up S3 artifact storage: bucket '$bucket', IAM user '$user' (region $region)"

  if aws s3api head-bucket --bucket "$bucket" --region "$region" >/dev/null 2>&1; then
    warn "S3 bucket '$bucket' already exists, skipping create"
  else
    if [[ "$region" == "us-east-1" ]]; then
      aws s3api create-bucket --bucket "$bucket" --region "$region" >/dev/null
    else
      aws s3api create-bucket --bucket "$bucket" --region "$region" \
        --create-bucket-configuration "LocationConstraint=${region}" >/dev/null
    fi
    aws s3api put-bucket-tagging --bucket "$bucket" \
      --tagging "TagSet=[{Key=cflt_managed_by,Value=user},{Key=cflt_managed_id,Value=${CFLT_USER}}]" >/dev/null
  fi

  if ! aws iam get-user --user-name "$user" >/dev/null 2>&1; then
    aws iam create-user --user-name "$user" \
      --tags "Key=cflt_managed_by,Value=user" "Key=cflt_managed_id,Value=${CFLT_USER}" >/dev/null
  fi
  # Scope the user to just this bucket (list on the bucket, object CRUD on its contents).
  aws iam put-user-policy --user-name "$user" --policy-name cpf-artifacts-s3 \
    --policy-document "$(cat <<EOF
{"Version":"2012-10-17","Statement":[
  {"Effect":"Allow","Action":["s3:ListBucket","s3:GetBucketLocation"],"Resource":"arn:aws:s3:::${bucket}"},
  {"Effect":"Allow","Action":["s3:GetObject","s3:PutObject","s3:DeleteObject"],"Resource":"arn:aws:s3:::${bucket}/*"}
]}
EOF
)" >/dev/null

  if artifacts_secret_exists; then
    warn "secret '$ARTIFACTS_CREDS_SECRET' already exists - reusing it (not minting a new access key)"
  else
    log "Minting an access key for IAM user '$user' and storing it in secret '$ARTIFACTS_CREDS_SECRET'"
    local keyjson
    keyjson=$(aws iam create-access-key --user-name "$user") \
      || die "could not create an access key for '$user' (org SCP may block IAM access keys) - see plan/README for the 'you supply keys' alternative"
    akid=$(echo "$keyjson" | jq -r '.AccessKey.AccessKeyId')
    secret=$(echo "$keyjson" | jq -r '.AccessKey.SecretAccessKey')
    kubectl -n "$CONFLUENT_NAMESPACE" create secret generic "$ARTIFACTS_CREDS_SECRET" \
      --from-literal=aws-access-key-id="$akid" \
      --from-literal=aws-secret-access-key="$secret" >/dev/null
  fi
  replicate_artifacts_secret
  log "S3 artifact storage ready: basePath ${CMF_ARTIFACTS_BASE_PATH}"
}

cmd_artifacts_gcp() {
  require_cmds gcloud
  local bucket="$ARTIFACTS_BUCKET" sa_id="$ARTIFACTS_BUCKET" sa_email keyfile
  # Reuse an existing SA (ARTIFACTS_GCS_SA) when set - needed when the project is at
  # its SA-per-project quota. Otherwise mint a dedicated one named after the bucket.
  if [[ -n "$ARTIFACTS_GCS_SA" ]]; then
    sa_email="$ARTIFACTS_GCS_SA"
  else
    [[ ${#sa_id} -le 30 ]] || die "artifact SA id '$sa_id' exceeds GCP's 30-char limit - shorten ARTIFACTS_BUCKET_PREFIX or --user"
    sa_email="${sa_id}@${PROJECT}.iam.gserviceaccount.com"
  fi
  log "Setting up GCS artifact storage: bucket 'gs://$bucket', service account '$sa_email' (location $ARTIFACTS_GCS_LOCATION)"

  if gcloud storage buckets describe "gs://${bucket}" --project "$PROJECT" >/dev/null 2>&1; then
    warn "GCS bucket 'gs://$bucket' already exists, skipping create"
  else
    # `gcloud storage buckets create` has no --labels flag; apply labels via update.
    gcloud storage buckets create "gs://${bucket}" --location="$ARTIFACTS_GCS_LOCATION" --project "$PROJECT"
    gcloud storage buckets update "gs://${bucket}" --project "$PROJECT" \
      --update-labels="cflt_managed_by=user,cflt_managed_id=${CFLT_USER}" >/dev/null
  fi

  if [[ -n "$ARTIFACTS_GCS_SA" ]]; then
    gcloud iam service-accounts describe "$sa_email" --project "$PROJECT" >/dev/null 2>&1 \
      || die "ARTIFACTS_GCS_SA '$sa_email' not found in project $PROJECT"
  elif ! gcloud iam service-accounts describe "$sa_email" --project "$PROJECT" >/dev/null 2>&1; then
    gcloud iam service-accounts create "$sa_id" --project "$PROJECT" \
      --display-name "CPF artifacts ${CFLT_USER}" \
      || die "could not create service account '$sa_id' (e.g. the project is at its service-account quota, a common case on shared projects) - set ARTIFACTS_GCS_SA=<an-existing-sa-email> to reuse an existing SA instead"
  fi
  gcloud storage buckets add-iam-policy-binding "gs://${bucket}" \
    --member="serviceAccount:${sa_email}" --role=roles/storage.objectAdmin >/dev/null

  if artifacts_secret_exists; then
    warn "secret '$ARTIFACTS_CREDS_SECRET' already exists - reusing it (not minting a new SA key)"
  else
    log "Minting a JSON key for '$sa_email' and storing it in secret '$ARTIFACTS_CREDS_SECRET'"
    keyfile=$(mktemp)
    gcloud iam service-accounts keys create "$keyfile" --iam-account="$sa_email" --project "$PROJECT" \
      || { rm -f "$keyfile"; die "could not create a JSON key for '$sa_email' (org policy may block SA key creation) - see plan/README for the 'you supply keys' alternative"; }
    kubectl -n "$CONFLUENT_NAMESPACE" create secret generic "$ARTIFACTS_CREDS_SECRET" \
      --from-file=gcs-key.json="$keyfile" >/dev/null
    # Record which key we minted + on which SA, so `down` can delete exactly this key
    # even when reusing an existing SA (where deleting the SA itself would be wrong).
    require_cmds jq
    kubectl -n "$CONFLUENT_NAMESPACE" annotate secret "$ARTIFACTS_CREDS_SECRET" --overwrite \
      "cpf.artifacts/gcs-key-id=$(jq -r '.private_key_id // empty' "$keyfile")" \
      "cpf.artifacts/gcs-sa=${sa_email}" >/dev/null
    rm -f "$keyfile"
  fi
  replicate_artifacts_secret
  log "GCS artifact storage ready: basePath ${CMF_ARTIFACTS_BASE_PATH}"
}

# Injects the Flink-cluster-side artifact storage access (filesystem plugin + creds
# from $ARTIFACTS_CREDS_SECRET) into a Flink spec read on stdin - needed because CMF
# does NOT pass its artifact creds to Flink clusters, so any cluster running a cmf://
# artifact must reach storage itself. No-op (passes the spec through) unless artifacts
# are enabled. $1 is the JSON path to the spec object to merge into (e.g. .spec.clusterSpec
# for a ComputePool, .spec for a FlinkApplication). Uses jq (already required).
render_flink_storage() {
  local spec_path="$1"
  if [[ "$CMF_ARTIFACTS_ENABLED" != "true" ]]; then cat; return 0; fi
  local scheme override
  case "$CLOUD" in
    aws) scheme=s3; override="$ARTIFACTS_S3_PLUGIN_JAR" ;;
    gcp) scheme=gs; override="$ARTIFACTS_GCS_PLUGIN_JAR" ;;
  esac
  # Credentials reach the built-in filesystem plugin via env vars (no flinkConfiguration
  # needed): AWS via the SDK's AWS_ACCESS_KEY_ID/SECRET chain; GCS via the recommended
  # GOOGLE_APPLICATION_CREDENTIALS pointing at the mounted key. The plugin jar name tracks
  # the spec's OWN image tag (flink-<scheme>-fs-hadoop-<tag>.jar), since a FlinkApplication
  # can use a different image than the compute pools; override with ARTIFACTS_*_PLUGIN_JAR.
  jq \
    --arg path "$spec_path" \
    --arg scheme "$scheme" \
    --arg override "$override" \
    --arg secret "$ARTIFACTS_CREDS_SECRET" \
    --arg cloud "$CLOUD" '
    ($path | ltrimstr(".") | split(".")) as $p
    | getpath($p) as $spec
    | (if $override != "" then $override
       else "flink-\($scheme)-fs-hadoop-\(($spec.image // "") | split(":") | last).jar" end) as $plugin
    | (if $cloud == "aws" then
         { env: [
             {name:"AWS_ACCESS_KEY_ID", valueFrom:{secretKeyRef:{name:$secret, key:"aws-access-key-id"}}},
             {name:"AWS_SECRET_ACCESS_KEY", valueFrom:{secretKeyRef:{name:$secret, key:"aws-secret-access-key"}}},
             {name:"ENABLE_BUILT_IN_PLUGINS", value:$plugin}
           ] }
       else
         { env: [
             {name:"GOOGLE_APPLICATION_CREDENTIALS", value:"/mnt/gcs/gcs-key.json"},
             {name:"ENABLE_BUILT_IN_PLUGINS", value:$plugin}
           ],
           volumeMounts: [ {name:"gcs-creds", mountPath:"/mnt/gcs", readOnly:true} ] }
       end) as $container
    | (if $cloud == "gcp" then
         { spec: { containers: [ ({name:"flink-main-container"} + $container) ],
                   volumes: [ {name:"gcs-creds", secret:{secretName:$secret}} ] } }
       else
         { spec: { containers: [ ({name:"flink-main-container"} + $container) ] } }
       end) as $podTemplate
    | setpath($p; $spec + {podTemplate: $podTemplate})
  '
}

# Renders a Flink spec file through render_flink_storage into a NEW temp file that KEEPS
# the original extension - the confluent CLI validates specs by extension (.json/.yaml/.yml)
# and rejects an extensionless mktemp path. Prints the path to use; when artifacts are off
# it prints the original file unchanged. Caller removes the temp file's parent dir when done.
render_spec_to_tmp() {
  local spec_path="$1" file="$2"
  if [[ "$CMF_ARTIFACTS_ENABLED" != "true" ]]; then printf '%s' "$file"; return 0; fi
  require_cmds jq
  local d out
  d=$(mktemp -d)
  out="${d}/$(basename "$file")"
  render_flink_storage "$spec_path" < "$file" > "$out"
  printf '%s' "$out"
}

cmd_cmf() {
  log "Installing Confluent Manager for Apache Flink (CMF) $CMF_VERSION"
  # Feature flags (see config.sh). Each --set maps to an exact chart value path;
  # CMF 2.4 validates the values schema, so an unknown key fails the upgrade.
  local set_args=(
    --set cmf.sql.production=false
    --set cmf.sql.environmentCatalog.enabled="$CMF_ENVIRONMENT_CATALOG_ENABLED"
    --set cmf.mcp.enabled="$CMF_MCP_ENABLED"
    --set cmf.mcp.writeTools.enabled="$CMF_MCP_WRITE_TOOLS_ENABLED"
    --set cmf.stackTraceLogging="$CMF_STACKTRACE_LOGGING"
  )
  # Artifact management refuses to start without a basePath, so only enable it when
  # one is configured. Credentials are read from $ARTIFACTS_CREDS_SECRET (created by
  # cmd_artifacts) - AWS keys come in as env vars referenced by ${..} placeholders in
  # the fs config; the GCS key is mounted as a file. Dotted fs keys are escaped so
  # helm treats them as literal map keys, not nested paths.
  if [[ "$CMF_ARTIFACTS_ENABLED" == "true" ]]; then
    [[ -n "$CMF_ARTIFACTS_BASE_PATH" ]] || die "CMF_ARTIFACTS_ENABLED=true requires CMF_ARTIFACTS_BASE_PATH (e.g. s3://bucket/cmf, gs://bucket/cmf) - CMF won't start otherwise"
    set_args+=(
      --set cmf.artifacts.enabled=true
      --set-string "cmf.artifacts.basePath=${CMF_ARTIFACTS_BASE_PATH}"
      --set-string "cmf.artifacts.maxUploadSize=${ARTIFACTS_MAX_UPLOAD_SIZE}"
    )
    case "$CLOUD" in
      aws)
        set_args+=(
          --set-string 'cmf.artifacts.configuration.fs\.s3a\.access\.key=${AWS_ACCESS_KEY_ID}'
          --set-string 'cmf.artifacts.configuration.fs\.s3a\.secret\.key=${AWS_SECRET_ACCESS_KEY}'
          --set "extraEnv[0].name=AWS_ACCESS_KEY_ID"
          --set "extraEnv[0].valueFrom.secretKeyRef.name=${ARTIFACTS_CREDS_SECRET}"
          --set "extraEnv[0].valueFrom.secretKeyRef.key=aws-access-key-id"
          --set "extraEnv[1].name=AWS_SECRET_ACCESS_KEY"
          --set "extraEnv[1].valueFrom.secretKeyRef.name=${ARTIFACTS_CREDS_SECRET}"
          --set "extraEnv[1].valueFrom.secretKeyRef.key=aws-secret-access-key"
        )
        ;;
      gcp)
        set_args+=(
          --set-string 'cmf.artifacts.configuration.fs\.gs\.auth\.type=SERVICE_ACCOUNT_JSON_KEYFILE'
          --set-string 'cmf.artifacts.configuration.fs\.gs\.auth\.service\.account\.json\.keyfile=${GCS_KEY_FILE}'
          --set-string "cmf.artifacts.configuration.fs\.gs\.project\.id=${PROJECT}"
          --set "extraEnv[0].name=GCS_KEY_FILE"
          --set-string "extraEnv[0].value=/mnt/gcs/gcs-key.json"
          --set "mountedVolumes.volumes[0].name=gcs-creds"
          --set "mountedVolumes.volumes[0].secret.secretName=${ARTIFACTS_CREDS_SECRET}"
          --set "mountedVolumes.volumeMounts[0].name=gcs-creds"
          --set-string "mountedVolumes.volumeMounts[0].mountPath=/mnt/gcs"
          --set "mountedVolumes.volumeMounts[0].readOnly=true"
        )
        ;;
    esac
  fi
  log "CMF feature flags: environmentCatalog=$CMF_ENVIRONMENT_CATALOG_ENABLED mcp=$CMF_MCP_ENABLED (writeTools=$CMF_MCP_WRITE_TOOLS_ENABLED) artifacts=$CMF_ARTIFACTS_ENABLED"
  helm upgrade --install cmf confluentinc/confluent-manager-for-apache-flink \
    --version "$CMF_VERSION" \
    "${set_args[@]}" \
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
  # When artifacts are on, inject the Flink-cluster storage access into clusterSpec so
  # this pool can fetch cmf:// UDF/artifact JARs (no-op passthrough otherwise).
  local rendered; rendered=$(render_spec_to_tmp .spec.clusterSpec "$file")
  # Check existence first so the create-vs-update branch (and its message) is accurate.
  if confluent flink compute-pool describe --environment "$env" "$name" >/dev/null 2>&1; then
    # Exists -> update via REST (the CLI has no `compute-pool update`).
    if curl -sf -X PUT -H "Content-Type: application/json" -d "@${rendered}" \
        "${CONFLUENT_CMF_URL}/cmf/api/v1/environments/${env}/compute-pools/${name}" >/dev/null; then
      log "Compute pool '$name' already existed in '$env' - updated it to match $file"
    else
      warn "compute pool '$name' exists in '$env' but couldn't be updated - it likely has active" \
        "statements in a non-updatable phase (stop them first: confluent --environment $env flink statement stop <name>)"
    fi
  else
    # Doesn't exist -> create. Retry with backoff: right after environment/catalog setup the
    # CLI create can transiently fail before CMF is ready to accept it (seen on both clouds).
    local i cperr
    cperr=$(mktemp)
    for i in 1 2 3 4 5 6; do
      if confluent flink compute-pool create "$rendered" --environment "$env" >"$cperr" 2>&1; then
        rm -f "$cperr"; cperr=""; break
      fi
      sleep 5
    done
    [[ -n "$cperr" ]] && { warn "failed to create compute pool '$name' in '$env' after retries: $(tail -1 "$cperr")"; rm -f "$cperr"; }
  fi
  # Not just a style choice: this being a bare `&&` as the function's last statement
  # means a false test (the common case, rendered==file) makes the function return 1,
  # which - since the call site uses it as a plain statement - kills the whole script
  # under `set -e`. Must stay an if/fi.
  if [[ "$rendered" != "$file" ]]; then rm -rf "$(dirname "$rendered")"; fi
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

  # By default every step runs on the SHARED pool - it's much faster since there's no
  # per-job cold start, and the bounded data load takes the grace-period path below
  # (SHARED pools don't report COMPLETED for bounded jobs). Pass a pool name as the 2nd
  # positional arg to force a different pool for every step, e.g. `statement prod pool`
  # to run the data load on the DEDICATED pool, where completion is verifiable.
  local ddl_pool="${2:-$SHARED_COMPUTE_POOL_NAME}"
  local insert_pool="${2:-$SHARED_COMPUTE_POOL_NAME}"

  log "Creating table 'demo_events' in '$env' (compute pool: $ddl_pool)"
  phase=$(run_pipeline_statement "$env" create-demo-events "$ddl_pool" "$CREATE_EVENTS_SQL_FILE" 60 COMPLETED) || true
  [[ "$phase" == "COMPLETED" ]] || die "creating demo_events failed (phase=$phase) - check: confluent --environment $env flink statement describe create-demo-events"
  delete_statement_quiet "$env" create-demo-events

  log "Creating table 'demo_aggregated' in '$env' (compute pool: $ddl_pool)"
  phase=$(run_pipeline_statement "$env" create-demo-aggregated "$ddl_pool" "$CREATE_AGGREGATED_SQL_FILE" 60 COMPLETED) || true
  [[ "$phase" == "COMPLETED" ]] || die "creating demo_aggregated failed (phase=$phase) - check: confluent --environment $env flink statement describe create-demo-aggregated"
  delete_statement_quiet "$env" create-demo-aggregated

  # One-shot bounded data load. On a DEDICATED pool it reports COMPLETED reliably
  # (just slower, since it cold-starts its own cluster). On a SHARED pool bounded
  # jobs actually finish but get stuck reporting a non-terminal phase forever (an
  # operator status-reporting quirk - see README troubleshooting), so there's
  # nothing reliable to poll - we give it a short grace period and move on. The
  # pool type is looked up so an overridden pool is handled correctly either way.
  local insert_pool_type
  insert_pool_type=$(confluent flink compute-pool describe --environment "$env" "$insert_pool" -o json 2>/dev/null | jq -r '.spec.type // empty') || true
  [[ -n "$insert_pool_type" ]] || die "compute pool '$insert_pool' not found in environment '$env'"
  [[ -f "$INSERT_DEMO_DATA_SQL_FILE" ]] || die "SQL file not found: $INSERT_DEMO_DATA_SQL_FILE"
  log "Inserting demo data into 'demo_events' in '$env' (compute pool: $insert_pool, type: $insert_pool_type)"
  delete_statement_quiet "$env" insert-demo-data
  confluent --environment "$env" flink statement create insert-demo-data \
    --catalog "$CATALOG_NAME" \
    --database "$DATABASE_NAME" \
    --compute-pool "$insert_pool" \
    --parallelism 1 \
    --sql "$(cat "$INSERT_DEMO_DATA_SQL_FILE")" >/dev/null
  if [[ "$insert_pool_type" == "SHARED" ]]; then
    sleep 10
    delete_statement_quiet "$env" insert-demo-data
    warn "insert ran on SHARED pool '$insert_pool' - completion isn't verifiable, but should already be done"
  else
    phase=$(wait_for_statement_phase "$env" insert-demo-data 150 COMPLETED) || true
    [[ "$phase" == "COMPLETED" ]] || die "inserting demo data failed (phase=$phase) - check: confluent --environment $env flink statement describe insert-demo-data"
    delete_statement_quiet "$env" insert-demo-data
  fi

  log "Starting continuous streaming aggregation '$STATEMENT_NAME' in '$env' (compute pool: $ddl_pool)"
  if confluent --environment "$env" flink statement describe "$STATEMENT_NAME" >/dev/null 2>&1; then
    warn "statement '$STATEMENT_NAME' already exists in '$env' - leaving it as is"
    return 0
  fi
  [[ -f "$STREAMING_SQL_FILE" ]] || die "SQL file not found: $STREAMING_SQL_FILE"
  confluent --environment "$env" flink statement create "$STATEMENT_NAME" \
    --catalog "$CATALOG_NAME" \
    --database "$DATABASE_NAME" \
    --compute-pool "$ddl_pool" \
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
  # When artifacts are on, inject Flink-cluster storage access into .spec so an app
  # whose job.jarURI is cmf://... can fetch it (no-op passthrough otherwise).
  local rendered; rendered=$(render_spec_to_tmp .spec "$file")
  log "Creating Flink application from $file in environment '$env'"
  confluent flink application create "$rendered" --environment "$env" \
    || warn "application may already exist in '$env'"
  if [[ "$rendered" != "$file" ]]; then rm -rf "$(dirname "$rendered")"; fi
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
  if [[ "$CMF_ARTIFACTS_ENABLED" == "true" ]]; then cmd_artifacts; fi
  cmd_cmf
  cmd_port_forward
  cmd_c3_forward
  cmd_flink_environments
  cmd_catalog
  cmd_compute_pool
  for env in $FLINK_ENVIRONMENTS; do
    cmd_verify "$env"
  done
  log "Environment is up. CMF: ${CONFLUENT_CMF_URL} - Control Center: http://localhost:${C3_LOCAL_PORT}/home"
  log "Try: ./demo.sh statement, then ./demo.sh status"
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
  # Read which SA key we minted (recorded as annotations on the creds secret) BEFORE the
  # cluster - and its secret - are deleted, so teardown can remove exactly that key.
  local gcs_key_id="" gcs_sa=""
  if command -v jq >/dev/null 2>&1 && kubectl -n "$CONFLUENT_NAMESPACE" get secret "$ARTIFACTS_CREDS_SECRET" >/dev/null 2>&1; then
    local ann
    ann=$(kubectl -n "$CONFLUENT_NAMESPACE" get secret "$ARTIFACTS_CREDS_SECRET" -o json 2>/dev/null)
    gcs_key_id=$(echo "$ann" | jq -r '.metadata.annotations["cpf.artifacts/gcs-key-id"] // empty')
    gcs_sa=$(echo "$ann" | jq -r '.metadata.annotations["cpf.artifacts/gcs-sa"] // empty')
  fi
  log "Deleting GKE cluster '$CLUSTER_NAME'"
  gcloud container clusters delete "$CLUSTER_NAME" --zone "$ZONE" --project "$PROJECT" --quiet
  teardown_artifacts_gcp "$gcs_key_id" "$gcs_sa"
}

# Best-effort teardown of the artifact bucket, minted SA key, and (if we created one)
# service account. Only touches resources that exist. Args: the key id + SA email we
# minted (captured from the secret above) - deleting just that key is what makes the
# reused-SA path clean without deleting someone else's shared SA.
teardown_artifacts_gcp() {
  local minted_key_id="${1:-}" minted_sa="${2:-}"
  local abucket="${ARTIFACTS_BUCKET_PREFIX}-${CFLT_USER}"
  local sa_email="${abucket}@${PROJECT}.iam.gserviceaccount.com"
  if gcloud storage buckets describe "gs://${abucket}" --project "$PROJECT" >/dev/null 2>&1; then
    log "Deleting artifact GCS bucket 'gs://${abucket}'"
    gcloud storage rm --recursive "gs://${abucket}/**" --project "$PROJECT" 2>/dev/null || true
    gcloud storage buckets delete "gs://${abucket}" --project "$PROJECT" 2>/dev/null \
      || warn "could not delete bucket 'gs://${abucket}' - delete manually: gcloud storage rm --recursive gs://${abucket}"
  fi
  # Delete exactly the key we minted (covers the reused-SA case, where the SA below
  # doesn't exist and must be left alone).
  if [[ -n "$minted_key_id" && -n "$minted_sa" ]]; then
    log "Deleting minted SA key '${minted_key_id}' on '${minted_sa}'"
    gcloud iam service-accounts keys delete "$minted_key_id" --iam-account="$minted_sa" --project "$PROJECT" --quiet 2>/dev/null \
      || warn "could not delete SA key '${minted_key_id}' on '${minted_sa}' - delete manually: gcloud iam service-accounts keys delete ${minted_key_id} --iam-account=${minted_sa}"
  fi
  # Delete the script-created SA (exists only when we minted one, not in reuse mode);
  # this also removes any remaining keys on it.
  if gcloud iam service-accounts describe "$sa_email" --project "$PROJECT" >/dev/null 2>&1; then
    log "Deleting artifact service account '$sa_email'"
    gcloud iam service-accounts delete "$sa_email" --project "$PROJECT" --quiet 2>/dev/null \
      || warn "could not delete SA '$sa_email'"
  fi
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

  teardown_artifacts_aws
}

# Best-effort teardown of the artifact S3 bucket + IAM user (only if they exist); the
# K8s secret goes away with the cluster. Access keys and the inline policy must be
# removed before the user can be deleted.
teardown_artifacts_aws() {
  local abucket="${ARTIFACTS_BUCKET_PREFIX}-${CFLT_USER}" k
  if aws s3api head-bucket --bucket "$abucket" --region "$EKS_REGION" >/dev/null 2>&1; then
    log "Deleting artifact S3 bucket '$abucket'"
    aws s3 rb "s3://${abucket}" --force --region "$EKS_REGION" >/dev/null 2>&1 \
      || warn "could not fully empty/delete bucket '$abucket' - delete manually: aws s3 rb s3://${abucket} --force"
  fi
  if aws iam get-user --user-name "$abucket" >/dev/null 2>&1; then
    log "Deleting artifact IAM user '$abucket'"
    for k in $(aws iam list-access-keys --user-name "$abucket" --query 'AccessKeyMetadata[].AccessKeyId' --output text 2>/dev/null); do
      aws iam delete-access-key --user-name "$abucket" --access-key-id "$k" 2>/dev/null || true
    done
    aws iam delete-user-policy --user-name "$abucket" --policy-name cpf-artifacts-s3 2>/dev/null || true
    aws iam delete-user --user-name "$abucket" 2>/dev/null || warn "could not delete IAM user '$abucket'"
  fi
}

cmd_help() {
  cat <<'EOF'
Usage: ./demo.sh --gcp|--aws --user <cflt-username> <subcommand> [args]

Cloud selection (required for every subcommand except help/-h/--help):
  --gcp                    Use GKE
  --aws                    Use EKS
  --user <name>            Required for both clouds. Names your cluster
                           (<prefix>-<name>, e.g. cpf-gke-demo-myusername /
                           cpf-eks-demo-myusername) so people sharing a project/
                           account never collide, and tags every cloud resource
                           this script creates (cflt_managed_by=user,
                           cflt_managed_id=<name>). Use the part of your
                           @confluent.io email before the @.
  Flags may appear anywhere on the command line, e.g.:
    ./demo.sh --gcp --user myusername up
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
  artifacts               Provision blob storage for CMF 2.4 artifacts (per-user bucket +
                          scoped static creds in a K8s secret). Only when
                          CMF_ARTIFACTS_ENABLED=true; `up` runs it before cmf. See config.sh.
  port-forward            Start a background port-forward to cmf-service:8080
  stop-port-forward       Stop all background port-forwards started by this script
  flink-environments      Create the prod/test Flink environments in CMF
  catalog                 Create the Kafka catalog + database (flink/catalogv2.json, databasev2.json)
  compute-pool            Create the DEDICATED pool (flink/compute-pool.json) and SHARED pool
                          (flink/compute-pool-shared.json) in prod and test
  verify [env]            Create/describe/drop a disposable table on the shared pool to
                          sanity-check the environment (default env: prod)
  statement [env] [pool]  Create demo_events/demo_aggregated, seed demo data, and start the
                          continuous windowed-aggregation job (default env: prod). By default
                          every step runs on shared-pool (fast - no cold start); pass a pool
                          name to force a different pool for all steps, e.g.
                          "statement prod pool" runs the data load on the DEDICATED pool
                          (slower, but confirms COMPLETED).
  generate-data [env] [count] [pool]  Insert more random rows into demo_events on demand
                          (default env: prod, count: 20, pool: shared-pool -
                          pass e.g. "pool" as 3rd arg to use the DEDICATED
                          pool instead, which is slower but confirms COMPLETED)
  application [env] [file] Deploy a simple FlinkApplication (default env: prod, default file: cpf_basic_app.json)

Utilities:
  c3-forward              Start a background port-forward to Control Center on :9021
  cmf-ui                  Ensure the CMF port-forward and open the CMF 2.4 web UI
                          (served at the root of :8080, same port as the REST API)
  status                  Show pod status and Flink environments
  help                    Show this message

Config can be overridden via environment variables, e.g.:
  PROJECT=my-proj ZONE=us-central1-a GKE_CLUSTER_NAME_PREFIX=my-demo ./demo.sh --gcp --user myusername cluster
  EKS_REGION=us-east-1 EKS_CLUSTER_NAME_PREFIX=my-demo ./demo.sh --aws --user myusername cluster
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
  [[ -n "$CLOUD" ]] || die "must specify a cloud: pass exactly one of --gcp or --aws (e.g. './demo.sh --gcp --user myusername up' or './demo.sh --aws --user myusername up')"
  [[ -z "$CFLT_USER" ]] && die "--user <cflt-username> is required (the part of your @confluent.io email before the @, e.g. --user myusername) - it names your cluster (<prefix>-<user>) and tags every cloud resource this script creates"
  # --user becomes part of the GKE/EKS cluster name (<prefix>-<user>) and a GCP
  # label value, so it must satisfy the strictest of those: lowercase letters,
  # digits, and internal hyphens only (no dots, underscores, uppercase, or
  # leading/trailing hyphen). Validate up front rather than letting gcloud/eksctl
  # reject the derived name with a cryptic error later. Note: an email local part
  # like 'first.last' is NOT valid - drop the dot (e.g. --user firstlast).
  [[ "$CFLT_USER" =~ ^[a-z0-9]([a-z0-9-]*[a-z0-9])?$ ]] \
    || die "invalid --user '$CFLT_USER': use only lowercase letters, digits, and internal hyphens (no dots, underscores, uppercase, or leading/trailing hyphen) - it must be valid in a cluster name and a GCP label, e.g. --user firstlast"
  # Fail fast with a clear message if the AWS session is missing/expired, rather than
  # letting some kubectl/eksctl call deep inside a subcommand fail confusingly later
  # (e.g. a stale SSO token surfacing as a kubectl port-forward or confluent CLI error).
  [[ "$CLOUD" == "aws" ]] && require_aws_identity
fi

# The real cluster name is always "<prefix>-<user>", so people sharing a GCP
# project or AWS account/region never collide on the same cluster.
[[ "$CLOUD" == "gcp" ]] && CLUSTER_NAME="${GKE_CLUSTER_NAME_PREFIX}-${CFLT_USER}"
[[ "$CLOUD" == "aws" ]] && EKS_CLUSTER_NAME="${EKS_CLUSTER_NAME_PREFIX}-${CFLT_USER}"

# Artifact storage bucket is also "<prefix>-<user>"; derive the basePath scheme from
# the cloud unless the user brought their own CMF_ARTIFACTS_BASE_PATH.
ARTIFACTS_BUCKET=""
if [[ -n "$CLOUD" ]]; then
  ARTIFACTS_BUCKET="${ARTIFACTS_BUCKET_PREFIX}-${CFLT_USER}"
  if [[ -z "$CMF_ARTIFACTS_BASE_PATH" ]]; then
    case "$CLOUD" in
      gcp) CMF_ARTIFACTS_BASE_PATH="gs://${ARTIFACTS_BUCKET}/cmf" ;;
      aws) CMF_ARTIFACTS_BASE_PATH="s3://${ARTIFACTS_BUCKET}/cmf" ;;
    esac
  fi
fi

case "$sub" in
  up) cmd_up "$@" ;;
  down) cmd_down "$@" ;;
  cluster) cmd_cluster "$@" ;;
  helm-repo) cmd_helm_repo "$@" ;;
  namespace) cmd_namespace "$@" ;;
  operator) cmd_operator "$@" ;;
  kafka) cmd_kafka "$@" ;;
  c3-forward) cmd_c3_forward "$@" ;;
  cmf-ui) cmd_cmf_ui "$@" ;;
  cert-manager) cmd_cert_manager "$@" ;;
  flink-operator) cmd_flink_operator "$@" ;;
  cmf) cmd_cmf "$@" ;;
  artifacts) cmd_artifacts "$@" ;;
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
