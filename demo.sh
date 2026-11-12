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
SECURITY_USERS_FILE=".demo-security-users"   # <user>=<password> lines, gitignored
KAFKA_USERS_FILE=".demo-kafka-users"         # Kafka SASL <user>=<password> lines, gitignored

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

# Run the confluent CLI with an isolated HOME (see CONFLUENT_CLI_HOME in config.sh)
# so a Confluent Cloud/Platform context selected in the user's own CLI config can't
# block `confluent flink`, and so the script never has to delete the user's contexts.
# Only `confluent` gets the alternate HOME - gcloud/kubectl/helm keep the real one.
confluent() {
  mkdir -p "$CONFLUENT_CLI_HOME"
  confluent_login_admin_if_needed
  HOME="$CONFLUENT_CLI_HOME" command confluent "$@"
}

# With CMF security on, every CMF call needs a bearer token, so the script's own CLI home
# is logged in (via `confluent login --url <cmf>`) as the super user. Tokens last 6h; a
# marker file keeps this to one login per hour instead of one per CLI call (many calls run
# in subshells, where a plain shell variable wouldn't carry over).
confluent_login_admin_if_needed() {
  [[ "$CMF_SECURITY_ENABLED" == "true" ]] || return 0
  local marker="$CONFLUENT_CLI_HOME/.mds-login" super="${CMF_SECURITY_USERS%% *}"
  [[ -n "$(find "$marker" -mmin -60 2>/dev/null)" ]] && return 0
  if HOME="$CONFLUENT_CLI_HOME" CONFLUENT_PLATFORM_USERNAME="$super" \
       CONFLUENT_PLATFORM_PASSWORD="$(security_password "$super")" \
       command confluent login --url "$CONFLUENT_CMF_URL" >/dev/null 2>&1; then
    touch "$marker"
  else
    warn "could not log the confluent CLI in to CMF as '$super' - is the CMF port-forward up and security enabled?"
  fi
}

# Preflight for every CMF-talking subcommand: probes `confluent flink environment
# list`. (`confluent flink` refuses to run while a Cloud/Platform context is selected,
# but the isolated HOME in the `confluent` wrapper above keeps user contexts out of
# it, so a failure here normally means CMF itself is unreachable - hence the
# port-forward hint.)
require_cmf_access() {
  confluent flink environment list >/dev/null 2>&1 \
    || die "confluent CLI can't reach CMF - make sure the CMF port-forward is up and \$CONFLUENT_CMF_URL ($CONFLUENT_CMF_URL) is reachable (check: kubectl -n $CONFLUENT_NAMESPACE get pods). The script runs the CLI with an isolated config dir ($CONFLUENT_CLI_HOME), so your own Confluent contexts are not the cause."
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
# ---------------------------------------------------------------------------
# Kafka security demo (SASL/PLAIN + ACLs) - see KAFKA_SECURITY_ENABLED in config.sh
# ---------------------------------------------------------------------------

# Kafka user for a Flink environment (underscore: usable as a JAAS option name).
kafka_env_user() { printf 'flink_%s' "$1"; }

# Password of a Kafka SASL user, generated and persisted on first use.
kafka_password() {
  local user="$1" pw
  touch "$KAFKA_USERS_FILE"; chmod 600 "$KAFKA_USERS_FILE"
  pw=$(awk -F= -v u="$user" '$1==u{sub(/^[^=]*=/,""); print; exit}' "$KAFKA_USERS_FILE")
  if [[ -z "$pw" ]]; then
    pw=$(openssl rand -hex 8)
    echo "${user}=${pw}" >> "$KAFKA_USERS_FILE"
  fi
  printf '%s' "$pw"
}

# All Kafka SASL users: the brokers' own identity, the platform components, one per Flink environment.
kafka_all_users() {
  local e; printf 'kafka sr c3'
  for e in $FLINK_ENVIRONMENTS; do printf ' %s' "$(kafka_env_user "$e")"; done
}

# `user_<name>="<pw>" ...` entries of the broker-side PlainLoginModule.
kafka_jaas_user_entries() {
  local u out=""
  for u in $(kafka_all_users); do out="${out} user_${u}=\"$(kafka_password "$u")\""; done
  printf '%s' "${out# }"
}

# Text blocks injected into cp/cp.yaml at the @@KAFKA_SECURITY_*@@ markers (6-space list items).
kafka_security_block() {
  local plain="org.apache.kafka.common.security.plain.PlainLoginModule"
  local kpw; kpw=$(kafka_password kafka)
  case "$1" in
    CONTROLLER)
      cat <<EOF
      - listener.security.protocol.map=CONTROLLER:SASL_PLAINTEXT,REPLICATION:PLAINTEXT
      - sasl.enabled.mechanisms=PLAIN
      - sasl.mechanism.controller.protocol=PLAIN
      - listener.name.controller.plain.sasl.jaas.config=${plain} required user_kafka="${kpw}";
      - authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer
      - super.users=User:kafka
EOF
      ;;
    BROKER)
      local entries; entries=$(kafka_jaas_user_entries)
      cat <<EOF
      - listener.security.protocol.map=BROKER:SASL_PLAINTEXT,EXTERNAL:SASL_PLAINTEXT,CONTROLLER:SASL_PLAINTEXT
      - sasl.enabled.mechanisms=PLAIN
      - sasl.mechanism.inter.broker.protocol=PLAIN
      - sasl.mechanism.controller.protocol=PLAIN
      - listener.name.broker.plain.sasl.jaas.config=${plain} required username="kafka" password="${kpw}" ${entries};
      - listener.name.external.plain.sasl.jaas.config=${plain} required username="kafka" password="${kpw}" ${entries};
      - listener.name.controller.plain.sasl.jaas.config=${plain} required username="kafka" password="${kpw}";
      - authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer
      - super.users=User:kafka;User:sr;User:c3
EOF
      ;;
    SR|C3)
      local u; u=$(printf '%s' "$1" | tr 'A-Z' 'a-z')
      cat <<EOF
      authentication:
        type: plain
        jaasConfig:
          secretRef: kafka-${u}-client
EOF
      ;;
  esac
}

# Replaces each @@KAFKA_SECURITY_<X>@@ marker line with its block, when Kafka security is on
# (the markers are plain YAML comments otherwise, so the manifest stays valid either way).
kafka_security_inject() {
  if [[ "$KAFKA_SECURITY_ENABLED" != "true" ]]; then cat; return 0; fi
  require_cmds openssl awk
  local d m; d=$(mktemp -d)
  for m in CONTROLLER BROKER SR C3; do kafka_security_block "$m" > "$d/$m"; done
  awk -v dir="$d" '
    /@@KAFKA_SECURITY_[A-Z0-9]+@@/ {
      m=$0; sub(/.*@@KAFKA_SECURITY_/,"",m); sub(/@@.*/,"",m)
      f=dir "/" m
      while ((getline line < f) > 0) print line
      close(f); next
    }
    { print }'
  rm -rf "$d"
}

# Client-credential secrets Schema Registry and Control Center read (plain.txt = username/password).
ensure_kafka_client_secrets() {
  local who
  for who in sr c3; do
    printf 'username=%s\npassword=%s\n' "$who" "$(kafka_password "$who")" > "/tmp/kafka-${who}-plain.txt.$$"
    kubectl -n "$CONFLUENT_NAMESPACE" create secret generic "kafka-${who}-client" \
      --from-file=plain.txt="/tmp/kafka-${who}-plain.txt.$$" --dry-run=client -o yaml \
      | kubectl -n "$CONFLUENT_NAMESPACE" apply -f - >/dev/null
    rm -f "/tmp/kafka-${who}-plain.txt.$$"
  done
}

render_kafka_manifest() {
  sed \
    -e "s#\${CP_SERVER_VERSION}#${CP_SERVER_VERSION}#g" \
    -e "s#\${INIT_CONTAINER_VERSION}#${INIT_CONTAINER_VERSION}#g" \
    -e "s#\${SCHEMA_REGISTRY_VERSION}#${SCHEMA_REGISTRY_VERSION}#g" \
    -e "s#\${CONTROL_CENTER_VERSION}#${CONTROL_CENTER_VERSION}#g" \
    -e "s#\${PROMETHEUS_VERSION}#${PROMETHEUS_VERSION}#g" \
    -e "s#\${ALERTMANAGER_VERSION}#${ALERTMANAGER_VERSION}#g" \
    "$1" | kafka_security_inject
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

# Stronger than is_port_listening: returns success only when the port completes an
# HTTP round trip (curl exit 0). A kubectl port-forward whose target pod has
# restarted often keeps the local socket open but no longer forwards - that answers
# is_port_listening (not "connection refused") yet fails here, which is exactly the
# state that left cmf-ui/c3-ui opening a browser against a dead port.
is_port_serving() {
  curl -s -o /dev/null --max-time 2 "http://localhost:$1/" 2>/dev/null
}

# Ensures a WORKING port-forward on $port: returns fast if the port already serves,
# otherwise reaps any stale/broken forward we started and (re)starts kubectl
# port-forward, waiting until it actually serves before returning. Verifying
# reachability - not just PID liveness - is what stops the callers from opening a
# browser against a forward kubectl kept "running" but stopped forwarding.
# Args: <pid-file> <local-port> <kubectl-target> <port-mapping> <logfile> <label>
ensure_port_forward() {
  local pid_file="$1" port="$2" target="$3" mapping="$4" logfile="$5" label="$6"
  if is_port_serving "$port"; then
    return 0
  fi
  # Reap a tracked forward that is dead, or alive-but-not-forwarding (broken).
  if [[ -f "$pid_file" ]]; then
    kill "$(cat "$pid_file")" 2>/dev/null || true
    rm -f "$pid_file"
  fi
  log "Starting background port-forward: ${target} -> localhost:${port} (${label})"
  kubectl -n "$CONFLUENT_NAMESPACE" port-forward "$target" "$mapping" >"$logfile" 2>&1 &
  echo $! > "$pid_file"
  # Poll until it actually answers (pod/image cold start can take several seconds).
  local waited=0
  while (( waited < 30 )); do
    kill -0 "$(cat "$pid_file")" 2>/dev/null \
      || { rm -f "$pid_file"; die "${label} port-forward exited immediately, see ${logfile}"; }
    is_port_serving "$port" && return 0
    sleep 1; waited=$((waited + 1))
  done
  die "${label} port-forward did not become reachable on localhost:${port} within 30s, see ${logfile}"
}

ensure_cmf_port_forward() {
  ensure_port_forward "$PORT_FORWARD_PID_FILE" "$CMF_LOCAL_PORT" \
    service/cmf-service "${CMF_LOCAL_PORT}:80" /tmp/demo-cmf-port-forward.log "CMF"
}

ensure_c3_port_forward() {
  ensure_port_forward "$C3_PORT_FORWARD_PID_FILE" "$C3_LOCAL_PORT" \
    controlcenter-ng-0 "${C3_LOCAL_PORT}:9021" /tmp/demo-c3-port-forward.log "Control Center"
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

# Preflight quota check: eksctl's default VPC uses natMode Single -> it creates exactly ONE
# NAT gateway, in an AZ it picks itself. The "NAT gateways per Availability Zone"
# quota (Service Quotas: service `vpc`, code L-FE5A380F, default 5) counts every
# NAT gateway in that AZ across ALL VPCs in the account/region, so a brand-new
# eksctl VPC can still fail if the AZ it lands in is already full (this is exactly
# why EKS_REGION defaults away from the shared account's saturated eu-west-1).
# We flag it as exceeded only when EVERY AZ in the region is at quota (a likely
# failure) and warn (without flagging) when only some AZs are saturated, since
# eksctl might still pick a full one. Exceeded checks increment the caller's
# `quota_exceeded` counter so cmd_check_quotas_aws can prompt once at the end.
preflight_nat_quota_aws() {
  local quota_code="L-FE5A380F" limit
  # Per-AZ NAT gateway quota. Fall back to the AWS default of 5 if the API is
  # unavailable or the caller lacks servicequotas:GetServiceQuota.
  limit=$(aws service-quotas get-service-quota \
            --region "$EKS_REGION" --service-code vpc --quota-code "$quota_code" \
            --query 'Quota.Value' --output text 2>/dev/null)
  if [[ -z "$limit" || "$limit" == "None" ]]; then
    warn "could not read the NAT-gateway-per-AZ quota (servicequotas:GetServiceQuota) - assuming AWS default of 5"
    limit=5
  fi
  limit=${limit%.*}   # service-quotas returns a float like "5.0"

  # Subnets of every NAT gateway already consuming a slot (pending + available).
  local nat_subnets az_counts azs_available azs_with_room=0 total_azs=0 saturated=""
  nat_subnets=$(aws ec2 describe-nat-gateways --region "$EKS_REGION" \
                  --filter 'Name=state,Values=pending,available' \
                  --query 'NatGateways[].SubnetId' --output text 2>/dev/null)
  if [[ -z "$nat_subnets" ]]; then
    log "NAT-gateway quota check: no existing NAT gateways in $EKS_REGION (quota ${limit}/AZ) - OK"
    return 0
  fi
  # NAT gateways don't report their AZ directly, so resolve subnet -> AZ and tally.
  az_counts=$(aws ec2 describe-subnets --region "$EKS_REGION" --subnet-ids $nat_subnets \
                --query 'Subnets[].AvailabilityZone' --output text \
                | tr '\t' '\n' | sort | uniq -c)

  local az used
  azs_available=$(aws ec2 describe-availability-zones --region "$EKS_REGION" \
                    --query 'AvailabilityZones[?State==`available`].ZoneName' \
                    --output text | tr '\t' '\n')
  for az in $azs_available; do
    total_azs=$((total_azs + 1))
    used=$(printf '%s\n' "$az_counts" | awk -v z="$az" '$2==z {print $1}')
    used=${used:-0}
    if (( used < limit )); then
      azs_with_room=$((azs_with_room + 1))
    else
      saturated="${saturated:+$saturated }$az"
    fi
  done

  if (( azs_with_room == 0 )); then
    warn "NAT-gateway quota EXCEEDED: every AZ in $EKS_REGION is at the quota (${limit}/AZ) - eksctl likely cannot create its NAT gateway. Request a quota increase (Service Quotas: vpc / $quota_code), release an existing NAT gateway, or set EKS_REGION to another region."
    quota_exceeded=$(( ${quota_exceeded:-0} + 1 ))
    quota_exceeded_list="${quota_exceeded_list:+$quota_exceeded_list, }NAT gateways per AZ"
    return 0
  fi
  if [[ -n "$saturated" ]]; then
    warn "NAT-gateway preflight: $azs_with_room/$total_azs AZs have headroom (quota ${limit}/AZ); saturated: $saturated. eksctl picks its own AZ (natMode Single) and could still land in a full one - if create fails, request a quota increase or pin an AZ with room."
  else
    log "NAT-gateway preflight: all $total_azs AZs in $EKS_REGION have headroom (quota ${limit}/AZ) - OK"
  fi
}

# Generic per-region quota preflight checks for the region-wide resources every eksctl
# cluster adds exactly one of. Unlike the NAT-gateway quota (per-AZ), these are a
# single region-wide count, so the check is just: (in use + need) <= quota. When it
# wouldn't fit we warn and increment the caller's `quota_exceeded` counter (rather
# than aborting) so cmd_check_quotas_aws can prompt once at the end. Falls back to the
# AWS default of 5 if servicequotas:GetServiceQuota is unavailable/denied.
# Args: <label> <service-code> <quota-code> <in-use count> <need> <fix-hint>
preflight_region_quota_aws() {
  local label="$1" service="$2" quota_code="$3" used="$4" need="$5" hint="$6" limit
  used=${used:-0}
  limit=$(aws service-quotas get-service-quota \
            --region "$EKS_REGION" --service-code "$service" --quota-code "$quota_code" \
            --query 'Quota.Value' --output text 2>/dev/null)
  if [[ -z "$limit" || "$limit" == "None" ]]; then
    warn "could not read the '$label' quota ($service/$quota_code) - assuming AWS default of 5"
    limit=5
  fi
  limit=${limit%.*}   # service-quotas returns a float like "5.0"
  if (( used + need > limit )); then
    warn "'$label' quota EXCEEDED in $EKS_REGION: $used/$limit already in use and eksctl needs $need more. $hint"
    quota_exceeded=$(( ${quota_exceeded:-0} + 1 ))
    quota_exceeded_list="${quota_exceeded_list:+$quota_exceeded_list, }$label"
    return 0
  fi
  log "$label quota check: $used/$limit in use in $EKS_REGION, eksctl needs $need more - OK"
}

# Cloud-agnostic quota preflight, run as the FIRST step of `up` so we fail fast
# (before creating any cloud resources) when a shared account/project doesn't have
# the headroom for another per-user cluster. AWS is implemented; GCP is a no-op stub.
cmd_check_quotas() {
  case "$CLOUD" in
    gcp) cmd_check_quotas_gcp ;;
    aws) cmd_check_quotas_aws ;;
  esac
}

# TODO(gcp): implement GCP quota preflight before `gcloud container clusters create`.
# The plausible ones for the default 3x e2-standard-4 GKE cluster (+ artifacts) are:
#   - CPUS (region): +12 vCPU per cluster (3x e2-standard-4)
#   - IN_USE_ADDRESSES (region): +3 ephemeral external IPs (one per node)
#   - SSD_TOTAL_GB / DISK_TOTAL_GB (region): node boot disks + PVCs
#   - IAM service accounts per project: +1 (already mitigated via ARTIFACTS_GCS_SA)
# Query with: gcloud compute regions describe "${ZONE%-*}" --project "$PROJECT"
#   --format="value(quotas)"  (or the Cloud Quotas API for SA/IAM limits).
cmd_check_quotas_gcp() {
  # Fail fast on expired/missing gcloud credentials (otherwise `up` only dies later,
  # inside `gcloud container clusters create`, with a token-refresh error).
  gcloud auth print-access-token >/dev/null 2>&1 \
    || die "gcloud credentials are missing or expired - run: gcloud auth login"
  warn "GCP quota preflight not implemented yet - skipping (see TODO in cmd_check_quotas_gcp)"
}

# Every `eksctl create cluster` (default config) builds a brand-new VPC with its
# own internet gateway plus one NAT gateway that consumes one Elastic IP - so each
# concurrent demo user needs one free slot in each of these region-wide quotas
# (all default to 5). Each sub-check WARNS (rather than aborting) when a quota is
# exceeded and bumps `quota_exceeded`; if any were, we ask the user whether to
# proceed regardless (default "n"), since creating the cluster may fail and leave a
# half-built CloudFormation stack to clean up.
cmd_check_quotas_aws() {
  require_cmds aws
  local vpc_used igw_used eip_used quota_exceeded=0 quota_exceeded_list="" reply
  vpc_used=$(aws ec2 describe-vpcs --region "$EKS_REGION" \
               --query 'length(Vpcs)' --output text 2>/dev/null)
  igw_used=$(aws ec2 describe-internet-gateways --region "$EKS_REGION" \
               --query 'length(InternetGateways)' --output text 2>/dev/null)
  eip_used=$(aws ec2 describe-addresses --region "$EKS_REGION" \
               --query 'length(Addresses)' --output text 2>/dev/null)

  preflight_region_quota_aws "VPCs per region" vpc L-F678F1CE "$vpc_used" 1 \
    "Request a quota increase (Service Quotas: vpc / L-F678F1CE), delete an unused VPC, or set EKS_REGION to another region."
  preflight_region_quota_aws "Internet gateways per region" vpc L-A4707A72 "$igw_used" 1 \
    "Request a quota increase (Service Quotas: vpc / L-A4707A72), delete an unused internet gateway, or set EKS_REGION to another region."
  preflight_region_quota_aws "Elastic IPs per region" ec2 L-0263D0A3 "$eip_used" 1 \
    "Request a quota increase (Service Quotas: ec2 / L-0263D0A3), release an unused Elastic IP, or set EKS_REGION to another region."
  preflight_nat_quota_aws

  if (( quota_exceeded > 0 )); then
    warn "$quota_exceeded AWS quota check(s) exceeded: ${quota_exceeded_list}. Cluster creation may fail and leave a half-built CloudFormation stack to clean up."
    read -r -p "Proceed anyway? [y/N] " reply || reply=""
    case "$reply" in
      [yY]|[yY][eE][sS]) warn "proceeding despite quota warnings (user confirmed)" ;;
      *) echo "Aborted."; exit 1 ;;
    esac
  fi
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
  if [[ "$KAFKA_SECURITY_ENABLED" == "true" ]]; then
    log "Kafka security: SASL/PLAIN + ACLs (users: $(kafka_all_users); passwords in $KAFKA_USERS_FILE)"
    ensure_kafka_client_secrets
  fi
  render_kafka_manifest cp/cp.yaml | kubectl apply -n "$CONFLUENT_NAMESPACE" -f -
  if [[ "$KAFKA_SECURITY_ENABLED" == "true" ]]; then
    # Switching an EXISTING cluster to SASL deadlocks CFK's staged roll: it won't restart the
    # controller until it can query it, but the (already updated) broker can't register with
    # the still-plaintext controller. This is a single-controller demo, so just let the
    # StatefulSet update the controller right away. Harmless on a fresh install.
    kubectl -n "$CONFLUENT_NAMESPACE" patch sts kraftcontroller --type merge \
      -p '{"spec":{"updateStrategy":{"rollingUpdate":{"partition":0}}}}' >/dev/null 2>&1 || true
  fi
  wait_pods_ready "$CONFLUENT_NAMESPACE" 600s
}

require_kafka_security_enabled() {
  [[ "$KAFKA_SECURITY_ENABLED" == "true" ]] \
    || die "Kafka security is off - run with KAFKA_SECURITY_ENABLED=true (see config.sh)"
}

# Runs a snippet of kafka CLI commands inside kafka-0 as the `kafka` super user. The snippet
# (and the admin client config with the password) goes over stdin, so no secret lands in a
# process argument list; the config file is removed again on exit. $A = base kafka-acls call.
kafka_admin_exec() {
  local snippet="$1"
  {
    printf 'set -e\ncat > /tmp/admin.properties <<CFG\nsecurity.protocol=SASL_PLAINTEXT\nsasl.mechanism=PLAIN\n'
    printf 'sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="kafka" password="%s";\nCFG\n' "$(kafka_password kafka)"
    printf 'trap "rm -f /tmp/admin.properties" EXIT\nB=localhost:9071\nC=/tmp/admin.properties\n'
    printf '%s\n' "$snippet"
  } | kubectl -n "$CONFLUENT_NAMESPACE" exec -i kafka-0 -c kafka -- bash -s
}

# ACLs: each Flink environment's Kafka user gets full control of topics starting with
# "<env>_" and nothing else on topics. Groups / transactional ids are not data, so those are
# open to the Flink users (needed for consumer offsets and exactly-once sinks); cluster
# Describe + IdempotentWrite are what producers need. CMF's Kafka catalog also keeps table
# metadata in the shared internal topic _confluent_sr_catalog, which every environment's user
# needs (see README: it is the one place metadata of all environments meets). Safe to re-run.
cmd_kafka_acls() {
  require_kafka_security_enabled
  require_cmds openssl
  local e u snippet=""
  for e in $FLINK_ENVIRONMENTS; do
    u=$(kafka_env_user "$e")
    log "ACLs for Kafka user '$u': topics '${e}_*' only"
    snippet+="kafka-acls --bootstrap-server \$B --command-config \$C --force --add --allow-principal User:${u} --operation All --topic ${e}_ --resource-pattern-type prefixed
kafka-acls --bootstrap-server \$B --command-config \$C --force --add --allow-principal User:${u} --operation All --group '*'
kafka-acls --bootstrap-server \$B --command-config \$C --force --add --allow-principal User:${u} --operation All --transactional-id '*'
kafka-acls --bootstrap-server \$B --command-config \$C --force --add --allow-principal User:${u} --operation Describe --operation IdempotentWrite --cluster
kafka-acls --bootstrap-server \$B --command-config \$C --force --add --allow-principal User:${u} --operation Read --operation Write --operation Describe --operation DescribeConfigs --operation Create --topic _confluent_sr_catalog
"
  done
  kafka_admin_exec "$snippet" >/dev/null
  log "Kafka ACLs now:"
  kafka_admin_exec 'kafka-acls --bootstrap-server $B --command-config $C --list' | grep -E "principal|ResourcePattern" | sed 's/^/  /' | head -40
}

cmd_c3_forward() {
  ensure_c3_port_forward
  log "Control Center reachable at http://localhost:${C3_LOCAL_PORT}/home"
}

# Convenience wrapper around the CMF port-forward that frames it as the CMF 2.5
# web UI (served at the root of the CMF service, same port as the REST API) and
# opens it in a browser where possible.
cmd_cmf_ui() {
  ensure_cmf_port_forward
  local url="${CONFLUENT_CMF_URL}/"
  log "CMF web UI: ${url}  (REST API: ${CONFLUENT_CMF_URL}/cmf/api/v1)"
  command -v open >/dev/null 2>&1 && open "$url" >/dev/null 2>&1 || true
}

# C3 analog of cmd_cmf_ui: ensures the Control Center port-forward and opens the
# C3 web UI in a browser where possible.
cmd_c3_ui() {
  ensure_c3_port_forward
  local url="http://localhost:${C3_LOCAL_PORT}/home"
  log "Control Center web UI: ${url}"
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

# Provisions the blob-storage bucket + scoped static credentials CMF 2.5 needs for
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
    # MERGE into any podTemplate the spec already has (e.g. Kafka credential env vars) instead
    # of replacing it: our env/volumeMounts are appended to flink-main-container, volumes appended.
    | ($spec.podTemplate // {}) as $pt
    | (($pt.spec.containers // []) | map(select(.name != "flink-main-container"))) as $others
    | ((($pt.spec.containers // []) | map(select(.name == "flink-main-container")) | first) // {name:"flink-main-container"}) as $main
    | ($main
        | .env = ((.env // []) + ($podTemplate.spec.containers[0].env // []))
        | (if ($podTemplate.spec.containers[0].volumeMounts // []) | length > 0
           then .volumeMounts = ((.volumeMounts // []) + $podTemplate.spec.containers[0].volumeMounts) else . end)) as $newmain
    | (($pt.spec.volumes // []) + ($podTemplate.spec.volumes // [])) as $vols
    | setpath($p; $spec + {podTemplate: ($pt * {spec: ({containers: ($others + [$newmain])}
                                                      + (if ($vols | length) > 0 then {volumes: $vols} else {} end))})})
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

# ---------------------------------------------------------------------------
# CMF security demo (embedded MDS + RBAC) - see CMF_SECURITY_ENABLED in config.sh
# ---------------------------------------------------------------------------

# Prints a demo user's password, generating and persisting one on first use.
security_password() {
  local user="$1" pw
  touch "$SECURITY_USERS_FILE"; chmod 600 "$SECURITY_USERS_FILE"
  pw=$(awk -F= -v u="$user" '$1==u{sub(/^[^=]*=/,""); print; exit}' "$SECURITY_USERS_FILE")
  if [[ -z "$pw" ]]; then
    pw=$(openssl rand -hex 8)
    echo "${user}=${pw}" >> "$SECURITY_USERS_FILE"
  fi
  printf '%s' "$pw"
}

# Creates the two secrets the embedded MDS mounts: the JWT signing key pair (only once -
# regenerating it would invalidate every issued token) and users.properties (re-applied
# every time so user/password changes take effect on the next `cmf` run).
ensure_security_secrets() {
  require_cmds openssl kubectl
  ensure_namespace "$CONFLUENT_NAMESPACE"
  local d u
  d=$(mktemp -d)
  if ! kubectl -n "$CONFLUENT_NAMESPACE" get secret "$SECURITY_KEYPAIR_SECRET" >/dev/null 2>&1; then
    log "Generating MDS token key pair -> secret '$SECURITY_KEYPAIR_SECRET'"
    openssl genrsa -out "$d/tokenKeypair.pem" 2048 2>/dev/null
    openssl rsa -in "$d/tokenKeypair.pem" -outform PEM -pubout -out "$d/tokenPublicKey.pem" 2>/dev/null
    kubectl -n "$CONFLUENT_NAMESPACE" create secret generic "$SECURITY_KEYPAIR_SECRET" \
      --from-file=tokenKeypair.pem="$d/tokenKeypair.pem" \
      --from-file=tokenPublicKey.pem="$d/tokenPublicKey.pem" >/dev/null
  fi
  : > "$d/users.properties"
  for u in $CMF_SECURITY_USERS; do
    echo "${u}: $(security_password "$u"),${u}-group" >> "$d/users.properties"
  done
  log "Applying user store secret '$SECURITY_USERS_SECRET' for: $CMF_SECURITY_USERS (passwords in $SECURITY_USERS_FILE)"
  kubectl -n "$CONFLUENT_NAMESPACE" create secret generic "$SECURITY_USERS_SECRET" \
    --from-file=users.properties="$d/users.properties" --dry-run=client -o yaml \
    | kubectl -n "$CONFLUENT_NAMESPACE" apply -f - >/dev/null
  rm -rf "$d"
}

cmd_cmf() {
  log "Installing Confluent Manager for Apache Flink (CMF) $CMF_VERSION"
  # Feature flags (see config.sh). Each --set maps to an exact chart value path;
  # CMF 2.5 validates the values schema, so an unknown key fails the upgrade.
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
          # CMF 2.5 compile workers do NOT inherit extraEnv - give them their own copy
          --set "cmf.statementService.workers.extraEnv[0].name=AWS_ACCESS_KEY_ID"
          --set "cmf.statementService.workers.extraEnv[0].valueFrom.secretKeyRef.name=${ARTIFACTS_CREDS_SECRET}"
          --set "cmf.statementService.workers.extraEnv[0].valueFrom.secretKeyRef.key=aws-access-key-id"
          --set "cmf.statementService.workers.extraEnv[1].name=AWS_SECRET_ACCESS_KEY"
          --set "cmf.statementService.workers.extraEnv[1].valueFrom.secretKeyRef.name=${ARTIFACTS_CREDS_SECRET}"
          --set "cmf.statementService.workers.extraEnv[1].valueFrom.secretKeyRef.key=aws-secret-access-key"
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
          # CMF 2.5 compile workers do NOT inherit extraEnv/mountedVolumes - give them
          # their own copy of the creds (mounts must be readOnly: true)
          --set "cmf.statementService.workers.extraEnv[0].name=GCS_KEY_FILE"
          --set-string "cmf.statementService.workers.extraEnv[0].value=/mnt/gcs/gcs-key.json"
          --set "cmf.statementService.workers.mountedVolumes.volumes[0].name=gcs-creds"
          --set "cmf.statementService.workers.mountedVolumes.volumes[0].secret.secretName=${ARTIFACTS_CREDS_SECRET}"
          --set "cmf.statementService.workers.mountedVolumes.volumeMounts[0].name=gcs-creds"
          --set-string "cmf.statementService.workers.mountedVolumes.volumeMounts[0].mountPath=/mnt/gcs"
          --set "cmf.statementService.workers.mountedVolumes.volumeMounts[0].readOnly=true"
        )
        ;;
    esac
  fi
  # Authentication + RBAC through CMF's embedded MDS (opt-in). Mount indices must come
  # after any mountedVolumes entries the artifact wiring above already added (GCP uses
  # volumes[0]/volumeMounts[0] for its key), since --set replaces list entries by index.
  if [[ "$CMF_SECURITY_ENABLED" == "true" ]]; then
    ensure_security_secrets
    local mv=0 super="${CMF_SECURITY_USERS%% *}"
    [[ "$CMF_ARTIFACTS_ENABLED" == "true" && "$CLOUD" == "gcp" ]] && mv=1
    local mv2=$((mv + 1))
    set_args+=(
      --set cmf.mds.enabled=true
      --set cmf.mds.port="$CMF_MDS_PORT"
      --set cmf.mds.authentication-method=BEARER
      --set-string cmf.mds.token-key-path=/mnt/secrets/mds/tokenKeypair.pem
      --set-string cmf.mds.public-key-path=/mnt/secrets/mds/tokenPublicKey.pem
      --set cmf.mds.user-store=FILE
      --set-string cmf.mds.user-store-file-path=/mnt/secrets/mds-users/users.properties
      --set-string "cmf.mds.super-users=User:${super}"
      --set cmf.authentication.type=oauth
      --set-string 'cmf.authentication.config.public\.key\.path=/mnt/secrets/mds/tokenPublicKey.pem'
      --set-string "cmf.authentication.config.confluent\.metadata\.bootstrap\.server\.urls=http://localhost:${CMF_MDS_PORT}"
      --set cmf.authorization.authority=cmf
      --set-string "cmf.kafka.oauthbearerAllowedUrls=http://localhost:${CMF_MDS_PORT}"
      --set cmf.ui.auth.basicAuthEnabled=true
      --set cmf.ui.auth.ssoEnabled=false
      --set "mountedVolumes.volumes[${mv}].name=mds-keypair"
      --set "mountedVolumes.volumes[${mv}].secret.secretName=${SECURITY_KEYPAIR_SECRET}"
      # CMF refuses a world-readable password file (k8s secret volumes default to 0644).
      # 288 = 0440 - helm --set can't take an octal literal. Applied to the key pair too.
      --set "mountedVolumes.volumes[${mv}].secret.defaultMode=288"
      --set "mountedVolumes.volumes[${mv2}].name=mds-users"
      --set "mountedVolumes.volumes[${mv2}].secret.secretName=${SECURITY_USERS_SECRET}"
      --set "mountedVolumes.volumes[${mv2}].secret.defaultMode=288"
      --set "mountedVolumes.volumeMounts[${mv}].name=mds-keypair"
      --set-string "mountedVolumes.volumeMounts[${mv}].mountPath=/mnt/secrets/mds"
      --set "mountedVolumes.volumeMounts[${mv}].readOnly=true"
      --set "mountedVolumes.volumeMounts[${mv2}].name=mds-users"
      --set-string "mountedVolumes.volumeMounts[${mv2}].mountPath=/mnt/secrets/mds-users"
      --set "mountedVolumes.volumeMounts[${mv2}].readOnly=true"
    )
    log "CMF security: embedded MDS + RBAC enabled (super user: $super; users: $CMF_SECURITY_USERS)"
  fi
  log "CMF feature flags: environmentCatalog=$CMF_ENVIRONMENT_CATALOG_ENABLED mcp=$CMF_MCP_ENABLED (writeTools=$CMF_MCP_WRITE_TOOLS_ENABLED) artifacts=$CMF_ARTIFACTS_ENABLED"
  helm upgrade --install cmf confluentinc/confluent-manager-for-apache-flink \
    --version "$CMF_VERSION" \
    "${set_args[@]}" \
    --namespace "$CONFLUENT_NAMESPACE"
  wait_pods_ready "$CONFLUENT_NAMESPACE" 180s
}

# Prints a bearer token for a demo user (empty on failure). Tokens come from the embedded
# MDS, which is reachable on the CMF URL itself under /security/1.0/.
cmf_token() {
  curl -s -m 10 -u "$1:$(security_password "$1")" "${CONFLUENT_CMF_URL}/security/1.0/authenticate" 2>/dev/null \
    | jq -r '.auth_token // empty' 2>/dev/null || true
}

# Logs a user into the embedded MDS with the CLI, in a per-user config dir (so it can't
# disturb the script's own CLI home). Needed for `confluent iam rbac ...`.
security_cli_login() {
  local user="$1" home="${CONFLUENT_CLI_HOME}/iam-$1"
  mkdir -p "$home"
  HOME="$home" CONFLUENT_PLATFORM_USERNAME="$user" CONFLUENT_PLATFORM_PASSWORD="$(security_password "$user")" \
    command confluent login --url "$CONFLUENT_CMF_URL" >/dev/null 2>&1 \
    || die "CLI login to CMF as '$user' failed - is security enabled (CMF_SECURITY_ENABLED=true ... cmf) and the port-forward up?"
}

require_security_enabled() {
  [[ "$CMF_SECURITY_ENABLED" == "true" ]] \
    || die "CMF security is off - run with CMF_SECURITY_ENABLED=true (see config.sh), e.g. CMF_SECURITY_ENABLED=true ./demo.sh --gcp --user <you> cmf"
  require_cmds curl jq confluent
}

# Creates the role bindings: the first user in CMF_SECURITY_USERS is the super user (set via
# helm, no binding needed). Every other user <u> is bound to the Flink environment of the
# same name: ClusterAdmin scoped to that environment (full control of its statements,
# applications, compute pools, artifacts) plus DeveloperRead on the FlinkEnvironment
# resource itself (so the environment shows up in listings). Nothing is granted on any
# other environment or at CMF-cluster scope. Safe to re-run.
cmd_security() {
  require_security_enabled
  ensure_cmf_port_forward
  local super="${CMF_SECURITY_USERS%% *}" u cid
  security_cli_login "$super"
  cid=$(curl -s -m 10 -H "Authorization: Bearer $(cmf_token "$super")" "${CONFLUENT_CMF_URL}/security/1.0/metadataClusterId")
  [[ -n "$cid" ]] || die "could not read the CMF cluster id - is security enabled on the running CMF?"
  log "CMF cluster id: $cid  (super user: $super)"
  for u in ${CMF_SECURITY_USERS#"$super"}; do
    log "Binding user '$u' to Flink environment '$u'"
    HOME="${CONFLUENT_CLI_HOME}/iam-$super" command confluent iam rbac role-binding create \
      --principal "User:$u" --role ClusterAdmin --cmf "$cid" --flink-environment "$u" >/dev/null \
      || warn "ClusterAdmin binding for '$u' on environment '$u' failed (does the environment exist?)"
    HOME="${CONFLUENT_CLI_HOME}/iam-$super" command confluent iam rbac role-binding create \
      --principal "User:$u" --role DeveloperRead --cmf "$cid" --resource "FlinkEnvironment:$u" >/dev/null \
      || warn "DeveloperRead binding for '$u' on FlinkEnvironment:$u failed"
    if [[ "$KAFKA_SECURITY_ENABLED" == "true" ]]; then
      # Each user also gets to SEE (not edit) its own catalog; the other environment's catalog
      # and every CMF secret stay invisible to it.
      HOME="${CONFLUENT_CLI_HOME}/iam-$super" command confluent iam rbac role-binding create \
        --principal "User:$u" --role DeveloperRead --cmf "$cid" --resource "FlinkCatalog:$(env_catalog "$u")" >/dev/null \
        || warn "DeveloperRead binding for '$u' on FlinkCatalog:$(env_catalog "$u") failed"
    fi
  done
  log "Role bindings:"
  for u in ${CMF_SECURITY_USERS#"$super"}; do
    echo "  $u: ClusterAdmin on environment '$u' + DeveloperRead on FlinkEnvironment '$u'$([[ "$KAFKA_SECURITY_ENABLED" == "true" ]] && echo " + FlinkCatalog '$(env_catalog "$u")'")"
  done
  echo "  $super: super user (all environments)"
  log "Credentials (user=password) are in $SECURITY_USERS_FILE; log in with: confluent login --url $CONFLUENT_CMF_URL"
}

# --- security-test helpers ---------------------------------------------------
SEC_PASS=0; SEC_FAIL=0
sec_check() {  # <description> <expected> <actual>
  if [[ "$2" == "$3" ]]; then
    SEC_PASS=$((SEC_PASS + 1)); printf '  \033[1;32mPASS\033[0m  %s\n' "$1"
  else
    SEC_FAIL=$((SEC_FAIL + 1)); printf '  \033[1;31mFAIL\033[0m  %s (expected %s, got %s)\n' "$1" "$2" "$3"
  fi
}
# HTTP status class: CMF answers a denied request with 401 or 403 depending on the path.
sec_class() { case "$1" in 200|201|204) echo allowed ;; 401|403) echo denied ;; *) echo "http-$1" ;; esac; }
# <token> <method> <path> [json-body] -> prints the status code, body in $SEC_BODY_FILE
sec_http() {
  local tok="$1" method="$2" path="$3" body="${4:-}"
  local args=(-s -m 20 -X "$method" -o "$SEC_BODY_FILE" -w '%{http_code}')
  [[ -n "$tok" ]] && args+=(-H "Authorization: Bearer $tok")
  [[ -n "$body" ]] && args+=(-H 'Content-Type: application/json' -d "$body")
  curl "${args[@]}" "${CONFLUENT_CMF_URL}/cmf/api/v1/${path}" 2>/dev/null || echo 000
}

# bash 3.2 (macOS) has no associative arrays: keep each user's token in SEC_TOK_<user>.
sec_set_tok() { printf -v "SEC_TOK_$1" '%s' "$2"; }
sec_tok() { local v="SEC_TOK_$1"; printf '%s' "${!v:-}"; }

# --- Kafka security checks (only when KAFKA_SECURITY_ENABLED=true) -----------------------------
sec_kv() { grep "^$1=" "$SEC_KV_FILE" | head -1 | cut -d= -f2-; }   # value of key $1 from the remote output

# CMF side: each user sees only its own catalog, and never any secret (they hold Kafka passwords).
sec_test_catalogs() {
  local u e t names want
  log "Catalogs and secrets (CMF RBAC)"
  for u in $CMF_SECURITY_USERS; do
    t=$(sec_tok "$u"); [[ -n "$t" ]] || continue
    sec_http_raw "$t" GET catalogs/kafka >/dev/null
    names=$(jq -r '[.items[]?.metadata.name] | sort | join(",")' "$SEC_BODY_FILE")
    if [[ "$u" == "${CMF_SECURITY_USERS%% *}" ]]; then want=$(for e in $FLINK_ENVIRONMENTS; do env_catalog "$e"; echo; done | sort | paste -sd, -); else want=$(env_catalog "$u"); fi
    sec_check "'$u' sees catalogs [$want]" "$want" "$names"
    sec_http_raw "$t" GET secrets >/dev/null
    names=$(jq -r '[.items[]?.metadata.name] | join(",")' "$SEC_BODY_FILE")
    if [[ "$u" == "${CMF_SECURITY_USERS%% *}" ]]; then want=$(for e in $FLINK_ENVIRONMENTS; do echo "${e}-kafka-secret"; done | sort | paste -sd, -); names=$(printf '%s' "$names" | tr ',' '\n' | sort | paste -sd, -); else want=""; fi
    sec_check "'$u' sees secrets [${want:-none}]" "$want" "$names"
    if [[ "$u" != "${CMF_SECURITY_USERS%% *}" ]]; then
      for e in $FLINK_ENVIRONMENTS; do
        code=$(sec_http_raw "$t" GET "secrets/${e}-kafka-secret"); sec_check "'$u' reads secret '${e}-kafka-secret'" denied "$(sec_class "$code")"
        want=denied; [[ "$e" == "$u" ]] && want=allowed
        code=$(sec_http_raw "$t" GET "catalogs/kafka/$(env_catalog "$e")"); sec_check "'$u' reads catalog '$(env_catalog "$e")'" "$want" "$(sec_class "$code")"
      done
    fi
  done
}
# GET/POST against the CMF API root (not /cmf/api/v1) for catalogs/secrets paths.
sec_http_raw() { sec_http "$@"; }

# Kafka side, with each environment's own SASL credentials, run inside kafka-0 in one session.
# For every user u and topic prefix t: create / produce / consume / list. A message from each
# user is written to every prefix's probe topic, and a super-user scan afterwards proves only
# the legitimate ones landed (the console tools exit 0 on errors, so results are read from
# their output and from the topics' real contents).
sec_test_kafka() {
  local e u t d script
  log "Kafka ACLs (per-environment SASL users, topic prefixes)"
  SEC_KV_FILE=$(mktemp)
  script=$(
    printf 'cd /tmp\n'
    for u in kafka $(for e in $FLINK_ENVIRONMENTS; do printf "%s " "$(kafka_env_user "$e")"; done); do
      printf "cat > /tmp/sec-%s.p <<'CFG'\nsecurity.protocol=SASL_PLAINTEXT\nsasl.mechanism=PLAIN\nsasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";\nCFG\n" "$u" "$u" "$(kafka_password "$u")"
    done
    printf "trap 'rm -f /tmp/sec-*.p' EXIT\nB=localhost:9071\nT=\"%s\"\n" "$FLINK_ENVIRONMENTS"
    cat <<'REMOTE'
K="kafka-topics --bootstrap-server $B --command-config /tmp/sec-kafka.p"
for t in $T; do
  $K --create --if-not-exists --topic ${t}_secprobe --partitions 1 --replication-factor 1 >/dev/null 2>&1
  echo "seed-$t" | kafka-console-producer --bootstrap-server $B --producer.config /tmp/sec-kafka.p --topic ${t}_secprobe >/dev/null 2>&1
done
for u in $T; do
  f=/tmp/sec-flink_$u.p
  for t in $T; do
    out=$(kafka-topics --bootstrap-server $B --command-config $f --create --topic ${t}_secnew_$u --partitions 1 --replication-factor 1 2>&1)
    echo "create.$u.$t=$(echo "$out" | grep -qi 'authoriz' && echo denied || echo allowed)"
    out=$(echo "msg-$u" | kafka-console-producer --bootstrap-server $B --producer.config $f --topic ${t}_secprobe 2>&1)
    echo "produce.$u.$t=$(echo "$out" | grep -qiE 'AUTHORIZATION_FAILED|not authorized' && echo denied || echo allowed)"
    out=$(kafka-console-consumer --bootstrap-server $B --consumer.config $f --topic ${t}_secprobe --from-beginning --max-messages 1 --timeout-ms 12000 2>&1)
    if echo "$out" | grep -qiE 'authoriz'; then r=denied; elif echo "$out" | grep -q "^seed-$t$"; then r=allowed; else r=nodata; fi
    echo "consume.$u.$t=$r"
  done
  echo "list.$u=$(kafka-topics --bootstrap-server $B --command-config $f --list 2>/dev/null | grep -v '^_' | sed 's/_.*//' | sort -u | paste -sd, -)"
done
sleep 2
for t in $T; do
  echo "content.$t=$(kafka-console-consumer --bootstrap-server $B --consumer.config /tmp/sec-kafka.p --topic ${t}_secprobe --from-beginning --timeout-ms 8000 2>/dev/null | grep -v '^Option' | tr '\n' ' ' | sed 's/ *$//')"
done
echo "unauth=$(timeout 30 kafka-topics --bootstrap-server $B --list >/dev/null 2>&1 && echo allowed || echo denied)"
printf 'security.protocol=SASL_PLAINTEXT\nsasl.mechanism=PLAIN\nsasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="flink_%s" password="wrong";\n' "${T%% *}" > /tmp/sec-bad.p
echo "badpw=$(timeout 30 kafka-topics --bootstrap-server $B --command-config /tmp/sec-bad.p --list >/dev/null 2>&1 && echo allowed || echo denied)"
for tp in $($K --list 2>/dev/null | grep -E '_sec(probe|new_)'); do $K --delete --topic $tp >/dev/null 2>&1; done
REMOTE
  )
  printf '%s\n' "$script" | kubectl -n "$CONFLUENT_NAMESPACE" exec -i kafka-0 -c kafka -- bash -s 2>/dev/null | grep -E '^[a-z]+(\.[a-z_]+)*=' > "$SEC_KV_FILE" || true
  sec_check "no credentials is rejected by the brokers" denied "$(sec_kv unauth)"
  sec_check "wrong password is rejected by the brokers" denied "$(sec_kv badpw)"
  for u in $FLINK_ENVIRONMENTS; do
    for t in $FLINK_ENVIRONMENTS; do
      want=denied; [[ "$u" == "$t" ]] && want=allowed
      sec_check "Kafka user flink_$u creates topic ${t}_*"   "$want" "$(sec_kv create.$u.$t)"
      sec_check "Kafka user flink_$u writes to ${t}_*"       "$want" "$(sec_kv produce.$u.$t)"
      sec_check "Kafka user flink_$u reads from ${t}_*"      "$want" "$(sec_kv consume.$u.$t)"
    done
    sec_check "Kafka user flink_$u sees only topic prefix [$u]" "$u" "$(sec_kv list.$u)"
  done
  for t in $FLINK_ENVIRONMENTS; do
    sec_check "topic ${t}_* holds only its own environment's messages (ground truth)" "seed-$t msg-$t" "$(sec_kv content.$t)"
  done
  rm -f "$SEC_KV_FILE"
}

# SQL side: real statements submitted by each logged-in CMF user through the CLI.
sec_sql() {  # <user> <environment> <catalog> <database> <sql> -> allowed | denied
  local user="$1" env="$2" cat="$3" db="$4" sql="$5" name phase i
  local home="${CONFLUENT_CLI_HOME}/iam-${user}"
  name="sectest-$RANDOM"
  if ! HOME="$home" command confluent --environment "$env" flink statement create "$name" --catalog "$cat" --database "$db" \
        --compute-pool "$SHARED_COMPUTE_POOL_NAME" --parallelism 1 --sql "$sql" >/dev/null 2>&1; then
    echo denied; return 0
  fi
  for i in $(seq 1 30); do
    phase=$(HOME="$home" command confluent --environment "$env" flink statement describe "$name" -o json 2>/dev/null | jq -r '.status.phase // "UNKNOWN"')
    case "$phase" in COMPLETED|FAILED|STOPPED) break ;; esac
    sleep 3
  done
  HOME="$home" command confluent --environment "$env" flink statement delete "$name" --force >/dev/null 2>&1 || true
  [[ "$phase" == "COMPLETED" ]] && echo allowed || echo denied
}

sec_test_sql() {
  local u o
  log "Flink SQL as each user (CMF RBAC + catalog mapping + Kafka ACLs together)"
  for u in $FLINK_ENVIRONMENTS; do
    security_cli_login "$u"
    sec_check "'$u' runs CREATE TABLE ${u}_sectest in $(env_catalog "$u")" allowed \
      "$(sec_sql "$u" "$u" "$(env_catalog "$u")" "$(env_database "$u")" "CREATE TABLE IF NOT EXISTS ${u}_sectest (id STRING) DISTRIBUTED INTO 1 BUCKETS")"
    sec_sql "$u" "$u" "$(env_catalog "$u")" "$(env_database "$u")" "DROP TABLE IF EXISTS ${u}_sectest" >/dev/null
    for o in $FLINK_ENVIRONMENTS; do
      [[ "$o" == "$u" ]] && continue
      sec_check "'$u' runs CREATE TABLE ${o}_sectest (other environment's prefix)" denied \
        "$(sec_sql "$u" "$u" "$(env_catalog "$u")" "$(env_database "$u")" "CREATE TABLE IF NOT EXISTS ${o}_sectest (id STRING) DISTRIBUTED INTO 1 BUCKETS")"
      sec_check "'$u' uses catalog '$(env_catalog "$o")' from environment '$u'" denied \
        "$(sec_sql "$u" "$u" "$(env_catalog "$o")" "$(env_database "$o")" "SHOW TABLES")"
      sec_check "'$u' submits SQL into environment '$o'" denied \
        "$(sec_sql "$u" "$o" "$(env_catalog "$o")" "$(env_database "$o")" "SHOW TABLES")"
    done
  done
}

# End-to-end test of the security setup: authentication, then for every user x environment
# whether it can see (list/get), count and write resources - each result compared with what
# the role bindings should allow. Uses harmless probes (a DEDICATED compute pool definition
# and a throwaway environment, both deleted again). Exits non-zero on any failure.
cmd_security_test() {
  require_security_enabled
  ensure_cmf_port_forward
  SEC_PASS=0; SEC_FAIL=0
  SEC_BODY_FILE=$(mktemp)
  local super="${CMF_SECURITY_USERS%% *}" u e t code want n_ref n_got names expected
  local pool_body
  pool_body=$(jq '.metadata.name="sec-probe"' "$COMPUTE_POOL_FILE" 2>/dev/null) \
    || die "cannot read compute pool template $COMPUTE_POOL_FILE"

  log "Authentication"
  code=$(sec_http "" GET environments);                       sec_check "no token is rejected" denied "$(sec_class "$code")"
  code=$(sec_http "not-a-real-token" GET environments);       sec_check "garbage bearer token is rejected" denied "$(sec_class "$code")"
  code=$(curl -s -m 10 -u "$super:wrong-password" -o /dev/null -w '%{http_code}' "${CONFLUENT_CMF_URL}/security/1.0/authenticate")
  sec_check "wrong password is rejected" denied "$(sec_class "$code")"
  for u in $CMF_SECURITY_USERS; do
    sec_set_tok "$u" "$(cmf_token "$u")"
    sec_check "user '$u' can log in (token issued)" yes "$([[ -n "$(sec_tok "$u")" ]] && echo yes || echo no)"
  done

  for u in $CMF_SECURITY_USERS; do
    [[ -n "$(sec_tok "$u")" ]] || continue
    log "Authorization as '$u'$([[ "$u" == "$super" ]] && echo ' (super user: everything)' || echo " (should only reach environment '$u')")"
    # What does the environment list show?
    sec_http "$(sec_tok "$u")" GET environments >/dev/null
    names=$(jq -r '[.items[]?.name] | sort | join(",")' "$SEC_BODY_FILE")
    if [[ "$u" == "$super" ]]; then expected=$(printf '%s\n' $FLINK_ENVIRONMENTS | sort | paste -sd, -); else expected="$u"; fi
    sec_check "environment list shows [$expected]" "$expected" "$names"
    for e in $FLINK_ENVIRONMENTS; do
      want=denied; [[ "$u" == "$super" || "$u" == "$e" ]] && want=allowed
      code=$(sec_http "$(sec_tok "$u")" GET "environments/$e");  sec_check "GET environment '$e'" "$want" "$(sec_class "$code")"
      # Resource visibility: compare how many compute pools the user sees with the admin's view.
      sec_http "$(sec_tok "$super")" GET "environments/$e/compute-pools" >/dev/null; n_ref=$(jq '[.items[]?]|length' "$SEC_BODY_FILE")
      sec_http "$(sec_tok "$u")" GET "environments/$e/compute-pools" >/dev/null;    n_got=$(jq '[.items[]?]|length' "$SEC_BODY_FILE")
      [[ "$want" == denied ]] && n_ref=0
      sec_check "compute pools visible in '$e'" "$n_ref" "$n_got"
      sec_http "$(sec_tok "$super")" GET "environments/$e/statements" >/dev/null; n_ref=$(jq '[.items[]?]|length' "$SEC_BODY_FILE")
      sec_http "$(sec_tok "$u")" GET "environments/$e/statements" >/dev/null;    n_got=$(jq '[.items[]?]|length' "$SEC_BODY_FILE")
      [[ "$want" == denied ]] && n_ref=0
      sec_check "statements visible in '$e'" "$n_ref" "$n_got"
      # Write: create a compute pool definition, then remove it (as the super user, so a
      # leftover from an unexpected success is cleaned up too).
      code=$(sec_http "$(sec_tok "$u")" POST "environments/$e/compute-pools" "$pool_body")
      sec_check "create compute pool in '$e'" "$want" "$(sec_class "$code")"
      sec_http "$(sec_tok "$super")" DELETE "environments/$e/compute-pools/sec-probe" >/dev/null
    done
    # Cluster-scope write: only the super user may create/delete environments.
    want=denied; [[ "$u" == "$super" ]] && want=allowed
    code=$(sec_http "$(sec_tok "$u")" POST environments '{"name":"sec-tmp","kubernetesNamespace":"default"}')
    sec_check "create a new environment (cluster scope)" "$want" "$(sec_class "$code")"
    sec_http "$(sec_tok "$super")" DELETE environments/sec-tmp >/dev/null
  done

  if [[ "$KAFKA_SECURITY_ENABLED" == "true" ]]; then
    sec_test_catalogs
    sec_test_kafka
    sec_test_sql
  fi

  rm -f "$SEC_BODY_FILE"
  echo
  if (( SEC_FAIL == 0 )); then
    log "Security test: all $SEC_PASS checks passed"
  else
    die "Security test: $SEC_FAIL of $((SEC_PASS + SEC_FAIL)) checks FAILED"
  fi
}

cmd_cmf_forward() {
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
  require_cmf_access
  for env in $FLINK_ENVIRONMENTS; do
    log "Creating Flink environment '$env'"
    confluent flink environment create "$env" --kubernetes-namespace "$env" \
      || warn "environment '$env' may already exist"
  done
  confluent flink environment list
}

# Per-environment names when Kafka security is on (one catalog/database per Flink
# environment, tables/topics prefixed "<env>_"); the single shared defaults otherwise.
env_catalog()      { if [[ "$KAFKA_SECURITY_ENABLED" == "true" ]]; then printf '%s-cat' "$1"; else printf '%s' "$CATALOG_NAME"; fi; }
env_database()     { if [[ "$KAFKA_SECURITY_ENABLED" == "true" ]]; then printf '%s-db' "$1"; else printf '%s' "$DATABASE_NAME"; fi; }
env_table_prefix() { if [[ "$KAFKA_SECURITY_ENABLED" == "true" ]]; then printf '%s_' "$1"; fi; }

# The demo SQL files use the bare table names demo_events / demo_aggregated; in Kafka security
# mode every table (= Kafka topic) must start with the environment's prefix, so rewrite them.
render_sql() {
  local p; p=$(env_table_prefix "$2")
  sed -e "s/demo_events/${p}demo_events/g" -e "s/demo_aggregated/${p}demo_aggregated/g" "$1"
}

# Kafka security mode: for every Flink environment <e> create
#   secret          <e>-kafka-secret   the environment's Kafka SASL credentials (user flink_<e>)
#   catalog         <e>-cat            + database <e>-db (DDL allowed only from <e>), whose Kafka
#                                      connection refers to the placeholder <e>-kafka-conn
#   secret mapping  <e>-kafka-conn     created ONLY in environment <e>, linking the placeholder
#                                      to that secret
# CMF merges the mapped secret into the database's connection config, and an environment with
# no mapping for a catalog can't use it at all - so each environment sees only its own
# catalog and connects to Kafka as its own user (whose ACLs cover only <e>_* topics).
cmd_catalog_per_env() {
  require_cmds confluent jq openssl
  local env user d jaas
  d=$(mktemp -d)
  for env in $FLINK_ENVIRONMENTS; do
    user=$(kafka_env_user "$env")
    jaas="org.apache.kafka.common.security.plain.PlainLoginModule required username=\"${user}\" password=\"$(kafka_password "$user")\";"
    log "Environment '$env': secret '${env}-kafka-secret' (Kafka user $user), catalog '$(env_catalog "$env")', database '$(env_database "$env")'"
    jq -n --arg n "${env}-kafka-secret" --arg jaas "$jaas" \
      '{apiVersion:"cmf.confluent.io/v1",kind:"Secret",metadata:{name:$n},
        spec:{data:{"security.protocol":"SASL_PLAINTEXT","sasl.mechanism":"PLAIN","sasl.jaas.config":$jaas}}}' > "$d/secret.json"
    confluent flink secret create "$d/secret.json" >/dev/null 2>&1 \
      || confluent flink secret update "$d/secret.json" >/dev/null \
      || warn "could not create or update secret '${env}-kafka-secret'"
    jq --arg n "$(env_catalog "$env")" '.metadata.name=$n' "$CATALOG_FILE" > "$d/catalog.json"
    confluent flink catalog create "$d/catalog.json" >/dev/null 2>&1 || warn "catalog '$(env_catalog "$env")' may already exist"
    jq --arg n "$(env_database "$env")" --arg id "${env}-kafka-conn" --arg e "$env" \
      '.metadata.name=$n | .spec.kafkaCluster.connectionSecretId=$id | .spec.ddlEnvironments=[$e]' "$DATABASE_FILE" > "$d/database.json"
    if ! confluent flink catalog database create "$d/database.json" --catalog "$(env_catalog "$env")" >/dev/null 2>&1; then
      confluent flink catalog database update "$d/database.json" --catalog "$(env_catalog "$env")" >/dev/null 2>&1 \
        || warn "could not create or update database '$(env_database "$env")'"
    fi
    jq -n --arg id "${env}-kafka-conn" --arg s "${env}-kafka-secret" \
      '{apiVersion:"cmf.confluent.io/v1",kind:"EnvironmentSecretMapping",metadata:{name:$id},spec:{secretName:$s}}' > "$d/mapping.json"
    confluent flink secret-mapping create "$d/mapping.json" --environment "$env" >/dev/null 2>&1 \
      || confluent flink secret-mapping update "$d/mapping.json" --environment "$env" >/dev/null \
      || warn "could not create or update the secret mapping in environment '$env'"
  done
  rm -rf "$d"
  confluent flink catalog list
}

cmd_catalog() {
  require_cmds confluent
  ensure_cmf_port_forward
  require_cmf_access
  if [[ "$KAFKA_SECURITY_ENABLED" == "true" ]]; then cmd_catalog_per_env; return 0; fi
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
    local auth_hdr=()
    [[ "$CMF_SECURITY_ENABLED" == "true" ]] && auth_hdr=(-H "Authorization: Bearer $(cmf_token "${CMF_SECURITY_USERS%% *}")")
    if curl -sf -X PUT -H "Content-Type: application/json" "${auth_hdr[@]}" -d "@${rendered}" \
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
  require_cmf_access
  local phase
  for env in $FLINK_ENVIRONMENTS; do
    create_or_update_compute_pool "$env" "$COMPUTE_POOL_NAME" "$COMPUTE_POOL_FILE" DEDICATED
    create_or_update_compute_pool "$env" "$SHARED_COMPUTE_POOL_NAME" "$SHARED_COMPUTE_POOL_FILE" SHARED
    # The SHARED pool backs all DDL/verify/streaming statements, and CMF refuses to
    # deploy to it until it's RUNNING. Wait here so cmd_verify/cmd_demo_pipeline right
    # after `up` don't race its (cold-start) provisioning.
    log "Waiting for compute pool '$SHARED_COMPUTE_POOL_NAME' in '$env' to reach RUNNING"
    phase=$(wait_for_compute_pool_phase "$env" "$SHARED_COMPUTE_POOL_NAME" 300 RUNNING) || true
    [[ "$phase" == "RUNNING" ]] || warn "compute pool '$SHARED_COMPUTE_POOL_NAME' in '$env' not RUNNING yet (phase=$phase) - statements may fail until it is; check './demo.sh status' and retry"
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

# Waits for a bounded statement on a SHARED pool. Historically these never left a
# non-terminal phase (so callers had to guess with a sleep), but on current CMF/operator
# versions they do reach COMPLETED - so poll for it, up to SHARED_INSERT_GRACE_SECONDS,
# which must cover a cold start (the first job on a fresh pool waits for a TaskManager
# pod) plus one 10s checkpoint, since deleting the statement before the Kafka sink
# commits silently drops the data. If COMPLETED never shows up, fall back to the old
# behaviour: warn and carry on. Prints the last phase seen; always returns 0.
wait_for_shared_bounded_statement() {
  local env="$1" name="$2" phase
  phase=$(wait_for_statement_phase "$env" "$name" "$SHARED_INSERT_GRACE_SECONDS" COMPLETED) || true
  [[ "$phase" == "COMPLETED" ]] \
    || warn "statement '$name' on a SHARED pool still reports '$phase' after ${SHARED_INSERT_GRACE_SECONDS}s - assuming it finished, but verify the data landed"
  return 0
}

# Polls a compute pool's phase until it matches one of the given "ok" phases
# (success), FAILED (failure), or the timeout elapses. Prints the final phase seen.
# Mirrors wait_for_statement_phase: CMF rejects statement submissions to a SHARED
# pool that isn't RUNNING yet (PENDING -> PROVISIONING -> RUNNING on a cold start),
# so callers wait here before running DDL/verify/the streaming job.
wait_for_compute_pool_phase() {
  local env="$1" name="$2" timeout="$3"; shift 3
  local waited=0 phase want
  while true; do
    phase=$(confluent flink compute-pool describe --environment "$env" "$name" -o json 2>/dev/null | jq -r '.status.phase // "UNKNOWN"') || true
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
    --catalog "$(env_catalog "$env")" \
    --database "$(env_database "$env")" \
    --compute-pool "$pool" \
    --parallelism 1 \
    --sql "$(render_sql "$sql_file" "$env")" >/dev/null
  wait_for_statement_phase "$env" "$name" "$timeout" "$@"
}

cmd_demo_pipeline() {
  require_cmds confluent jq
  ensure_cmf_port_forward
  require_cmf_access
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
    --catalog "$(env_catalog "$env")" \
    --database "$(env_database "$env")" \
    --compute-pool "$insert_pool" \
    --parallelism 1 \
    --sql "$(render_sql "$INSERT_DEMO_DATA_SQL_FILE" "$env")" >/dev/null
  if [[ "$insert_pool_type" == "SHARED" ]]; then
    wait_for_shared_bounded_statement "$env" insert-demo-data
    delete_statement_quiet "$env" insert-demo-data
    log "Observe statements in the CMF or C3 UI: Flink Environment > $env > Statements"
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
    --catalog "$(env_catalog "$env")" \
    --database "$(env_database "$env")" \
    --compute-pool "$ddl_pool" \
    --parallelism 1 \
    --sql "$(render_sql "$STREAMING_SQL_FILE" "$env")" >/dev/null
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
  require_cmf_access
  local env="${1:-prod}" count="${2:-$GENERATE_DATA_ROW_COUNT}" pool="${3:-$SHARED_COMPUTE_POOL_NAME}"
  local categories=(A B) suffix tmpfile name phase i offset val cat sep pool_type

  [[ "$count" =~ ^[0-9]+$ ]] || die "row count must be a positive integer, got: $count"
  (( count > 0 )) || die "row count must be greater than 0, got: $count"

  pool_type=$(confluent flink compute-pool describe --environment "$env" "$pool" -o json 2>/dev/null | jq -r '.spec.type // empty') || true
  [[ -n "$pool_type" ]] || die "compute pool '$pool' not found in environment '$env'"

  suffix=$(date +%s)
  tmpfile=$(mktemp)
  {
    echo "INSERT INTO $(env_table_prefix "$env")demo_events"
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
    --catalog "$(env_catalog "$env")" \
    --database "$(env_database "$env")" \
    --compute-pool "$pool" \
    --parallelism 1 \
    --sql "$(cat "$tmpfile")" >/dev/null
  rm -f "$tmpfile"

  if [[ "$pool_type" == "SHARED" ]]; then
    # See wait_for_shared_bounded_statement: poll for COMPLETED instead of guessing.
    wait_for_shared_bounded_statement "$env" "$name"
    delete_statement_quiet "$env" "$name"
    log "Requested $count new events into 'demo_events' in '$env' (SHARED pool)"
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
    --catalog "$(env_catalog "$env")" \
    --database "$(env_database "$env")" \
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
  require_cmf_access
  local env="${1:-prod}"
  local probe_table; probe_table="$(env_table_prefix "$env")__verify_probe"
  log "Running startup verification statements in environment '$env'"
  run_verify_statement "$env" verify-show-tables "SHOW TABLES;"
  # Uses its own disposable table rather than depending on any pre-existing
  # topic, so this works standalone even before ./demo.sh demo-pipeline has run.
  run_verify_statement "$env" verify-create-probe \
    "CREATE TABLE IF NOT EXISTS ${probe_table} (\`id\` STRING, \`value\` INT) DISTRIBUTED INTO 1 BUCKETS;"
  run_verify_statement "$env" verify-describe-probe "DESCRIBE ${probe_table};"
  run_verify_statement "$env" verify-show-create-probe "SHOW CREATE TABLE ${probe_table};"
  run_verify_statement "$env" verify-drop-probe "DROP TABLE IF EXISTS ${probe_table};"
}

cmd_application() {
  require_cmds confluent
  ensure_cmf_port_forward
  require_cmf_access
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

# Creates a Kafka topic as the super user (topic auto-creation is off on this cluster).
kafka_ensure_topic() {
  local topic="$1" create="kafka-topics --bootstrap-server \$B --create --if-not-exists --topic $1 --partitions 1 --replication-factor 1"
  if [[ "$KAFKA_SECURITY_ENABLED" == "true" ]]; then
    kafka_admin_exec "${create} --command-config \$C" >/dev/null
  else
    kubectl -n "$CONFLUENT_NAMESPACE" exec kafka-0 -c kafka -- bash -c "B=localhost:9071; ${create}" >/dev/null
  fi
}

# Deploys the custom Java job (app/kafka-demo-job: Kafka demo_events -> per-category totals ->
# Kafka <prefix>java_category_totals) as a FlinkApplication from a cmf:// artifact. Per
# environment it: builds the JAR if needed, uploads it as an artifact, creates the sink topic,
# (with Kafka security) puts the environment's Kafka credentials in a k8s secret the pod reads
# as env vars, and deploys. Needs CMF_ARTIFACTS_ENABLED=true.
cmd_java_app() {
  require_cmds confluent jq kubectl
  [[ "$CMF_ARTIFACTS_ENABLED" == "true" ]] \
    || die "the Java demo app is deployed from a cmf:// artifact - run with CMF_ARTIFACTS_ENABLED=true (and CMF_ARTIFACTS_BASE_PATH on the cmf step)"
  ensure_cmf_port_forward
  require_cmf_access
  local env="${1:-prod}" p jar sink user spec d
  p=$(env_table_prefix "$env")
  jar="$JAVA_DEMO_APP_DIR/target/kafka-demo-job-1.0.jar"
  if [[ ! -f "$jar" ]]; then
    require_cmds mvn java
    log "Building $JAVA_DEMO_APP_DIR (mvn package)"
    (cd "$JAVA_DEMO_APP_DIR" && mvn -q -B package -DskipTests) || die "maven build failed"
  fi
  if confluent flink artifact describe "$JAVA_DEMO_JAR_NAME" --environment "$env" >/dev/null 2>&1; then
    log "Artifact '$JAVA_DEMO_JAR_NAME' already uploaded to environment '$env'"
  else
    log "Uploading artifact '$JAVA_DEMO_JAR_NAME' to environment '$env'"
    confluent flink artifact create "$JAVA_DEMO_JAR_NAME" --artifact-file "$jar" --environment "$env" >/dev/null \
      || die "artifact upload failed"
  fi
  sink="${p}java_category_totals"
  log "Ensuring sink topic '$sink'"
  kafka_ensure_topic "$sink"
  d=$(mktemp -d); spec="$d/kafka-demo-job.json"
  jq --arg env "$env" --arg src "${p}demo_events" --arg sink "$sink" --arg jar "$JAVA_DEMO_JAR_NAME" '
      .spec.job.jarURI = "cmf://\($env)/\($jar)?version=1"
      | .spec.job.args = ["--source-topic", $src, "--sink-topic", $sink]' "$JAVA_DEMO_APP_FILE" > "$spec"
  if [[ "$KAFKA_SECURITY_ENABLED" == "true" ]]; then
    user=$(kafka_env_user "$env")
    log "Kafka credentials for '$user' -> secret kafka-client-creds in namespace '$env'"
    printf 'username=%s\npassword=%s\n' "$user" "$(kafka_password "$user")" > "$d/creds.env"
    kubectl -n "$env" create secret generic kafka-client-creds --from-env-file="$d/creds.env" \
      --dry-run=client -o yaml | kubectl -n "$env" apply -f - >/dev/null
    jq '.spec.podTemplate = {spec:{containers:[{name:"flink-main-container", env:[
          {name:"KAFKA_USERNAME", valueFrom:{secretKeyRef:{name:"kafka-client-creds", key:"username"}}},
          {name:"KAFKA_PASSWORD", valueFrom:{secretKeyRef:{name:"kafka-client-creds", key:"password"}}}]}]}}' "$spec" > "$spec.tmp" \
      && mv "$spec.tmp" "$spec"
  fi
  cmd_application "$env" "$spec"
  rm -rf "$d"
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
  cmd_check_quotas
  cmd_cluster
  cmd_helm_repo
  cmd_namespace
  cmd_operator
  cmd_kafka
  if [[ "$KAFKA_SECURITY_ENABLED" == "true" ]]; then cmd_kafka_acls; fi
  cmd_cert_manager
  cmd_flink_operator
  if [[ "$CMF_ARTIFACTS_ENABLED" == "true" ]]; then cmd_artifacts; fi
  cmd_cmf
  cmd_cmf_forward
  cmd_c3_forward
  cmd_flink_environments
  cmd_catalog
  if [[ "$CMF_SECURITY_ENABLED" == "true" ]]; then cmd_security; fi
  cmd_compute_pool
  for env in $FLINK_ENVIRONMENTS; do
    cmd_verify "$env"
  done
  local open_cmd="./demo.sh --${CLOUD} --user ${CFLT_USER}"
  log "Environment is up"
  log "CMF web UI: ${CONFLUENT_CMF_URL}/  (REST API: ${CONFLUENT_CMF_URL}/cmf/api/v1)"
  log "C3 web UI: http://localhost:${C3_LOCAL_PORT}/home"
  log "(or open these in a browser: '${open_cmd} cmf-ui' / '${open_cmd} c3-ui')"
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
  # Record the PD disks backing this cluster's PVs BEFORE deleting it: GKE leaves
  # dynamically provisioned PV disks behind (Retain-like orphans) once the cluster is
  # gone, and on a shared project they're otherwise indistinguishable from other
  # people's. Taking the names from the PVs themselves is exact - no label/date guessing.
  local pv_disks
  pv_disks=$(list_cluster_pv_disks_gcp)
  log "Deleting GKE cluster '$CLUSTER_NAME'"
  gcloud container clusters delete "$CLUSTER_NAME" --zone "$ZONE" --project "$PROJECT" --quiet
  delete_pv_disks_gcp "$pv_disks"
  teardown_artifacts_gcp "$gcs_key_id" "$gcs_sa"
}

# Prints "<disk-name> <zone>" for every GCE PD that backs a PV in this cluster. Uses an
# explicit kubectl --context (the current context may point at another cluster) and
# only trusts the PD CSI driver's volumeHandle (projects/<p>/zones/<z>/disks/<name>),
# restricted to our project. Prints nothing (never fails) if the cluster is unreachable.
list_cluster_pv_disks_gcp() {
  command -v jq >/dev/null 2>&1 || return 0
  local ctx="gke_${PROJECT}_${ZONE}_${CLUSTER_NAME}"
  kubectl --context "$ctx" get pv -o json 2>/dev/null | jq -r --arg p "$PROJECT" '
    .items[] | select(.spec.csi.driver == "pd.csi.storage.gke.io")
    | .spec.csi.volumeHandle
    | capture("^projects/(?<proj>[^/]+)/zones/(?<zone>[^/]+)/disks/(?<name>[^/]+)$")
    | select(.proj == $p) | "\(.name) \(.zone)"' 2>/dev/null || true
}

# Deletes the disks listed by list_cluster_pv_disks_gcp (stdin-style arg: one
# "<name> <zone>" per line). Cluster deletion detaches them asynchronously, so retry a
# few times; a disk that's already gone is fine. Best-effort: warns, never aborts.
delete_pv_disks_gcp() {
  local list="${1:-}" name zone i
  if [[ -z "$list" ]]; then
    log "No PVC-backed disks recorded for '$CLUSTER_NAME' - nothing to clean up"
    return 0
  fi
  log "Deleting PVC-backed disks left behind by the cluster"
  while read -r name zone; do
    [[ -n "$name" ]] || continue
    if ! gcloud compute disks describe "$name" --zone "$zone" --project "$PROJECT" >/dev/null 2>&1; then
      log "  disk $name already gone"
      continue
    fi
    for i in 1 2 3 4 5 6; do
      if gcloud compute disks delete "$name" --zone "$zone" --project "$PROJECT" --quiet >/dev/null 2>&1; then
        log "  deleted disk $name"
        continue 2
      fi
      sleep 10
    done
    warn "could not delete disk $name (zone $zone) - remove manually: gcloud compute disks delete $name --zone $zone --project $PROJECT"
  done <<< "$list"
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
  up                      Run all the steps under "Steps run by up" below, in order
  down [--yes]            Stop port-forwards and delete the GKE/EKS cluster

Steps run by `up` (in this order; each is also runnable standalone):
   1  check-quotas        Preflight cloud quotas before anything is created;
                          warns on any exceeded quota and asks whether to proceed
                          (default no). AWS: VPCs/internet gateways/Elastic IPs per
                          region + NAT gateways per AZ. GCP: not implemented, no-op
   2  cluster             Create the GKE/EKS cluster and fetch kubectl credentials
                          (EKS also installs the EBS CSI driver add-on and a
                          default gp3 StorageClass, so PVCs bind like on GKE)
   3  helm-repo           Add/update the confluentinc helm repo
   4  namespace           Create the confluent namespace and set it as default context
   5  operator            Install the confluent-for-kubernetes operator
   6  kafka               Apply cp/cp.yaml (Kafka, Schema Registry, Control Center)
   7  cert-manager        Install cert-manager (required by the Flink k8s operator)
   8  flink-operator      Create prod/test namespaces, install the Flink Kubernetes Operator
   9  artifacts           Provision blob storage for CMF 2.5 artifacts (per-user bucket +
                          scoped static creds in a K8s secret). Conditional - only runs
                          when CMF_ARTIFACTS_ENABLED=true. See config.sh.
  10  cmf                 Install Confluent Manager for Apache Flink (CMF)
  11  cmf-forward         Start a background port-forward to cmf-service:8080 (CMF UI/API)
  12  c3-forward          Start a background port-forward to Control Center on :9021
  13  flink-environments  Create the prod/test Flink environments in CMF
  14  catalog             Create the Kafka catalog + database (flink/catalogv2.json, databasev2.json)
  15  compute-pool        Create the DEDICATED pool (flink/compute-pool.json) and SHARED pool
                          (flink/compute-pool-shared.json) in prod and test, then wait for
                          shared-pool to reach RUNNING
  16  verify [env]        Create/describe/drop a disposable table on the shared pool to
                          sanity-check the environment. Repeated once per environment in
                          FLINK_ENVIRONMENTS (default: prod and test)

Standalone commands (not run by `up`):
  demo-pipeline [env] [pool]  Create demo_events/demo_aggregated, seed demo data, and start
                          the continuous windowed-aggregation job (default env: prod). By
                          default every step runs on shared-pool (fast - no cold start); pass a
                          pool name to force a different pool for all steps, e.g.
                          "demo-pipeline prod pool" runs the data load on the DEDICATED pool
                          (slower, but confirms COMPLETED).
  generate-data [env] [count] [pool]  Insert more random rows into demo_events on demand
                          (default env: prod, count: 20, pool: shared-pool -
                          pass e.g. "pool" as 3rd arg to use the DEDICATED
                          pool instead, which is slower but confirms COMPLETED)
  application [env] [file] Deploy a simple FlinkApplication (default env: prod, default file: app/cpf_basic_app.json)
  kafka-acls              Create the prefixed Kafka ACLs (needs KAFKA_SECURITY_ENABLED=true on the
                          `kafka` step): each Flink environment's user only gets <env>_* topics
  security                Create the RBAC role bindings for the demo users (needs
                          CMF_SECURITY_ENABLED=true on the `cmf` step): the first user in
                          CMF_SECURITY_USERS is the super user, every other user is bound to
                          the Flink environment of the same name only
  security-test           Run the end-to-end security test (authentication + per-user/
                          per-environment read and write access), PASS/FAIL per check
  java-app [env]          Deploy the custom Java job (app/kafka-demo-job) in an environment: uploads
                          the JAR as a cmf:// artifact, creates its sink topic (+ Kafka credentials
                          with KAFKA_SECURITY_ENABLED) and starts it (needs CMF_ARTIFACTS_ENABLED=true)
  cmf-ui                  Ensure the CMF port-forward (same as step 11) and open the CMF 2.5
                          web UI in a browser (root of :8080, same port as the REST API)
  c3-ui                   Ensure the Control Center port-forward (same as step 12) and open
                          the C3 web UI in a browser (http://localhost:9021/home)
  status                  Show pod status and Flink environments
  stop-port-forward       Stop all background port-forwards started by this script
                          (both the CMF and Control Center forwards)
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
    # Catch near-misses like --username/--user-name before they fall through to the
    # positional catch-all and surface later as the confusing "--user is required".
    --user*) die "unknown option '$1' - did you mean --user? (e.g. --user myusername)" ;;
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
  check-quotas) cmd_check_quotas "$@" ;;
  cluster) cmd_cluster "$@" ;;
  helm-repo) cmd_helm_repo "$@" ;;
  namespace) cmd_namespace "$@" ;;
  operator) cmd_operator "$@" ;;
  kafka) cmd_kafka "$@" ;;
  c3-forward) cmd_c3_forward "$@" ;;
  cmf-ui) cmd_cmf_ui "$@" ;;
  c3-ui) cmd_c3_ui "$@" ;;
  cert-manager) cmd_cert_manager "$@" ;;
  flink-operator) cmd_flink_operator "$@" ;;
  cmf) cmd_cmf "$@" ;;
  artifacts) cmd_artifacts "$@" ;;
  cmf-forward) cmd_cmf_forward "$@" ;;
  security) cmd_security "$@" ;;
  kafka-acls) cmd_kafka_acls "$@" ;;
  security-test) cmd_security_test "$@" ;;
  stop-port-forward) cmd_stop_port_forward "$@" ;;
  flink-environments) cmd_flink_environments "$@" ;;
  catalog) cmd_catalog "$@" ;;
  compute-pool) cmd_compute_pool "$@" ;;
  demo-pipeline) cmd_demo_pipeline "$@" ;;
  generate-data) cmd_generate_data "$@" ;;
  verify) cmd_verify "$@" ;;
  application) cmd_application "$@" ;;
  java-app) cmd_java_app "$@" ;;
  status) cmd_status "$@" ;;
  help|-h|--help) cmd_help ;;
  *) die "unknown subcommand '$sub' (see ./demo.sh help)" ;;
esac
