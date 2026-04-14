#!/usr/bin/env bash
# =============================================================================
# veeam-kasten-collector.sh  v1.2.0
#
# Bash port of the veeam-kasten-collector Go collector.
# Generates a self-contained HTML report of a Kubernetes cluster state
# and Kasten K10 installation.
#
# Requirements : kubectl  jq  python3
# Usage        : ./veeam-kasten-collector.sh [OPTIONS]
# =============================================================================
 
set -euo pipefail
 
# ─── Metadata ────────────────────────────────────────────────────────────────
SCRIPT_VERSION="1.2.0"
SCRIPT_NAME="$(basename "$0")"
 
# ─── ANSI Colors ─────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'
 
# ─── Defaults ────────────────────────────────────────────────────────────────
KUBECONFIG_PATH="${KUBECONFIG:-$HOME/.kube/config}"
OUTPUT_DIR="./build"
SKIP_HELM=false
MASK_IPS=false
CTX=""
TIMEOUT=60
 
# ─── Temporary directory ─────────────────────────────────────────────────────
TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT INT TERM
 
# ─── Logging ─────────────────────────────────────────────────────────────────
log_info()    { echo -e "${GREEN}[INFO]${NC}  $*"; }
log_warn()    { echo -e "${YELLOW}[WARN]${NC}  $*" >&2; }
log_error()   { echo -e "${RED}[ERROR]${NC} $*" >&2; }
log_section() { echo -e "\n${CYAN}${BOLD}──── $* ────${NC}"; }
 
# ─── Usage ───────────────────────────────────────────────────────────────────
usage() {
  cat <<EOF
${BOLD}${SCRIPT_NAME} v${SCRIPT_VERSION}${NC} — Veeam Kasten Cluster Inventory Collector
 
Collects Kubernetes cluster and Veeam Kasten information,
then generates a self-contained HTML report.
 
${BOLD}USAGE:${NC}
  ${SCRIPT_NAME} [OPTIONS]
 
${BOLD}OPTIONS:${NC}
  --kubeconfig <path>      Path to the kubeconfig file
                           (default: \$KUBECONFIG or ~/.kube/config)
  --context <name>         Kubeconfig context to use
                           (default: current context)
  --output-dir <path>      Output directory for the HTML report
                           (default: ./build)
  --no-helm                Skip Helm values collection (security)
  --no-ip-services         Mask IP addresses in the Services section
  --timeout <seconds>      kubectl timeout in seconds (default: 60)
  -h, --help               Show this help
 
${BOLD}REQUIREMENTS:${NC}
  kubectl  jq  python3
 
${BOLD}EXAMPLES:${NC}
  ${SCRIPT_NAME}
  ${SCRIPT_NAME} --context prod-cluster --output-dir /tmp/reports
  ${SCRIPT_NAME} --no-helm --no-ip-services --timeout 120
EOF
}
 
# ─── Argument parsing ────────────────────────────────────────────────────────
parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --kubeconfig)     KUBECONFIG_PATH="$2"; shift 2 ;;
      --context)        CTX="$2"; shift 2 ;;
      --output-dir)     OUTPUT_DIR="$2"; shift 2 ;;
      --no-helm)        SKIP_HELM=true; shift ;;
      --no-ip-services) MASK_IPS=true; shift ;;
      --timeout)        TIMEOUT="$2"; shift 2 ;;
      -h|--help)        usage; exit 0 ;;
      *)                log_error "Unknown option: $1"; usage; exit 1 ;;
    esac
  done
}
 
# ─── Prerequisites check ─────────────────────────────────────────────────────
check_prerequisites() {
  local missing=()
  for cmd in kubectl jq python3; do
    command -v "$cmd" &>/dev/null || missing+=("$cmd")
  done
  if [[ ${#missing[@]} -gt 0 ]]; then
    log_error "Missing tools: ${missing[*]}"
    log_error "Please install them before running this script."
    exit 1
  fi
  log_info "Prerequisites OK"
}
 
# ─── kubectl wrapper ─────────────────────────────────────────────────────────
kube() {
  kubectl \
    --kubeconfig="$KUBECONFIG_PATH" \
    ${CTX:+--context="$CTX"} \
    --request-timeout="${TIMEOUT}s" \
    "$@"
}
 
# ─── kubectl with no fatal error ─────────────────────────────────────────────
kube_safe() {
  kube "$@" 2>/dev/null || true
}
 
# ─── Save JSON with fallback ─────────────────────────────────────────────────
save_json() {
  local file="$1"; shift
  local fallback="${1:-{\"items\":[]}}"
  shift
  kube_safe "$@" > "$file" || echo "$fallback" > "$file"
  jq empty "$file" 2>/dev/null || echo "$fallback" > "$file"
}
 
# =============================================================================
# RAW DATA COLLECTION (kubectl → JSON files)
# =============================================================================
 
collect_raw_data() {
  log_section "Raw data collection"
 
  # Cluster version
  log_info "Cluster version..."
  kube_safe version -o json > "$TMP_DIR/version.json" 2>/dev/null || echo '{}' > "$TMP_DIR/version.json"
  jq empty "$TMP_DIR/version.json" 2>/dev/null || echo '{}' > "$TMP_DIR/version.json"
 
  # Current context
  CONTEXT_NAME=$(kubectl --kubeconfig="$KUBECONFIG_PATH" ${CTX:+--context="$CTX"} \
    config current-context 2>/dev/null || echo "unknown")
  echo "\"$CONTEXT_NAME\"" > "$TMP_DIR/context.json"
 
  # Namespaces
  log_info "Namespaces..."
  save_json "$TMP_DIR/namespaces.json" '{"items":[]}' get namespaces -o json
 
  # Nodes
  log_info "Nodes..."
  save_json "$TMP_DIR/nodes.json" '{"items":[]}' get nodes -o json
 
  # Pods (all namespaces)
  log_info "Pods..."
  save_json "$TMP_DIR/pods.json" '{"items":[]}' get pods --all-namespaces -o json
 
  # Services
  log_info "Services..."
  save_json "$TMP_DIR/services.json" '{"items":[]}' get services --all-namespaces -o json
 
  # StorageClasses
  log_info "StorageClasses..."
  save_json "$TMP_DIR/storageclasses.json" '{"items":[]}' get storageclass -o json
 
  # PersistentVolumes
  log_info "PersistentVolumes..."
  save_json "$TMP_DIR/pvs.json" '{"items":[]}' get pv -o json
 
  # PersistentVolumeClaims
  log_info "PersistentVolumeClaims..."
  save_json "$TMP_DIR/pvcs.json" '{"items":[]}' get pvc --all-namespaces -o json
 
  # CSI Drivers
  log_info "CSI Drivers..."
  save_json "$TMP_DIR/csidrivers.json" '{"items":[]}' get csidriver -o json
 
  # VolumeSnapshotClasses (optional API)
  log_info "VolumeSnapshotClasses..."
  kube_safe get volumesnapshotclass -o json > "$TMP_DIR/vsc.json" 2>/dev/null || echo '{"items":[]}' > "$TMP_DIR/vsc.json"
  jq empty "$TMP_DIR/vsc.json" 2>/dev/null || echo '{"items":[]}' > "$TMP_DIR/vsc.json"
 
  # CRDs
  log_info "CRDs..."
  save_json "$TMP_DIR/crds.json" '{"items":[]}' get crd -o json
 
  # OLM ClusterServiceVersions (operators)
  log_info "Operators (OLM)..."
  kube_safe get csv --all-namespaces -o json > "$TMP_DIR/operators.json" 2>/dev/null || echo '{"items":[]}' > "$TMP_DIR/operators.json"
  jq empty "$TMP_DIR/operators.json" 2>/dev/null || echo '{"items":[]}' > "$TMP_DIR/operators.json"
 
  # Network Policies
  log_info "Network Policies..."
  save_json "$TMP_DIR/netpols.json" '{"items":[]}' get networkpolicy --all-namespaces -o json
 
  # kube-system pods (for CNI detection)
  log_info "kube-system pods (CNI detection)..."
  save_json "$TMP_DIR/kube_system_pods.json" '{"items":[]}' get pods -n kube-system -o json
 
  # Events
  log_info "Events..."
  save_json "$TMP_DIR/events.json" '{"items":[]}' get events --all-namespaces -o json
 
  # ─── Kasten K10 Section ──────────────────────────────────────────────────
  log_info "Kasten K10..."
 
  if kube_safe get namespace kasten-io &>/dev/null; then
    save_json "$TMP_DIR/kasten_pods.json"   '{"items":[]}' get pods -n kasten-io -o json
    save_json "$TMP_DIR/kasten_cms.json"    '{"items":[]}' get configmap -n kasten-io -o json
    save_json "$TMP_DIR/kasten_policies.json" '{"items":[]}' \
      get policies.config.kio.kasten.io -n kasten-io -o json 2>/dev/null || \
      echo '{"items":[]}' > "$TMP_DIR/kasten_policies.json"
    save_json "$TMP_DIR/kasten_profiles.json" '{"items":[]}' \
      get profiles.config.kio.kasten.io -n kasten-io -o json 2>/dev/null || \
      echo '{"items":[]}' > "$TMP_DIR/kasten_profiles.json"
    save_json "$TMP_DIR/kasten_policypresets.json" '{"items":[]}' \
      get policypresets.config.kio.kasten.io -n kasten-io -o json 2>/dev/null || \
      echo '{"items":[]}' > "$TMP_DIR/kasten_policypresets.json"
 
    # Restore points for DR
    kube_safe get restorepoints.apps.kio.kasten.io -n kasten-io -o json \
      > "$TMP_DIR/kasten_restorepoints.json" 2>/dev/null || \
      echo '{"items":[]}' > "$TMP_DIR/kasten_restorepoints.json"
 
    # K10 DR config
    kube_safe get configmap k10-config -n kasten-io -o json \
      > "$TMP_DIR/k10_config.json" 2>/dev/null || \
      echo '{}' > "$TMP_DIR/k10_config.json"
 
    # Policy Run Actions (for error detection)
    log_info "Kasten Policy Run Actions..."
    kube_safe get policyrunactions.actions.kio.kasten.io -n kasten-io -o json \
      > "$TMP_DIR/kasten_policyrunactions.json" 2>/dev/null || \
      echo '{"items":[]}' > "$TMP_DIR/kasten_policyrunactions.json"
    jq empty "$TMP_DIR/kasten_policyrunactions.json" 2>/dev/null || \
      echo '{"items":[]}' > "$TMP_DIR/kasten_policyrunactions.json"
 
    # Run Actions (all namespaces — for per-namespace last backup date)
    log_info "Kasten Run Actions (all namespaces)..."
    kube_safe get runactions.actions.kio.kasten.io --all-namespaces -o json \
      > "$TMP_DIR/kasten_runactions.json" 2>/dev/null || \
      echo '{"items":[]}' > "$TMP_DIR/kasten_runactions.json"
    jq empty "$TMP_DIR/kasten_runactions.json" 2>/dev/null || \
      echo '{"items":[]}' > "$TMP_DIR/kasten_runactions.json"

    # BackupActions (all namespaces — for failed backup detection)
    log_info "Kasten BackupActions (all namespaces)..."
    kube_safe get backupactions.actions.kio.kasten.io --all-namespaces -o json \
      > "$TMP_DIR/kasten_backupactions.json" 2>/dev/null || \
      echo '{"items":[]}' > "$TMP_DIR/kasten_backupactions.json"
    jq empty "$TMP_DIR/kasten_backupactions.json" 2>/dev/null || \
      echo '{"items":[]}' > "$TMP_DIR/kasten_backupactions.json"
 
    # Helm release secret for values
    if [[ "$SKIP_HELM" == "false" ]]; then
      log_info "Kasten Helm values..."
      kube_safe get secret -n kasten-io -l "owner=helm,name=k10" -o json \
        > "$TMP_DIR/kasten_helm_secrets.json" 2>/dev/null || \
        echo '{"items":[]}' > "$TMP_DIR/kasten_helm_secrets.json"
    else
      echo '{"items":[]}' > "$TMP_DIR/kasten_helm_secrets.json"
    fi

    # Blueprints (Kanister) — all namespaces (may live outside kasten-io)
    log_info "Kasten Blueprints..."
    kube_safe get blueprints.cr.kanister.io --all-namespaces -o json \
      > "$TMP_DIR/kasten_blueprints.json" 2>/dev/null || \
      echo '{"items":[]}' > "$TMP_DIR/kasten_blueprints.json"
    jq empty "$TMP_DIR/kasten_blueprints.json" 2>/dev/null || \
      echo '{"items":[]}' > "$TMP_DIR/kasten_blueprints.json"

    # BlueprintBindings — all namespaces
    log_info "Kasten BlueprintBindings..."
    kube_safe get blueprintbindings.config.kio.kasten.io --all-namespaces -o json \
      > "$TMP_DIR/kasten_blueprintbindings.json" 2>/dev/null || \
      echo '{"items":[]}' > "$TMP_DIR/kasten_blueprintbindings.json"
    jq empty "$TMP_DIR/kasten_blueprintbindings.json" 2>/dev/null || \
      echo '{"items":[]}' > "$TMP_DIR/kasten_blueprintbindings.json"

    # TransformSets — all namespaces
    log_info "Kasten TransformSets..."
    kube_safe get transformsets.config.kio.kasten.io --all-namespaces -o json \
      > "$TMP_DIR/kasten_transformsets.json" 2>/dev/null || \
      echo '{"items":[]}' > "$TMP_DIR/kasten_transformsets.json"
    jq empty "$TMP_DIR/kasten_transformsets.json" 2>/dev/null || \
      echo '{"items":[]}' > "$TMP_DIR/kasten_transformsets.json"

    # ReportActions
    log_info "Kasten ReportActions..."
    kube_safe get reportactions.actions.kio.kasten.io -n kasten-io -o json \
      > "$TMP_DIR/kasten_reportactions.json" 2>/dev/null || \
      echo '{"items":[]}' > "$TMP_DIR/kasten_reportactions.json"
    jq empty "$TMP_DIR/kasten_reportactions.json" 2>/dev/null || \
      echo '{"items":[]}' > "$TMP_DIR/kasten_reportactions.json"

    # RestoreActions — all namespaces
    log_info "Kasten RestoreActions..."
    kube_safe get restoreactions.actions.kio.kasten.io --all-namespaces -o json \
      > "$TMP_DIR/kasten_restoreactions.json" 2>/dev/null || \
      echo '{"items":[]}' > "$TMP_DIR/kasten_restoreactions.json"
    jq empty "$TMP_DIR/kasten_restoreactions.json" 2>/dev/null || \
      echo '{"items":[]}' > "$TMP_DIR/kasten_restoreactions.json"

  else
    log_warn "Namespace kasten-io not found — Veeam Kasten not installed"
    echo '{"items":[]}' > "$TMP_DIR/kasten_pods.json"
    echo '{"items":[]}' > "$TMP_DIR/kasten_cms.json"
    echo '{"items":[]}' > "$TMP_DIR/kasten_policies.json"
    echo '{"items":[]}' > "$TMP_DIR/kasten_profiles.json"
    echo '{"items":[]}' > "$TMP_DIR/kasten_policypresets.json"
    echo '{"items":[]}' > "$TMP_DIR/kasten_restorepoints.json"
    echo '{}'           > "$TMP_DIR/k10_config.json"
    echo '{"items":[]}' > "$TMP_DIR/kasten_policyrunactions.json"
    echo '{"items":[]}' > "$TMP_DIR/kasten_runactions.json"
    echo '{"items":[]}' > "$TMP_DIR/kasten_backupactions.json"
    echo '{"items":[]}' > "$TMP_DIR/kasten_helm_secrets.json"
    echo '{"items":[]}' > "$TMP_DIR/kasten_blueprints.json"
    echo '{"items":[]}' > "$TMP_DIR/kasten_blueprintbindings.json"
    echo '{"items":[]}' > "$TMP_DIR/kasten_transformsets.json"
    echo '{"items":[]}' > "$TMP_DIR/kasten_reportactions.json"
    echo '{"items":[]}' > "$TMP_DIR/kasten_restoreactions.json"
  fi
 
  # Node metrics (kubectl top — optional)
  log_info "Node metrics (optional)..."
  if kube_safe top nodes --no-headers > "$TMP_DIR/node_metrics.txt" 2>/dev/null; then
    log_info "  Node metrics available"
  else
    log_warn "  Node metrics unavailable (metrics-server missing?)"
    > "$TMP_DIR/node_metrics.txt"
  fi
 
  log_info "Raw data collection complete"
}
 
# =============================================================================
# HTML REPORT GENERATION (embedded Python3 script)
# =============================================================================
 
write_html_generator() {
  cat > "$TMP_DIR/generate_html.py" << 'PYEOF'
#!/usr/bin/env python3
"""
Veeam Kasten Inventory — HTML Report Generator
Reads JSON files collected by the bash script and generates an HTML report.
"""
 
import json, os, sys, base64, gzip, re
from datetime import datetime, timezone
 
# ─── Paths ───────────────────────────────────────────────────────────────────
TMP   = os.environ["KASTEN_TMP_DIR"]
OUT   = os.environ["KASTEN_OUTPUT"]
MASK  = os.environ.get("KASTEN_MASK_IPS", "false").lower() == "true"
SKIP_HELM = os.environ.get("KASTEN_SKIP_HELM", "false").lower() == "true"
 
def load(filename, default=None):
    path = os.path.join(TMP, filename)
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return default if default is not None else {}
 
def load_text(filename):
    path = os.path.join(TMP, filename)
    try:
        with open(path) as f:
            return f.read().strip()
    except Exception:
        return ""
 
def items(data):
    if isinstance(data, dict):
        return data.get("items", [])
    return []
 
def h(text):
    """Escape HTML entities."""
    if text is None:
        return ""
    return (str(text)
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;"))
 
def mask_ip(ip):
    if not MASK or not ip:
        return ip
    parts = ip.split(".")
    if len(parts) == 4:
        return f"{parts[0]}.{parts[1]}.x.x"
    return ip
 
def calc_age(ts):
    if not ts:
        return "N/A"
    try:
        created = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        delta = datetime.now(timezone.utc) - created
        d, rem = divmod(int(delta.total_seconds()), 86400)
        h, rem = divmod(rem, 3600)
        m = rem // 60
        if d > 0:
            return f"{d}d {h}h"
        if h > 0:
            return f"{h}h {m}m"
        return f"{m}m"
    except Exception:
        return "N/A"
 
def fmt_date(ts):
    """Format ISO timestamp as readable date."""
    if not ts:
        return "—"
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d %H:%M UTC")
    except Exception:
        return ts
 
def fmt_bytes(b):
    try:
        b = int(b)
    except Exception:
        return "N/A"
    for unit in ["B", "KiB", "MiB", "GiB", "TiB"]:
        if b < 1024:
            return f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.1f} PiB"
 
def status_badge(status):
    s = str(status).lower()
    if s in ("running", "ready", "true", "bound", "active", "succeeded", "complete", "completed"):
        cls = "badge-green"
    elif s in ("pending", "warning", "unknown", "terminating"):
        cls = "badge-yellow"
    elif s in ("failed", "error", "false", "notready", "crashloopbackoff"):
        cls = "badge-red"
    else:
        cls = "badge-gray"
    return f'<span class="badge {cls}">{h(status)}</span>'
 
# =============================================================================
# DATA PROCESSING
# =============================================================================
 
# ─── Version / Cluster ───────────────────────────────────────────────────────
version_data   = load("version.json", {})
context_raw    = load_text("context.json").strip('"')
server_version = (version_data.get("serverVersion") or {}).get("gitVersion", "Unknown")
platform       = (version_data.get("serverVersion") or {}).get("platform", "Unknown")
 
namespaces_data = load("namespaces.json", {"items": []})
ns_count = len(items(namespaces_data))
 
# ─── Distribution ────────────────────────────────────────────────────────────
dist_type = "Kubernetes"
ns_names = {n.get("metadata", {}).get("name", "") for n in items(namespaces_data)}
if "openshift-config" in ns_names:
    dist_type = "OpenShift"
elif "cattle-system" in ns_names:
    dist_type = "Rancher/RKE"
elif "k3s-upgrader" in ns_names:
    dist_type = "K3s"
 
nodes_raw = items(load("nodes.json", {"items": []}))
if dist_type == "Kubernetes" and nodes_raw:
    provider_id = nodes_raw[0].get("spec", {}).get("providerID", "")
    if "azure" in provider_id:
        dist_type = "AKS"
    elif "aws" in provider_id:
        dist_type = "EKS"
    elif "gce" in provider_id:
        dist_type = "GKE"
    elif "harvester" in provider_id:
        dist_type = "Harvester"
if "k3s" in server_version.lower():
    dist_type = "K3s"
 
# ─── Node metrics ────────────────────────────────────────────────────────────
node_metrics = {}
for line in load_text("node_metrics.txt").splitlines():
    parts = line.split()
    if len(parts) >= 4:
        name = parts[0]
        cpu_m  = parts[1].rstrip("m")
        mem_mi = parts[3].rstrip("Mi")
        try:
            node_metrics[name] = {"cpu_m": int(cpu_m), "mem_mi": int(mem_mi)}
        except ValueError:
            pass
 
# ─── Nodes ───────────────────────────────────────────────────────────────────
def process_nodes():
    results = []
    for node in nodes_raw:
        meta    = node.get("metadata", {})
        spec    = node.get("spec", {})
        status  = node.get("status", {})
        labels  = meta.get("labels", {})
 
        roles = [k.replace("node-role.kubernetes.io/", "")
                 for k in labels if k.startswith("node-role.kubernetes.io/")]
        if not roles:
            roles = ["worker"]
 
        instance_type = labels.get("node.kubernetes.io/instance-type", "N/A")
        provider_id   = spec.get("providerID", "")
        cloud = "Unknown"
        if "azure" in provider_id: cloud = "Azure"
        elif "aws" in provider_id:  cloud = "AWS"
        elif "gce"  in provider_id: cloud = "GCP"
 
        node_status = "Unknown"
        for cond in status.get("conditions", []):
            if cond.get("type") == "Ready":
                node_status = "Ready" if cond.get("status") == "True" else "NotReady"
 
        capacity = status.get("capacity", {})
        cpu_cap  = capacity.get("cpu", "N/A")
        mem_raw  = capacity.get("memory", "0Ki")
        try:
            mem_bytes = int(mem_raw.rstrip("Ki")) * 1024 if "Ki" in mem_raw else int(mem_raw)
            mem_gi = f"{mem_bytes/(1024**3):.1f} Gi"
        except Exception:
            mem_gi = mem_raw
 
        nm = node_metrics.get(meta.get("name", ""), {})
        cpu_cap_m = 0
        try:
            cpu_cap_m = int(cpu_cap) * 1000
        except Exception:
            pass
 
        cpu_usage = nm.get("cpu_m", 0)
        mem_usage = nm.get("mem_mi", 0)
        cpu_pct   = f"{100*cpu_usage/cpu_cap_m:.1f}%" if cpu_cap_m > 0 else "N/A"
        mem_bytes_cap = 0
        try:
            mem_bytes_cap = int(mem_raw.rstrip("Ki")) * 1024 if "Ki" in mem_raw else 0
        except Exception:
            pass
        mem_pct = f"{100*mem_usage*1024*1024/mem_bytes_cap:.1f}%" if mem_bytes_cap > 0 else "N/A"
 
        taints = [
            f"{t.get('key','')}={t.get('value','')}:{t.get('effect','')}"
            for t in spec.get("taints", [])
        ]
        fmt_labels = [f"{k}={v}" for k, v in labels.items()][:15]
 
        results.append({
            "name":          meta.get("name", ""),
            "roles":         roles,
            "status":        node_status,
            "instance_type": instance_type,
            "cloud":         cloud,
            "cpu":           cpu_cap,
            "cpu_usage":     f"{cpu_usage}m" if cpu_usage else "N/A",
            "cpu_pct":       cpu_pct,
            "memory":        mem_gi,
            "mem_usage":     f"{mem_usage}Mi" if mem_usage else "N/A",
            "mem_pct":       mem_pct,
            "version":       status.get("nodeInfo", {}).get("kubeletVersion", "Unknown"),
            "age":           calc_age(meta.get("creationTimestamp", "")),
            "labels":        fmt_labels,
            "taints":        taints,
        })
    return results
 
# ─── Pods ─────────────────────────────────────────────────────────────────────
def process_pods():
    results = []
    for pod in items(load("pods.json", {"items": []})):
        meta   = pod.get("metadata", {})
        spec   = pod.get("spec", {})
        status = pod.get("status", {})
 
        containers       = spec.get("containers", [])
        cont_statuses    = status.get("containerStatuses", [])
        ready_count      = sum(1 for cs in cont_statuses if cs.get("ready"))
        restart_count    = sum(cs.get("restartCount", 0) for cs in cont_statuses)
        phase            = status.get("phase", "Unknown")
 
        owners = meta.get("ownerReferences", [{}])
        owner  = owners[0] if owners else {}
 
        cpu_req = mem_req = cpu_lim = mem_lim = ""
        for c in containers:
            r = c.get("resources", {})
            if not cpu_req: cpu_req = r.get("requests", {}).get("cpu", "")
            if not mem_req: mem_req = r.get("requests", {}).get("memory", "")
            if not cpu_lim: cpu_lim = r.get("limits",   {}).get("cpu", "")
            if not mem_lim: mem_lim = r.get("limits",   {}).get("memory", "")
 
        cont_infos = []
        for cs in cont_statuses:
            st = cs.get("state", {})
            sname = "Waiting"
            if "running" in st:     sname = "Running"
            elif "terminated" in st: sname = "Terminated"
            cont_infos.append({
                "name":          cs.get("name", ""),
                "ready":         cs.get("ready", False),
                "restart_count": cs.get("restartCount", 0),
                "state":         sname,
            })
 
        results.append({
            "name":             meta.get("name", ""),
            "namespace":        meta.get("namespace", ""),
            "status":           phase,
            "node_name":        spec.get("nodeName", ""),
            "ready":            f"{ready_count}/{len(containers)}",
            "restart_count":    restart_count,
            "age":              calc_age(meta.get("creationTimestamp", "")),
            "cpu_request":      cpu_req or "—",
            "cpu_limit":        cpu_lim or "—",
            "mem_request":      mem_req or "—",
            "mem_limit":        mem_lim or "—",
            "ip":               status.get("podIP", ""),
            "owner_kind":       owner.get("kind", ""),
            "owner_name":       owner.get("name", ""),
            "containers":       cont_infos,
        })
    return results
 
# ─── Services ─────────────────────────────────────────────────────────────────
def process_services():
    results = []
    for svc in items(load("services.json", {"items": []})):
        meta   = svc.get("metadata", {})
        spec   = svc.get("spec", {})
        status = svc.get("status", {})
 
        cluster_ip = mask_ip(spec.get("clusterIP", ""))
        ingress    = status.get("loadBalancer", {}).get("ingress", [])
        lb_ip      = mask_ip(ingress[0].get("ip", ingress[0].get("hostname", "")) if ingress else "")
        ext_ips    = [mask_ip(ip) for ip in spec.get("externalIPs", [])]
 
        ports = []
        for p in spec.get("ports", []):
            ports.append({
                "name":        p.get("name", ""),
                "protocol":    p.get("protocol", "TCP"),
                "port":        p.get("port", 0),
                "target_port": str(p.get("targetPort", "")),
                "node_port":   p.get("nodePort", 0),
            })
 
        results.append({
            "name":          meta.get("name", ""),
            "namespace":     meta.get("namespace", ""),
            "type":          spec.get("type", "ClusterIP"),
            "cluster_ip":    cluster_ip,
            "lb_ip":         lb_ip,
            "external_ips":  ext_ips,
            "ports":         ports,
            "selector":      spec.get("selector", {}),
            "age":           calc_age(meta.get("creationTimestamp", "")),
        })
    return results
 
# ─── Storage ──────────────────────────────────────────────────────────────────
def process_storage():
    # StorageClasses
    sc_map = {}
    for sc in items(load("storageclasses.json", {"items": []})):
        meta = sc.get("metadata", {})
        spec = sc.get("spec", {})
        ann  = meta.get("annotations", {})
        is_default = ann.get("storageclass.kubernetes.io/is-default-class") == "true"
        sc_map[meta.get("name", "")] = {
            "name":         meta.get("name", ""),
            "provisioner":  sc.get("provisioner", ""),
            "reclaim":      sc.get("reclaimPolicy", spec.get("reclaimPolicy", "Delete")),
            "binding_mode": sc.get("volumeBindingMode", spec.get("volumeBindingMode", "Immediate")),
            "expandable":   sc.get("allowVolumeExpansion", False),
            "is_default":   is_default,
            "pv_count":     0,
            "total_capacity": 0,
        }
 
    # PVs
    pvs = []
    for pv in items(load("pvs.json", {"items": []})):
        meta = pv.get("metadata", {})
        spec = pv.get("spec", {})
        st   = pv.get("status", {})
        sc   = spec.get("storageClassName", "")
        capacity_raw = spec.get("capacity", {}).get("storage", "")
        claim_ref    = spec.get("claimRef") or {}
        modes        = [str(m) for m in spec.get("accessModes", [])]
 
        csi_spec = spec.get("csi") or {}
        if csi_spec:
            vol_attrs = csi_spec.get("volumeAttributes", {})
        elif spec.get("hostPath"):
            vol_attrs = {"path": spec["hostPath"].get("path", "")}
        elif spec.get("nfs"):
            vol_attrs = {"server": spec["nfs"].get("server", ""), "path": spec["nfs"].get("path", "")}
        else:
            vol_attrs = {}
 
        pvs.append({
            "name":            meta.get("name", ""),
            "storage_class":   sc,
            "status":          st.get("phase", "Unknown"),
            "capacity":        capacity_raw,
            "access_modes":    modes,
            "reclaim":         spec.get("persistentVolumeReclaimPolicy", "Delete"),
            "claim":           f"{claim_ref.get('namespace','')}/{claim_ref.get('name','')}" if claim_ref else "",
            "volume_mode":     str(spec.get("volumeMode", "Filesystem")),
            "age":             calc_age(meta.get("creationTimestamp", "")),
        })
 
        if sc in sc_map:
            sc_map[sc]["pv_count"] += 1
 
    # PVCs
    pvcs = []
    for pvc in items(load("pvcs.json", {"items": []})):
        meta = pvc.get("metadata", {})
        spec = pvc.get("spec", {})
        st   = pvc.get("status", {})
        sc   = spec.get("storageClassName") or ""
        capacity_raw = (st.get("capacity") or {}).get("storage", "")
        modes = [str(m) for m in spec.get("accessModes", [])]
 
        pvcs.append({
            "name":          meta.get("name", ""),
            "namespace":     meta.get("namespace", ""),
            "status":        st.get("phase", "Unknown"),
            "storage_class": sc,
            "access_modes":  modes,
            "capacity":      capacity_raw,
            "volume":        spec.get("volumeName", ""),
            "volume_mode":   str((spec.get("volumeMode") or "Filesystem")),
            "age":           calc_age(meta.get("creationTimestamp", "")),
        })
 
    # CSI Drivers
    csi_drivers = []
    for d in items(load("csidrivers.json", {"items": []})):
        meta = d.get("metadata", {})
        spec = d.get("spec", {})
        csi_drivers.append({
            "name":            meta.get("name", ""),
            "attach_required": spec.get("attachRequired"),
            "pod_info_mount":  spec.get("podInfoOnMount"),
            "storage_capacity": spec.get("storageCapacity"),
            "lifecycle_modes": [str(m) for m in spec.get("volumeLifecycleModes", [])],
            "age":             calc_age(meta.get("creationTimestamp", "")),
        })
 
    # VolumeSnapshotClasses
    vscs = []
    for vsc in items(load("vsc.json", {"items": []})):
        meta = vsc.get("metadata", {})
        ann  = meta.get("annotations", {}) or {}
        is_default_vsc = ann.get("snapshot.storage.kubernetes.io/is-default-class") == "true"
        vscs.append({
            "name":            meta.get("name", ""),
            "driver":          vsc.get("driver", ""),
            "deletion_policy": vsc.get("deletionPolicy", ""),
            "is_default":      is_default_vsc,
            "age":             calc_age(meta.get("creationTimestamp", "")),
        })
 
    return {
        "storage_classes": list(sc_map.values()),
        "pvs":             pvs,
        "pvcs":            pvcs,
        "csi_drivers":     csi_drivers,
        "vscs":            vscs,
    }
 
# ─── CRDs ─────────────────────────────────────────────────────────────────────
def process_crds():
    results = []
    for crd in items(load("crds.json", {"items": []})):
        meta  = crd.get("metadata", {})
        spec  = crd.get("spec", {})
        names = spec.get("names", {})
        versions = spec.get("versions", [{}])
        latest_v = versions[-1].get("name", "") if versions else ""
 
        established = False
        for cond in crd.get("status", {}).get("conditions", []):
            if cond.get("type") == "Established" and cond.get("status") == "True":
                established = True
 
        results.append({
            "name":           meta.get("name", ""),
            "group":          spec.get("group", ""),
            "scope":          spec.get("scope", ""),
            "kind":           names.get("kind", ""),
            "plural":         names.get("plural", ""),
            "latest_version": latest_v,
            "established":    established,
            "age":            calc_age(meta.get("creationTimestamp", "")),
        })
    return results
 
# ─── Operators ───────────────────────────────────────────────────────────────
def process_operators():
    results = []
    seen = set()
    for csv in items(load("operators.json", {"items": []})):
        meta = csv.get("metadata", {})
        spec = csv.get("spec", {})
        st   = csv.get("status", {})
        name = spec.get("displayName") or meta.get("name", "")
        if name in seen:
            continue
        seen.add(name)
 
        labels   = meta.get("labels", {})
        provider = spec.get("provider", {}).get("name", "") or \
                   labels.get("operatorframework.io/vendor", "") or \
                   "Other"
 
        results.append({
            "name":        meta.get("name", ""),
            "display":     name,
            "namespace":   meta.get("namespace", ""),
            "version":     spec.get("version", ""),
            "provider":    provider,
            "phase":       st.get("phase", "Unknown"),
            "channel":     labels.get("operators.coreos.com/channel", ""),
            "age":         calc_age(meta.get("creationTimestamp", "")),
        })
    return results
 
# ─── Kasten K10 ───────────────────────────────────────────────────────────────
def process_kasten():
    kasten_pods_raw = items(load("kasten_pods.json", {"items": []}))
    installed = len(kasten_pods_raw) > 0
 
    # Version from ConfigMaps (label app=k10)
    version = "Unknown"
    for cm in items(load("kasten_cms.json", {"items": []})):
        labels = cm.get("metadata", {}).get("labels", {})
        if labels.get("app") == "k10":
            chart = labels.get("helm.sh/chart", "")
            if chart:
                parts = chart.split("-")
                if len(parts) > 1:
                    version = parts[1]
                    break
 
    # Pods
    pods = []
    running = 0
    for pod in kasten_pods_raw:
        meta   = pod.get("metadata", {})
        spec   = pod.get("spec", {})
        status = pod.get("status", {})
        phase  = status.get("phase", "Unknown")
        if phase == "Running":
            ready = True
            for cond in status.get("conditions", []):
                if cond.get("type") == "Ready" and cond.get("status") != "True":
                    ready = False
            if ready:
                running += 1
        cont = spec.get("containers", [])
        cont_st = status.get("containerStatuses", [])
        ready_c = sum(1 for cs in cont_st if cs.get("ready"))
        restarts = sum(cs.get("restartCount", 0) for cs in cont_st)
        pods.append({
            "name":     meta.get("name", ""),
            "status":   phase,
            "ready":    f"{ready_c}/{len(cont)}",
            "restarts": restarts,
            "age":      calc_age(meta.get("creationTimestamp", "")),
        })
 
    # ── Policy Run Actions: build per-policy last run status ─────────────────
    pra_data = items(load("kasten_policyrunactions.json", {"items": []}))
    policy_run_status = {}
    for pra in pra_data:
        meta_pra  = pra.get("metadata", {})
        spec_pra  = pra.get("spec", {})
        st_pra    = pra.get("status", {})
        # Policy name: try spec.policy, then labels, then derive from name
        pol_name = (spec_pra.get("policy", "")
                    or meta_pra.get("labels", {}).get("k10.kasten.io/policyName", ""))
        if not pol_name:
            # Format is usually "<policy-name>-<hash>"
            pra_name = meta_pra.get("name", "")
            parts = pra_name.rsplit("-", 1)
            if len(parts) > 1:
                pol_name = parts[0]
        ts    = meta_pra.get("creationTimestamp", "")
        state = st_pra.get("state", "Unknown")
        error = st_pra.get("error", "")
        if pol_name and ts > policy_run_status.get(pol_name, {}).get("ts", ""):
            policy_run_status[pol_name] = {"ts": ts, "state": state, "error": error}
 
    # ── Policies ─────────────────────────────────────────────────────────────
    policies = []
    for pol in items(load("kasten_policies.json", {"items": []})):
        meta = pol.get("metadata", {})
        spec = pol.get("spec", {})
        pol_name = meta.get("name", "")
        actions_raw = spec.get("actions", [])
        actions = [a.get("action", "") for a in actions_raw if isinstance(a, dict)]
 
        # Frequency and retention
        frequency = retention = ""
        for a in actions_raw:
            if isinstance(a, dict) and a.get("action") == "backup":
                bp = a.get("backupParameters") or {}
                freq_obj = bp.get("frequency") or spec.get("frequency", "")
                if isinstance(freq_obj, str):
                    frequency = freq_obj
                elif isinstance(freq_obj, dict):
                    frequency = freq_obj.get("@time", str(freq_obj))
                ret_obj = bp.get("retention") or spec.get("retention", "")
                if isinstance(ret_obj, dict):
                    day_count = ret_obj.get("daily", ret_obj.get("days", ""))
                    if day_count:
                        retention = f"{day_count}d"
                elif isinstance(ret_obj, str):
                    retention = ret_obj
 
        # Targeted namespaces
        ns_list = []
        selector = spec.get("selector") or {}
        for ns in selector.get("matchLabels", {}).get("namespaces", []):
            ns_list.append(ns)
        for expr in selector.get("matchExpressions", []):
            if isinstance(expr, dict) and expr.get("key") == "k10.kasten.io/appNamespace":
                ns_list.extend(expr.get("values", []))
 
        # Export location profile (from export action)
        export_profile = ""
        for a in actions_raw:
            if isinstance(a, dict) and a.get("action") == "export":
                ep   = a.get("exportParameters", {})
                prof = ep.get("profile", {})
                if isinstance(prof, dict):
                    export_profile = prof.get("name", "")
                elif isinstance(prof, str):
                    export_profile = prof
                break
 
        # Last run info from PolicyRunActions
        run_info = policy_run_status.get(pol_name, {})
        last_run_state = run_info.get("state", "—")
        last_run_ts    = run_info.get("ts", "")
        last_run_error = run_info.get("error", "")
 
        policies.append({
            "name":            pol_name,
            "namespace":       meta.get("namespace", ""),
            "actions":         actions,
            "namespaces":      ns_list,
            "export_profile":  export_profile,
            "frequency":       frequency,
            "retention":       retention,
            "age":             calc_age(meta.get("creationTimestamp", "")),
            "last_run_state":  last_run_state,
            "last_run_time":   fmt_date(last_run_ts),
            "last_run_age":    calc_age(last_run_ts) if last_run_ts else "—",
            "last_run_error":  last_run_error,
        })
 
    # Location Profiles
    profiles = []
    for prof in items(load("kasten_profiles.json", {"items": []})):
        meta = prof.get("metadata", {})
        spec = prof.get("spec", {})
        loc  = spec.get("locationSpec", {})
        loc_type  = loc.get("type", "Unknown")
        obj_store = loc.get("objectStore", {})
        cred      = loc.get("credential", {})
        immutable = obj_store.get("immutable", False) or bool(obj_store.get("protectionPeriod", ""))
 
        secret_type = "Unknown"
        if obj_store.get("endpoint"):
            secret_type = "Generic S3"
        elif cred.get("secretType"):
            st = cred["secretType"].lower()
            if "aws" in st:      secret_type = "AwsAccessKey"
            elif "az" in st:     secret_type = "AzStorageAccount"
            elif "google" in st: secret_type = "GcpServiceAccount"
            else:                secret_type = cred["secretType"]
 
        profiles.append({
            "name":        meta.get("name", ""),
            "store_type":  loc_type,
            "secret_type": secret_type,
            "immutable":   immutable,
            "status":      "Active" if spec.get("enabled", True) else "Disabled",
            "age":         calc_age(meta.get("creationTimestamp", "")),
            "bucket":      obj_store.get("bucketName", obj_store.get("name", "")),
            "region":      obj_store.get("region", ""),
        })
 
    # DR Info
    dr_info = {"enabled": False, "type": "None", "policy": "", "profile": "", "immutable": False}
    k10cfg = load("k10_config.json", {})
    if isinstance(k10cfg, dict):
        cfg_data = k10cfg.get("data", {})
        if isinstance(cfg_data, dict):
            if cfg_data.get("quickDisasterRecoveryEnabled") == "true":
                dr_info = {"enabled": True, "type": "QuickDR", "policy": "", "profile": "", "immutable": False}
            elif "quickDisasterRecoveryEnabled" in cfg_data:
                dr_info = {"enabled": True, "type": "LegacyDR", "policy": "", "profile": "", "immutable": False}
 
    # Last restore point timestamp
    last_rp_time = ""
    for rp in items(load("kasten_restorepoints.json", {"items": []})):
        ts = rp.get("metadata", {}).get("creationTimestamp", "")
        if ts > last_rp_time:
            last_rp_time = ts
 
    # Helm Values
    helm_values_yaml = "# Helm values collection skipped (--no-helm)" if SKIP_HELM else ""
    if not SKIP_HELM:
        helm_secrets = items(load("kasten_helm_secrets.json", {"items": []}))
        if helm_secrets:
            def helm_version(s):
                name = s.get("metadata", {}).get("name", "")
                try:
                    return int(name.split(".")[-1].lstrip("v"))
                except Exception:
                    return 0
            latest = sorted(helm_secrets, key=helm_version)[-1]
            release_b64 = latest.get("data", {}).get("release", "")
            if release_b64:
                try:
                    decoded = base64.b64decode(release_b64)
                    try:
                        decoded2 = base64.b64decode(decoded)
                        decoded = decoded2
                    except Exception:
                        pass
                    decompressed = gzip.decompress(decoded)
                    release_obj  = json.loads(decompressed)
                    config = release_obj.get("config", {})
                    helm_values_yaml = json.dumps(config, indent=2, ensure_ascii=False)
                except Exception as e:
                    helm_values_yaml = f"# Error decoding Helm values: {e}"
        else:
            helm_values_yaml = "# No Helm release secret found for K10"
 
    # Check Kasten CRDs
    kasten_crds = ["policies.config.kio.kasten.io", "k10s.config.kio.kasten.io",
                   "backupactions.actions.kio.kasten.io"]
    all_crds = {c.get("name","") for c in items(load("crds.json",{"items":[]}))}
    crds_installed = any(c in all_crds for c in kasten_crds)
 
    # ── Blueprints ───────────────────────────────────────────────────────────
    blueprints = []
    for bp in items(load("kasten_blueprints.json", {"items": []})):
        meta_bp = bp.get("metadata", {})
        spec_bp = bp.get("spec", {})
        actions_bp = list(spec_bp.get("actions", {}).keys()) if isinstance(spec_bp.get("actions"), dict) else []
        blueprints.append({
            "name":      meta_bp.get("name", ""),
            "namespace": meta_bp.get("namespace", "kasten-io"),
            "actions":   actions_bp,
            "age":       calc_age(meta_bp.get("creationTimestamp", "")),
        })

    # ── BlueprintBindings ─────────────────────────────────────────────────────
    blueprint_bindings = []
    for bb in items(load("kasten_blueprintbindings.json", {"items": []})):
        meta_bb = bb.get("metadata", {})
        spec_bb = bb.get("spec", {})
        blueprint_bindings.append({
            "name":      meta_bb.get("name", ""),
            "namespace": meta_bb.get("namespace", "kasten-io"),
            "blueprint": spec_bb.get("blueprintRef", {}).get("name", "—"),
            "subject":   spec_bb.get("subject", {}).get("name", "—"),
            "age":       calc_age(meta_bb.get("creationTimestamp", "")),
        })

    # ── TransformSets ─────────────────────────────────────────────────────────
    transform_sets = []
    for ts in items(load("kasten_transformsets.json", {"items": []})):
        meta_ts = ts.get("metadata", {})
        spec_ts = ts.get("spec", {})
        transform_sets.append({
            "name":       meta_ts.get("name", ""),
            "namespace":  meta_ts.get("namespace", "kasten-io"),
            "transforms": len(spec_ts.get("transforms", [])),
            "age":        calc_age(meta_ts.get("creationTimestamp", "")),
        })

    # ── Reports ───────────────────────────────────────────────────────────────
    report_policy = next(
        (p for p in policies if p["name"] == "k10-system-reports-policy"), None
    )
    report_actions = []
    for ra in items(load("kasten_reportactions.json", {"items": []})):
        meta_ra = ra.get("metadata", {})
        st_ra   = ra.get("status", {})
        report_actions.append({
            "name":  meta_ra.get("name", ""),
            "state": st_ra.get("state", "Unknown"),
            "age":   calc_age(meta_ra.get("creationTimestamp", "")),
        })
    report_actions.sort(key=lambda x: x["age"])

    # ── RestoreActions ────────────────────────────────────────────────────────
    restore_actions = []
    for rsa in items(load("kasten_restoreactions.json", {"items": []})):
        meta_rsa = rsa.get("metadata", {})
        st_rsa   = rsa.get("status", {})
        err_rsa  = st_rsa.get("error", {})
        err_msg  = err_rsa.get("message", "") if isinstance(err_rsa, dict) else ""
        restore_actions.append({
            "name":      meta_rsa.get("name", ""),
            "namespace": meta_rsa.get("namespace", ""),
            "state":     st_rsa.get("state", "Unknown"),
            "error":     err_msg,
            "age":       calc_age(meta_rsa.get("creationTimestamp", "")),
            "ts":        meta_rsa.get("creationTimestamp", ""),
        })
    restore_actions.sort(key=lambda x: x["ts"], reverse=True)

    return {
        "installed":          installed,
        "crds_installed":     crds_installed,
        "version":            version,
        "running_pods":       running,
        "total_pods":         len(pods),
        "pods":               pods,
        "policies":           policies,
        "profiles":           profiles,
        "dr_info":            dr_info,
        "last_restore_point": calc_age(last_rp_time) if last_rp_time else "—",
        "helm_values":        helm_values_yaml,
        "blueprints":         blueprints,
        "blueprint_bindings": blueprint_bindings,
        "transform_sets":     transform_sets,
        "report_policy":      report_policy,
        "report_actions":     report_actions,
        "restore_actions":    restore_actions,
    }
 
# ─── Namespace Protection ─────────────────────────────────────────────────────
def process_namespace_protection():
    """
    For each namespace, determine:
    - Whether it is protected by a Kasten backup policy
    - Which policy covers it
    - Last successful backup date (from RunActions)
    - Which location profile(s) are used for export (per policy)
    """
    all_ns_list = [n.get("metadata", {}).get("name", "") for n in items(namespaces_data)]
 
    # ns -> {protected, policies, policy_profiles, last_backup_ts, last_backup_fmt}
    protection = {
        ns: {
            "protected":      False,
            "policies":       [],
            "policy_profiles": {},   # pol_name -> export_profile_name
            "last_backup_ts": "",
            "last_backup_fmt": "—",
        }
        for ns in all_ns_list
    }
 
    # Build ns -> last successful backup timestamp from RunActions
    ns_last_backup = {}
    for ra in items(load("kasten_runactions.json", {"items": []})):
        meta_ra = ra.get("metadata", {})
        ns      = meta_ra.get("namespace", "")
        st_ra   = ra.get("status", {})
        state   = st_ra.get("state", "")
        ts      = meta_ra.get("creationTimestamp", "")
        if state in ("Complete", "Succeeded", "Success") and ns:
            if ts > ns_last_backup.get(ns, ""):
                ns_last_backup[ns] = ts
 
    # Map policies to namespaces (and collect export profile per policy)
    for pol in items(load("kasten_policies.json", {"items": []})):
        meta_pol = pol.get("metadata", {})
        spec_pol = pol.get("spec", {})
        pol_name = meta_pol.get("name", "")
        targeted = set()
        selector = spec_pol.get("selector") or {}
 
        # Explicit namespace list
        for ns_name in selector.get("matchLabels", {}).get("namespaces", []):
            targeted.add(ns_name)
 
        # matchExpressions with k10.kasten.io/appNamespace
        for expr in selector.get("matchExpressions", []):
            if isinstance(expr, dict):
                key = expr.get("key", "")
                op  = expr.get("operator", "")
                if key == "k10.kasten.io/appNamespace" and op in ("In", "Equals", "In"):
                    targeted.update(expr.get("values", []))
 
        # Fallback: use RunActions labelled with this policy
        if not targeted:
            for ra in items(load("kasten_runactions.json", {"items": []})):
                ra_meta   = ra.get("metadata", {})
                ra_labels = ra_meta.get("labels", {})
                if ra_labels.get("k10.kasten.io/policyName") == pol_name:
                    ns = ra_meta.get("namespace", "")
                    if ns:
                        targeted.add(ns)
 
        # Extract export location profile for this policy
        export_profile = ""
        for a in (spec_pol.get("actions") or []):
            if isinstance(a, dict) and a.get("action") == "export":
                ep   = a.get("exportParameters", {})
                prof = ep.get("profile", {})
                if isinstance(prof, dict):
                    export_profile = prof.get("name", "")
                elif isinstance(prof, str):
                    export_profile = prof
                break
 
        for ns in targeted:
            if ns in protection:
                protection[ns]["protected"] = True
                if pol_name not in protection[ns]["policies"]:
                    protection[ns]["policies"].append(pol_name)
                if export_profile:
                    protection[ns]["policy_profiles"][pol_name] = export_profile
 
    # Attach last backup info
    for ns, info in protection.items():
        lb_ts = ns_last_backup.get(ns, "")
        if lb_ts:
            info["last_backup_ts"]  = lb_ts
            info["last_backup_fmt"] = fmt_date(lb_ts)
        elif info["protected"]:
            info["last_backup_fmt"] = "No record yet"
 
    return protection
 
# ─── Failed BackupActions ─────────────────────────────────────────────────────
def process_failed_backup_actions():
    """
    Load BackupActions from all app namespaces, keep only Failed ones,
    parse the nested cause chain for a meaningful error message, and return
    two lookup dicts:
      by_policy[policy_name] -> most recent failure entry
      by_ns[namespace]       -> most recent failure entry
    """
    def _extract_msg(obj, max_depth=4, depth=0):
        """Recurse into the nested cause JSON chain and return the deepest message."""
        if not isinstance(obj, dict) or depth > max_depth:
            return ""
        msg       = obj.get("message", "")
        cause_raw = obj.get("cause", "")
        if not cause_raw:
            return msg
        try:
            cause_obj = json.loads(cause_raw) if isinstance(cause_raw, str) else cause_raw
            deeper = _extract_msg(cause_obj, max_depth, depth + 1)
            return deeper if deeper else msg
        except Exception:
            return msg

    by_policy = {}
    by_ns     = {}

    for ba in items(load("kasten_backupactions.json", {"items": []})):
        status = ba.get("status") or {}
        if status.get("state") != "Failed":
            continue
        meta        = ba.get("metadata") or {}
        labels      = meta.get("labels") or {}
        ns          = labels.get("k10.kasten.io/appNamespace") or meta.get("namespace", "")
        policy      = labels.get("k10.kasten.io/policyName", "")
        ts          = meta.get("creationTimestamp", "")
        err_obj     = status.get("error") or {}
        top_msg     = err_obj.get("message", "")
        display_msg = _extract_msg(err_obj, max_depth=4)
        if not display_msg:
            display_msg = top_msg
        if len(display_msg) > 180:
            display_msg = display_msg[:180] + "\u2026"

        entry = {
            "ns":          ns,
            "policy":      policy,
            "timestamp":   ts,
            "age":         calc_age(ts),
            "display_msg": display_msg,
        }
        # Keep only most recent failure per policy / namespace
        if policy and (policy not in by_policy or ts > by_policy[policy]["timestamp"]):
            by_policy[policy] = entry
        if ns and (ns not in by_ns or ts > by_ns[ns]["timestamp"]):
            by_ns[ns] = entry

    return {"by_policy": by_policy, "by_ns": by_ns}

# ─── Network / CNI ───────────────────────────────────────────────────────────
def process_network():
    cni_type = "Unknown"
    cni_pods = []
    ks_pods  = items(load("kube_system_pods.json", {"items": []}))
    cni_patterns = {
        "cilium":   "Cilium",
        "calico":   "Calico",
        "flannel":  "Flannel",
        "weave":    "Weave",
        "canal":    "Canal",
        "antrea":   "Antrea",
        "multus":   "Multus",
        "kube-ovn": "OVN",
        "ovs-cni":  "OVS",
    }
    for pod in ks_pods:
        name = pod.get("metadata", {}).get("name", "").lower()
        for pattern, label in cni_patterns.items():
            if pattern in name:
                if cni_type == "Unknown":
                    cni_type = label
                meta = pod.get("metadata", {})
                cont = pod.get("spec", {}).get("containers", [])
                st   = pod.get("status", {})
                cni_pods.append({
                    "name":      meta.get("name", ""),
                    "namespace": meta.get("namespace", ""),
                    "status":    st.get("phase", "Unknown"),
                    "version":   cont[0].get("image", "").split(":")[-1] if cont else "",
                })
                break
 
    components = [
        {"name": p["name"], "ns": p["namespace"], "status": p["status"], "version": p["version"]}
        for p in cni_pods
    ]
 
    net_policies = []
    for np in items(load("netpols.json", {"items": []})):
        meta = np.get("metadata", {})
        spec = np.get("spec", {})
        policy_types = spec.get("policyTypes", [])
        net_policies.append({
            "name":      meta.get("name", ""),
            "namespace": meta.get("namespace", ""),
            "ingress":   "Ingress" in policy_types,
            "egress":    "Egress" in policy_types,
            "age":       calc_age(meta.get("creationTimestamp", "")),
        })
 
    ipv6_enabled = False
    for node in nodes_raw:
        for addr in node.get("status", {}).get("addresses", []):
            if ":" in addr.get("address", ""):
                ipv6_enabled = True
                break
 
    return {
        "cni_type":     cni_type,
        "components":   components,
        "ipv6":         ipv6_enabled,
        "net_policies": net_policies,
        "np_count":     len(net_policies),
    }
 
# ─── Events ───────────────────────────────────────────────────────────────────
def process_events():
    results = []
    for ev in items(load("events.json", {"items": []})):
        meta = ev.get("metadata", {})
        iv   = ev.get("involvedObject", {})
        results.append({
            "namespace":   meta.get("namespace", ""),
            "type":        ev.get("type", "Normal"),
            "reason":      ev.get("reason", ""),
            "message":     ev.get("message", ""),
            "object_kind": iv.get("kind", ""),
            "object_name": iv.get("name", ""),
            "count":       ev.get("count", 1),
            "last_time":   ev.get("lastTimestamp", ev.get("eventTime", "")),
            "age":         calc_age(ev.get("lastTimestamp") or ev.get("eventTime") or
                                    meta.get("creationTimestamp", "")),
        })
    results.sort(key=lambda e: (0 if e["type"] == "Warning" else 1, e["namespace"]))
    return results
 
# =============================================================================
# REPORT ASSEMBLY
# =============================================================================
TIMESTAMP = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
CONTEXT   = load_text("context.json").strip('"')
 
nodes         = process_nodes()
pods          = process_pods()
services      = process_services()
storage       = process_storage()
crds          = process_crds()
operators     = process_operators()
kasten        = process_kasten()
network       = process_network()
events        = process_events()
ns_protection = process_namespace_protection()
failed_backup_actions = process_failed_backup_actions()
 
total_pods   = len(pods)
running_pods = sum(1 for p in pods if p["status"] == "Running")
warn_events  = sum(1 for e in events if e["type"] == "Warning")

# Policies with a recent failed BackupAction (source of truth for failure status)
failed_policies = [
    p for p in kasten["policies"]
    if p["name"] in failed_backup_actions["by_policy"]
]

# Namespaces with at least one failed BackupAction
failed_ns_set = set(failed_backup_actions["by_ns"].keys())

# Protected = has a policy AND no recent failure
protected_ns = sum(
    1 for ns, info in ns_protection.items()
    if info["protected"] and ns not in failed_ns_set
)
 
# =============================================================================
# HTML GENERATION
# =============================================================================
 
def groups(items_list, key):
    g = {}
    for item in items_list:
        k = item.get(key, "")
        g.setdefault(k, []).append(item)
    return g
 
def table_row(*cells, tr_class=""):
    cls = f' class="{tr_class}"' if tr_class else ""
    tds = "".join(f"<td>{c}</td>" for c in cells)
    return f"<tr{cls}>{tds}</tr>\n"
 
def th_row(*headers):
    ths = "".join(f"<th>{h(hdr)}</th>" for hdr in headers)
    return f"<thead><tr>{ths}</tr></thead>\n"
 
# ─── HTML Sections ────────────────────────────────────────────────────────────
 
def render_overview():
    kasten_status = (
        f'<span class="badge badge-green">Installed v{h(kasten["version"])}</span>'
        if kasten["installed"]
        else '<span class="badge badge-gray">Not detected</span>'
    )
    sc_default = next((s["name"] for s in storage["storage_classes"] if s["is_default"]), "—")
    cni_badge  = f'<span class="badge badge-blue">{h(network["cni_type"])}</span>'
    failed_ns_count = sum(1 for ns, info in ns_protection.items() if info["protected"] and ns in failed_ns_set)
    ns_prot_badge = f'<span class="badge badge-green">{protected_ns}</span>'
    ns_fail_badge = (f'&nbsp;<span class="badge badge-red">{failed_ns_count} failed</span>' if failed_ns_count else "")
    failed_badge  = (
        f'<span class="badge badge-red">{len(failed_policies)} failed</span>'
        if failed_policies else
        '<span class="badge badge-green">All OK</span>'
    )

    return f"""
<section id="overview">
  <details class="section-toggle" open>
    <summary class="section-summary"><h2>Overview</h2></summary>
    <div class="section-body">
    <div class="cards-grid">
      <a href="#overview" class="card-link"><div class="card clickable">
        <div class="card-icon">&#x2388;</div>
        <div class="card-title">Cluster</div>
        <div class="card-value">{h(CONTEXT)}</div>
        <div class="card-sub">Version {h(server_version)}</div>
      </div></a>
      <a href="#overview" class="card-link"><div class="card clickable">
        <div class="card-icon">&#x1F4E6;</div>
        <div class="card-title">Distribution</div>
        <div class="card-value">{h(dist_type)}</div>
        <div class="card-sub">{h(platform)}</div>
      </div></a>
      <a href="#nodes" class="card-link"><div class="card clickable">
        <div class="card-icon">&#x1F5A5;</div>
        <div class="card-title">Nodes</div>
        <div class="card-value">{len(nodes)}</div>
        <div class="card-sub">{sum(1 for n in nodes if n['status']=='Ready')} Ready</div>
      </div></a>
      <a href="#pods" class="card-link"><div class="card clickable">
        <div class="card-icon">&#x1F4BB;</div>
        <div class="card-title">Pods</div>
        <div class="card-value">{total_pods}</div>
        <div class="card-sub">{running_pods} Running</div>
      </div></a>
      <a href="#kasten-namespaces" class="card-link"><div class="card clickable">
        <div class="card-icon">&#x1F4C1;</div>
        <div class="card-title">Namespaces</div>
        <div class="card-value">{ns_count}</div>
        <div class="card-sub">{ns_prot_badge} protected{ns_fail_badge}</div>
      </div></a>
      <a href="#storage" class="card-link"><div class="card clickable">
        <div class="card-icon">&#x1F4BE;</div>
        <div class="card-title">PVCs</div>
        <div class="card-value">{len(storage['pvcs'])}</div>
        <div class="card-sub">Default SC: {h(sc_default)}</div>
      </div></a>
      <a href="#kasten" class="card-link"><div class="card clickable">
        <div class="card-icon">&#x1F6E1;</div>
        <div class="card-title">Veeam Kasten</div>
        <div class="card-value">{kasten_status}</div>
        <div class="card-sub">{kasten['running_pods']}/{kasten['total_pods']} pods running</div>
      </div></a>
      <a href="#kasten-backup-policies" class="card-link"><div class="card{'  card-warn' if failed_policies else ''} clickable">
        <div class="card-icon">&#x1F4CB;</div>
        <div class="card-title">Backup Policies</div>
        <div class="card-value">{len(kasten['policies'])}</div>
        <div class="card-sub">
          {failed_badge}
          {'&nbsp;<span class="badge badge-orange">' + str(ns_count - protected_ns - failed_ns_count) + ' ns unprotected</span>' if (ns_count - protected_ns - failed_ns_count) > 0 else ""}
        </div>
      </div></a>
      <a href="#network" class="card-link"><div class="card clickable">
        <div class="card-icon">&#x1F310;</div>
        <div class="card-title">Network (CNI)</div>
        <div class="card-value">{cni_badge}</div>
        <div class="card-sub">{network['np_count']} NetworkPolicies</div>
      </div></a>
      <a href="#events" class="card-link"><div class="card{'  card-warn' if warn_events > 0 else ''} clickable">
        <div class="card-icon">&#x26A0;</div>
        <div class="card-title">Warning Events</div>
        <div class="card-value {'text-warn' if warn_events > 0 else ''}">{warn_events}</div>
        <div class="card-sub">{len(events)} total events</div>
      </div></a>
    </div>
    </div>
  </details>
</section>
"""
 
def render_nodes():
    rows = ""
    for n in nodes:
        roles_str  = ", ".join(n["roles"])
        labels_tip = "&#10;".join(h(l) for l in n["labels"][:10])
        taints_tip = "&#10;".join(h(t) for t in n["taints"]) if n["taints"] else "No taints"
        metrics_cpu = f'{n["cpu_usage"]} ({n["cpu_pct"]})' if n["cpu_usage"] != "N/A" else "N/A"
        metrics_mem = f'{n["mem_usage"]} ({n["mem_pct"]})' if n["mem_usage"] != "N/A" else "N/A"
        rows += table_row(
            h(n["name"]),
            status_badge(n["status"]),
            h(roles_str),
            h(n["version"]),
            h(n["instance_type"]),
            h(n["cloud"]),
            h(n["cpu"]),
            metrics_cpu,
            h(n["memory"]),
            metrics_mem,
            h(n["age"]),
            f'<span title="{labels_tip}" class="hint">👁 {len(n["labels"])} labels</span>'
            + (f' <span title="{taints_tip}" class="badge badge-yellow">⚠ {len(n["taints"])} taints</span>'
               if n["taints"] else ""),
        )
    return f"""
<section id="nodes">
  <details class="section-toggle">
    <summary class="section-summary"><h2>Nodes <span class="count">{len(nodes)}</span></h2></h2></summary>
    <div class="section-body">
    <div class="table-wrap">
      <table class="data-table">
        {th_row("Name","Status","Roles","Version","Instance","Cloud",
                "CPU (cap.)","CPU (usage)","RAM (cap.)","RAM (usage)","Age","Info")}
        <tbody>{rows}</tbody>
      </table>
    </div>
    </div>
  </details>
</section>
"""
 
def render_pods():
    by_ns = groups(pods, "namespace")
    sections = ""
    for ns in sorted(by_ns):
        ns_pods = by_ns[ns]
        rows = ""
        for p in ns_pods:
            rows += table_row(
                h(p["name"]),
                status_badge(p["status"]),
                h(p["ready"]),
                str(p["restart_count"]),
                h(p["node_name"]),
                h(p["cpu_request"]),
                h(p["cpu_limit"]),
                h(p["mem_request"]),
                h(p["mem_limit"]),
                h(p["age"]),
                f'{h(p["owner_kind"])}/<span title="{h(p["owner_name"])}">{h(p["owner_name"][:30])}</span>'
                if p["owner_kind"] else "—",
            )
        sections += f"""
  <details>
    <summary class="ns-header">
      <span class="ns-name">{h(ns)}</span>
      <span class="ns-count">{len(ns_pods)} pods</span>
    </summary>
    <div class="table-wrap">
      <table class="data-table">
        {th_row("Name","Status","Ready","Restarts","Node","CPU Req","CPU Lim","Mem Req","Mem Lim","Age","Owner")}
        <tbody>{rows}</tbody>
      </table>
    </div>
  </details>"""
    return f"""
<section id="pods">
  <details class="section-toggle">
    <summary class="section-summary"><h2>Pods <span class="count">{total_pods}</span>
    <span class="sub-count">{running_pods} Running</span>
  </h2></h2></summary>
    <div class="section-body">
    {sections}
    </div>
  </details>
</section>
"""
 
def render_services():
    by_ns = groups(services, "namespace")
    sections = ""
    for ns in sorted(by_ns):
        ns_svcs = by_ns[ns]
        rows = ""
        for s in ns_svcs:
            ports_str = ", ".join(
                f"{p['port']}/{p['protocol']}"
                + (f"→{p['target_port']}" if p["target_port"] else "")
                + (f" (nodePort:{p['node_port']})" if p["node_port"] else "")
                for p in s["ports"]
            )
            ext = s["lb_ip"] or ", ".join(s["external_ips"]) or "—"
            rows += table_row(
                h(s["name"]),
                status_badge(s["type"]),
                h(s["cluster_ip"]),
                h(ext),
                h(ports_str),
                h(s["age"]),
            )
        sections += f"""
  <details>
    <summary class="ns-header">
      <span class="ns-name">{h(ns)}</span>
      <span class="ns-count">{len(ns_svcs)} services</span>
    </summary>
    <div class="table-wrap">
      <table class="data-table">
        {th_row("Name","Type","ClusterIP","External IP","Ports","Age")}
        <tbody>{rows}</tbody>
      </table>
    </div>
  </details>"""
    return f"""
<section id="services">
  <details class="section-toggle">
    <summary class="section-summary"><h2>Services <span class="count">{len(services)}</span></h2></h2></summary>
    <div class="section-body">
    {sections}
    </div>
  </details>
</section>
"""
 
def render_storage():
    # Build driver → list of VolumeSnapshotClass objects for quick lookup
    _driver_vscs = {}
    for _v in storage["vscs"]:
        _driver_vscs.setdefault(_v["driver"], []).append(_v)

    sc_rows = ""
    for sc in storage["storage_classes"]:
        default_badge = '<span class="badge badge-blue">default</span>' if sc["is_default"] else ""
        expand_badge  = '<span class="badge badge-green">yes</span>' if sc["expandable"] \
                        else '<span class="badge badge-gray">no</span>'

        # VolumeSnapshotClass cell — one badge per matching VSC
        matched_vscs = _driver_vscs.get(sc["provisioner"], [])
        if matched_vscs:
            vsc_badges = []
            for _v in matched_vscs:
                _def = '<span class="badge badge-blue" style="font-size:10px">default</span>&nbsp;' \
                       if _v["is_default"] else ""
                vsc_badges.append(
                    f'{_def}<span class="badge badge-green">{h(_v["name"])}</span>'
                )
            vsc_cell = " ".join(vsc_badges)
        else:
            vsc_cell = '<span class="badge badge-gray">—</span>'

        sc_rows += table_row(
            h(sc["name"]) + " " + default_badge,
            h(sc["provisioner"]),
            h(sc["reclaim"]),
            h(sc["binding_mode"]),
            expand_badge,
            str(sc["pv_count"]),
            vsc_cell,
        )
 
    pv_rows = ""
    for pv in storage["pvs"]:
        pv_rows += table_row(
            h(pv["name"]),
            status_badge(pv["status"]),
            h(pv["capacity"]),
            h(pv["storage_class"]),
            h(", ".join(pv["access_modes"])),
            h(pv["reclaim"]),
            h(pv["volume_mode"]),
            (f'<a href="#pvc-{pv["claim"].replace("/", "-")}">{h(pv["claim"])}</a>'
             if pv.get("claim") else "—"),
            h(pv["age"]),
        )
 
    pvc_rows_by_ns = ""
    pvcs_by_ns = groups(storage["pvcs"], "namespace")
    for ns in sorted(pvcs_by_ns):
        rows = ""
        for pvc in pvcs_by_ns[ns]:
            pvc_anchor_id = f'pvc-{ns}-{pvc["name"]}'
            rows += f'<tr id="{pvc_anchor_id}"><td>{h(pvc["name"])}</td>'
            rows += "".join(f"<td>{c}</td>" for c in [
                status_badge(pvc["status"]),
                h(pvc["capacity"]),
                h(pvc["storage_class"]),
                h(", ".join(pvc["access_modes"])),
                h(pvc["volume_mode"]),
                h(pvc["volume"] or "—"),
                h(pvc["age"]),
            ]) + "</tr>\n"
            continue
        for pvc in []:  # dead loop – rows built above
            rows += table_row(
                h(pvc["name"]),
                status_badge(pvc["status"]),
                h(pvc["capacity"]),
                h(pvc["storage_class"]),
                h(", ".join(pvc["access_modes"])),
                h(pvc["volume_mode"]),
                h(pvc["volume"] or "—"),
                h(pvc["age"]),
            )
        pvc_rows_by_ns += f"""
    <details>
      <summary class="ns-header">
        <span class="ns-name">{h(ns)}</span>
        <span class="ns-count">{len(pvcs_by_ns[ns])} PVCs</span>
      </summary>
      <div class="table-wrap">
        <table class="data-table">
          {th_row("Name","Status","Capacity","StorageClass","Modes","VolumeMode","Volume","Age")}
          <tbody>{rows}</tbody>
        </table>
      </div>
    </details>"""
 
    csi_rows = ""
    for d in storage["csi_drivers"]:
        csi_rows += table_row(
            h(d["name"]),
            "yes" if d.get("attach_required") else "no",
            "yes" if d.get("pod_info_mount") else "no",
            "yes" if d.get("storage_capacity") else "no",
            h(", ".join(d["lifecycle_modes"]) or "—"),
            h(d["age"]),
        )
 
    vsc_rows = ""
    for v in storage["vscs"]:
        vsc_rows += table_row(
            h(v["name"]),
            h(v["driver"]),
            h(v["deletion_policy"]),
            h(v["age"]),
        )
 
    return f"""
<section id="storage">
  <details class="section-toggle">
    <summary class="section-summary"><h2>Storage</h2></h2></summary>
    <div class="section-body">

    <h3>CSI Drivers <span class="count">{len(storage['csi_drivers'])}</span></h3>
    <div class="table-wrap">
      <table class="data-table">
        {th_row("Name","AttachRequired","PodInfoOnMount","StorageCapacity","LifecycleModes","Age")}
        <tbody>{csi_rows}</tbody>
      </table>
    </div>

    <h3>StorageClasses <span class="count">{len(storage['storage_classes'])}</span></h3>
    <div class="table-wrap">
      <table class="data-table">
        {th_row("Name","Provisioner","ReclaimPolicy","BindingMode","Expandable","PVs","VolumeSnapshotClass")}
        <tbody>{sc_rows}</tbody>
      </table>
    </div>

    <h3>VolumeSnapshotClasses <span class="count">{len(storage['vscs'])}</span></h3>
    {"<p class='empty'>No VolumeSnapshotClass detected</p>" if not storage['vscs'] else f'''
    <div class="table-wrap">
      <table class="data-table">
        {th_row("Name","Driver","DeletionPolicy","Age")}
        <tbody>{vsc_rows}</tbody>
      </table>
    </div>'''}

    <h3>PersistentVolumes <span class="count">{len(storage['pvs'])}</span></h3>
    <div class="table-wrap">
      <table class="data-table">
        {th_row("Name","Status","Capacity","StorageClass","Modes","Reclaim","VolumeMode","Claim","Age")}
        <tbody>{pv_rows}</tbody>
      </table>
    </div>

    <h3>PersistentVolumeClaims <span class="count">{len(storage['pvcs'])}</span></h3>
    {pvc_rows_by_ns}

    </div>
  </details>
</section>
"""
 
def render_crds():
    by_group = groups(crds, "group")
    sections = ""
    for grp in sorted(by_group):
        grp_crds = by_group[grp]
        rows = ""
        for c in grp_crds:
            est_badge = '<span class="badge badge-green">✓</span>' if c["established"] \
                       else '<span class="badge badge-red">✗</span>'
            rows += table_row(
                h(c["kind"]),
                h(c["plural"]),
                h(c["latest_version"]),
                h(c["scope"]),
                est_badge,
                h(c["age"]),
            )
        grp_display = grp or "(core)"
        sections += f"""
  <details>
    <summary class="ns-header">
      <span class="ns-name">{h(grp_display)}</span>
      <span class="ns-count">{len(grp_crds)} CRDs</span>
    </summary>
    <div class="table-wrap">
      <table class="data-table">
        {th_row("Kind","Plural","Version","Scope","Established","Age")}
        <tbody>{rows}</tbody>
      </table>
    </div>
  </details>"""
    return f"""
<section id="crds">
  <details class="section-toggle">
    <summary class="section-summary"><h2>Custom Resource Definitions <span class="count">{len(crds)}</span></h2></h2></summary>
    <div class="section-body">
    {sections}
    </div>
  </details>
</section>
"""
 
def render_operators():
    if not operators:
        return """
<section id="operators">
  <details class="section-toggle">
    <summary class="section-summary"><h2>Operators</h2></h2></summary>
    <div class="section-body">
    <p class="empty">No OLM operator (ClusterServiceVersion) detected on this cluster.</p>
    </div>
  </details>
</section>
"""
    by_provider = groups(operators, "provider")
    sections = ""
    for prov in sorted(by_provider):
        prov_ops = by_provider[prov]
        rows = ""
        for op in prov_ops:
            rows += table_row(
                h(op["display"]),
                h(op["namespace"]),
                h(op["version"]),
                status_badge(op["phase"]),
                h(op["channel"] or "—"),
                h(op["age"]),
            )
        sections += f"""
  <details>
    <summary class="ns-header">
      <span class="ns-name">{h(prov)}</span>
      <span class="ns-count">{len(prov_ops)} operator(s)</span>
    </summary>
    <div class="table-wrap">
      <table class="data-table">
        {th_row("Name","Namespace","Version","Status","Channel","Age")}
        <tbody>{rows}</tbody>
      </table>
    </div>
  </details>"""
    return f"""
<section id="operators">
  <details class="section-toggle">
    <summary class="section-summary"><h2>Operators <span class="count">{len(operators)}</span></h2></h2></summary>
    <div class="section-body">
    {sections}
    </div>
  </details>
</section>
"""
 
def _parse_storage_bytes(cap_str):
    """Convert a Kubernetes capacity string (e.g. '8Gi', '500Mi') to bytes."""
    if not cap_str:
        return 0
    cap_str = cap_str.strip()
    units = {
        "Ki": 1024, "Mi": 1024**2, "Gi": 1024**3, "Ti": 1024**4,
        "K":  1000, "M":  1000**2, "G":  1000**3, "T":  1000**4,
    }
    for suffix, mult in units.items():
        if cap_str.endswith(suffix):
            try:
                return int(float(cap_str[:-len(suffix)]) * mult)
            except ValueError:
                return 0
    try:
        return int(cap_str)
    except ValueError:
        return 0

def _fmt_storage(total_bytes):
    """Format a byte count as a human-readable string (Gi / Mi / Ki)."""
    if total_bytes >= 1024**3:
        return f"{total_bytes / 1024**3:.1f} Gi"
    if total_bytes >= 1024**2:
        return f"{total_bytes / 1024**2:.0f} Mi"
    if total_bytes >= 1024:
        return f"{total_bytes / 1024:.0f} Ki"
    return f"{total_bytes} B"

# Build per-namespace PVC stats (count + total capacity)
_ns_pvcs = {}
for _pvc in storage.get("pvcs", []):
    _ns = _pvc.get("namespace", "")
    _bytes = _parse_storage_bytes(_pvc.get("capacity", ""))
    if _ns not in _ns_pvcs:
        _ns_pvcs[_ns] = {"count": 0, "bytes": 0}
    _ns_pvcs[_ns]["count"] += 1
    _ns_pvcs[_ns]["bytes"] += _bytes

def render_namespaces():
    """
    Returns a <div id="kasten-namespaces"> block for integration inside the
    Veeam Kasten section.  Includes a Location Profile column showing which
    export profile each policy uses for the namespace, plus PVC count and total
    storage per namespace, and a Last Restore column.
    """
    # Build immutable profile set for badge colouring
    immutable_profiles = {p["name"] for p in kasten["profiles"] if p["immutable"]}

    # Build per-namespace latest RestoreAction map
    ns_last_restore = {}  # ns -> latest RestoreAction dict
    for rsa in kasten.get("restore_actions", []):
        ns = rsa["namespace"]
        existing = ns_last_restore.get(ns)
        if existing is None or rsa["ts"] > existing["ts"]:
            ns_last_restore[ns] = rsa
 
    rows = ""
    for ns_name in sorted(ns_protection.keys()):
        info = ns_protection[ns_name]
        ba_fail = failed_backup_actions["by_ns"].get(ns_name)

        if info["protected"]:
            # For kasten-io: only k10-disaster-recovery-policy counts for protection
            if ns_name == "kasten-io":
                dr_pol_present = "k10-disaster-recovery-policy" in info["policies"]
                display_policies = ["k10-disaster-recovery-policy"] if dr_pol_present else []
                is_protected_here = dr_pol_present
                if not is_protected_here:
                    prot_badge = '<span class="badge badge-orange">&#x26A0; Unprotected</span>'
                elif ba_fail:
                    prot_badge = '<span class="badge badge-red">&#x2716; Failed</span>'
                else:
                    prot_badge = '<span class="badge badge-green">&#x2713; Protected</span>'
                policies_str = ", ".join(display_policies) if display_policies else "—"
            else:
                display_policies = info["policies"]
                if ba_fail:
                    prot_badge = '<span class="badge badge-red">&#x2716; Failed</span>'
                else:
                    prot_badge = '<span class="badge badge-green">&#x2713; Protected</span>'
                policies_str = ", ".join(info["policies"]) if info["policies"] else "—"

            # Location Profile cell — one badge per (policy, profile) pair
            pp = info.get("policy_profiles", {})
            _pol_list = display_policies if ns_name == "kasten-io" else info["policies"]
            if pp:
                profile_badges = []
                for pol_name in _pol_list:
                    prof_name = pp.get(pol_name, "")
                    if prof_name:
                        badge_cls = "badge-green" if prof_name in immutable_profiles else "badge-gray"
                        title_attr = f' title="{h(pol_name)}"'
                        profile_badges.append(
                            f'<span class="badge {badge_cls}"{title_attr}>{h(prof_name)}</span>'
                        )
                    else:
                        profile_badges.append(
                            f'<span class="badge badge-gray" title="{h(pol_name)} — no export">—</span>'
                        )
                loc_profile_cell = " ".join(profile_badges)
            else:
                loc_profile_cell = "—"

        else:
            prot_badge       = '<span class="badge badge-orange">&#x26A0; Unprotected</span>'
            policies_str     = "—"
            loc_profile_cell = "—"

        # PVC stats for this namespace
        pvc_info = _ns_pvcs.get(ns_name, {"count": 0, "bytes": 0})
        pvc_count = pvc_info["count"]
        pvc_storage = _fmt_storage(pvc_info["bytes"]) if pvc_info["bytes"] > 0 else "—"
        pvc_cell = f"{pvc_count} &nbsp;<span style='color:var(--text-muted);font-size:11px'>({pvc_storage})</span>" if pvc_count > 0 else "0"

        # Last backup status from failed BackupActions
        if ba_fail and info["protected"]:
            backup_cell = (
                '<span class="badge badge-red">&#x2716; Failed</span>'
                f'<div class="bp-detail" style="color:var(--red)">'
                f'&#x26A0; {h(ba_fail["display_msg"])}</div>'
                f'<div class="bp-detail" style="color:var(--text-muted)">'
                f'Policy: {h(ba_fail["policy"])} &nbsp;·&nbsp; {h(ba_fail["age"])} ago</div>'
            )
        elif info["protected"]:
            backup_cell = '<span class="badge badge-green">&#x2713; OK</span>'
        else:
            backup_cell = "—"

        # Last restore status from RestoreActions
        rsa = ns_last_restore.get(ns_name)
        if rsa:
            rsa_state = rsa["state"]
            if rsa_state in ("Failed", "Error"):
                restore_cell = (
                    '<span class="badge badge-red">&#x2716; Failed</span>'
                    + (f'<div class="bp-detail" style="color:var(--red)">&#x26A0; {h(rsa["error"])}</div>'
                       if rsa["error"] else "")
                    + f'<div class="bp-detail" style="color:var(--text-muted)">{h(rsa["age"])} ago</div>'
                )
            elif rsa_state in ("Complete", "Succeeded", "Success"):
                restore_cell = (
                    '<span class="badge badge-green">&#x2713; OK</span>'
                    f'<div class="bp-detail" style="color:var(--text-muted)">{h(rsa["age"])} ago</div>'
                )
            else:
                restore_cell = (
                    h(rsa_state)
                    + f'<div class="bp-detail" style="color:var(--text-muted)">{h(rsa["age"])} ago</div>'
                )
        else:
            restore_cell = "—"

        rows += table_row(
            h(ns_name),
            prot_badge,
            h(policies_str),
            loc_profile_cell,
            pvc_cell,
            backup_cell,
            restore_cell,
        )

    # Counts for alert blocks
    failed_ns_count_local = sum(1 for ns in ns_protection if ns_protection[ns]["protected"] and ns in failed_ns_set)
    unprotected_count     = sum(1 for ns, info in ns_protection.items() if not info["protected"])

    failed_ns_alert = (
        f'<div class="alert alert-danger"><strong>&#x2716; {failed_ns_count_local} namespace(s) have a failed backup.</strong>'
        f' Check the policy and BackupAction logs for: '
        + ", ".join(
            f'<strong>{h(ns)}</strong>'
            for ns in sorted(ns_protection)
            if ns_protection[ns]["protected"] and ns in failed_ns_set
        ) + "</div>"
    ) if failed_ns_count_local > 0 else ""

    unprotected_alert = (
        f'<div class="alert alert-warn"><strong>&#x26A0; {unprotected_count} namespace(s) have no backup policy.</strong>'
        ' Review and assign policies as needed.</div>'
        if unprotected_count > 0 else
        ('' if failed_ns_count_local > 0 else
         '<div class="alert alert-success">All namespaces are covered by at least one backup policy.</div>')
    )

    sub_count_parts = []
    if protected_ns > 0:
        sub_count_parts.append(f'<span style="color:var(--green)">{protected_ns} protected</span>')
    if failed_ns_count_local > 0:
        sub_count_parts.append(f'<span style="color:var(--red)">{failed_ns_count_local} failed</span>')
    if unprotected_count > 0:
        sub_count_parts.append(f'<span style="color:var(--orange)">{unprotected_count} unprotected</span>')
    sub_count_html = " &nbsp;·&nbsp; ".join(sub_count_parts)

    return f"""
  <div id="kasten-namespaces">
  <h3>Namespaces <span class="count">{ns_count}</span> <span class="sub-count">{sub_count_html}</span></h3>
  {failed_ns_alert}
  {unprotected_alert}
  <div class="table-wrap">
    <table class="data-table">
      {th_row("Namespace","Protection","Backup Policy","Location Profile","PVCs (Storage)","Last Backup","Last Restore")}
      <tbody>{rows}</tbody>
    </table>
  </div>
  </div>
"""
 
def render_kasten():
    if not kasten["installed"] and not kasten["crds_installed"]:
        return """
<section id="kasten">
  <details class="section-toggle">
    <summary class="section-summary"><h2>&#x1F6E1; Veeam Kasten</h2></summary>
    <div class="section-body">
    <div class="alert alert-info">
      Veeam Kasten is not installed on this cluster (kasten-io namespace absent or CRDs missing).
    </div>
    </div>
  </details>
</section>
"""
    status_str   = f"{kasten['running_pods']}/{kasten['total_pods']} pods running"
    install_badge = (
        status_badge("Running")
        if kasten["running_pods"] == kasten["total_pods"] and kasten["total_pods"] > 0
        else status_badge("Warning")
    )
 
    # Pods table
    pod_rows = ""
    for p in kasten["pods"]:
        pod_rows += table_row(
            h(p["name"]),
            status_badge(p["status"]),
            h(p["ready"]),
            str(p["restarts"]),
            h(p["age"]),
        )
 
    # ── Policy exclusions ────────────────────────────────────────────────────
    EXCLUDED_POLICIES = {"k10-disaster-recovery-policy", "k10-system-reports-policy"}

    def _pol_rows(pol_list):
        rows = ""
        for pol in pol_list:
            actions_str = " + ".join(pol["actions"])
            ns_str      = ", ".join(pol["namespaces"]) if pol["namespaces"] else "<em>via labels</em>"
            run_state   = pol.get("last_run_state", "—")
            run_badge   = status_badge(run_state) if run_state not in ("—", "Unknown") else h(run_state)
            ba_fail = failed_backup_actions["by_policy"].get(pol["name"])
            if ba_fail:
                run_badge = (
                    '<span class="badge badge-red">&#x2716; Failed</span>'
                    f'<div class="bp-detail" style="color:var(--red)">'
                    f'&#x26A0; {h(ba_fail["display_msg"])}</div>'
                    f'<div class="bp-detail" style="color:var(--text-muted)">'
                    f'{h(ba_fail["age"])} ago</div>'
                )
            rows += table_row(
                h(pol["name"]),
                h(actions_str),
                h(ns_str),
                h(pol["frequency"] or "—"),
                h(pol["retention"] or "—"),
                run_badge,
            )
        return rows

    def _pol_table(pol_list, empty_msg):
        if not pol_list:
            return f'<div class="alert alert-info">{empty_msg}</div>'
        return (
            '<div class="table-wrap"><table class="data-table">'
            + th_row("Name","Actions","Targeted Namespaces","Frequency","Retention","Status")
            + f'<tbody>{_pol_rows(pol_list)}</tbody></table></div>'
        )

    def _import_pol_rows(pol_list):
        rows = ""
        for pol in pol_list:
            actions_str = " + ".join(pol["actions"])
            rows += table_row(
                h(pol["name"]),
                h(actions_str),
                h(pol["frequency"] or "—"),
            )
        return rows

    def _import_pol_table(pol_list, empty_msg):
        if not pol_list:
            return f'<div class="alert alert-info">{empty_msg}</div>'
        return (
            '<div class="table-wrap"><table class="data-table">'
            + th_row("Name","Actions","Frequency")
            + f'<tbody>{_import_pol_rows(pol_list)}</tbody></table></div>'
        )

    def _restore_actions_section(ra_list):
        if not ra_list:
            return '<div class="alert alert-info">No RestoreAction found in the cluster.</div>'
        rows = ""
        for ra in ra_list[:20]:
            state = ra["state"]
            if state in ("Failed", "Error"):
                state_cell = (
                    '<span class="badge badge-red">&#x2716; ' + h(state) + '</span>'
                    + (f'<div class="bp-detail" style="color:var(--red)">&#x26A0; {h(ra["error"])}</div>'
                       if ra["error"] else "")
                )
            elif state in ("Complete", "Succeeded", "Success"):
                state_cell = '<span class="badge badge-green">&#x2713; ' + h(state) + '</span>'
            elif state in ("Running", "InProgress"):
                state_cell = '<span class="badge badge-blue">&#x23F3; ' + h(state) + '</span>'
            else:
                state_cell = h(state)
            rows += table_row(h(ra["name"]), h(ra["namespace"]), state_cell, h(ra["age"]))
        return (
            '<div class="table-wrap"><table class="data-table">'
            + th_row("Name","Target Namespace","Status","Age")
            + f'<tbody>{rows}</tbody></table></div>'
        )

    backup_pols = [p for p in kasten["policies"]
                   if p["name"] not in EXCLUDED_POLICIES
                   and any(a in ("backup", "export") for a in p["actions"])]
    import_pols = [p for p in kasten["policies"]
                   if p["name"] not in EXCLUDED_POLICIES
                   and any(a in ("import", "restore") for a in p["actions"])]

    pol_section = ""  # kept for compat – not used below
 
    # Profiles table — Immutable column shows "Yes" (not "WORM")
    prof_rows = ""
    for p in kasten["profiles"]:
        immut_badge = '<span class="badge badge-green">Yes ✓</span>' if p["immutable"] \
                     else '<span class="badge badge-gray">No</span>'
        prof_rows += table_row(
            h(p["name"]),
            h(p["store_type"]),
            h(p["secret_type"]),
            h(p["bucket"] or "—"),
            h(p["region"] or "—"),
            immut_badge,
            status_badge(p["status"]),
            h(p["age"]),
        )
 
    prof_section = (
        '<p class="empty">No location profile configured.</p>' if not kasten["profiles"] else
        '<div class="table-wrap"><table class="data-table">'
        + th_row("Name","Type","Secret","Bucket","Region","Immutable","Status","Age")
        + f'<tbody>{prof_rows}</tbody></table></div>'
    )
 
    # DR Info — with export profile immutability warning
    dr = kasten["dr_info"]
    immutable_profiles = {p["name"] for p in kasten["profiles"] if p["immutable"]}
 
    # Identify the DR export profile: look for k10-disaster-recovery-policy export action
    dr_export_profile = dr.get("profile", "")
    if not dr_export_profile:
        for pol in kasten["policies"]:
            if pol["name"] == "k10-disaster-recovery-policy":
                dr_export_profile = pol.get("export_profile", "")
                break
 
    dr_content = ""
    if dr["enabled"]:
        # Export profile badge and immutability warning
        if dr_export_profile:
            is_immutable = dr_export_profile in immutable_profiles
            prof_badge_cls = "badge-green" if is_immutable else "badge-yellow"
            prof_label     = h(dr_export_profile) + (" ✓ Immutable" if is_immutable else " ⚠ Not immutable")
            export_info = (
                f'<div class="alert alert-info" style="margin-top:8px">'
                f'<strong>&#x1F4E6; Export profile:</strong> '
                f'<span class="badge {prof_badge_cls}">{prof_label}</span>'
                f'</div>'
            )
            if not is_immutable:
                export_warning = (
                    '<div class="alert alert-warn" style="margin-top:8px">'
                    '&#x26A0; <strong>Warning:</strong> The DR export target '
                    f'<strong>{h(dr_export_profile)}</strong> is <strong>not immutable</strong>. '
                    'Recovery points may be at risk of deletion or tampering. '
                    'Consider using a WORM-enabled location profile for Disaster Recovery.'
                    '</div>'
                )
            else:
                export_warning = ""
        else:
            # No export action found on the DR policy
            export_info = ""
            export_warning = (
                '<div class="alert alert-warn" style="margin-top:8px">'
                '&#x26A0; <strong>Warning:</strong> No export location profile could be '
                'determined for the DR policy. Verify that the '
                '<strong>k10-disaster-recovery-policy</strong> includes an export action '
                'targeting an immutable location profile.'
                '</div>'
            )
 
        dr_content = f"""
    <div class="alert alert-{'success' if dr['type'] == 'QuickDR' else 'info'}">
      <strong>DR enabled</strong> — Type: {h(dr['type'])}
      {('<br>Policy: ' + h(dr['policy'])) if dr['policy'] else ''}
      {('<br>Last restore point: ' + h(kasten['last_restore_point'])) if kasten['last_restore_point'] != '—' else ''}
    </div>
    {export_info}
    {export_warning}"""
    else:
        dr_content = (
            '<div class="alert alert-warn">Disaster Recovery is not configured on this cluster.</div>'
            '<div class="alert alert-warn" style="margin-top:8px">'
            '&#x26A0; <strong>Warning:</strong> No DR export is configured. '
            'Veeam Kasten catalog data is not being exported to any location profile. '
            'Configure Disaster Recovery to protect against cluster-level failures.'
            '</div>'
        )
 
    helm_content = f'<pre class="code-block">{h(kasten["helm_values"])}</pre>'
 
    # ── Blueprints / BlueprintBindings / TransformSets tables ───────────────
    bp_rows = ""
    for bp in kasten["blueprints"]:
        bp_rows += table_row(h(bp["name"]), h(bp["namespace"]),
                             h(", ".join(bp["actions"]) or "—"), h(bp["age"]))
    bb_rows = ""
    for bb in kasten["blueprint_bindings"]:
        bb_rows += table_row(h(bb["name"]), h(bb["namespace"]),
                             h(bb["blueprint"]), h(bb["subject"]), h(bb["age"]))
    ts_rows = ""
    for ts in kasten["transform_sets"]:
        ts_rows += table_row(h(ts["name"]), h(ts["namespace"]),
                             str(ts["transforms"]), h(ts["age"]))

    bp_section = (
        '<div class="alert alert-info">No Blueprint found in the cluster.</div>'
        if not kasten["blueprints"] else
        '<div class="table-wrap"><table class="data-table">'
        + th_row("Name","Namespace","Actions","Age")
        + f'<tbody>{bp_rows}</tbody></table></div>'
    )
    bb_section = (
        '<div class="alert alert-info">No BlueprintBinding found in the cluster.</div>'
        if not kasten["blueprint_bindings"] else
        '<div class="table-wrap"><table class="data-table">'
        + th_row("Name","Namespace","Blueprint","Subject","Age")
        + f'<tbody>{bb_rows}</tbody></table></div>'
    )
    ts_section = (
        '<div class="alert alert-info">No TransformSet found in the cluster.</div>'
        if not kasten["transform_sets"] else
        '<div class="table-wrap"><table class="data-table">'
        + th_row("Name","Namespace","Transforms","Age")
        + f'<tbody>{ts_rows}</tbody></table></div>'
    )

    # ── Reports section ──────────────────────────────────────────────────────
    rp = kasten.get("report_policy")
    ra_list = kasten.get("report_actions", [])
    if rp:
        rp_freq = h(rp.get("frequency") or "—")
        rp_age  = h(rp.get("age", "—"))
        report_content = f'''
    <div class="alert alert-success">
      <strong>&#x1F4CA; k10-system-reports-policy</strong> is configured
      &nbsp;·&nbsp; Frequency: {rp_freq} &nbsp;·&nbsp; Age: {rp_age}
    </div>'''
    else:
        report_content = '<div class="alert alert-info">&#x1F4CA; <strong>k10-system-reports-policy</strong> is not configured on this cluster.</div>'

    if ra_list:
        ra_rows = "".join(
            table_row(h(r["name"]), status_badge(r["state"]), h(r["age"]))
            for r in ra_list[:10]
        )
        report_content += (
            '<h4 style="font-size:13px;font-weight:600;margin:12px 0 6px">Recent Report Actions</h4>'
            '<div class="table-wrap"><table class="data-table">'
            + th_row("Name","State","Age")
            + f'<tbody>{ra_rows}</tbody></table></div>'
        )

    return f"""
<section id="kasten">
  <details class="section-toggle">
    <summary class="section-summary"><h2>&#x1F6E1; Veeam Kasten</h2></summary>
    <div class="section-body">

    <div class="kasten-header">
      <div class="kasten-stat">
        <span class="kstat-label">Version</span>
        <span class="kstat-value">{h(kasten['version'])}</span>
      </div>
      <div class="kasten-stat">
        <span class="kstat-label">Pods</span>
        <span class="kstat-value">{install_badge} {status_str}</span>
      </div>
      <div class="kasten-stat">
        <span class="kstat-label">Policies</span>
        <span class="kstat-value">{len(kasten['policies'])}</span>
      </div>
      <div class="kasten-stat">
        <span class="kstat-label">Failed Policies</span>
        <span class="kstat-value">{'<span class="badge badge-red">' + str(len(failed_policies)) + '</span>' if failed_policies else '<span class="badge badge-green">0</span>'}</span>
      </div>
      <div class="kasten-stat">
        <span class="kstat-label">Profiles</span>
        <span class="kstat-value">{len(kasten['profiles'])}</span>
      </div>
    </div>

    <h3>Pods</h3>
    <div class="table-wrap">
      <table class="data-table">
        {th_row("Name","Status","Ready","Restarts","Age")}
        <tbody>{pod_rows}</tbody>
      </table>
    </div>

    <h3 id="kasten-backup-policies">Backup &amp; Export Policies <span class="count">{len(backup_pols)}</span></h3>
    {_pol_table(backup_pols, "No backup or export policy configured.")}

    <h3 id="kasten-import-policies">Import &amp; Restore Policies <span class="count">{len(import_pols)}</span></h3>
    {_import_pol_table(import_pols, "No import or restore policy configured.")}

    <h3 id="kasten-restore-actions">Restore Actions <span class="count">{len(kasten.get("restore_actions", []))}</span></h3>
    {_restore_actions_section(kasten.get("restore_actions", []))}

    <h3 id="kasten-profiles">Profiles <span class="count">{len(kasten['profiles'])}</span></h3>
    {prof_section}

    {render_namespaces()}

    <h3 id="kasten-dr">Disaster Recovery</h3>
    {dr_content}

    <h3 id="kasten-reports">Reports</h3>
    {report_content}

    <h3 id="kasten-blueprints">Kanister Resources</h3>
    <h4 style="font-size:13px;font-weight:600;color:var(--text);margin:16px 0 6px">Blueprints</h4>
    {bp_section}
    <h4 style="font-size:13px;font-weight:600;color:var(--text);margin:16px 0 6px">BlueprintBindings</h4>
    {bb_section}
    <h4 style="font-size:13px;font-weight:600;color:var(--text);margin:16px 0 6px">TransformSets</h4>
    {ts_section}

    {render_best_practices()}

    <h3>Helm Values (Veeam Kasten)</h3>
    <details>
      <summary>Show Helm values</summary>
      {helm_content}
    </details>
    </div>
  </details>
</section>
"""
 
def render_best_practices():
    """
    Checks cluster configuration against Veeam Kasten best practices.
    Ref: https://docs.kasten.io/latest/references/best-practices/
    """
    all_policies    = kasten["policies"]
    immutable_profs = {p["name"] for p in kasten["profiles"] if p["immutable"]}
    checks = []
 
    # ── BP-01 : namespaces utilisateur sans policy ────────────────────────────
    SYSTEM_NS = {"kube-system", "kube-public", "kube-node-lease"}
    unprotected_user = sorted(
        ns for ns, info in ns_protection.items()
        if not info["protected"] and ns not in SYSTEM_NS
    )
    if unprotected_user:
        checks.append({
            "sev": "warn", "cat": "Coverage",
            "msg": f"{len(unprotected_user)} user namespace(s) without backup policy",
            "detail": "Namespaces: " + ", ".join(unprotected_user),
        })
    else:
        checks.append({"sev": "ok", "cat": "Coverage",
                       "msg": "All user namespaces are covered by at least one backup policy"})
 
    # ── BP-02 : policies sans action export ───────────────────────────────────
    no_export = [p["name"] for p in all_policies
                 if "export" not in p["actions"]
                 and p["name"] != "k10-disaster-recovery-policy"]
    if no_export:
        checks.append({
            "sev": "warn", "cat": "Export",
            "msg": f"{len(no_export)} policy(ies) without export action (local snapshot only)",
            "detail": (
                "Local snapshots protect against app-level failures only. "
                "Add an export action to a location profile for cluster-level DR. "
                "Policies: " + ", ".join(no_export)
            ),
        })
    else:
        checks.append({"sev": "ok", "cat": "Export",
                       "msg": "All application policies include an export action"})
 
    # ── BP-03 : exports vers profil non-immutable ─────────────────────────────
    non_immut_exports = [
        f"{p['name']} → {p['export_profile']}"
        for p in all_policies
        if "export" in p["actions"]
        and p.get("export_profile")
        and p["export_profile"] not in immutable_profs
    ]
    if non_immut_exports:
        checks.append({
            "sev": "warn", "cat": "Immutability",
            "msg": f"{len(non_immut_exports)} export(s) targeting a non-immutable profile",
            "detail": (
                "Non-immutable exports can be deleted by ransomware or accidental operations. "
                "Switch to a WORM-enabled profile. "
                + "; ".join(non_immut_exports)
            ),
        })
    elif any("export" in p["actions"] for p in all_policies):
        checks.append({"sev": "ok", "cat": "Immutability",
                       "msg": "All exports target an immutable (WORM) profile"})
 
    # ── BP-04 : aucun profil immutable configuré ──────────────────────────────
    if kasten["profiles"] and not immutable_profs:
        checks.append({
            "sev": "warn", "cat": "Immutability",
            "msg": "No immutable location profile configured",
            "detail": (
                "Configure at least one object-lock (WORM) profile to protect backups "
                "from ransomware and accidental deletion."
            ),
        })
 
    # ── BP-05 : Disaster Recovery ─────────────────────────────────────────────
    dr = kasten["dr_info"]
    if not dr["enabled"]:
        checks.append({
            "sev": "warn", "cat": "Disaster Recovery",
            "msg": "Disaster Recovery is not configured",
            "detail": (
                "Veeam Kasten catalog data is unprotected. Enable DR (QuickDR) to export Veeam Kasten state "
                "to a location profile and allow recovery from cluster-level failures."
            ),
        })
    else:
        dr_prof = dr.get("profile", "")
        if not dr_prof:
            for pol in all_policies:
                if pol["name"] == "k10-disaster-recovery-policy":
                    dr_prof = pol.get("export_profile", "")
                    break
        if dr_prof and dr_prof not in immutable_profs:
            checks.append({
                "sev": "warn", "cat": "Disaster Recovery",
                "msg": f"DR export profile '{dr_prof}' is not immutable",
                "detail": "Use a WORM-enabled location profile for DR to protect against ransomware.",
            })
        elif dr_prof:
            checks.append({"sev": "ok", "cat": "Disaster Recovery",
                           "msg": f"DR enabled — exporting to immutable profile '{dr_prof}'"})
        else:
            checks.append({
                "sev": "warn", "cat": "Disaster Recovery",
                "msg": "DR is enabled but export profile cannot be determined",
                "detail": "Verify the DR policy includes an export action targeting an immutable profile.",
            })
 
    # ── BP-06 : sélecteurs label-only (wildcard potentiel) ────────────────────
    label_only = [
        p["name"] for p in all_policies
        if not p["namespaces"] and p["name"] != "k10-disaster-recovery-policy"
    ]
    if label_only:
        checks.append({
            "sev": "info", "cat": "Policy Design",
            "msg": f"{len(label_only)} policy(ies) use label-based namespace selectors",
            "detail": (
                "Verify these are not wildcard selectors (matching all namespaces). "
                "Prefer explicit per-application policies. "
                "Policies: " + ", ".join(label_only)
            ),
        })
 
    # ── BP-07 : policies planifiées sans rétention ────────────────────────────
    no_ret = [
        p["name"] for p in all_policies
        if not p["retention"]
        and p.get("frequency") and p["frequency"] not in ("@onDemand", "")
        and p["name"] != "k10-disaster-recovery-policy"
    ]
    if no_ret:
        checks.append({
            "sev": "warn", "cat": "Retention",
            "msg": f"{len(no_ret)} scheduled policy(ies) without explicit retention",
            "detail": (
                "Without retention, restore points accumulate indefinitely. "
                "Policies: " + ", ".join(no_ret)
            ),
        })
    else:
        checks.append({"sev": "ok", "cat": "Retention",
                       "msg": "All scheduled policies have a retention period configured"})
 
    # ── BP-08 : PolicyPresets ─────────────────────────────────────────────────
    preset_count = len(items(load("kasten_policypresets.json", {"items": []})))
    if preset_count == 0 and len(all_policies) > 1:
        checks.append({
            "sev": "info", "cat": "Standardization",
            "msg": "No PolicyPreset configured",
            "detail": (
                "PolicyPresets standardize backup/export retention across policies. "
                "Recommended when managing multiple applications."
            ),
        })
    elif preset_count > 0:
        checks.append({"sev": "ok", "cat": "Standardization",
                       "msg": f"{preset_count} PolicyPreset(s) in use"})
 
    # ── BP-09 : profils NFS/SMB (préférer object storage) ─────────────────────
    nfs_profs = [
        p["name"] for p in kasten["profiles"]
        if p["store_type"].lower() in ("nfs", "smb", "filestore")
    ]
    if nfs_profs:
        checks.append({
            "sev": "info", "cat": "Storage",
            "msg": f"{len(nfs_profs)} NFS/SMB profile(s) detected",
            "detail": (
                "Object storage (S3, Azure Blob, GCS) is recommended over NFS/SMB: "
                "supports immutability, versioning, and better scalability. "
                "Profiles: " + ", ".join(nfs_profs)
            ),
        })
 
    # ── BP-10 : authentification basique ─────────────────────────────────────
    helm_vals = kasten.get("helm_values", "")
    if '"basicAuth"' in helm_vals and '"enabled": true' in helm_vals:
        checks.append({
            "sev": "info", "cat": "Authentication",
            "msg": "Basic authentication (htpasswd) is active",
            "detail": (
                "Suitable for lab/POC only. For production, use OIDC, Active Directory/LDAPS "
                "or OpenShift OAuth to enable multi-tenancy and RBAC."
            ),
        })
 
    # ── BP-11 : ressources cluster-scoped ─────────────────────────────────────
    has_cluster_pol = False
    for pol_raw in items(load("kasten_policies.json", {"items": []})):
        spec_r = pol_raw.get("spec", {})
        sel_r  = spec_r.get("selector", {}) or {}
        if sel_r.get("matchLabels", {}).get("k10.kasten.io/appType") == "cluster":
            has_cluster_pol = True
            break
        for a in (spec_r.get("actions") or []):
            if isinstance(a, dict):
                bp_r = a.get("backupParameters") or {}
                if bp_r.get("includeClusterResources") is True:
                    has_cluster_pol = True
                    break
        if has_cluster_pol:
            break
    if not has_cluster_pol and all_policies:
        checks.append({
            "sev": "info", "cat": "Coverage",
            "msg": "No policy protecting cluster-scoped resources detected",
            "detail": (
                "Enable 'Include Cluster-Scoped Resources' on a dedicated policy to back up "
                "CRDs, ClusterRoles, StorageClasses, and other cluster-level objects."
            ),
        })
 
    # ── Rendu ─────────────────────────────────────────────────────────────────
    sev_order = {"warn": 0, "info": 1, "ok": 2}
    checks.sort(key=lambda c: sev_order.get(c["sev"], 3))
 
    warn_n = sum(1 for c in checks if c["sev"] == "warn")
    info_n = sum(1 for c in checks if c["sev"] == "info")
    ok_n   = sum(1 for c in checks if c["sev"] == "ok")
 
    summary_html = ""
    if warn_n:
        summary_html += f'<span class="badge badge-red">{warn_n} Warning(s)</span> '
    if info_n:
        summary_html += f'<span class="badge badge-yellow">{info_n} Info</span> '
    if ok_n:
        summary_html += f'<span class="badge badge-green">{ok_n} OK</span>'
 
    rows = ""
    for c in checks:
        sev = c["sev"]
        if sev == "warn":
            status_cell = '<span class="badge badge-red">&#x26A0; Warning</span>'
        elif sev == "info":
            status_cell = '<span class="badge badge-yellow">&#x2139; Info</span>'
        else:
            status_cell = '<span class="badge badge-green">&#x2713; OK</span>'
        detail = c.get("detail", "")
        detail_html = f'<div class="bp-detail">{h(detail)}</div>' if detail else ""
        rows += table_row(status_cell, h(c["cat"]), h(c["msg"]) + detail_html)
 
    return f"""
  <h3>&#x1F4CB; Best Practices {summary_html}</h3>
  <div class="alert alert-info">
    Checks based on <a href="https://docs.kasten.io/latest/references/best-practices/" target="_blank">Veeam Kasten Best Practices</a>.
  </div>
  <div class="table-wrap">
    <table class="data-table">
      {th_row("Status", "Category", "Finding")}
      <tbody>{rows}</tbody>
    </table>
  </div>
"""
 
 
def render_network():
    comp_rows = ""
    for c in network["components"]:
        comp_rows += table_row(
            h(c["name"]),
            h(c["ns"]),
            status_badge(c["status"]),
            h(c["version"] or "—"),
        )
 
    comp_section = ""
    if network["components"]:
        comp_section = f"""
  <h3>CNI Components</h3>
  <div class="table-wrap">
    <table class="data-table">
      {th_row("Pod","Namespace","Status","Image/Version")}
      <tbody>{comp_rows}</tbody>
    </table>
  </div>"""
 
    np_by_ns = groups(network["net_policies"], "namespace")
    np_sections = ""
    for ns in sorted(np_by_ns):
        rows = ""
        for np in np_by_ns[ns]:
            ingress_badge = '<span class="badge badge-blue">Ingress</span>' if np["ingress"] else ""
            egress_badge  = '<span class="badge badge-yellow">Egress</span>' if np["egress"] else ""
            rows += table_row(
                h(np["name"]),
                ingress_badge + " " + egress_badge,
                h(np["age"]),
            )
        np_sections += f"""
  <details>
    <summary class="ns-header">
      <span class="ns-name">{h(ns)}</span>
      <span class="ns-count">{len(np_by_ns[ns])} policies</span>
    </summary>
    <div class="table-wrap">
      <table class="data-table">
        {th_row("Name","Types","Age")}
        <tbody>{rows}</tbody>
      </table>
    </div>
  </details>"""
 
    ipv6_badge = '<span class="badge badge-green">enabled</span>' if network["ipv6"] \
                else '<span class="badge badge-gray">not detected</span>'
 
    return f"""
<section id="network">
  <details class="section-toggle">
    <summary class="section-summary"><h2>Network</h2></h2></summary>
    <div class="section-body">
 
    <div class="kasten-header">
      <div class="kasten-stat">
        <span class="kstat-label">CNI</span>
        <span class="kstat-value"><span class="badge badge-blue">{h(network['cni_type'])}</span></span>
      </div>
      <div class="kasten-stat">
        <span class="kstat-label">IPv6</span>
        <span class="kstat-value">{ipv6_badge}</span>
      </div>
      <div class="kasten-stat">
        <span class="kstat-label">NetworkPolicies</span>
        <span class="kstat-value">{network['np_count']}</span>
      </div>
    </div>
 
    {comp_section}
 
    <h3>NetworkPolicies <span class="count">{network['np_count']}</span></h3>
    {'<p class="empty">No NetworkPolicy detected.</p>' if not network['net_policies'] else np_sections}
    </div>
  </details>
</section>
"""
 
def render_events():
    warn_rows = ""
    info_rows = ""
    for ev in events:
        row = table_row(
            h(ev["namespace"]),
            status_badge(ev["type"]),
            h(ev["reason"]),
            f'{h(ev["object_kind"])}/{h(ev["object_name"])}',
            h(ev["message"][:120] + ("…" if len(ev["message"]) > 120 else "")),
            str(ev["count"]),
            h(ev["age"]),
            tr_class="row-warn" if ev["type"] == "Warning" else "",
        )
        if ev["type"] == "Warning":
            warn_rows += row
        else:
            info_rows += row
 
    _th_ev = th_row("Namespace","Type","Reason","Object","Message","Count","Age")
    warn_section = (
        "<p class='empty'>No warnings.</p>" if not warn_rows else
        '<div class="table-wrap"><table class="data-table">'
        + _th_ev
        + f'<tbody>{warn_rows}</tbody></table></div>'
    )
    info_section = (
        "<p class='empty'>No informational events.</p>" if not info_rows else
        '<div class="table-wrap"><table class="data-table">'
        + _th_ev
        + f'<tbody>{info_rows}</tbody></table></div>'
    )
    return f"""
<section id="events">
  <details class="section-toggle">
    <summary class="section-summary"><h2>Events <span class="count">{len(events)}</span>
    <span class="sub-count text-warn">{warn_events} warnings</span>
  </h2></h2></summary>
    <div class="section-body">
 
    <details>
      <summary class="ns-header warn-summary">
        <span class="ns-name">&#x26A0; Warnings</span>
        <span class="ns-count">{warn_events}</span>
      </summary>
      {warn_section}
    </details>
 
    <details>
      <summary class="ns-header">
        <span class="ns-name">&#x2139; Informational</span>
        <span class="ns-count">{len(events) - warn_events}</span>
      </summary>
      {info_section}
    </details>
    </div>
  </details>
</section>
"""
 
# =============================================================================
# FULL HTML PAGE
# =============================================================================
 
CSS = """
*, *::before, *::after { box-sizing: border-box; }
:root {
  --primary:    #005FAB;
  --primary-dk: #003F73;
  --green:      #009A44;
  --red:        #D32F2F;
  --orange:     #F57C00;
  --blue:       #1976D2;
  --gray:       #607D8B;
  --bg:         #F4F6F9;
  --card-bg:    #FFFFFF;
  --border:     #DDE3EC;
  --text:       #1A2332;
  --text-muted: #546E7A;
  --sidebar-w:  220px;
  --header-h:   60px;
}
body { margin: 0; font: 14px/1.5 -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
       color: var(--text); background: var(--bg); }
a { color: var(--primary); text-decoration: none; }
a:hover { text-decoration: underline; }
 
/* Header */
#site-header {
  position: fixed; top: 0; left: 0; right: 0; height: var(--header-h);
  background: var(--primary); color: #fff; display: flex; align-items: center;
  padding: 0 20px; gap: 14px; z-index: 100; box-shadow: 0 2px 6px rgba(0,0,0,.25);
}
.logo-mark { font-size: 26px; font-weight: 900; letter-spacing: -1px;
             background: var(--green); padding: 2px 8px; border-radius: 4px; }
.site-title { font-size: 17px; font-weight: 600; flex: 1; }
.header-meta { font-size: 12px; opacity: .8; }
 
/* Sidebar */
#sidebar {
  position: fixed; top: var(--header-h); left: 0; bottom: 0; width: var(--sidebar-w);
  background: var(--card-bg); border-right: 1px solid var(--border);
  overflow-y: auto; padding: 12px 0; z-index: 90;
}
#sidebar ul { list-style: none; margin: 0; padding: 0; }
#sidebar li a {
  display: flex; align-items: center; gap: 8px; padding: 8px 18px;
  font-size: 13px; color: var(--text); border-left: 3px solid transparent;
  transition: background .15s, border-color .15s;
}
#sidebar li a:hover  { background: #EEF3FA; text-decoration: none; }
#sidebar li a.active { background: #EEF3FA; border-left-color: var(--primary);
                       color: var(--primary); font-weight: 600; }
.nav-icon { font-size: 16px; width: 20px; text-align: center; }
 
/* Content */
#main-content {
  margin-left: var(--sidebar-w); margin-top: var(--header-h);
  padding: 24px 28px; max-width: 1600px;
}
section { margin-bottom: 40px; }
h2 { font-size: 20px; font-weight: 700; color: var(--primary-dk);
     border-bottom: 2px solid var(--primary); padding-bottom: 8px; margin: 0 0 16px; }
h3 { font-size: 15px; font-weight: 600; color: var(--text); margin: 24px 0 10px; }
 
/* Count badges */
.count     { background: var(--primary); color: #fff; font-size: 12px; font-weight: 700;
             padding: 2px 8px; border-radius: 10px; margin-left: 8px; vertical-align: middle; }
.sub-count { background: transparent; color: var(--text-muted); font-size: 13px;
             font-weight: 400; margin-left: 8px; }
.text-warn { color: var(--red); }
 
/* Cards grid */
.cards-grid { display: grid;
              grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: 14px; }
.card { background: var(--card-bg); border: 1px solid var(--border); border-radius: 8px;
        padding: 16px; display: flex; flex-direction: column; gap: 4px;
        box-shadow: 0 1px 3px rgba(0,0,0,.06); }
.card-warn { border-color: var(--red); }
.card-icon  { font-size: 24px; }
.card-title { font-size: 11px; text-transform: uppercase; letter-spacing: .5px; color: var(--text-muted); }
.card-value { font-size: 22px; font-weight: 700; color: var(--text); }
.card-sub   { font-size: 12px; color: var(--text-muted); }

/* Clickable overview cards */
a.card-link { text-decoration: none; color: inherit; display: block; }
.card.clickable { cursor: pointer; transition: transform .15s, box-shadow .15s; }
.card.clickable:hover { transform: translateY(-3px);
  box-shadow: 0 6px 14px rgba(0,0,0,.10); border-color: var(--primary); }

/* Sidebar sub-links */
.nav-sub a { padding-left: 34px !important; font-size: 12px !important; opacity: .85; }
 
/* Tables */
.table-wrap { overflow-x: auto; border-radius: 6px; border: 1px solid var(--border);
              margin-bottom: 12px; }
.data-table  { width: 100%; border-collapse: collapse; font-size: 13px; }
.data-table thead th { background: #EEF3FA; color: var(--primary-dk); font-weight: 600;
                       padding: 9px 12px; text-align: left; white-space: nowrap;
                       border-bottom: 2px solid var(--border); }
.data-table tbody tr:nth-child(even) { background: #FAFBFD; }
.data-table tbody tr:hover { background: #EEF3FA; }
.data-table td { padding: 7px 12px; border-bottom: 1px solid var(--border);
                 vertical-align: top; word-break: break-word; max-width: 340px; }
.row-warn td { border-left: 3px solid var(--red); }
 
/* Badges */
.badge      { display: inline-block; font-size: 11px; font-weight: 600; border-radius: 4px;
              padding: 2px 7px; white-space: nowrap; }
.badge-green  { background: #E8F5E9; color: #2E7D32; }
.badge-red    { background: #FFEBEE; color: #C62828; }
.badge-orange { background: #FFF3E0; color: #E65100; }
.badge-yellow { background: #FFFDE7; color: #F57F17; }
.badge-blue   { background: #E3F2FD; color: #1565C0; }
.badge-gray   { background: #ECEFF1; color: #455A64; }
 
/* Namespace headers (details/summary) */
details > summary { cursor: pointer; user-select: none; list-style: none; }
details > summary::-webkit-details-marker { display: none; }
.ns-header {
  display: flex; align-items: center; gap: 10px; padding: 8px 14px;
  background: #EEF3FA; border: 1px solid var(--border); border-radius: 6px;
  margin-bottom: 6px; font-size: 13px;
}
.ns-header:hover { background: #E1EAF7; }
.ns-name  { font-weight: 600; flex: 1; color: var(--primary-dk); }
.ns-count { font-size: 11px; background: var(--primary); color: #fff;
            padding: 2px 8px; border-radius: 10px; }
.warn-summary { background: #FFF3E0; border-color: var(--orange); }
.warn-summary .ns-name { color: var(--orange); }
 
/* Kasten header stats */
.kasten-header { display: flex; gap: 20px; flex-wrap: wrap; margin-bottom: 20px;
                 background: var(--card-bg); border: 1px solid var(--border);
                 border-radius: 8px; padding: 16px 20px; }
.kasten-stat   { display: flex; flex-direction: column; gap: 4px; min-width: 100px; }
.kstat-label   { font-size: 11px; text-transform: uppercase; color: var(--text-muted);
                 letter-spacing: .5px; }
.kstat-value   { font-size: 15px; font-weight: 700; }
 
/* Alerts */
.alert { border-radius: 6px; padding: 12px 16px; margin-bottom: 12px; font-size: 13px; }
.alert-info    { background: #E3F2FD; border-left: 4px solid var(--blue); }
.alert-success { background: #E8F5E9; border-left: 4px solid var(--green); }
.alert-warn    { background: #FFF3E0; border-left: 4px solid var(--orange); }
.alert-danger  { background: #FFEBEE; border-left: 4px solid var(--red); }
 
/* Code block */
.code-block { background: #1E2730; color: #A8C7FA; border-radius: 6px; padding: 16px;
              font: 12px/1.6 "JetBrains Mono", "Fira Code", monospace; overflow-x: auto;
              max-height: 500px; overflow-y: auto; white-space: pre; }
 
/* Error message in policy table */
.error-msg { color: var(--red); font-family: monospace; font-size: 12px;
             word-break: break-word; white-space: pre-wrap; }
 
/* Misc */
.empty { color: var(--text-muted); font-style: italic; padding: 8px 0; }
.hint  { cursor: help; color: var(--primary); font-size: 12px; }
 
/* Print */
@media print {
  #site-header, #sidebar { display: none; }
  #main-content { margin: 0; padding: 12px; }
  .code-block { max-height: none; }
  details { open: true; }
  details > summary { display: none; }
  details > * { display: block !important; }
}
"""
 
JS = """
(function() {
  // Sidebar active link on scroll
  var sections = document.querySelectorAll('section[id]');
  var links    = document.querySelectorAll('#sidebar a');
 
  function setActive() {
    var scrollY = window.scrollY + 80;
    var current = '';
    sections.forEach(function(s) {
      if (s.offsetTop <= scrollY) current = s.id;
    });
    links.forEach(function(a) {
      a.classList.toggle('active', a.getAttribute('href') === '#' + current);
    });
  }
 
  window.addEventListener('scroll', setActive, { passive: true });
  setActive();
 
  // Search filter
  var searchInput = document.getElementById('table-search');
  if (searchInput) {
    searchInput.addEventListener('input', function() {
      var q = this.value.toLowerCase();
      document.querySelectorAll('.data-table tbody tr').forEach(function(row) {
        row.style.display = q === '' || row.textContent.toLowerCase().includes(q) ? '' : 'none';
      });
    });
  }
})();
"""
 
nav_items = [
    ("#overview",   "🏠", "Overview"),
    ("#nodes",      "🖥", f"Nodes ({len(nodes)})"),
    ("#pods",       "📦", f"Pods ({total_pods})"),
    ("#services",   "🔗", f"Services ({len(services)})"),
    ("#storage",    "💾", "Storage"),
    ("#crds",       "📋", f"CRDs ({len(crds)})"),
    ("#operators",  "⚙️",  f"Operators ({len(operators)})"),
    ("#kasten-namespaces", "🗂",  f"Namespaces ({ns_count})"),
    ("#kasten",           "🛡",  "Veeam Kasten"),
    ("#network",    "🌐", "Network"),
    ("#events",         "⚠️",  f"Events ({len(events)})"),
]

kasten_sub = [
    ("#kasten-backup-policies",  "📋", "↳ Backup Policies"),
    ("#kasten-import-policies",  "📥", "↳ Import Policies"),
    ("#kasten-restore-actions",  "♻️",  "↳ Restore Actions"),
    ("#kasten-profiles",         "🗄",  "↳ Profiles"),
    ("#kasten-dr",               "🔄", "↳ Disaster Recovery"),
    ("#kasten-reports",          "📊", "↳ Reports"),
    ("#kasten-blueprints",       "🧩", "↳ Blueprints"),
]

nav_html_parts = []
for href, icon, label in nav_items:
    nav_html_parts.append(
        f'<li><a href="{href}"><span class="nav-icon">{icon}</span>{h(label)}</a></li>'
    )
    if href == "#kasten":
        for shref, sicon, slabel in kasten_sub:
            nav_html_parts.append(
                f'<li class="nav-sub"><a href="{shref}"><span class="nav-icon">{sicon}</span>{h(slabel)}</a></li>'
            )
nav_html = "\n".join(nav_html_parts)
 
html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Kubernetes Inventory — {h(CONTEXT)}</title>
  <style>{CSS}</style>
</head>
<body>
 
<header id="site-header">
  <div class="site-title">Veeam Kasten — Cluster Inventory</div>
  <div class="header-meta">
    <strong>{h(CONTEXT)}</strong> &nbsp;|&nbsp;
    {h(dist_type)} {h(server_version)} &nbsp;|&nbsp;
    {h(TIMESTAMP)}
  </div>
</header>
 
<nav id="sidebar">
  <ul>{nav_html}</ul>
</nav>
 
<main id="main-content">
  <div style="margin-bottom:16px">
    <input id="table-search" type="search" placeholder="🔍 Filter all tables..."
           style="padding:7px 12px;border:1px solid var(--border);border-radius:6px;
                  font-size:13px;width:320px;outline:none;">
  </div>
 
  {render_overview()}
  {render_nodes()}
  {render_pods()}
  {render_services()}
  {render_storage()}
  {render_crds()}
  {render_operators()}
  {render_kasten()}
  {render_network()}
  {render_events()}
 
  <footer style="margin-top:40px;padding-top:16px;border-top:1px solid var(--border);
                 color:var(--text-muted);font-size:12px;text-align:center;">
    Generated by <strong>veeam-kasten-collector.sh</strong> v1.2.0 &nbsp;·&nbsp;
    {h(TIMESTAMP)} &nbsp;·&nbsp; Cluster: <strong>{h(CONTEXT)}</strong>
  </footer>
</main>
 
<script>{JS}</script>
</body>
</html>"""
 
with open(OUT, "w", encoding="utf-8") as f:
    f.write(html)
 
print(f"Report generated: {OUT}")
PYEOF
}
 
# =============================================================================
# MAIN
# =============================================================================
 
main() {
  parse_args "$@"
  check_prerequisites
 
  echo -e "\n${BOLD}${BLUE}╔══════════════════════════════════════════╗${NC}"
  echo -e "${BOLD}${BLUE}║  Veeam Kasten — Cluster Inventory v${SCRIPT_VERSION}  ║${NC}"
  echo -e "${BOLD}${BLUE}╚══════════════════════════════════════════╝${NC}\n"
 
  # Check cluster connectivity
  log_info "Checking cluster connectivity..."
  if ! kubectl --kubeconfig="$KUBECONFIG_PATH" ${CTX:+--context="$CTX"} \
       cluster-info &>/dev/null; then
    log_error "Cannot connect to the Kubernetes cluster."
    log_error "Check your kubeconfig and network connectivity."
    exit 1
  fi
  log_info "Cluster reachable ✓"
 
  # Create output directory
  mkdir -p "$OUTPUT_DIR"
 
  # Output filename
  TIMESTAMP_FILE=$(date +"%Y%m%d-%H%M%S")
  OUTPUT_FILE="${OUTPUT_DIR}/inventory-${TIMESTAMP_FILE}.html"
 
  # Collect raw data
  collect_raw_data
 
  # Write the Python HTML generator
  log_section "HTML report generation"
  write_html_generator
 
  # Export environment variables for Python
  export KASTEN_TMP_DIR="$TMP_DIR"
  export KASTEN_OUTPUT="$OUTPUT_FILE"
  export KASTEN_MASK_IPS="$MASK_IPS"
  export KASTEN_SKIP_HELM="$SKIP_HELM"
 
  # Run the generator
  if python3 "$TMP_DIR/generate_html.py"; then
    echo ""
    echo -e "${GREEN}${BOLD}✓ Report generated successfully!${NC}"
    echo -e "  ${BOLD}File:${NC} $(realpath "$OUTPUT_FILE" 2>/dev/null || echo "$OUTPUT_FILE")"
    echo ""
  else
    log_error "HTML report generation failed."
    exit 1
  fi
}
 
main "$@"