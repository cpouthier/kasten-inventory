#!/usr/bin/env bash
set -euo pipefail

# ------------------------------------------------------------------------------
# Kasten K10 License & Cluster Coverage Check
#
# What this script does:
#  1) Reads the K10 license from the Kubernetes secret (k10-license).
#  2) Extracts:
#     - license start date
#     - license end date
#     - licensed node limit (restrictions.nodes)
#  3) Checks if the license is still valid (end date > now).
#  4) Counts cluster nodes:
#     - total nodes
#     - worker nodes (excluding control-plane/master)
#     - worker nodes with at least one taint having effect=NoSchedule
#  5) Compares licensed node limit with total nodes (this is what matters for K10).
#
# Requirements:
#  - kubectl configured with access to the cluster
#  - jq installed
#  - GNU date (on Linux it's fine; on macOS use gdate from coreutils)
#
# Usage:
#  ./k10_license_check.sh
#
# Optional env vars:
#  K10_NAMESPACE  (default: kasten-io)
#  LICENSE_SECRET (default: k10-license)
# ------------------------------------------------------------------------------

K10_NAMESPACE="${K10_NAMESPACE:-kasten-io}"
LICENSE_SECRET="${LICENSE_SECRET:-k10-license}"

# ---- Helpers -----------------------------------------------------------------

die() {
  echo "ERROR: $*" >&2
  exit 1
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "Missing required command: $1"
}

# Convert an RFC3339/ISO8601 date to epoch seconds.
# Note: This uses GNU date. If you run this on macOS, install coreutils and use gdate.
to_epoch() {
  local iso="$1"
  date -d "$iso" +%s
}

# ---- Pre-flight checks --------------------------------------------------------

need_cmd kubectl
need_cmd jq
need_cmd base64
need_cmd date

# ---- Fetch and parse license --------------------------------------------------

# Pull the license YAML-like text from the secret, decode from base64.
LICENSE_TEXT="$(
  kubectl get secret "${LICENSE_SECRET}" -n "${K10_NAMESPACE}" -o jsonpath='{.data.license}' 2>/dev/null \
  | base64 -d
)" || die "Unable to read secret ${LICENSE_SECRET} in namespace ${K10_NAMESPACE}"

# Extract fields using simple pattern matching.
# License content is YAML-like, e.g.:
# dateend: "2026-12-19T00:00:00.000Z"
# restrictions:
#   nodes: "5"
LICENSE_START="$(echo "${LICENSE_TEXT}" | awk -F'"' '/^datestart:/ {print $2}')"
LICENSE_END="$(echo "${LICENSE_TEXT}"   | awk -F'"' '/^dateend:/   {print $2}')"
LICENSE_NODES="$(echo "${LICENSE_TEXT}" | awk -F'"' '/^[[:space:]]*nodes:/ {print $2}' | head -n1)"

# Basic sanity checks
[ -n "${LICENSE_START}" ] || die "Could not parse datestart from license"
[ -n "${LICENSE_END}" ]   || die "Could not parse dateend from license"
[ -n "${LICENSE_NODES}" ] || die "Could not parse restrictions.nodes from license"

# ---- License validity check ---------------------------------------------------

NOW_EPOCH="$(date +%s)"
END_EPOCH="$(to_epoch "${LICENSE_END}")" || die "Failed to parse license end date with GNU date: ${LICENSE_END}"

if [ "${END_EPOCH}" -gt "${NOW_EPOCH}" ]; then
  LICENSE_STATUS="VALID"
else
  LICENSE_STATUS="EXPIRED"
fi

# ---- Cluster node counts ------------------------------------------------------

# Total nodes (control-plane + workers)
TOTAL_NODES="$(kubectl get nodes -o json | jq '.items | length')"

# Worker nodes = nodes that are NOT labeled as control-plane or master
WORKER_NODES="$(kubectl get nodes -o json | jq '
  [.items[]
    | select(.metadata.labels["node-role.kubernetes.io/control-plane"] == null)
    | select(.metadata.labels["node-role.kubernetes.io/master"] == null)
  ] | length
')"

# Worker nodes with at least one taint having effect=NoSchedule
NO_SCHEDULE_WORKERS="$(kubectl get nodes -o json | jq '
  [.items[]
    | select(.metadata.labels["node-role.kubernetes.io/control-plane"] == null)
    | select(.metadata.labels["node-role.kubernetes.io/master"] == null)
    | select((.spec.taints // []) | any(.effect == "NoSchedule"))
  ] | length
')"

# ---- Coverage check -----------------------------------------------------------
# IMPORTANT:
# Kasten's license node limit applies to the number of nodes in the cluster.
# Taints (NoSchedule) and schedulability do NOT remove a node from licensing count.
#
# So, the real coverage check is:
#   licensed_nodes >= total_nodes
#
# The NoSchedule worker count is printed as additional context only.

# Ensure LICENSE_NODES is numeric
if ! [[ "${LICENSE_NODES}" =~ ^[0-9]+$ ]]; then
  die "Parsed license node limit is not numeric: ${LICENSE_NODES}"
fi

if [ "${TOTAL_NODES}" -le "${LICENSE_NODES}" ]; then
  COVERAGE_STATUS="OK"
else
  COVERAGE_STATUS="OVER_LIMIT"
fi

# ---- Output -------------------------------------------------------------------

echo "------------------------------------------------------------"
echo "Kasten K10 License Check"
echo "------------------------------------------------------------"
echo "Namespace / Secret : ${K10_NAMESPACE} / ${LICENSE_SECRET}"
echo "License Start      : ${LICENSE_START}"
echo "License End        : ${LICENSE_END}"
echo "License Status     : ${LICENSE_STATUS}"
echo "Licensed Nodes     : ${LICENSE_NODES}"
echo
echo "Cluster Nodes"
echo "  Total nodes              : ${TOTAL_NODES}"
echo "  Worker nodes             : ${WORKER_NODES}"
echo "  Worker nodes (NoSchedule): ${NO_SCHEDULE_WORKERS}"
echo
echo "Coverage (based on TOTAL nodes): ${COVERAGE_STATUS}"
echo "------------------------------------------------------------"

# ---- Exit codes ---------------------------------------------------------------
#  0: license valid AND coverage OK
#  2: license expired OR coverage over limit
if [ "${LICENSE_STATUS}" = "VALID" ] && [ "${COVERAGE_STATUS}" = "OK" ]; then
  exit 0
else
  exit 2
fi
