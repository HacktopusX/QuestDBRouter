#!/usr/bin/env bash
# Apply QuestDB-recommended kernel limits on the WSL/Linux host.
#
# Prefer `docker compose up` — questdb-0 raises vm.max_map_count automatically.
# Run this script once if you want the limit persisted across WSL reboots
# (Docker Desktop may still reset it until questdb-0 starts).

set -euo pipefail

SYSCTL_FILE="/etc/sysctl.d/99-questdb.conf"
WSL_CONF="/etc/wsl.conf"
MAP_COUNT=1048576

need_sudo() {
  if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then
    echo "Re-running with sudo..."
    exec sudo bash "$0" "$@"
  fi
}

need_sudo "$@"

cat >"$SYSCTL_FILE" <<EOF
# QuestDB recommended minimums for local/docker development
vm.max_map_count=${MAP_COUNT}
fs.file-max=${MAP_COUNT}
EOF

if [[ -f "$WSL_CONF" ]] && grep -q '^\[boot\]' "$WSL_CONF"; then
  if grep -q 'vm.max_map_count=' "$WSL_CONF"; then
    sed -i "s/vm\\.max_map_count=[0-9]*/vm.max_map_count=${MAP_COUNT}/g" "$WSL_CONF"
  elif grep -q '^command=' "$WSL_CONF"; then
    sed -i "s|^command=\\(.*\\)'$|command=\\1 vm.max_map_count=${MAP_COUNT}'|" "$WSL_CONF"
  else
    printf '\ncommand=bash -c '"'"'sysctl -w vm.max_map_count=%s >/dev/null 2>&1'"'"'\n' "$MAP_COUNT" >>"$WSL_CONF"
  fi
fi

sysctl --system >/dev/null

echo "Applied host limits:"
sysctl vm.max_map_count fs.file-max
echo "Restart WSL if limits should apply before Docker starts: wsl --shutdown (from Windows PowerShell)"
