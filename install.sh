#!/usr/bin/env bash
set -euo pipefail
umask 077

repo=YeJianbo/ravel-gost
version=latest
panel=
machine_id=
token_stdin=0
usage() {
  echo 'Usage: bash install.sh --panel https://panel.example.com --machine-id 1 [--version v0.1.0] [--token-stdin]'
}
while (($#)); do
  case "$1" in
    --panel) panel=${2:?missing panel URL}; shift 2 ;;
    --machine-id) machine_id=${2:?missing machine ID}; shift 2 ;;
    --version) version=${2:?missing version}; shift 2 ;;
    --token-stdin) token_stdin=1; shift ;;
    --help|-h) usage; exit 0 ;;
    *) usage >&2; exit 2 ;;
  esac
done
[[ $EUID == 0 && $(uname -s) == Linux ]] || { echo 'Linux root installation required' >&2; exit 1; }
[[ $panel =~ ^https://[a-zA-Z0-9.-]+(:[0-9]+)?/?$ && $machine_id =~ ^[1-9][0-9]*$ ]] || { usage >&2; exit 2; }
[[ $version == latest || $version =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo 'Invalid release version' >&2; exit 2; }
for command in curl jq unzip sha256sum install; do
  command -v "$command" >/dev/null || { echo "Missing dependency: $command" >&2; exit 1; }
done
if [[ -d /run/systemd/system ]]; then
  init=systemd
  commands=(systemctl useradd runuser)
  service_path=/etc/systemd/system/ravel-gost.service
elif command -v rc-service >/dev/null && command -v supervise-daemon >/dev/null; then
  init=openrc
  commands=(rc-service rc-update adduser addgroup su)
  service_path=/etc/init.d/ravel-gost
else
  echo 'systemd or OpenRC with supervise-daemon is required' >&2
  exit 1
fi
for command in "${commands[@]}"; do
  command -v "$command" >/dev/null || { echo "Missing dependency: $command" >&2; exit 1; }
done
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
if [[ $init == systemd ]]; then
  service_source=$script_dir/packaging/ravel-gost.service
else
  service_source=$script_dir/packaging/ravel-gost.openrc
fi
[[ -f $service_source ]] || { echo 'Service definition missing from installation bundle' >&2; exit 1; }
for path in /usr/local/ravel-gost /etc/ravel-gost /var/lib/ravel-gost "$service_path"; do
  [[ ! -e $path && ! -L $path ]] || { echo "Existing installation path: $path; refusing to overwrite" >&2; exit 1; }
done
if id ravel-gost &>/dev/null; then
  echo 'User ravel-gost already exists; refusing to reuse an unknown account' >&2
  exit 1
fi
case $(uname -m) in
  x86_64) arch=64 ;;
  aarch64|arm64) arch=arm64-v8a ;;
  *) echo 'Supported architectures: amd64, arm64' >&2; exit 1 ;;
esac
if ((token_stdin)); then
  IFS= read -r token || [[ -n ${token:-} ]]
else
  read -r -s -p 'Machine token: ' token </dev/tty
  echo >/dev/tty
fi
[[ $token =~ ^[a-zA-Z0-9_-]{16,512}$ ]] || { echo 'Invalid machine token' >&2; exit 2; }
stage=$(mktemp -d)
trap 'unset token; rm -rf -- "$stage"' EXIT
curl_options=(--fail --silent --show-error --location --proto '=https' --proto-redir '=https' --connect-timeout 15 --max-time 300)
if [[ $version == latest ]]; then
  version=$(curl "${curl_options[@]}" "https://api.github.com/repos/$repo/releases/latest" | jq -er '.tag_name')
  [[ $version =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] || exit 1
fi
asset=ravel-gost-linux-$arch.zip
base=https://github.com/$repo/releases/download/$version
curl "${curl_options[@]}" "$base/$asset" -o "$stage/$asset"
curl "${curl_options[@]}" "$base/$asset.sha256" -o "$stage/checksum"
expected=$(awk 'NR==1 {print $1}' "$stage/checksum")
[[ $expected =~ ^[a-fA-F0-9]{64}$ ]] || { echo 'Invalid checksum' >&2; exit 1; }
printf '%s  %s\n' "$expected" "$stage/$asset" | sha256sum -c -
# Extract only fixed members, never archive-controlled paths or service commands.
unzip -p "$stage/$asset" ravel >"$stage/ravel"
unzip -p "$stage/$asset" gost >"$stage/gost"
chmod 0755 "$stage/ravel" "$stage/gost"
printf '%s' "$token" | jq -Rs --arg panel "$panel" --argjson id "$machine_id" \
  '{panel_url:$panel,machine_id:$id,machine_token:.,protocol:"ravel-v1",sync_interval:30,status_interval:120}' >"$stage/state.json"
unset token
if [[ $init == systemd ]]; then
  useradd --system --user-group --no-create-home --home-dir /nonexistent --shell /usr/sbin/nologin ravel-gost
else
  addgroup -S ravel-gost
  adduser -S -D -H -h /nonexistent -s /sbin/nologin -G ravel-gost ravel-gost
fi
install -d -o ravel-gost -g ravel-gost -m 0750 /usr/local/ravel-gost /var/lib/ravel-gost
install -d -o root -g ravel-gost -m 0750 /etc/ravel-gost
install -o ravel-gost -g ravel-gost -m 0750 "$stage/ravel" /usr/local/ravel-gost/ravel
install -o root -g root -m 0755 "$stage/gost" /usr/local/ravel-gost/gost
install -o root -g ravel-gost -m 0640 "$stage/state.json" /etc/ravel-gost/state.json
if [[ $init == systemd ]]; then
  runuser -u ravel-gost -- /usr/local/ravel-gost/ravel version
  runuser -u ravel-gost -- /usr/local/ravel-gost/gost -V
  install -o root -g root -m 0644 "$service_source" "$service_path"
  systemctl daemon-reload
  systemctl enable --now ravel-gost.service
  echo "Installed $version. Status: systemctl status ravel-gost.service"
else
  su -s /bin/sh -c '/usr/local/ravel-gost/ravel version && /usr/local/ravel-gost/gost -V' ravel-gost
  install -o root -g root -m 0755 "$service_source" "$service_path"
  install -o ravel-gost -g ravel-gost -m 0600 /dev/null /var/log/ravel-gost.log
  rc-update add ravel-gost default
  rc-service ravel-gost start
  echo "Installed $version. Status: rc-service ravel-gost status"
fi
