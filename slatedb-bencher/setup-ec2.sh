#!/usr/bin/env bash
#
# Bootstrap a fresh EC2 instance (Amazon Linux 2023 or Ubuntu 22.04/24.04)
# for running SlateDB and the slatedb-bencher workload against S3.
#
# Usage:
#   curl -fsSL <raw-url>/setup-ec2.sh | bash
#   # or, after SSHing in:
#   ./setup-ec2.sh
#
# Env vars (optional):
#   SLATEDB_REPO   git URL to clone       (default: https://github.com/slatedb/slatedb.git)
#   SLATEDB_REF    branch/tag/sha         (default: main)
#   SLATEDB_DIR    where to clone into    (default: $HOME/slatedb)
#   AWS_REGION     baked into aws.env     (default: us-east-1)
#   AWS_BUCKET     baked into aws.env     (default: unset — fill in later)
#   SKIP_BUILD=1   skip `cargo build -r`  (default: build)

set -euo pipefail

SLATEDB_REPO="${SLATEDB_REPO:-https://github.com/slatedb/slatedb.git}"
SLATEDB_REF="${SLATEDB_REF:-main}"
SLATEDB_DIR="${SLATEDB_DIR:-$HOME/slatedb}"
AWS_REGION_DEFAULT="${AWS_REGION:-us-east-1}"
AWS_BUCKET_DEFAULT="${AWS_BUCKET:-}"

log() { printf '\033[1;34m[setup-ec2]\033[0m %s\n' "$*"; }

# --- detect package manager ------------------------------------------------
if command -v dnf >/dev/null 2>&1; then
  PKG=dnf
  SUDO_UPDATE=(sudo dnf -y update)
  SUDO_INSTALL=(sudo dnf -y install)
  PKGS=(gcc gcc-c++ make cmake pkgconf-pkg-config openssl-devel
    git python3 gnuplot tar gzip which jq htop)
elif command -v apt-get >/dev/null 2>&1; then
  PKG=apt
  export DEBIAN_FRONTEND=noninteractive
  SUDO_UPDATE=(sudo apt-get update -y)
  SUDO_INSTALL=(sudo apt-get install -y)
  PKGS=(build-essential cmake pkg-config libssl-dev ca-certificates
    curl git python3 gnuplot jq htop)
else
  echo "unsupported distro: need dnf or apt" >&2
  exit 1
fi
log "package manager: $PKG"

# --- system deps -----------------------------------------------------------
log "installing system packages"
"${SUDO_UPDATE[@]}"
"${SUDO_INSTALL[@]}" "${PKGS[@]}"

# --- rust via rustup (rust-toolchain.toml pins the exact version) ----------
if ! command -v rustup >/dev/null 2>&1; then
  log "installing rustup"
  curl --proto '=https' --tlsv1.2 -fsSL https://sh.rustup.rs |
    sh -s -- -y --default-toolchain none --profile minimal
fi
# shellcheck disable=SC1091
source "$HOME/.cargo/env"

# --- build bencher ---------------------------------------------------------
if [ -z "${SKIP_BUILD:-}" ]; then
  log "building slatedb-bencher (release)"
  (cd "$SLATEDB_DIR" && cargo build -r --package slatedb-bencher)
fi

# --- aws env template ------------------------------------------------------
ENV_FILE="$SLATEDB_DIR/aws.env"
if [ ! -f "$ENV_FILE" ]; then
  log "writing $ENV_FILE template (source it before running benchmarks)"
  cat >"$ENV_FILE" <<EOF
# Source this before running the bencher against S3:
#   set -a; source aws.env; set +a
export CLOUD_PROVIDER=aws
export AWS_REGION=${AWS_REGION_DEFAULT}
export AWS_BUCKET=${AWS_BUCKET_DEFAULT}
# Prefer an IAM instance role over static keys. Uncomment only if needed:
# export AWS_ACCESS_KEY_ID=
# export AWS_SECRET_ACCESS_KEY=
# export AWS_SESSION_TOKEN=
# export AWS_ENDPOINT=
# export AWS_ALLOW_HTTP=
EOF
fi

log "done. Next steps:"
cat <<EOF
  cd $SLATEDB_DIR
  set -a; source aws.env; set +a          # edit AWS_BUCKET first
  ./slatedb-bencher/benchmark-db.sh       # or benchmark-blobdb.sh, etc.
EOF
