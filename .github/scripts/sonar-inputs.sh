#!/usr/bin/env bash
# Reuse complete shard admission before coverage is merged and sent to the scanner.
# Each shard declares its execution data; e2e integration shards intentionally have none.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
exec "$here/ci-aggregate.sh" verify "$@"
