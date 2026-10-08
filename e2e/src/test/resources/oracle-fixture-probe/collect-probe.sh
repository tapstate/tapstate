#!/bin/bash
set -eu
directory=/tmp/tapstate-capture-probe
if [ ! -d "$directory" ]; then exit 0; fi
bash /tmp/tapstate-probe-stage.sh java-before-container-stop
for file in "$directory/main.pid" "$directory/holder.pid" "$directory/supervisor.pid" \
            "$directory/wait-sampler.pid" "$directory/holder.exit" \
            "$directory/stages.log" "$directory/main.log" \
            /tmp/tapstate-transaction.log "$directory/holder-supervisor.log" "$directory/waits.log"; do
  printf 'capture-probe-file path=%s\n' "$file"
  if [ -f "$file" ]; then
    wc -c < "$file"
    head -c 32768 "$file"
    printf '\n'
  else
    printf 'UNKNOWN: file not produced\n'
  fi
done
