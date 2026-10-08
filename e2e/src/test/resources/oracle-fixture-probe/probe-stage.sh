#!/bin/bash
set -eu
directory=/tmp/tapstate-capture-probe
label=${1:?stage label required}
read -r uptime _ < /proc/uptime
line="capture-probe-stage utc=$(date -u '+%Y-%m-%dT%H:%M:%S.%NZ') uptime=$uptime pid=$BASHPID label=$label"
printf '%s\n' "$line" >> "$directory/stages.log"
printf '%s\n' "$line"
