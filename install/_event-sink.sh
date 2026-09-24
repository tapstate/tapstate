#!/usr/bin/env bash
# The local install-event sink: a stand-in for the event endpoint, so the cases that read what an
# install sent run with no network and no credentials, and assert on what actually arrived rather than
# on what a script claims it sends.
#
# Sourced, never run. The installer smoke and the quickstart smoke both need a sink, and they need the
# same one: the server and the wait for its port are where a fix lands, and with a copy in each smoke
# every fix had to land twice, while a copy that missed one kept failing for a reason already fixed.
#
# The program stays on python3's stdin rather than in a file of its own, because each smoke's verdict
# test recognises the sink as the one program the smoke hands python3 on stdin.

# Starts the sink and succeeds once it has published the port it bound and is still alive after that.
# Sets SINK_PID; SINK_DIR, whose log holds each event body that arrived, one per line; and, on success,
# SINK_URL, the endpoint to hand the script under test. Fails when the sink exits, before or after it
# bound a port, or has bound no port after about 15 seconds, and then sets SINK_FAILURE to which of these
# it was, worded to follow "the local sink".
start_sink() {
  SINK_DIR="$(mktemp -d)"
  : > "$SINK_DIR/log"
  python3 - "$SINK_DIR" <<'PYEOF' &
import http.server, os, socketserver, sys
d = sys.argv[1]
class H(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        n = int(self.headers.get('Content-Length') or 0)
        body = self.rfile.read(n).decode('utf-8', 'replace')
        with open(os.path.join(d, 'log'), 'a') as fh:
            fh.write(body + "\n")
        self.send_response(204); self.end_headers()
    def log_message(self, *a): pass
# TCPServer and not http.server.HTTPServer, which looks up a name for the address it bound before it
# listens: on the macOS runners that lookup stalls for about 35 seconds, far past any wait for the port.
srv = socketserver.TCPServer(('127.0.0.1', 0), H)
with open(os.path.join(d, 'port'), 'w') as fh:
    fh.write(str(srv.server_address[1]))
srv.serve_forever()
PYEOF
  SINK_PID=$!
  # About 15s for a slow machine, and never longer than the sink lives: one that died is not waited on.
  for _ in $(seq 1 150); do
    [ -s "$SINK_DIR/port" ] && break
    kill -0 "$SINK_PID" 2>/dev/null || break
    sleep 0.1
  done
  # A published port is not yet a started sink. One that died once it had bound, as it does when its
  # server fails on the way up, leaves its port behind, and every case pointed at that port is refused
  # and fails as though the script under test had sent nothing. So the sink counts as started only if it
  # is still alive once its port has been read.
  if [ -s "$SINK_DIR/port" ]; then
    local port
    port="$(cat "$SINK_DIR/port")"
    if kill -0 "$SINK_PID" 2>/dev/null; then
      # shellcheck disable=SC2034  # read by whoever sources this
      SINK_URL="http://127.0.0.1:$port/e"
      return 0
    fi
  fi
  # A sink that crashed and one still stuck before it listens, as the name lookup on the macOS runners
  # kept it, are fixed in different places, and so are a crash before the sink bound and one after. The
  # failure says which it was, and for a crash the status it exited with, which the wait in stop_sink
  # would otherwise discard. A dead sink's port file no longer changes, so it tells the two crashes apart.
  local status=0
  # shellcheck disable=SC2034  # read by whoever sources this
  if kill -0 "$SINK_PID" 2>/dev/null; then
    SINK_FAILURE="was still running after about 15 seconds without having bound a port"
  else
    wait "$SINK_PID" || status=$?
    if [ -s "$SINK_DIR/port" ]; then
      SINK_FAILURE="exited with status $status after it bound a port"
    else
      SINK_FAILURE="exited with status $status before it bound a port"
    fi
  fi
  return 1
}

# Stops the sink, whether or not it ever bound a port, and removes what it wrote.
stop_sink() {
  kill "$SINK_PID" 2>/dev/null; wait "$SINK_PID" 2>/dev/null; rm -rf "$SINK_DIR"
}
