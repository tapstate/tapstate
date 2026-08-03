# The release gate

What must be true before an Alpha release ships, beyond the build being green. Each item names its
check; "green build" appears nowhere below because a green build is the conclusion these items keep
honest, not one of them.

## Automatic checks

| # | Check | Enforced by |
|---|---|---|
| 1 | Zero fixed-duration sleeps in the e2e module | `FixedSleepGateTest`, every build |
| 2 | Every witness in `e2e/witness-manifest.txt` executed and passed, verified name by name | `.github/scripts/witness-gate.sh` against the run's witness ledger, in the real-connector lane |

A witness that was skipped or aborted, or an environment that could not run one, fails check 2 by
absence. That is deliberate: an environment problem is a gate failure, never a scenario exemption.

## Review obligations

| # | Obligation | How |
|---|---|---|
| 3 | Every witness-manifest assertion carries unexpired mutation evidence | Release review walks the manifest case by case; evidence rules in CONTRIBUTING.md, "Mutation evidence". Evidence that cannot be confirmed counts as absent |

## Release-notes obligations

The honesty of the gate extends to what the release says about itself. The Alpha release notes:

- **must disclose** the insert-only blind spot: on an insert-only (non-upsert) target, overlapping
  snapshot and CDC reads can produce duplicate rows, and no shipped scenario covers that case;
- **must disclose** the change-stream positioning window: on real connectors, changes written to the
  source between the snapshot read and the change stream becoming positioned may be lost — the
  position recorded before the snapshot is not yet handed to the connector's stream read, and no
  shipped scenario covers the window (the e2e executor's redelivery exists to keep tests stable
  across it, not to close it);
- **must not claim production readiness**;
- **must not claim exactly-once delivery**.

Wording changes to these four requirements go through review together with this file.
