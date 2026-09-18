---
status: engineering-draft
publication: handoff
target: https://tapstate.dev/docs/guides/cluster
---

# Cluster preview

Cluster mode is opt-in. An installation that does not set a discovery mode remains a single member,
binds the member protocol to `127.0.0.1`, and cannot discover another Tapstate process.

Every cluster member needs:

- one shared `tapstate.cluster.id`;
- one stable, unique `tapstate.cluster.node-id`;
- an absolute, routable `tapstate.control.advertise-url`;
- an explicit member discovery mode and a routable `tapstate.hz.bind-address`; and
- a MongoDB replica set available to the members as the majority coordination store.

The member port serves an unauthenticated Hazelcast protocol. Expose it only on a private network or
behind a NetworkPolicy. Cluster mode does not add transport authentication or TLS to that port.

## Profiles

`tapstate.cluster.profile` defines the safety contract. It is not a tuning hint.

| Profile | Bootstrap members | Contract |
|---|---:|---|
| `single` | one | Default. Discovery stays off and the member port stays on loopback. |
| `process-failure-only` | exactly two | Supports process-loss recovery testing. It does not provide network-partition safety and emits a warning at startup. |
| `production-ha` | at least three | Business work requires a strict majority of the last committed active member set. A four-member 2-2 split fails closed on both sides. |

Before the production threshold is reached, members may discover each other but cannot acquire or
renew business-workload claims. A node-session claim is different: it reserves a stable node ID
before join and does not authorize pipeline side effects.

## Failure-detection budget

The defaults are deliberately two separate clocks:

| Property | Default | Meaning |
|---|---:|---|
| `tapstate.hz.heartbeat-interval` | `5s` | How often a member sends a liveness heartbeat. |
| `tapstate.hz.maximum-no-heartbeat` | `30s` | How long membership waits before declaring a member lost. |
| `tapstate.cluster.node-session-renew-interval` | `10s` | How often a stable node renews its reservation. |
| `tapstate.cluster.node-session-ttl` | `30s` | How long the reservation remains valid without renewal. |
| `tapstate.cluster.workload-claim-renew-interval` | `10s` | How often a business owner renews its claim. |
| `tapstate.cluster.workload-claim-ttl` | `30s` | How long a business claim remains valid without renewal. |

The maximum no-heartbeat duration must be longer than the heartbeat interval. Recovery waits for
both topology loss detection and the relevant workload lease boundary; it is not an instantaneous
failover promise.

## Discovery

Set `tapstate.hz.discovery.mode` to `tcp-ip` for explicit VM or bare-metal seed addresses, or to
`kubernetes` for headless-service DNS or Kubernetes API discovery. Auto-detection and multicast stay
disabled. All members must use the same stable cluster ID, while node IDs and advertised control URLs
identify individual members.
