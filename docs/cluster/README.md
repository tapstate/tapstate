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
- one shared `tapstate.control.auth.jwt-secret`;
- an explicit member discovery mode and a routable `tapstate.hz.bind-address`;
- at least 4 GiB of JVM heap; and
- a MongoDB replica set available to the members as the majority coordination store.

Leave the signing secret out and each member mints its own at startup, which is a working
single-node default and a confusing cluster: a client authenticates against the member it logged in
to and is refused by every other one, because a session token is verified with the key that signed
it. The startup log warns about it. Set the same value on every member.

The member port serves an unauthenticated Hazelcast protocol. Expose it only on a private network or
behind a NetworkPolicy. Cluster mode does not add transport authentication or TLS to that port.

## Heap

The requirement is the same one a single server has - at least 4 GiB of JVM heap, and why that is a
requirement rather than advice is in
[what the server needs](../running-on-your-own-databases.md#what-the-server-needs). What changes on a
cluster is only this:

**Adding members does not divide it.** Each member holds the state of the partitions it owns, so a
larger cluster holds more in total and no less on any one member. Where a map keeps a copy for another
member the copy is paid for as well: a join step's maps are replicated and a nest's are not, so size
from twice a join step's resident budget for every join map a member runs.

## Profiles

`tapstate.cluster.profile` defines the safety contract. It is not a tuning hint.

| Profile | Bootstrap members | Contract |
|---|---:|---|
| `single` | one | Default. Discovery stays off and the member port stays on loopback. |
| `process-failure-only` | exactly two | Supports process-loss recovery testing. It does not provide network-partition safety and emits a warning at startup. |
| `production-ha` | at least three | Business work requires a strict majority of the last committed active member set. A four-member 2-2 split fails closed on both sides. |

The bootstrap-members column is a second setting, not a description of the profile:
`tapstate.cluster.bootstrap-min-members` must be **exactly 2** under `process-failure-only` and **3
or more** under `production-ha`. Its default is 3, so a `process-failure-only` member started without
it is refused at boot with `boot.cluster-profile-invalid` rather than quietly running with the wrong
threshold.

Before the production threshold is reached, members may discover each other but cannot acquire or
renew business-workload claims. A node-session claim is different: it reserves a stable node ID
before join and does not authorize pipeline side effects.

The committed set grows when a member joins and does not shrink when one stops answering. A member
that has died and a member on the other side of a network cut look the same from here, and writing
the smaller set down on that evidence is how one half of an even split would make itself a majority.
So losing members is survivable while a majority of the committed set is still running -- three of
four, say -- and a cluster that permanently loses a majority stays refused. To bring it back, start
replacement members with the node IDs that are still committed: each is admitted once the session
its predecessor left behind has expired. There is not yet a way to remove a member from the
committed set.

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

## Connecting to a cluster

Every control-plane node answers every request, and answers it the same. A client reaches the cluster
by reaching any healthy member; there is no leader to find and no member that has to be asked first.

- Point a client at one or more members, or at a load balancer or virtual IP in front of them. Both
  are supported and neither is preferred: a single entry point is easier to hand out, and a seed list
  keeps working when that entry point is the thing that is down.
- A client may move between members mid-session. Its credential belongs to the cluster rather than to
  the member that issued it, and a read that was interrupted is re-attached to another member and
  continued rather than restarted.
- **Never give a client a member port address.** The member port speaks the unauthenticated cluster
  protocol between members, and `tapstate.hz.bind-address` is where that protocol listens. The address
  a client uses is `tapstate.control.advertise-url`, and the two are separate settings because they are
  separate networks: the first belongs inside your private network, the second is what you publish.
- Set `tapstate.control.advertise-url` to an address your clients can actually reach. Members hand it
  out to clients as the other places they can go, so a value that resolves only inside the cluster
  gives every client a list of addresses it cannot dial.

The CLI stores one or more seed URLs per context. After it authenticates it asks the cluster who its
members are and adds their advertised URLs to the ones it will fail over to, so a cluster that has
grown since the context was written is still reachable in full. A seed that reports a different
cluster is refused rather than merged, because a client holding one cluster's credential must never
find itself talking to another.

A browser front end uses the same contract: any healthy member, or one entry point in front of them.
It must not try to work out which member to talk to for itself.
