---
status: engineering-draft
publication: handoff
target: https://tapstate.dev/docs/storage
---

# Set up, protect and recover cluster storage

This guide describes the three-layer installation in this checkout. Use it with a matching
server and Cluster Web build. An older storage layout cannot be upgraded by pointing this
binary at its database: use a separate new installation and retain the old data and backups.
There is no automatic old-layout migration or mixed-version operation.

## Choose the databases and credentials

| Layer | What it holds | What protects it |
| --- | --- | --- |
| Configuration | Cluster identity and keys, storage binding, authored resources, OP accounts, necessary connector notes and audit facts | A consistent database backup containing the keys and encrypted configuration together |
| Durable cache | Sessions, runtime ownership, checkpoints, replay data, discovered models and operator state | Retained state for ordinary restart; explicit recovery when the required state is lost |
| Application data | Rows written through the system-owned default target Source | Your target database's backup and recovery policy |

Use three different database names. OP setup takes one MongoDB or Atlas base URI and three
names; the defaults are `tapstate_config`, `tapstate_cache` and `tapstate_views`. Cloud supplies
Configuration separately; you enter your Atlas base URI and two distinct names for Durable
cache and Application data. The Configuration card is read-only in Cloud.

Configuration and Durable cache require MongoDB replica-set transactions. They may be on
separate deployments; their credentials and trust settings are independent. A standalone
`mongod` is insufficient. Give each Cluster its own databases and credentials. The old platform
`_operator` and `_views` databases are not required by a three-layer Cloud installation.

Grant the storage accounts the read, write, collection and index operations needed on their
named databases. A MongoDB `readWrite` role on each assigned database is the starting point;
custom roles must pass the actual connection tests. Do not grant access to unrelated databases
to bypass a failed test. A source connector may need additional source-specific permissions;
a successful storage test does not certify that source's CDC prerequisites.

Database addresses, DNS and firewall rules must work **from every Cluster member**, not just
from the browser or an administrator's laptop. Keep credentials out of command-line URIs,
terminal transcripts and support bundles; enter them in the protected setup form or an
owner-readable configuration input.

### TLS and Atlas

| Connection | Current scope |
| --- | --- |
| Public-CA server TLS | Configuration, Durable cache and the default application target |
| Custom-CA server TLS | Native Configuration and Durable cache clients; PEM bundles up to 64 KiB each |
| Custom-CA-only application target through the MongoDB/Atlas connector | Not supported in this release scope |
| Client certificates and private keys (mTLS) | Not supported |

With public CA certificates, leave the custom CA fields empty and use the controlled JVM trust
store. For custom CA, use **Custom CA for configuration and cache** in setup. Cloud Configuration
trust must be available to the server before it connects, through the deployment's protected
`tapstate.store.mongo.tls-ca-file` input. OP retains the Configuration bundle in its installation
directory; the cache bundle is retained with the encrypted binding. Preserve these materials
when restarting or replacing a member. Do not change global JVM trust to compensate for a wrong
bundle, and do not disable certificate-chain or hostname verification.

Standard `mongodb://` and `mongodb+srv://` connection strings are accepted. Preserve URI options
and the intended authentication database when choosing the three logical names. Ordinary MongoDB
may use an existing non-TLS deployment; selecting TLS requires successful verification.

User-supplied Atlas storage requires all of the following:

- Public endpoints and working SRV/TXT DNS where an SRV URI is used.
- The actual outbound IP of every participating Cluster member in the Atlas access list.
  A browser's IP is insufficient. Ask the deployment owner for the exact egress addresses;
  do not open the access list to the whole internet as a diagnostic shortcut.
- TLS and username/password authentication with `authSource=admin`.
  When the URI omits an authentication mechanism, the MongoDB driver negotiates the user's
  advertised SCRAM support, preferring `SCRAM-SHA-256` and accepting `SCRAM-SHA-1` accounts.
  Explicit `SCRAM-SHA-256` and `SCRAM-SHA-1` choices are preserved. An authentication failure
  does not trigger a retry with another mechanism. Other mechanisms and disabled TLS are rejected.
- Permissions on the selected databases, checked from the running Cluster.

Other Atlas authentication methods, VPC Peering and Private Endpoint are outside this scope.
Future privately deployed Cloud environments are not supported by these instructions. The
platform-supplied Configuration URI retains its platform authentication policy; the restrictions
above apply to the user's Atlas setup inputs.

## Install OP and create its first administrator

Use the same OS account for the installation command and server. Keep the installation directory
on persistent local storage with owner-only permissions. Its parent must exist; the selected
installation directory must not already exist on the first initialization.

From the unpacked server distribution:

```sh
./bin/tapstate-server installation --initialize \
  --tapstate.installation.directory=/var/lib/tapstate/installation

./bin/tapstate-server --role=all \
  --tapstate.installation.directory=/var/lib/tapstate/installation
```

The distribution launcher supplies the required JVM module-opening option. If using a built
JAR directly, the equivalent service start is:

```sh
java -Xmx4g --add-opens java.base/java.lang=ALL-UNNAMED \
  -jar app/target/app-<version>-boot.jar --role=all \
  --tapstate.installation.directory=/var/lib/tapstate/installation
```

Use JDK 21 and the normal deployment heap, connector seed directory, service port
and TLS configuration. Keep the packaged verified connector JARs available: connection tests
and default-target validation need the registered connector implementation. Initialization
creates authorization files; it does not start the service or create a database on its own.

1. Read `managementUrl` from the owner-only `stage.json` in that directory. It points to a
   dedicated `127.0.0.1` port selected by the server. Open that exact address.
2. Privately read the current token from `capability.json` and enter it as the **Installation
   credential**. It is valid for 15 minutes and for its current installation purpose only.
   Do not put it in a URL, chat or command-line argument.
3. Select MongoDB or Atlas, enter one base URI and three different database names, and supply
   any supported custom CA bundles. Run **Test connection** on all three cards.
4. After all tests pass, select **Save storage settings**. Editing an input invalidates its
   tests; expired tests must be rerun. A passed test is not a saved binding.
5. When **Restart required** appears, stop the original server gracefully with your deployment
   tools and start it again with the **same installation directory and deployment settings**.
   Wait for startup to complete. The save operation does not restart the process for you.
6. In another terminal, issue fresh authorization for this original installation:

   ```sh
   ./bin/tapstate-server installation --authorize \
     --tapstate.installation.directory=/var/lib/tapstate/installation
   ```

   Open the current management address from `stage.json`, enter the new credential, and use
   **Create the first administrator**. A setup credential cannot create this account; the
   command issues a first-administrator credential only after the required restart and checks.
7. Open the normal Cluster service address and sign in with the account you created. The
   management channel is not an ordinary login endpoint. Use Sources to test/discover an input,
   then create and run a Pipeline against the system-owned default target `views`.

For remote installation, explicitly forward the management port over SSH. Use the same local
port number as the current management address so its Host/Origin checks still match. Replace
both occurrences below with that port, and keep the local bind on loopback:

```sh
ssh -N -L 127.0.0.1:MANAGEMENT_PORT:127.0.0.1:MANAGEMENT_PORT user@server
```

Open the original `http://127.0.0.1:MANAGEMENT_PORT` through the tunnel. Do not expose the
management connector through a reverse proxy or to the network; the public service endpoint
cannot become a management endpoint through forwarded headers.

### Interrupted installation or expired authorization

Resume the server with the original directory. If authorization is missing or expired, use
`installation --authorize` against that same directory and enter the new token. Reauthorization
keeps the original operation and invalidates the old credential. After a process restart, obtain
a current credential; do not assume a previously displayed token remains usable.

If saving timed out, use **Refresh status** first. The binding may already be committed. Keep
`stage.json`, `configuration.uri`, `config-ca.pem` when present, and the original operation;
do not edit the files, reset a stage, change databases or run `--initialize` again to repair it.
If first-administrator creation timed out, check status: when it completed, use ordinary sign-in
instead of creating another account or resetting the password.

A narrowly interrupted first installation can offer an explicit recovery preview on the
management channel when the original binding is trusted and there are no users, Pipelines or
execution history. Review and confirm only the offered original scope. This prepares the initial
cache; it still ends with restart, first-administrator creation and ordinary login. It does not
make a setup credential a general disaster-recovery credential. Partial or conflicting state
requires diagnosis and remains blocked.

## Enter Cloud and finish storage setup

The deployment supplies its Cloud base URL, service token, Configuration URI and Cluster ID as a
complete set. A partial set is refused. A new Cluster also needs the platform's one-time
Configuration initialization authorization; an empty Configuration database alone is not proof
that it is new. An unavailable or mismatched authorization is a deployment problem, not a reason
to initialize a replacement database manually.

1. Sign in through the existing Cloud console and open this Cluster. Cloud manages its users;
   there is no local account or first-administrator installation step.
2. In **Set up cluster storage**, enter the Atlas URI, Durable cache name and Application data
   name. The URI is hidden by default; use its show/hide button when needed. Test Durable cache
   and Application data. The platform Configuration check is included automatically with the
   first database test and needs no separate click. OP still tests all three user-selected layers.
   Status icons retain readable labels: green means passed, red means failed, and an expired
   check must run again before saving.
3. Save once all tests pass. A successful setup retains one default target, `atlas-store`.
   Its application database is separate from the cache; ordinary Source labels do not create a
   managed target.
4. After saving, use **Return to Cloud sign-in** and enter the same Cluster through Cloud again.
   The server continues in the same process. The temporary setup session is closed, not upgraded
   into a normal session. Cancelling or failing this sign-in does not undo saved storage.
5. In Settings, check storage health and the default target. Test/discover your input Source
   before starting a Pipeline. Editor deep links obey the same storage gate.

If the browser loses the response, refresh or re-enter through Cloud and inspect the original
setup result. Do not provide a new cache URI, create another target or request a new Cluster
merely to retry a saved operation.

## Read health before deciding how to recover

Settings and the entry gate share the storage status. Layer health and setup completion are
separate: a previously saved setup can have a currently unavailable database. `/healthz` only
proves the process is reachable; it does not prove that storage or a Pipeline is admitted.

| What you see | Next step |
| --- | --- |
| Storage setup required | Complete the original three-layer setup |
| Preparing storage / Rebuilding runtime state | Refresh or check the original operation; do not create another one |
| Restart required | OP: restart the original installation, then authorize first-administrator creation |
| Sign-in ready / Rebuild confirmation required | Sign in normally and review recovery; business work is still gated |
| Storage admitted | Check the separate layer health and individual Pipeline state |
| Session expired / HTTP 401 | Re-enter through Cloud, or use the original OP account; installation credentials use the owner command |
| Permission refused / HTTP 403 | Check account role or installation purpose, expiry and management channel |
| State changed / conflict | Query the original result, then review a fresh preview of that same scope |
| Storage unavailable / dependency failure / HTTP 503 | Restore access to the original database, credentials, trust and network; sign-in cannot bypass it |
| Cleanup needs verification | Follow the cleanup procedure below; keep the binding and operation |

When only connectivity failed and all retained state is intact, restore the original service and
let the runtime verify it. Do not request full recovery just because a connection timed out.
Missing or damaged Configuration, its keyring, binding or OP account cannot be repaired by
creating a new key or administrator. Restore a matching Configuration backup and deployment
materials, then recheck. Partial cache loss or identity conflicts are not treated as an empty
new cache; retain the evidence and resolve the reported dependency first.

### Recover a wholly lost durable cache

1. Use your deployment tools to stop and isolate **every old Cluster member**. Verify that old
   source/target connections and in-flight writes have ended. An unresponsive process or machine
   is not proven stopped. Keep it isolated throughout recovery. The Cluster checks its local
   cleanup and storage identity; cross-machine isolation is the administrator's responsibility.
2. Bring up the intended recovery member with the original Configuration and binding. When
   Configuration is trusted and the bound cache is completely empty and writable, the runtime
   can prepare only the authentication structures. Sign in via the original Cloud entry or OP
   account. Old credentials are rejected; no business rebuild has been authorized yet.
3. In **Recover runtime storage and pipelines**, choose **Runtime storage for the cluster**, then
   **Preview impact**. Review every affected Pipeline. Confirm the isolation statement only when
   you have established it, then choose **Confirm runtime storage recovery**.
4. Keep the operation ID and use **Check original result** until the operation is complete.
   Every affected Pipeline remains stopped. Runtime storage being ready does not mean its
   Pipelines or target data have recovered.
5. Choose **Select one pipeline to rebuild**. Preview one Pipeline, check its source full-load
   capability, required models, target handling and old-resource retirement, then explicitly
   **Confirm full rebuild**. Repeat only for Pipelines you intend to run. Ordinary Run/Start
   cannot bypass a pending full-recovery requirement.
6. Verify the Pipeline's actual state and target values, not only the storage operation result.
   Keep unaffected, stopped or completed work stopped unless separately authorized to run.

The preview displays the configured target policy: **CLEAR** clears selected target tables before
loading, **APPEND** keeps rows and uses the configured write mode, and **FAIL** refuses a full load
when selected target tables contain rows. Confirm only the intended policy. A full rebuild may
reread all source data and alter targets; it is not continuation from a lost checkpoint.

If a preview becomes stale, **Recheck this scope** and review it again. After an execution failure,
inspect the Pipeline error, stop its failed run where required, and use **Continue this operation**
only when offered for the original operation. If the server requests a previous recovery ID, use
the recorded predecessor; do not invent an ID or silently redirect an old request to a new scope.

### Rebuild a pending Pipeline

Use **One pipeline full rebuild** for a Pipeline that the runtime explicitly marks as requiring
full recovery after the confirmed runtime-storage replacement. Enter its ID, choose **Preview
impact**, and review its source, model, target policy and old-resource retirement before
confirming. Use the original operation for result queries and offered continuations.

This action requires the retained recovery record and a stopped Pipeline. It is not a general
full-reload or database-migration command for an arbitrary healthy Pipeline. Do not create
artificial cache loss or change a binding to make the action available.

Review every listed shared dependency. Rebuilding one consumer does not authorize resetting
another consumer's cursor or dropping a shared log. An unknown source position must not be
replaced with "current" or "earliest" as if it were a proven continuation. A scope conflict or
unavailable preview is a refusal that needs investigation, not permission to bypass the check.

### Cleanup that cannot be confirmed

Keep the original operation ID and diagnostic code. Stop and isolate the owning process through
the deployment tools and verify that its source/target work has ended. Fix the reported connection
or permission failure, then refresh and query that operation. Use the offered continuation only
after the original scope and cleanup are verified. If no safe continuation is offered, retain the
state and have the deployment maintainer investigate the exact resource named by the diagnostic.

A physically retained resource that has been safely retired is different from a possibly active
writer. For a reported pending deletion, the maintainer must verify that exact retired identity
and its required permission before retrying cleanup. Never replace this check with a database-wide
drop, deletion of a claim or administrator record, manual generation edits, or a new operation ID.

## Transfer selected configuration

Use **Transfer configuration** in Settings with ordinary administrator authorization and admitted
storage. The section is collapsed by default; select its heading to open the controls.
This moves selected definitions; it is not a backup of the installation or runtime state.

1. Generate an independent random 32-byte key encoded as unpadded base64url (43 characters).
   Keep it securely and separately from the exported file; the server does not retain or return it.
2. Enter selected resource IDs, and optionally OP usernames, then **Download encrypted package**.
   Necessary published definitions and author layouts accompany the selection. Drafts, source
   keyring, binding, runtime state, sessions, recovery mappings and business rows do not.
3. In the destination Cluster, select the package and enter the same transfer key. Review the
   displayed mapping from the source default target to the destination's existing default target.
4. Use **Check import**, record its operation ID, review the insertion and mapping, then
   **Confirm import**. Existing resources are not overwritten; conflicts reject the whole import.
   Pipelines do not start automatically. OP accounts can move only to another OP installation;
   Cloud rejects a package containing accounts before writing anything.
5. If the response is lost, use **Check original result** with that ID. A committed receipt stays
   the original result even if someone later edits or deletes an imported resource. UNKNOWN or
   COMMITTING means wait and resolve that result, not create another import. Only after the
   original is proven uncommitted may you preflight again following a conflict or expiry.

The current limits are 64 objects, 1 MiB per object and 4 MiB total decoded configuration, including
required dependencies. A preflight lasts 30 minutes. There are at most 16 pending imports and 256
retained committed/unknown results per Cluster. There is no receipt-purge tool in this scope; do
not delete records to evade a limit or split a refused transaction into partly visible batches.

For automation, the CLI uses the same operations, with JSON request files rather than secret
inline arguments. After normal authentication against the intended Cluster:

```sh
tapstate storage status
tapstate storage recovery status --file recovery-status.json
tapstate configuration import status --file import-status.json
```

The two result-query files contain `{"operationId":"THE_ORIGINAL_RECOVERY_ID"}` and
`{"importId":"THE_ORIGINAL_IMPORT_ID"}` respectively. Keep any request file containing a URI,
CA material or transfer key owner-readable and out of source control. Writes use the same preview
and confirmation requirements; an unknown write result is not automatically retried. MCP exposes
the same operation schemas; write tools require explicit `--allow-write`.

## Back up Configuration and its keyring together

An encrypted configuration-transfer package omits Cluster identity, keys and runtime facts. It
cannot replace a full Configuration backup. Losing the keyring while keeping encrypted resources
is not recoverable through sign-in, reinitialization or full source reload.

1. Record the Cluster identity, original database endpoints/names, installed binary versions,
   default-target identity and protected credential/trust references. Do not put secret values in
   an ordinary inventory or support ticket.
2. Establish a maintenance point: stop writes to configuration and stop/isolate all members and
   their source/target work. Use your database's supported consistent-backup tooling to back up
   the **entire Configuration database**, including its `cluster_keys` data and all related facts,
   in one recoverable set. Do not back up encrypted Sources and keys at unrelated times.
3. Retain the matching OP installation directory, including stage, Configuration URI and CA
   materials, or the corresponding Cloud deployment identity and protected Configuration inputs.
   Keep ownership and restrictive permissions. Treat any copied capability as an expiring secret,
   not a permanent recovery credential; obtain a current one through the installation command
   when that purpose is still allowed.
4. Protect the backup as credentials: encrypt it, restrict access and retain the keys needed to
   restore it. Keep the original CACHE and application database backups under their own policy
   when continuation matters. Independent database backups are not automatically a coordinated
   checkpoint of source, runtime and target state.
5. Test restoration in an isolated deployment with all old writers disabled. Restore the matching
   Configuration/keyring and deployment materials through the database/deployment tools, then
   verify identity, decryption, original binding and layer health before allowing work. Do not
   start a restored copy beside an active copy of the same Cluster.
6. If the retained cache is complete, let the runtime verify its original recovery state. If it is
   wholly lost, follow the explicit all-stop/per-Pipeline procedure above. An inconsistent or
   partial restore remains blocked. Validate actual target data separately before declaring the
   deployment recovered.

The product does not provide online storage rebinding, a keyring-regeneration repair, arbitrary
state copying between databases, or an old-layout migration. A per-Nest `state.database` can
select an authorized additional database on the **same cache client**; it must not name
Configuration, Application data or a system database. Do not move existing state with ad-hoc
Mongo scripts. An existing placement must not be changed through an ad-hoc copy or restart. This recovery page
does not provide a general placement-migration workflow; retain the original placement and state
while the deployment maintainer resolves the reported dependency.

## Verification boundary of this draft

Local real-Mongo and process/browser checks cover OP setup, restart, first administrator, login
and selected Pipeline/target flows, and Cloud consumer authentication/recovery with local provider
fixtures. Both URI formats are covered by input validation. These checks do not certify a
particular Atlas endpoint, outbound allowlist, TLS deployment or live Cloud route. The full
preview Cloud plus authorized Atlas path, real standard/SRV connections and replacement, and
human interaction review still need their environment-specific evidence. A connection-test pass
reports only what was actually checked at that time.
