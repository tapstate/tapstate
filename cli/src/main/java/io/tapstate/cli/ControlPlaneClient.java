package io.tapstate.cli;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;

/**
 * The CLI's transport seam to a running Tapstate server (rule R6: the CLI reaches services over HTTP
 * only). A reachability probe, the authentication call, and the connected artifact verbs (apply / get /
 * list), each authenticated by a bearer credential. Every call resolves to a result rather than throwing,
 * so the caller walks seeds, fails over and renders outcomes without try/catch. The interface exists so
 * the REPL logic is testable with a network-free fake and the production {@link HttpControlPlaneClient}
 * is swapped in only at the process entry point.
 */
interface ControlPlaneClient extends AutoCloseable {

    /** Reads anonymous stable cluster identity from the well-known endpoint. */
    default DiscoveryOutcome discover(URI baseUrl) {
        return new DiscoveryOutcome.Unreachable();
    }

    /** Whether {@code GET {baseUrl}/healthz} answers 200; any I/O failure counts as not healthy. */
    boolean isHealthy(URI baseUrl);

    /**
     * Verifies a username / password via {@code POST {baseUrl}/auth/login} and returns the outcome: a
     * bearer token on success, a coded rejection when the server refuses, or unreachable on any I/O
     * failure. Never throws.
     */
    LoginOutcome login(URI baseUrl, String username, String password);

    /**
     * Exchanges an opaque cached user session via {@code POST {baseUrl}/auth/session}; the presented
     * credential uses the {@code TapstateSession} auth scheme and returns a short-lived bearer token.
     */
    default SessionExchangeOutcome exchangeSession(URI baseUrl, String sessionToken) {
        return new SessionExchangeOutcome.Unreachable();
    }

    /**
     * Applies a batch of authored drafts via {@code POST {baseUrl}/api/artifacts:apply}, authenticated by
     * the bearer {@code credential}: the per-artifact results on success, a coded rejection when the server
     * refuses (a validation failure is a {@code dsl.*} code), or unreachable on any I/O failure. Never throws.
     */
    ApplyOutcome apply(URI baseUrl, String credential, List<LocalDraft> drafts);

    /**
     * Reads one artifact by id via {@code GET {baseUrl}/api/artifacts/{id}}, authenticated by the bearer
     * {@code credential}: found with its canonical form, absent on a 404, a coded rejection, or unreachable
     * on any I/O failure. Never throws.
     */
    GetOutcome get(URI baseUrl, String credential, String id);

    /**
     * Removes one artifact by id via {@code DELETE {baseUrl}/api/artifacts/{id}}, authenticated by the
     * bearer {@code credential} and conditioned on {@code expectedContentHash} travelling as a quoted
     * {@code If-Match}: removed on success, a coded rejection carrying the server's named parameters when
     * it refuses (still referenced, a pipeline that is not stopped, a stale or missing precondition), or
     * unreachable on any I/O failure. Never throws.
     */
    DeleteOutcome delete(URI baseUrl, String credential, String id, String expectedContentHash);

    /**
     * Lists stored artifacts via {@code GET {baseUrl}/api/artifacts}, optionally filtered by {@code kind}
     * ({@code null} = all), authenticated by the bearer {@code credential}: the artifacts on success, a
     * coded rejection, or unreachable on any I/O failure. Never throws.
     */
    ListOutcome list(URI baseUrl, String credential, String kind);

    /**
     * Runs a connection test via {@code POST {baseUrl}/api/connections:test}, authenticated by the bearer
     * {@code credential}: the request carries the connection {@code id} (the key its result is stored under),
     * the {@code connectorId} it configures, and the {@code settings} to run that connector with. Returns the
     * structured report on success (pass or fail alike), a coded rejection when the server refuses, or
     * unreachable on any I/O failure. Never throws.
     */
    ConnectionTestOutcome test(
            URI baseUrl, String credential, String id, String connectorId, Map<String, Object> settings);

    /**
     * Reads a connection's latest test result via {@code GET {baseUrl}/api/connections/{id}/test-result},
     * authenticated by the bearer {@code credential}: the stored report on success, absent on a 404 (the
     * connection has never been tested), a coded rejection when the server refuses, or unreachable on any I/O
     * failure. Never throws.
     */
    ConnectionTestResultOutcome testResult(URI baseUrl, String credential, String id);

    /**
     * Runs a schema discovery via {@code POST {baseUrl}/api/connections:discover-schema}, authenticated by
     * the bearer {@code credential}: the request carries the connection {@code id} (the key its model is
     * stored under), the {@code connectorId} it configures, and the {@code settings} to run that connector
     * with. Returns the discovered model on success, a coded rejection when the server refuses, or
     * unreachable on any I/O failure. Never throws.
     */
    ConnectionDiscoverSchemaOutcome discoverSchema(
            URI baseUrl, String credential, String id, String connectorId, Map<String, Object> settings);

    /**
     * Reads a connection's latest discovered source model via {@code GET {baseUrl}/api/connections/{id}/schema},
     * authenticated by the bearer {@code credential}: the stored model on success, absent on a 404 (the
     * connection has never been discovered), a coded rejection when the server refuses, or unreachable on any
     * I/O failure. Never throws.
     */
    ConnectionSchemaOutcome schema(URI baseUrl, String credential, String id);

    /**
     * Registers a connector artifact via {@code POST {baseUrl}/api/connectors:register}, authenticated by
     * the bearer {@code credential}: the {@code artifact} bytes are uploaded (base64-encoded in the body),
     * and the server introspects and content-hash idempotently stores them. Returns what was registered on
     * success (newly, or an already-registered no-op), a coded rejection when the server refuses (a bad
     * artifact, an id conflict), or unreachable on any I/O failure. Never throws.
     */
    ConnectorRegisterOutcome register(URI baseUrl, String credential, byte[] artifact);

    /**
     * Lists the connectors the online catalog exposes via {@code GET {baseUrl}/api/connectors},
     * authenticated by the bearer {@code credential}: the bundled snapshot union the rows derived for
     * registered connectors, each tagged bundled or registered. Returns the connectors on success
     * (possibly empty), a coded rejection when the server refuses, or unreachable on any I/O failure.
     * Never throws.
     */
    ConnectorListOutcome connectorList(URI baseUrl, String credential);

    /**
     * Lists the collections a declared source's own database holds, via
     * {@code GET {baseUrl}/api/sources/{sourceId}/collections}, authenticated by the bearer
     * {@code credential}: what the connector reported, a coded rejection when the server refuses (an id
     * that is not a stored source), or unreachable on any I/O failure. Never throws.
     */
    DataBrowserOutcome.Collections collections(URI baseUrl, String credential, String sourceId);

    /**
     * Reads what the connector knows about one collection's size, via
     * {@code GET {baseUrl}/api/sources/{sourceId}/collections/{collection}/stats}, authenticated by the
     * bearer {@code credential}: the report on success, a coded rejection when the server refuses (a
     * collection the source's database does not hold), or unreachable on any I/O failure. Never throws.
     */
    DataBrowserOutcome.Stats stats(URI baseUrl, String credential, String sourceId, String collection);

    /**
     * Reads rows from one collection, via
     * {@code POST {baseUrl}/api/sources/{sourceId}/collections/{collection}:find}, authenticated by the
     * bearer {@code credential}. {@code filter} is the request's filter as the shell parsed it, in
     * Tapstate's own vocabulary; {@code sort} and {@code limit} may be null, which asks for no particular
     * order and for the control plane's own size. The read is one-shot, so nothing here names a position
     * to resume from. Never throws.
     */
    DataBrowserOutcome.Find find(URI baseUrl, String credential, String sourceId, String collection,
                                 Object filter, DataBrowserCall.Order sort, Integer limit);

    default TokenCreateOutcome tokenCreate(URI baseUrl, String credential, String scope) {
        return new TokenCreateOutcome.Unreachable();
    }

    default TokenListOutcome tokenList(URI baseUrl, String credential) {
        return new TokenListOutcome.Unreachable();
    }

    default TokenRevokeOutcome tokenRevoke(URI baseUrl, String credential, String tokenId) {
        return new TokenRevokeOutcome.Unreachable();
    }

    /**
     * Issues a pipeline lifecycle verb ({@code start} / {@code stop} / {@code pause} / {@code resume}) via
     * {@code POST {baseUrl}/api/pipelines/{pipelineId}:{verb}}, authenticated by the bearer
     * {@code credential}: the pipeline's new desired state on success, a coded rejection when the server
     * refuses (an unknown pipeline, a forbidden transition, or a stale revision), or unreachable on any I/O
     * failure. Never throws.
     */
    LifecycleOutcome lifecycle(URI baseUrl, String credential, String pipelineId, String verb);

    /**
     * Reads a pipeline's lifecycle state via {@code GET {baseUrl}/api/pipelines/{pipelineId}/status},
     * authenticated by the bearer {@code credential}: the state on success, a coded rejection when the
     * server refuses (a pipeline with no published observation is {@code monitor.no-observation}), or
     * unreachable on any I/O failure. Never throws.
     */
    StatusOutcome status(URI baseUrl, String credential, String pipelineId);

    /**
     * Reads a pipeline's open map of run statistics via {@code GET {baseUrl}/api/pipelines/{pipelineId}/metrics},
     * authenticated by the bearer {@code credential}: the metrics on success (empty when no source is wired
     * yet), a coded rejection when the server refuses, or unreachable on any I/O failure. Never throws.
     */
    MetricsOutcome metrics(URI baseUrl, String credential, String pipelineId);

    /**
     * Reads a pipeline's per-table initial-load progress via
     * {@code GET {baseUrl}/api/pipelines/{pipelineId}/snapshot}, authenticated by the bearer
     * {@code credential}: the per-table progress on success (empty outside a snapshot phase), a coded
     * rejection when the server refuses, or unreachable on any I/O failure. Never throws.
     */
    SnapshotOutcome snapshot(URI baseUrl, String credential, String pipelineId);

    /**
     * Reads a pipeline's recent log lines via {@code GET {baseUrl}/api/pipelines/{pipelineId}/logs},
     * authenticated by the bearer {@code credential}: the tail on success (empty when the pipeline has
     * logged nothing on the served node), a coded rejection when the server refuses, or unreachable on any
     * I/O failure. Never throws.
     */
    LogsOutcome logs(URI baseUrl, String credential, String pipelineId);

    /**
     * Watches a pipeline's status over a websocket ({@code /api/pipelines/{pipelineId}/status/watch}),
     * delivering each state — the current one, then each change — to {@code sink} until the stream ends or
     * {@code stop} signals (a {@code true} return between frames). On a dropped connection after a
     * successful handshake it re-attaches until stopped — unless the server closed the stream deliberately
     * with a coded refusal (an id that will never resolve, for one), which is terminal: re-attaching would
     * only be refused the same way forever. That refusal's code is the return value; {@code null} means
     * the watch ended by {@code stop} or an unreachable/refused handshake. Blocks the caller until it
     * returns. Never throws.
     */
    String watchStatus(URI baseUrl, String credential, String pipelineId, StatusStream sink, BooleanSupplier stop);

    /**
     * Follows a pipeline's node-local logs over a websocket ({@code /api/pipelines/{pipelineId}/logs/follow}),
     * delivering each batch of newly appended lines to {@code sink} until the stream ends or {@code stop}
     * signals. Re-attaches and terminates exactly as {@link #watchStatus} does, returning a coded refusal
     * the server closed with, or {@code null}. Blocks the caller until it returns. Never throws.
     */
    String followLogs(URI baseUrl, String credential, String pipelineId, LogStream sink, BooleanSupplier stop);

    /**
     * Follows one collection's changes over the websocket, delivering each to {@code sink} until
     * {@code stop} says so. Returns the code of a refusal that will never be served, or null for every
     * other ending. Never throws: a stream that ended is not a failure of the caller's command.
     */
    String tail(URI baseUrl, String credential, String sourceId, String collection, Object filter,
                TailStream sink, BooleanSupplier stop);

    @Override
    default void close() {
    }
}
