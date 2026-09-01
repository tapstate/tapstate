package io.tapstate.control.core;

import java.util.List;
import java.util.Map;

/**
 * The canonical set of control operations, declared as code constants (no classpath or reflection
 * scanning). This is the single source of truth from which every face derives its surface, and the
 * seed an {@link OperationRegistry} is built over.
 *
 * <p>Each face reads its exposed operations from this registry. The MCP surface is the online authoring
 * closure — reading the catalog, drafting and applying a workspace, removing one, and driving a
 * pipeline — plus the data-browser read face; protocol adapters must derive tool names and schemas from
 * these entries rather than maintaining an independent catalog.
 *
 * <p>The audit flag marks the operations that mutate persisted control-plane state (an artifact, a
 * connection's persisted probe or discovery result, a user, a token) and therefore leave a record;
 * read and list operations carry no audit flag.
 */
public final class ControlOperations {

    // Every operation is staged at the one stage this build ships at, on every face it is open on. The
    // stage is not part of these names on purpose: a name that spelled it would go stale the moment the
    // build moved on, and a per-face stage is what lets one face open a surface the rest do not.
    private static final Map<Frontend, Maturity> CLI_ONLY = Map.of(Frontend.CLI, Maturity.CURRENT);
    private static final Map<Frontend, Maturity> CLI_AND_MCP =
            Map.of(Frontend.CLI, Maturity.CURRENT, Frontend.MCP, Maturity.CURRENT);

    // system domain
    public static final Operation SYSTEM_VERSION = new Operation(
            "system.version", Scope.READ, false, ControlApiSchema.ref("system.version"),
            "Report which version of Tapstate this server is, plus the authoring grammar versions it "
                    + "accepts and the schema version of its system data. Ask before reasoning about "
                    + "what this server can do: a client and a server are installed by different paths "
                    + "and are often different builds.",
            CLI_AND_MCP);

    // artifact domain
    public static final Operation ARTIFACT_APPLY = mcp(
            "artifact.apply", Scope.WRITE, true,
            "Apply a complete tapstate/v1 workspace after validation and return per-resource change results.");
    public static final Operation ARTIFACT_VALIDATE = mcp(
            "artifact.validate", Scope.READ, false,
            "Validate a complete tapstate/v1 workspace without writing artifacts or audit records.");
    // The read every precondition-bearing write depends on. It is exposed alongside artifact.delete
    // rather than on the CLI alone because the removal demands a content hash a remote caller cannot
    // compute for itself; without this read on the same face, that verb is callable and unusable.
    public static final Operation ARTIFACT_GET = mcp(
            "artifact.get", Scope.READ, false,
            "Read one applied resource of any kind by id, as its canonical tapstate/v1 YAML plus the "
                    + "content hash of those exact bytes. Pass that hash back as the expectedContentHash "
                    + "of a removal, or as a per-resource precondition when applying an edit.");
    public static final Operation ARTIFACT_LIST = new Operation("artifact.list", Scope.READ, false, null, CLI_ONLY);
    // The removal verb, one path for every kind. It destroys a named resource for good, which no other
    // operation on this surface does, so its description says so plainly rather than leaving a caller to
    // infer it: a remote model reads this text and nothing else before deciding to call it.
    public static final Operation ARTIFACT_DELETE = mcp(
            "artifact.delete", Scope.WRITE, true,
            "Permanently remove one applied resource of any kind by id. This is not reversible and leaves "
                    + "no tombstone; restoring means applying the resource again. The expectedContentHash "
                    + "must be the hash of the version just read, and the removal is refused if the stored "
                    + "version has moved on, if another resource still references the id, or if the id is a "
                    + "pipeline that is not stopped.");

    // Source CRUD remains available to the authenticated REST face while its MCP projection is retired.
    // The draft operation is the only Source operation exposed to MCP.
    public static final Operation SOURCE_CREATE = new Operation(
            "source.create", Scope.WRITE, true, ControlApiSchema.ref("source.create"),
            "Create and persist one Source through the Server control API.", CLI_ONLY);
    public static final Operation SOURCE_DRAFT = mcp(
            "source.draft", Scope.READ, false,
            "Render canonical YAML for a Source with a known connector through the live connector contract."
                    + " This does not create an artifact or audit record.");
    public static final Operation SOURCE_LIST = new Operation(
            "source.list", Scope.READ, false, ControlApiSchema.ref("source.list"),
            "List Sources with secret-redacted config and configured-secret field names.", CLI_ONLY);
    public static final Operation SOURCE_GET = new Operation(
            "source.get", Scope.READ, false, ControlApiSchema.ref("source.get"),
            "Get one Source with secret-redacted config and configured-secret field names.", CLI_ONLY);
    public static final Operation SOURCE_UPDATE = new Operation(
            "source.update", Scope.WRITE, true, null,
            "Replace one Source through the Server control API.", CLI_ONLY);
    public static final Operation SOURCE_DELETE = new Operation(
            "source.delete", Scope.WRITE, true, null,
            "Delete one Source through the Server control API.", CLI_ONLY);

    // connection domain: each probing verb runs an external probe and persists its result for later query
    // and display, so it mutates persisted state (a write) and is audited; its read-back peer returns the
    // latest persisted result (or a 404 when the connection was never probed), mutates nothing, and is
    // read and unaudited. connection.test / connection.test-result answer "does it connect"; their pair
    // connection.discover-schema / connection.schema answer "what is inside" (the discovered source model).

    public static final Operation CONNECTION_TEST = mcp(
            "connection.test", Scope.WRITE, true,
            "Test a connector configuration and persist the latest test result for later reads.");
    public static final Operation CONNECTION_TEST_RESULT =
            mcp("connection.test-result", Scope.READ, false,
                    "Read the latest persisted connection test result.");
    public static final Operation CONNECTION_DISCOVER_SCHEMA =
            mcp("connection.discover-schema", Scope.WRITE, true,
                    "Discover source schema through a connector and persist the latest result.");
    public static final Operation CONNECTION_SCHEMA =
            mcp("connection.schema", Scope.READ, false,
                    "Read the latest persisted source schema discovered for a connection.");

    // connector domain: registering a connector artifact ingests executable connector code into the
    // distribution store, so it mutates persisted state (a write) and is audited. A remote caller hands
    // over the artifact bytes; the operation classloads and stores in the control process rather than
    // dispatching to the runtime, so it adds no member to the synchronous control-to-runtime whitelist.
    public static final Operation CONNECTOR_REGISTER =
    new Operation("connector.register", Scope.WRITE, true, null, CLI_ONLY);
    // connector.list reads the online catalog view — the bundled snapshot union the rows derived for
    // registered connectors — so a registered connector becomes visible without a restart. It reads
    // derived catalog state, mutates nothing, and needs no member on the synchronous control-to-runtime
    // whitelist; it is read-scoped and unaudited.
    public static final Operation CONNECTOR_LIST =
            mcp("connector.list", Scope.READ, false,
                    "List connectors currently visible to the online Tapstate Server.");
    public static final Operation CONNECTOR_GET =
            mcp("connector.get", Scope.READ, false,
                    "Get a connector's complete live config spec, content hash, origin, and runtime availability.");
    // Connector icons are authenticated assets. The web picker uses the anonymous projection, while this
    // authenticated peer remains available to any CLI or REST client that needs the protected asset.
    public static final Operation CONNECTOR_ICON = new Operation(
            "connector.icon", Scope.READ, false, null,
            "Read the registered connector icon asset.", CLI_ONLY);

    // data-browser domain: the read face over a declared source's own database — list its collections,
    // read one collection's rows, report one collection's size. All three read through to the connector
    // and persist nothing, not even the result, so they are read-scoped and unaudited. That is what
    // separates them from the two connection probes, which look similar but store what they found.
    //
    // Which database a read reaches follows from the source's own connection; no verb takes one, and the
    // find request has no field for one. The control plane's tables sit on the same server as the data,
    // so that confinement is the point of the shape rather than a simplification of it.
    //
    // All three are open on the MCP face as well, which costs nothing beyond the mark: the tool
    // catalog derives its names and schemas from these entries, so no adapter holds a second list.
    // What that face does need is a listing that says more than the names — a caller with no person
    // behind it decides what to read next from this answer alone.
    public static final Operation DATA_BROWSER_COLLECTIONS = mcp(
            "data-browser.collections", Scope.READ, false,
            "List the collections a declared Source's own database holds, each with what is known "
                    + "about it: the kind of collection, the fields discovery found, and whatever the "
                    + "workspace said about it. These are the collections the database actually holds, "
                    + "not the ones the workspace declared. A field list or a description that nobody "
                    + "answered is left out rather than sent empty.");
    public static final Operation DATA_BROWSER_FIND = mcp(
            "data-browser.find", Scope.READ, false,
            "Read rows from one collection of a declared Source's own database. A preview of the first "
                    + "rows, not a page: the read is one-shot, and there is no way to ask for the next "
                    + "ones. Filtering uses this API's own small vocabulary, not the database's query "
                    + "language.");
    public static final Operation DATA_BROWSER_STATS = mcp(
            "data-browser.stats", Scope.READ, false,
            "Report what the connector knows about the size of one collection of a declared Source's "
                    + "own database.");

    // cluster domain: topology is sensitive, so listing members is a registry operation (authenticated
    // like every other verb) rather than an anonymous endpoint — only the process-liveness probe stays
    // outside the registry. Reading topology mutates nothing, so it is read-scoped and unaudited.
    public static final Operation CLUSTER_MEMBERS = new Operation("cluster.members", Scope.READ, false, null, CLI_ONLY);

    // pipeline domain: static projection reads, conditional definition replacement, and the four lifecycle
    // verbs. Definition replacement is audited as an artifact write; each lifecycle verb writes the
    // pipeline's desired state (an intent the runtime later converges). There is no rewind verb — a re-dig
    // is stop then start composed at the surface.
    public static final Operation PIPELINE_LIST = new Operation(
            "pipeline.list", Scope.READ, false, null,
            "List static Pipeline artifacts with resolved Source summaries.", CLI_ONLY);
    public static final Operation PIPELINE_GET = new Operation(
            "pipeline.get", Scope.READ, false, null,
            "Get one static Pipeline artifact with resolved Source summaries.", CLI_ONLY);
    public static final Operation PIPELINE_LAYOUT_GET = new Operation(
            "pipeline.layout.get", Scope.READ, false, null,
            "Read editor-only node positions and viewport state for one Pipeline.", CLI_ONLY);
    public static final Operation PIPELINE_LAYOUT_UPDATE = new Operation(
            "pipeline.layout.update", Scope.WRITE, false, null,
            "Replace editor-only node positions and viewport state for one Pipeline.", CLI_ONLY);
    public static final Operation PIPELINE_CREATE = new Operation(
            "pipeline.create", Scope.WRITE, true, null,
            "Create one Pipeline definition after its referenced Sources and composition have been validated.",
            CLI_ONLY);
    public static final Operation PIPELINE_UPDATE = new Operation(
            "pipeline.update", Scope.WRITE, true, null,
            "Replace one Pipeline definition while its content hash precondition still matches.", CLI_ONLY);
    public static final Operation PIPELINE_START = mcp(
            "pipeline.start", Scope.WRITE, true,
            "Set a Pipeline's desired state to running after its workspace has been applied.");
    public static final Operation PIPELINE_STOP = mcp(
            "pipeline.stop", Scope.WRITE, true,
            "Set a Pipeline's desired state to stopped.");
    public static final Operation PIPELINE_PAUSE = new Operation("pipeline.pause", Scope.WRITE, true, null, CLI_ONLY);
    public static final Operation PIPELINE_RESUME = new Operation("pipeline.resume", Scope.WRITE, true, null, CLI_ONLY);

    // pipeline observation reads: the four read faces. status/metrics/snapshot are store-backed over the
    // per-pipeline observation doc (status = lifecycle state, metrics = open stat map, snapshot = per-table
    // load progress); logs tails the node-local process log output for the pipeline. Each reads and mutates
    // nothing, so all four are read-scoped and unaudited.
    public static final Operation PIPELINE_STATUS = mcp(
            "pipeline.status", Scope.READ, false,
            "Read a Pipeline's current lifecycle status.");
    public static final Operation PIPELINE_METRICS = mcp(
            "pipeline.metrics", Scope.READ, false,
            "Read the bounded metrics snapshot most recently published for a Pipeline.");
    public static final Operation PIPELINE_SNAPSHOT = mcp(
            "pipeline.snapshot", Scope.READ, false,
            "Read per-table snapshot progress most recently published for a Pipeline.");
    public static final Operation PIPELINE_LOGS = mcp(
            "pipeline.logs", Scope.READ, false,
            "Read the bounded, secret-redacted log tail for a Pipeline.");

    // security domain: all admin-scoped. The mutating ones are audited; the list queries are not.
    public static final Operation USER_CREATE = new Operation("user.create", Scope.ADMIN, true, null, CLI_ONLY);
    public static final Operation USER_PASSWD = new Operation("user.passwd", Scope.ADMIN, true, null, CLI_ONLY);
    public static final Operation USER_LIST = new Operation("user.list", Scope.ADMIN, false, null, CLI_ONLY);
    public static final Operation TOKEN_CREATE = new Operation("token.create", Scope.ADMIN, true, null, CLI_ONLY);
    public static final Operation TOKEN_REVOKE = new Operation("token.revoke", Scope.ADMIN, true, null, CLI_ONLY);
    public static final Operation TOKEN_LIST = new Operation("token.list", Scope.ADMIN, false, null, CLI_ONLY);

    private static final List<Operation> ALL = List.of(
            SYSTEM_VERSION,
            ARTIFACT_APPLY,
            ARTIFACT_VALIDATE,
            ARTIFACT_GET,
            ARTIFACT_LIST,
            ARTIFACT_DELETE,
            SOURCE_CREATE,
            SOURCE_DRAFT,
            SOURCE_LIST,
            SOURCE_GET,
            SOURCE_UPDATE,
            SOURCE_DELETE,
            CONNECTION_TEST,
            CONNECTION_TEST_RESULT,
            CONNECTION_DISCOVER_SCHEMA,
            CONNECTION_SCHEMA,
            CONNECTOR_REGISTER,
            CONNECTOR_LIST,
            CONNECTOR_GET,
            CONNECTOR_ICON,
            DATA_BROWSER_COLLECTIONS,
            DATA_BROWSER_FIND,
            DATA_BROWSER_STATS,
            CLUSTER_MEMBERS,
            PIPELINE_LIST,
            PIPELINE_GET,
            PIPELINE_LAYOUT_GET,
            PIPELINE_LAYOUT_UPDATE,
            PIPELINE_CREATE,
            PIPELINE_UPDATE,
            PIPELINE_START,
            PIPELINE_STOP,
            PIPELINE_PAUSE,
            PIPELINE_RESUME,
            PIPELINE_STATUS,
            PIPELINE_METRICS,
            PIPELINE_SNAPSHOT,
            PIPELINE_LOGS,
            USER_CREATE,
            USER_PASSWD,
            USER_LIST,
            TOKEN_CREATE,
            TOKEN_REVOKE,
            TOKEN_LIST);

    private ControlOperations() {
    }

    private static Operation mcp(String id, Scope scope, boolean audited, String description) {
        return new Operation(id, scope, audited, ControlApiSchema.ref(id), description, CLI_AND_MCP);
    }

    /** All canonical operations, in a stable declaration order. */
    public static List<Operation> all() {
        return ALL;
    }

    /** A registry built over the canonical operation set. */
    public static OperationRegistry registry() {
        return OperationRegistry.of(ALL);
    }
}
