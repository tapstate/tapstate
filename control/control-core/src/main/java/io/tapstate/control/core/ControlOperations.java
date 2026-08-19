package io.tapstate.control.core;

import java.util.List;
import java.util.Map;

/**
 * The canonical set of control operations, declared as code constants (no classpath or reflection
 * scanning). This is the single source of truth from which every face derives its surface, and the
 * seed an {@link OperationRegistry} is built over.
 *
 * <p>Each face reads its exposed operations from this registry. The MCP surface at {@code BETA} is the
 * online authoring closure — reading the catalog, drafting and applying a workspace, removing one, and
 * driving a pipeline; protocol adapters must derive tool names and schemas from these entries rather
 * than maintaining an independent catalog.
 *
 * <p>The audit flag marks the operations that mutate persisted control-plane state (an artifact, a
 * connection's persisted probe or discovery result, a user, a token) and therefore leave a record;
 * read and list operations carry no audit flag.
 */
public final class ControlOperations {

    private static final Map<Frontend, Maturity> CLI_POC = Map.of(Frontend.CLI, Maturity.POC);
    private static final Map<Frontend, Maturity> CLI_POC_MCP_BETA =
            Map.of(Frontend.CLI, Maturity.POC, Frontend.MCP, Maturity.BETA);

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
    public static final Operation ARTIFACT_LIST = new Operation("artifact.list", Scope.READ, false, null, CLI_POC);
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
            "Create and persist one Source through the Server control API.", CLI_POC);
    public static final Operation SOURCE_DRAFT = mcp(
            "source.draft", Scope.READ, false,
            "Render canonical YAML for a Source with a known connector through the live connector contract."
                    + " This does not create an artifact or audit record.");
    public static final Operation SOURCE_LIST = new Operation(
            "source.list", Scope.READ, false, ControlApiSchema.ref("source.list"),
            "List Sources with secret-redacted config and configured-secret field names.", CLI_POC);
    public static final Operation SOURCE_GET = new Operation(
            "source.get", Scope.READ, false, ControlApiSchema.ref("source.get"),
            "Get one Source with secret-redacted config and configured-secret field names.", CLI_POC);
    public static final Operation SOURCE_UPDATE = new Operation(
            "source.update", Scope.WRITE, true, null,
            "Replace one Source through the Server control API.", CLI_POC);
    public static final Operation SOURCE_DELETE = new Operation(
            "source.delete", Scope.WRITE, true, null,
            "Delete one Source through the Server control API.", CLI_POC);

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
            new Operation("connector.register", Scope.WRITE, true, null, CLI_POC);
    // connector.list reads registered authoring candidates from the online catalog view, so a newly
    // registered connector becomes visible without a restart. It reads derived catalog state, mutates
    // nothing, and needs no member on the synchronous control-to-runtime whitelist; it is read-scoped and
    // unaudited.
    public static final Operation CONNECTOR_LIST =
            mcp("connector.list", Scope.READ, false,
                    "List connectors currently visible to the online Tapstate Server.");
    public static final Operation CONNECTOR_GET =
            mcp("connector.get", Scope.READ, false,
                    "Get a connector's complete live config spec, content hash, origin, and runtime availability.");

    // cluster domain: topology is sensitive, so listing members is a registry operation (authenticated
    // like every other verb) rather than an anonymous endpoint — only the process-liveness probe stays
    // outside the registry. Reading topology mutates nothing, so it is read-scoped and unaudited.
    public static final Operation CLUSTER_MEMBERS = new Operation("cluster.members", Scope.READ, false, null, CLI_POC);

    // pipeline domain: the four lifecycle verbs. Each writes the pipeline's desired state (an intent the
    // runtime later converges), so all four are write-scoped and audited. There is no rewind verb — a
    // re-dig is stop then start composed at the surface.
    public static final Operation PIPELINE_START = mcp(
            "pipeline.start", Scope.WRITE, true,
            "Set a Pipeline's desired state to running after its workspace has been applied.");
    public static final Operation PIPELINE_STOP = mcp(
            "pipeline.stop", Scope.WRITE, true,
            "Set a Pipeline's desired state to stopped.");
    public static final Operation PIPELINE_PAUSE = new Operation("pipeline.pause", Scope.WRITE, true, null, CLI_POC);
    public static final Operation PIPELINE_RESUME = new Operation("pipeline.resume", Scope.WRITE, true, null, CLI_POC);

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
    public static final Operation USER_CREATE = new Operation("user.create", Scope.ADMIN, true, null, CLI_POC);
    public static final Operation USER_PASSWD = new Operation("user.passwd", Scope.ADMIN, true, null, CLI_POC);
    public static final Operation USER_LIST = new Operation("user.list", Scope.ADMIN, false, null, CLI_POC);
    public static final Operation TOKEN_CREATE = new Operation("token.create", Scope.ADMIN, true, null, CLI_POC);
    public static final Operation TOKEN_REVOKE = new Operation("token.revoke", Scope.ADMIN, true, null, CLI_POC);
    public static final Operation TOKEN_LIST = new Operation("token.list", Scope.ADMIN, false, null, CLI_POC);

    private static final List<Operation> ALL = List.of(
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
            CLUSTER_MEMBERS,
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
        return new Operation(id, scope, audited, ControlApiSchema.ref(id), description, CLI_POC_MCP_BETA);
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
