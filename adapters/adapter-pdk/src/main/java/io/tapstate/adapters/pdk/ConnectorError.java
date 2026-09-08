package io.tapstate.adapters.pdk;

import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.core.common.Severity;

import java.util.Set;

/**
 * The {@code connector} domain's error codes: the first-party, diagnosable failures the PDK bridge
 * raises when it loads, level-gates, drives or projects a connector. These are user-facing — the
 * operator pointed the bridge at a connector jar that will not load, declared a class that is not a
 * connector, requires a newer PDK API than the bridge provides, or produced an event the codec could
 * not project.
 *
 * <p>These are two-segment {@code connector.<symbol>} codes and go through the full build-time gate
 * set like any other first-party domain. They are deliberately distinct from the reserved
 * {@code connector.<connector-id>.<symbol>} namespace a connector's OWN thrown codes occupy: those
 * carry a connector-id segment, are longer by construction, and are deduped at runtime rather than at
 * build time (they come from jars the build cannot see).
 *
 * <p>Programmer errors / invariant violations stay bare and are allowed to crash — they are not
 * laundered into a {@code connector.*} code that would hide the defect. {@code placeholders()} is the
 * named-argument contract: every throw site supplies a value for each name, and the build-time
 * placeholder gate checks the catalog templates against it.
 */
public enum ConnectorError implements TapstateErrorCode {

    /**
     * No artifact is registered in the distribution store for this connector id, so it resolves to
     * nothing and cannot be loaded. {@code connector} is the connector id that resolved to no artifact.
     */
    NOT_REGISTERED("connector.not-registered", Set.of("connector")),

    /**
     * A connector id resolves to more than one registered artifact — different artifact bytes
     * registered under one id — so which to load is ambiguous; selecting among connector versions is
     * not supported and a silent wrong-version load is never taken. {@code connector} is the connector
     * id; {@code artifacts} lists the competing content hashes.
     */
    AMBIGUOUS_REGISTRATION("connector.ambiguous-registration", Set.of("connector", "artifacts")),

    /**
     * A different artifact is being registered under a connector id that already has one — same id,
     * different bytes. A single active artifact is kept per id; selecting among versions is not
     * supported, so the incoming artifact is refused at register time rather than stored to blow up at
     * load. {@code connector} is the connector id; {@code existing} is the content hash already
     * registered; {@code incoming} is the content hash refused.
     */
    REGISTRATION_CONFLICT("connector.registration-conflict", Set.of("connector", "existing", "incoming")),

    /**
     * A runtime register names a connector outside the officially supported set, so it is refused
     * rather than stored. This is a support boundary, not a defect in the artifact: the set grows as
     * connectors are certified, and a refused artifact may well be perfectly functional.
     * {@code connector} is the id the artifact declared; {@code official} lists the ids supported
     * today, so the message states the boundary rather than leaving the caller to guess it.
     */
    NOT_OFFICIAL("connector.not-official", Set.of("connector", "official")),

    /**
     * The connector jar or its classpath could not be opened / linked. {@code connector} is the
     * connector id being loaded.
     */
    LOAD_FAILED("connector.load-failed", Set.of("connector")),

    /**
     * The declared entry class is not on the connector's classpath or is not a connector.
     * {@code connector} is the connector id; {@code class} is the class name that was looked up.
     */
    CLASS_NOT_FOUND("connector.class-not-found", Set.of("connector", "class")),

    /**
     * A registered artifact carries no connector entry class — no class it contains is annotated as a
     * connector. {@code artifact} names the artifact that was scanned. Self-scan raises this before a
     * connector id is known, so it is keyed by the artifact rather than by an id.
     */
    NO_CONNECTOR_CLASS("connector.no-connector-class", Set.of("artifact")),

    /**
     * A registered artifact carries more than one unrelated connector entry class, so which connector
     * it registers is ambiguous (variants that subclass a shared base are not: the most-derived wins).
     * {@code artifact} names the artifact; {@code classes} lists the competing entry classes.
     */
    AMBIGUOUS_CONNECTOR_CLASS("connector.ambiguous-connector-class", Set.of("artifact", "classes")),

    /**
     * A connector's {@code @TapConnectorClass} annotation names a spec resource the artifact does not
     * contain. {@code artifact} names the artifact; {@code spec} is the resource path the annotation
     * named.
     */
    SPEC_NOT_FOUND("connector.spec-not-found", Set.of("artifact", "spec")),

    /**
     * A connector's spec resource does not yield the connector's identity — it is not valid JSON, or
     * it declares no {@code properties.id}. Registration is keyed by that id, so the artifact is
     * refused. {@code artifact} names the artifact; {@code spec} is the spec resource path;
     * {@code detail} says what was wrong with it.
     */
    SPEC_INVALID("connector.spec-invalid", Set.of("artifact", "spec", "detail")),

    /**
     * The connector requires a newer PDK API level than the bridge provides, so it is refused rather
     * than silently downgraded. {@code connector} is the connector id; {@code required} is the level
     * it asked for; {@code provided} is the level the bridge provides.
     */
    API_LEVEL_INCOMPATIBLE("connector.api-level-incompatible", Set.of("connector", "required", "provided")),

    /**
     * The connector declares a PDK API version the Tapstate-side level registry has no row for, so the
     * bridge cannot place it on the compatibility axis and refuses it rather than guessing. This is a
     * Tapstate-side registry gap, not the connector being too new — the fix is to register the version if
     * it is API-compatible, or to use a build whose version the bridge recognizes. {@code connector} is
     * the connector id; {@code version} is the unrecognized declared PDK API version.
     */
    API_LEVEL_UNKNOWN("connector.api-level-unknown", Set.of("connector", "version")),

    /**
     * A PDK event could not be projected to or from the tapstate envelope. {@code connector} is the
     * connector id; {@code detail} is the projection failure.
     */
    PROJECTION_FAILED("connector.projection-failed", Set.of("connector", "detail")),

    /**
     * The connector failed while reading (snapshot or cdc). {@code connector} is the connector id;
     * {@code detail} is the failure the connector reported.
     */
    CAPTURE_FAILED("connector.capture-failed", Set.of("connector", "detail")),

    /**
     * The connector's own connection test could not be run to completion — the connector threw out of
     * {@code connectionTest} rather than reporting a failed check. A reported failed check is a normal
     * FAILED result, not this code. {@code connector} is the connector id; {@code detail} is the failure
     * the connector reported.
     */
    TEST_FAILED("connector.test-failed", Set.of("connector", "detail")),

    /**
     * The connector's schema discovery could not be run to completion — the connector threw out of
     * {@code discoverSchema}. {@code connector} is the connector id; {@code detail} is the failure the
     * connector reported.
     */
    DISCOVER_FAILED("connector.discover-failed", Set.of("connector", "detail")),

    /**
     * The connector failed while writing a batch. {@code connector} is the connector id;
     * {@code detail} is the failure the connector reported.
     */
    WRITE_FAILED("connector.write-failed", Set.of("connector", "detail")),

    /**
     * The connector failed while reading the store — it threw out of the read, or reported the failure
     * through the result it handed back. {@code connector} is the connector id; {@code detail} is the
     * failure the connector reported. Distinct from a capture read: that one is a pipeline moving data,
     * this one is a user looking at it, and the two fail for different reasons and reach different
     * people.
     */
    READ_FAILED("connector.read-failed", Set.of("connector", "detail")),

    /**
     * The connector gave up part way through a read and returned as if it had not. {@code connector} is
     * the connector id. A read loop asks whether it is still alive between batches and, when it is not,
     * returns without throwing and without reporting — dropping the rows it had already gathered. That
     * leaves a short answer no other signal contradicts, and a short answer is indistinguishable from a
     * small collection, so it would otherwise be handed to a caller as a complete one.
     */
    READ_ABANDONED("connector.read-abandoned", Set.of("connector")),

    /**
     * The connector registers no function for the capability this read needs, so the read cannot be
     * served at all. {@code connector} is the connector id; {@code capability} names the function that
     * is absent. This is a user-facing refusal rather than an invariant violation: which connectors can
     * serve a read is a property of the connector, not something the request was validated against, so
     * a user can reach it by asking to read a source whose connector does not offer it.
     */
    CAPABILITY_MISSING("connector.capability-missing", Set.of("connector", "capability")),

    /**
     * Every live instance of this connection stayed busy for the whole wait, so the read was refused
     * rather than queued behind a query that may never end. {@code connector} is the connector id;
     * {@code timeout} is how long the read waited. A caller that sees this repeatedly is contending
     * with other reads of the same connection, not with a broken one.
     */
    INSTANCES_BUSY("connector.instances-busy", Set.of("connector", "timeout")),

    /**
     * The host already holds as many live connector instances as it allows and every one of them is
     * busy, so none could be closed to make room. {@code limit} is the ceiling that was reached. The
     * ceiling is counted in instances because each one carries its connector's own connection pool;
     * this is a host-wide condition rather than a property of the connection being read.
     */
    INSTANCE_LIMIT_REACHED("connector.instance-limit-reached", Set.of("limit")),

    /**
     * The read ran longer than the read face allows and was abandoned. {@code connector} is the
     * connector id; {@code timeout} is how long it was given. The instance it ran on is closed rather
     * than reused, because the abandoned read is still inside it.
     */
    READ_TIMEOUT("connector.read-timeout", Set.of("connector", "timeout")),

    /**
     * A connector's own stream position could not be written down, so this source has no position to
     * record and nothing a later run could resume from. {@code connector} is the connector id;
     * {@code detail} is why the position could not be rendered. Reporting no position instead would
     * claim the source has nowhere to resume from, which is a different and untrue statement.
     */
    POSITION_UNRENDERABLE("connector.position-unrenderable", Set.of("connector", "detail")),

    /**
     * A recorded position could not be read back into something this connector accepts — most often its
     * position type has changed shape since the position was written. {@code connector} is the connector
     * id; {@code detail} is why it could not be read. Refusing is deliberate: starting from the present
     * instead would silently drop every change made since the position was recorded.
     */
    POSITION_UNREADABLE("connector.position-unreadable", Set.of("connector", "detail"));

    private final String code;
    private final Set<String> placeholders;

    ConnectorError(String code, Set<String> placeholders) {
        this.code = code;
        this.placeholders = placeholders;
    }

    @Override
    public String code() {
        return code;
    }

    @Override
    public Severity severity() {
        return Severity.ERROR;
    }

    @Override
    public Set<String> placeholders() {
        return placeholders;
    }
}
