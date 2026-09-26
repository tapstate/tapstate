package io.tapstate.app;

import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.core.common.Severity;

import java.util.Set;

/**
 * The {@code actuation} domain's error codes: the assembly root failing to resolve a pipeline's runnable
 * topology from its stored artifact when a start actuates it. A desired-to-run pipeline whose artifact is
 * absent, or an id that names a resource of another kind, is a user-facing, diagnosable failure carried
 * through the error-code system and rendered through the shared message catalog - distinct from the
 * {@code engine} domain, which polices operating the Jet job once the topology is built.
 *
 * <p>{@code placeholders()} is the named-argument contract: every throw site supplies a value for each
 * name, and the build-time placeholder gate checks the catalog templates against it.
 */
enum ActuationError implements TapstateErrorCode {

    /** A start named a pipeline id with no stored artifact to run: {@code pipeline} is the id given. */
    PIPELINE_NOT_FOUND("actuation.pipeline-not-found", Set.of("pipeline")),

    /**
     * A start named an id that resolves to a resource of another kind: {@code pipeline} is the id given and
     * {@code kind} is the kind actually stored under it.
     */
    NOT_A_PIPELINE("actuation.not-a-pipeline", Set.of("pipeline", "kind")),

    /** A source omitted discovery required to expand its table selection; {@code source} is its id. */
    SOURCE_SCHEMA_NOT_DISCOVERED("actuation.source-schema-not-discovered", Set.of("source")),

    /** A source reference names no discovered table; {@code source} and {@code table} identify it. */
    SOURCE_TABLE_NOT_DISCOVERED("actuation.source-table-not-discovered", Set.of("source", "table")),

    /** A source selector set expands to no tables; {@code source} is its id. */
    SOURCE_TABLE_SELECTION_EMPTY("actuation.source-table-selection-empty", Set.of("source")),

    /** A source table selector is not valid Java regex syntax; {@code source} and {@code regex} carry the input. */
    SOURCE_TABLE_REGEX_INVALID("actuation.source-table-regex-invalid", Set.of("source", "regex")),

    /** A bare table name is selected by several sources; {@code sources} lists the conflicting source ids. */
    UNWIND_PARENT_KEY_UNRESOLVED("actuation.unwind-parent-key-unresolved", Set.of("step", "stream", "reason")),
    SOURCE_TABLE_AMBIGUOUS("actuation.source-table-ambiguous", Set.of("table", "sources")),

    /** A table object carries settings the current capture path does not implement; fields lists their names. */
    SOURCE_TABLE_SPEC_UNSUPPORTED("actuation.source-table-spec-unsupported", Set.of("source", "table", "fields")),

    /**
     * A join's driving source declares no key, so nothing identifies the row a change is about;
     * {@code step} is the join step and {@code table} the table it is driven from. Every row a join
     * mirrors and every entry in its reverse index is filed under that key, so without one two
     * different rows land in one entry - which is not an error anywhere, it simply builds the wide row
     * out of whichever of them was written last.
     */
    JOIN_SOURCE_KEY_MISSING("actuation.join-source-key-missing", Set.of("step", "table")),

    /**
     * A join's SQL names a source by the table it stands for, where the step declared an alias for that
     * table; {@code step} is the join step and {@code name} the spelling the SQL used. Both spellings
     * are legal SQL here and both derive, but only a declared alias reaches the topology - the vertex
     * wiring resolves each source through the step's own from-map - so this one has nothing behind it.
     * Refused while the SQL is being read, because the failure it reaches otherwise is an internal one
     * at start, naming a concept the author never wrote.
     */
    JOIN_SOURCE_NOT_DECLARED("actuation.join-source-not-declared", Set.of("step", "name")),

    /**
     * A join's {@code from:} map names another step of the pipeline rather than a source table;
     * {@code step} is the join, {@code alias} the alias and {@code ref} the step it names. Validation
     * refuses this shape, so only a pipeline stored before it did reaches here. A step's output has no
     * discovered columns and no key, so the SQL resolves none of its columns and the start could only
     * fail the same way on every attempt.
     */
    JOIN_INPUT_NOT_A_TABLE("actuation.join-input-not-a-table", Set.of("step", "alias", "ref")),

    /**
     * A join's SQL cannot be compiled against the discovered columns of the tables it reads; {@code step}
     * is the join and {@code detail} the front end's diagnosis, which names what it could not resolve or
     * run. Validation only parses the SQL, because the columns are known only once the sources are
     * discovered, so a statement naming a column no source has - or one a source has since dropped - is
     * first caught at start, and fails the same way at every start until one of the two changes.
     */
    JOIN_SQL_INVALID("actuation.join-sql-invalid", Set.of("step", "detail")),

    /**
     * A join's SELECT does not publish the driving table's key, so nothing identifies a result row;
     * {@code step} is the join step, {@code table} the driving table and {@code column} the key column
     * missing from the projection. A target keyed on anything less collapses rows the SQL says are
     * distinct, and the collapse is invisible: the write succeeds and the target holds fewer rows than
     * it should with no error anywhere. A column reaching the output only through an expression does
     * not publish it - the value is a function of the key, and a function need not be injective.
     */
    JOIN_OUTPUT_KEY_NOT_PUBLISHED("actuation.join-output-key-not-published",
            Set.of("step", "table", "column")),

    /**
     * A join's output columns no longer match the ones it was recorded producing, and its sources are
     * what moved: {@code pipeline} and {@code step} name the join, and {@code added} / {@code removed} /
     * {@code retyped} carry the difference. Ordinary in a change-data product - a column widened, a
     * type changed - and the operator's to rule on, which is why it is told apart from the same
     * difference arriving for our reasons ({@link #JOIN_OUTPUT_SCHEMA_ENGINE_CHANGED}). Refused rather
     * than written through: the target was built for the recorded shape, so the writes succeed and
     * whatever no longer fits is truncated or rounded with nothing reporting it.
     */
    JOIN_OUTPUT_SCHEMA_SOURCE_CHANGED("actuation.join-output-schema-source-changed",
            Set.of("pipeline", "step", "added", "removed", "retyped")),

    /**
     * A join's output columns no longer match the ones it was recorded producing, and neither the query
     * nor the source columns moved - so what changed is how we work them out: {@code pipeline} and
     * {@code step} name the join, {@code added} / {@code removed} / {@code retyped} carry the
     * difference, and {@code recordedBy} / {@code nowBy} name the derivation on each side. This is our
     * compatibility break rather than the operator's, and it should have been caught by the derivation
     * goldens long before it reached anybody; reaching a user at all means one of them is missing the
     * shape that moved.
     */
    JOIN_OUTPUT_SCHEMA_ENGINE_CHANGED("actuation.join-output-schema-engine-changed",
            Set.of("pipeline", "step", "added", "removed", "retyped", "recordedBy", "nowBy")),

    /** A serve.from regex is invalid; {@code regex} carries the expression. */
    FROM_REGEX_INVALID("actuation.from-regex-invalid", Set.of("regex")),

    /** A serve.from regex matches no upstream vertex; {@code regex} carries the expression. */
    FROM_REGEX_EMPTY("actuation.from-regex-empty", Set.of("regex")),

    /**
     * A view declares no key, so nothing identifies the record a change updates; {@code view} is its id.
     * Materializing without one would append a copy per change rather than converge on the record.
     */
    VIEW_KEY_MISSING("actuation.view-key-missing", Set.of("view")),

    /**
     * A view declares a storage tier this release does not materialize; {@code view} is its id and
     * {@code tier} names the tier. Refused rather than ignored: a silently dropped tier reads as working.
     */
    VIEW_STORAGE_TIER_UNSUPPORTED("actuation.view-storage-tier-unsupported", Set.of("view", "tier")),

    /**
     * A pipeline declares a view but the managed state store it materializes into is not configured;
     * {@code store} is the source id expected to supply it.
     */
    VIEW_STORE_NOT_CONFIGURED("actuation.view-store-not-configured", Set.of("store")),

    /**
     * A pipeline declares a view and the managed state store it materializes into is configured but does
     * not answer; {@code store} is the source id and {@code reason} what the probe reported. Distinct
     * from {@link #VIEW_STORE_NOT_CONFIGURED} on purpose: that one says nobody set the store up, this one
     * says it is set up and unreachable, and the two send an operator to different places.
     */
    VIEW_STORE_UNREACHABLE("actuation.view-store-unreachable", Set.of("store", "reason")),

    /**
     * A view's declared key is not a unique identity of what feeds it; {@code view} is its id,
     * {@code key} the view's key, and {@code identity} an identity the feed does declare.
     * The sink upserts on the view's key and indexes it uniquely, so records that differ only on the
     * columns the view's key leaves out would silently replace each other. Refused where the pipeline
     * is built, because at write time the loss is invisible: right collection, right count on any
     * single snapshot. A discovered primary key is only a default and does not override a different
     * explicitly selected identity when discovery records that identity as unique too.
     */
    VIEW_KEY_NOT_FEED_IDENTITY("actuation.view-key-not-feed-identity", Set.of("view", "key", "identity")),

    /**
     * A view is fed by more than one stream with no assembly collapsing them; {@code view} is its id,
     * {@code tables} the streams. Every stream is upserted into the one collection on the one view
     * key, so rows from different tables sharing a key value would take turns overwriting the same
     * document.
     */
    VIEW_FED_BY_MANY_TABLES("actuation.view-fed-by-many-tables", Set.of("view", "tables")),

    /**
     * The resource resolved under the managed state store's id declares capture settings, so it is an
     * authored source rather than the deployment's store; {@code store} is the id. Refused rather than
     * written into: the store is resolved by its id alone, and materializing a view into a database an
     * author is capturing from writes into one the deployment does not own.
     */
    VIEW_STORE_IS_A_CAPTURE_SOURCE("actuation.view-store-is-a-capture-source", Set.of("store")),

    /**
     * A model refresh was requested before the pipeline was at rest. Both actual and desired states
     * matter: an actual run may still be stopping, or a new run may already have been requested.
     * A paused run also retains its assembly, which a resume with an unchanged artifact may reuse.
     */
    SCHEMA_SYNC_WHILE_RUNNING("actuation.schema-sync-while-running",
            Set.of("pipeline", "state", "desired")),

    /**
     * A node's cluster-wide target has no width the members of this run can take within every budget:
     * {@code node} is the node, {@code requested} the target, {@code members} the members taking part and
     * {@code candidates} each per-member count tried with the first budget it broke. Refused before anything
     * starts, because running over a budget and running at a width nobody asked for are both worse than not
     * running.
     */
    NO_SAFE_PARALLELISM("actuation.no-safe-parallelism",
            Set.of("pipeline", "node", "requested", "members", "candidates")),

    /**
     * A node was explicitly asked to run wider than one processor, and it can only run as one: {@code node}
     * is the node, {@code requested} the target and {@code reason} why - a stream reaching it carries no key
     * to route it by, or every row it writes lands in one target table that has none. With no key, two
     * processors would apply one row's changes in an order nobody decides.
     */
    PARALLELISM_NEEDS_A_KEY("actuation.parallelism-needs-a-key",
            Set.of("pipeline", "node", "requested", "reason")),

    /**
     * A member a run would take part on cannot load a connector the pipeline's sinks open: {@code pipeline} is
     * the pipeline, {@code member} the member by stable id, {@code connector} the connector id and {@code reason}
     * what the member answered - its own coded refusal, the failure it hit, or that it did not answer in time.
     * Every member is asked before the run starts anything, because a member that finds out only as its sink
     * opens fails a run that is already reading, and the reason stays on that member.
     */
    CONNECTOR_UNAVAILABLE_ON_MEMBER("actuation.connector-unavailable-on-member",
            Set.of("pipeline", "member", "connector", "reason")),

    /**
     * The members a run would take part on load different artifacts for one connector: {@code pipeline} is the
     * pipeline, {@code connector} the connector id and {@code artifacts} the content hash each member loaded. The
     * writers of one sink would run different code - members reading different registries, or a registration
     * replaced while the run was starting.
     */
    CONNECTOR_DIFFERS_ACROSS_MEMBERS("actuation.connector-differs-across-members",
            Set.of("pipeline", "connector", "artifacts"));

    private final String code;
    private final Set<String> placeholders;

    ActuationError(String code, Set<String> placeholders) {
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
