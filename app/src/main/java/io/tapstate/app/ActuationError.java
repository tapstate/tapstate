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
     * A view's declared key is not the identity of what feeds it; {@code view} is its id, {@code key}
     * the view's key, {@code identity} the feed's - a nest's root key, or a table's discovered key.
     * The sink upserts on the view's key and indexes it uniquely, so records that differ only on the
     * columns the view's key leaves out would silently replace each other. Refused where the pipeline
     * is built, because at write time the loss is invisible: right collection, right count on any
     * single snapshot. A feed with no identity on record - an undiscovered table - is not held to
     * this; there the view's key is the only identity there is.
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
    VIEW_STORE_IS_A_CAPTURE_SOURCE("actuation.view-store-is-a-capture-source", Set.of("store"));

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
