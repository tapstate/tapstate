package io.tapstate.app;

import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.core.common.Severity;
import io.tapstate.core.common.ProductErrorDefinition;
import io.tapstate.core.common.ActuationErrorDefinitions;

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

    /** A bounded Pipeline preview could not be completed without weakening its read or size guarantees. */
    PREVIEW_REFUSED(ActuationErrorDefinitions.PREVIEW_REFUSED),

    /** A Map removed a field required to match rows during bounded preview. */
    PREVIEW_ASSOCIATION_KEY_DROPPED(ActuationErrorDefinitions.PREVIEW_ASSOCIATION_KEY_DROPPED),

    /** A start named a pipeline id with no stored artifact to run: {@code pipeline} is the id given. */
    PIPELINE_NOT_FOUND(ActuationErrorDefinitions.PIPELINE_NOT_FOUND),

    /**
     * A start named an id that resolves to a resource of another kind: {@code pipeline} is the id given and
     * {@code kind} is the kind actually stored under it.
     */
    NOT_A_PIPELINE(ActuationErrorDefinitions.NOT_A_PIPELINE),

    /** A source omitted discovery required to expand its table selection; {@code source} is its id. */
    SOURCE_SCHEMA_NOT_DISCOVERED(ActuationErrorDefinitions.SOURCE_SCHEMA_NOT_DISCOVERED),

    /** A source reference names no discovered table; {@code source} and {@code table} identify it. */
    SOURCE_TABLE_NOT_DISCOVERED(ActuationErrorDefinitions.SOURCE_TABLE_NOT_DISCOVERED),

    /** A source selector set expands to no tables; {@code source} is its id. */
    SOURCE_TABLE_SELECTION_EMPTY(ActuationErrorDefinitions.SOURCE_TABLE_SELECTION_EMPTY),

    /** A source table selector is not valid Java regex syntax; {@code source} and {@code regex} carry the input. */
    SOURCE_TABLE_REGEX_INVALID(ActuationErrorDefinitions.SOURCE_TABLE_REGEX_INVALID),

    /** A bare table name is selected by several sources; {@code sources} lists the conflicting source ids. */
    UNWIND_PARENT_KEY_UNRESOLVED(ActuationErrorDefinitions.UNWIND_PARENT_KEY_UNRESOLVED),
    SOURCE_TABLE_AMBIGUOUS(ActuationErrorDefinitions.SOURCE_TABLE_AMBIGUOUS),

    /** A table object carries settings the current capture path does not implement; fields lists their names. */
    SOURCE_TABLE_SPEC_UNSUPPORTED(ActuationErrorDefinitions.SOURCE_TABLE_SPEC_UNSUPPORTED),

    /**
     * A join's driving source declares no key, so nothing identifies the row a change is about;
     * {@code step} is the join step and {@code table} the table it is driven from. Every row a join
     * mirrors and every entry in its reverse index is filed under that key, so without one two
     * different rows land in one entry - which is not an error anywhere, it simply builds the wide row
     * out of whichever of them was written last.
     */
    JOIN_SOURCE_KEY_MISSING(ActuationErrorDefinitions.JOIN_SOURCE_KEY_MISSING),

    /**
     * A join's SQL names a source by the table it stands for, where the step declared an alias for that
     * table; {@code step} is the join step and {@code name} the spelling the SQL used. Both spellings
     * are legal SQL here and both derive, but only a declared alias reaches the topology - the vertex
     * wiring resolves each source through the step's own from-map - so this one has nothing behind it.
     * Refused while the SQL is being read, because the failure it reaches otherwise is an internal one
     * at start, naming a concept the author never wrote.
     */
    JOIN_SOURCE_NOT_DECLARED(ActuationErrorDefinitions.JOIN_SOURCE_NOT_DECLARED),

    /**
     * A join's {@code from:} map names another step of the pipeline rather than a source table;
     * {@code step} is the join, {@code alias} the alias and {@code ref} the step it names. Validation
     * refuses this shape, so only a pipeline stored before it did reaches here. A step's output has no
     * discovered columns and no key, so the SQL resolves none of its columns and the start could only
     * fail the same way on every attempt.
     */
    JOIN_INPUT_NOT_A_TABLE(ActuationErrorDefinitions.JOIN_INPUT_NOT_A_TABLE),

    /**
     * A join's SQL cannot be compiled against the discovered columns of the tables it reads; {@code step}
     * is the join and {@code detail} the front end's diagnosis, which names what it could not resolve or
     * run. Validation only parses the SQL, because the columns are known only once the sources are
     * discovered, so a statement naming a column no source has - or one a source has since dropped - is
     * first caught at start, and fails the same way at every start until one of the two changes.
     */
    JOIN_SQL_INVALID(ActuationErrorDefinitions.JOIN_SQL_INVALID),

    /**
     * A join's SELECT does not publish the driving table's key, so nothing identifies a result row;
     * {@code step} is the join step, {@code table} the driving table and {@code column} the key column
     * missing from the projection. A target keyed on anything less collapses rows the SQL says are
     * distinct, and the collapse is invisible: the write succeeds and the target holds fewer rows than
     * it should with no error anywhere. A column reaching the output only through an expression does
     * not publish it - the value is a function of the key, and a function need not be injective.
     */
    JOIN_OUTPUT_KEY_NOT_PUBLISHED(ActuationErrorDefinitions.JOIN_OUTPUT_KEY_NOT_PUBLISHED),

    /**
     * A join's output columns no longer match the ones it was recorded producing, and its sources are
     * what moved: {@code pipeline} and {@code step} name the join, and {@code added} / {@code removed} /
     * {@code retyped} carry the difference. Ordinary in a change-data product - a column widened, a
     * type changed - and the operator's to rule on, which is why it is told apart from the same
     * difference arriving for our reasons ({@link #JOIN_OUTPUT_SCHEMA_ENGINE_CHANGED}). Refused rather
     * than written through: the target was built for the recorded shape, so the writes succeed and
     * whatever no longer fits is truncated or rounded with nothing reporting it.
     */
    JOIN_OUTPUT_SCHEMA_SOURCE_CHANGED(ActuationErrorDefinitions.JOIN_OUTPUT_SCHEMA_SOURCE_CHANGED),

    /**
     * A join's output columns no longer match the ones it was recorded producing, and neither the query
     * nor the source columns moved - so what changed is how we work them out: {@code pipeline} and
     * {@code step} name the join, {@code added} / {@code removed} / {@code retyped} carry the
     * difference, and {@code recordedBy} / {@code nowBy} name the derivation on each side. This is our
     * compatibility break rather than the operator's, and it should have been caught by the derivation
     * goldens long before it reached anybody; reaching a user at all means one of them is missing the
     * shape that moved.
     */
    JOIN_OUTPUT_SCHEMA_ENGINE_CHANGED(ActuationErrorDefinitions.JOIN_OUTPUT_SCHEMA_ENGINE_CHANGED),

    /** A serve.from regex is invalid; {@code regex} carries the expression. */
    FROM_REGEX_INVALID(ActuationErrorDefinitions.FROM_REGEX_INVALID),

    /** A serve.from regex matches no upstream vertex; {@code regex} carries the expression. */
    FROM_REGEX_EMPTY(ActuationErrorDefinitions.FROM_REGEX_EMPTY),

    /**
     * A view declares no key, so nothing identifies the record a change updates; {@code view} is its id.
     * Materializing without one would append a copy per change rather than converge on the record.
     */
    VIEW_KEY_MISSING(ActuationErrorDefinitions.VIEW_KEY_MISSING),

    /**
     * A view declares a storage tier this release does not materialize; {@code view} is its id and
     * {@code tier} names the tier. Refused rather than ignored: a silently dropped tier reads as working.
     */
    VIEW_STORAGE_TIER_UNSUPPORTED(ActuationErrorDefinitions.VIEW_STORAGE_TIER_UNSUPPORTED),

    /**
     * A pipeline declares a view but the managed state store it materializes into is not configured;
     * {@code store} is the source id expected to supply it.
     */
    VIEW_STORE_NOT_CONFIGURED(ActuationErrorDefinitions.VIEW_STORE_NOT_CONFIGURED),

    /**
     * A pipeline declares a view and the managed state store it materializes into is configured but does
     * not answer; {@code store} is the source id and {@code reason} what the probe reported. Distinct
     * from {@link #VIEW_STORE_NOT_CONFIGURED} on purpose: that one says nobody set the store up, this one
     * says it is set up and unreachable, and the two send an operator to different places.
     */
    VIEW_STORE_UNREACHABLE(ActuationErrorDefinitions.VIEW_STORE_UNREACHABLE),

    /**
     * A view's declared key is not a unique identity of what feeds it; {@code view} is its id,
     * {@code key} the view's key, and {@code identity} an identity the feed does declare.
     * The sink upserts on the view's key and indexes it uniquely, so records that differ only on the
     * columns the view's key leaves out would silently replace each other. Refused where the pipeline
     * is built, because at write time the loss is invisible: right collection, right count on any
     * single snapshot. A discovered primary key is only a default and does not override a different
     * explicitly selected identity when discovery records that identity as unique too.
     */
    VIEW_KEY_NOT_FEED_IDENTITY(ActuationErrorDefinitions.VIEW_KEY_NOT_FEED_IDENTITY),

    /**
     * A view is fed by more than one stream with no assembly collapsing them; {@code view} is its id,
     * {@code tables} the streams. Every stream is upserted into the one collection on the one view
     * key, so rows from different tables sharing a key value would take turns overwriting the same
     * document.
     */
    VIEW_FED_BY_MANY_TABLES(ActuationErrorDefinitions.VIEW_FED_BY_MANY_TABLES),

    /**
     * The resource resolved under the managed state store's id declares capture settings, so it is an
     * authored source rather than the deployment's store; {@code store} is the id. Refused rather than
     * written into: the store is resolved by its id alone, and materializing a view into a database an
     * author is capturing from writes into one the deployment does not own.
     */
    VIEW_STORE_IS_A_CAPTURE_SOURCE(ActuationErrorDefinitions.VIEW_STORE_IS_A_CAPTURE_SOURCE),

    /**
     * A model refresh was requested before the pipeline was at rest. Both actual and desired states
     * matter: an actual run may still be stopping, or a new run may already have been requested.
     * A paused run also retains its assembly, which a resume with an unchanged artifact may reuse.
     */
    SCHEMA_SYNC_WHILE_RUNNING(ActuationErrorDefinitions.SCHEMA_SYNC_WHILE_RUNNING),

    /**
     * A node's cluster-wide target has no width the members of this run can take within every budget:
     * {@code node} is the node, {@code requested} the target, {@code members} the members taking part and
     * {@code candidates} each per-member count tried with the first budget it broke. Refused before anything
     * starts, because running over a budget and running at a width nobody asked for are both worse than not
     * running.
     */
    NO_SAFE_PARALLELISM(ActuationErrorDefinitions.NO_SAFE_PARALLELISM),

    /**
     * A node was explicitly asked to run wider than one processor, and it can only run as one: {@code node}
     * is the node, {@code requested} the target and {@code reason} why - a stream reaching it carries no key
     * to route it by, or every row it writes lands in one target table that has none. With no key, two
     * processors would apply one row's changes in an order nobody decides.
     */
    PARALLELISM_NEEDS_A_KEY(ActuationErrorDefinitions.PARALLELISM_NEEDS_A_KEY),

    /**
     * Rows of one stream would reach one sink two ways that land them differently: {@code stream} reaches it as
     * the documents of {@code nest} - a nest with nothing to assemble, whose rows keep their own stream - and as
     * {@code other}, the stream itself or another such nest, and the two land it in different tables or match it
     * on different keys. A sink names one target per stream, so the rows of one way would be routed and matched
     * on a key they were not given.
     */
    STREAM_LANDS_TWO_WAYS(ActuationErrorDefinitions.STREAM_LANDS_TWO_WAYS),

    /**
     * A sync that matches writes on a key is fed a stream whose published target has no key:
     * {@code pipeline} is the pipeline and {@code stream} the stream. The stream's source table declared
     * a key and the target cannot carry it - a projection dropped or replaced a component of it - so
     * nothing identifies the row a later change is about. Refused where the pipeline is built, because
     * at write time the loss is invisible: the rows arrive, the table fills, and a run that looks like
     * it converged has appended instead. Append mode is not judged, because it never matches a write to
     * an existing row.
     */
    SYNC_OUTPUT_IDENTITY_UNRESOLVED(ActuationErrorDefinitions.SYNC_OUTPUT_IDENTITY_UNRESOLVED),

    /**
     * A member a run would take part on cannot load a connector the pipeline's sinks open: {@code pipeline} is
     * the pipeline, {@code member} the member by stable id, {@code connector} the connector id and {@code reason}
     * what the member answered - its own coded refusal, the failure it hit, or that it did not answer in time.
     * Every member is asked before the run starts anything, because a member that finds out only as its sink
     * opens fails a run that is already reading, and the reason stays on that member.
     */
    CONNECTOR_UNAVAILABLE_ON_MEMBER(ActuationErrorDefinitions.CONNECTOR_UNAVAILABLE_ON_MEMBER),

    /**
     * The members a run would take part on load different artifacts for one connector: {@code pipeline} is the
     * pipeline, {@code connector} the connector id and {@code artifacts} the content hash each member loaded. The
     * writers of one sink would run different code - members reading different registries, or a registration
     * replaced while the run was starting.
     */
    CONNECTOR_DIFFERS_ACROSS_MEMBERS(ActuationErrorDefinitions.CONNECTOR_DIFFERS_ACROSS_MEMBERS),

    /** The original run cannot prepare its operator bindings on a required member. */
    OPERATOR_STATE_UNAVAILABLE_ON_MEMBER(ActuationErrorDefinitions.OPERATOR_STATE_UNAVAILABLE_ON_MEMBER),

    /** Every original data member must prove this shared recovery attempt before storage replacement. */
    SHARED_RECOVERY_MEMBER_UNAVAILABLE(ActuationErrorDefinitions.SHARED_RECOVERY_MEMBER_UNAVAILABLE);

    private final ProductErrorDefinition definition;

    ActuationError(ProductErrorDefinition definition) { this.definition = definition; }

    @Override public String code() { return definition.code(); }
    @Override public Severity severity() { return definition.severity(); }
    @Override public Set<String> placeholders() { return definition.parameters(); }
}
