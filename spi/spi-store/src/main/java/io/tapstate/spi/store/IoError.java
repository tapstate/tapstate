package io.tapstate.spi.store;

import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.core.common.Severity;
import io.tapstate.core.common.ProductErrorDefinition;
import io.tapstate.core.common.IoErrorDefinitions;

import java.util.Set;

/**
 * The {@code io} domain's error codes: the user-facing, diagnosable failures a store data operation
 * raises at runtime — reaching the store to save / get / list / read / swap, or reading a stored
 * document back into its model. These are distinct from the {@code store} domain, which polices
 * reaching the store at startup (unreachable, not-a-replica-set, TLS policy); {@code io} is the
 * data-plane counterpart, raised while operating on an already-verified store.
 *
 * <p>Declared beside the store ports rather than inside one implementation of them: these are what a
 * store raises whatever it is built on, so a caller can both expect them and tell them apart. A read
 * face deciding whether a failure is about one damaged document or about reaching the store at all
 * needs to name that difference, and it cannot name a classification that lives in an adapter it must
 * not depend on. Each implementation translates its own driver's exceptions into these, so no driver
 * type escapes the adapter (rule R3). Programmer errors / invariant violations (a compare-and-swap on an
 * unseeded pipeline, say) stay bare and are allowed to crash — they are not laundered into an
 * {@code io.*} code that would hide the defect. {@code placeholders()} is the named-argument
 * contract: every throw site supplies a value for each name, and the build-time placeholder gate
 * checks the catalog templates against it.
 */
public enum IoError implements TapstateErrorCode {

    /**
     * A store data operation could not complete against the store — a timeout, a dropped connection,
     * a failed TLS handshake, or a command error. {@code detail} is the failure the driver reported
     * (host and cluster state, never a credential).
     */
    STORE_UNAVAILABLE(IoErrorDefinitions.STORE_UNAVAILABLE),

    /** The endpoint explicitly refused a write because its storage quota is exhausted. Carries no server data. */
    STORE_QUOTA_EXCEEDED(IoErrorDefinitions.STORE_QUOTA_EXCEEDED),

    /**
     * The store rejected the credentials for the operation. Carries no placeholder on purpose: echoing
     * anything back risks leaking a credential.
     */
    STORE_UNAUTHORIZED(IoErrorDefinitions.STORE_UNAUTHORIZED),

    /**
     * A stored document could not be read back into its model — a corrupt body, or one written by a
     * newer grammar, or a document missing a field this version requires. {@code id} is the stored
     * document's id.
     */
    DOCUMENT_UNREADABLE(IoErrorDefinitions.DOCUMENT_UNREADABLE),

    /** A failed or losing upload cannot confirm cleanup of its own unpublished GridFS resource. */
    CONNECTOR_ARTIFACT_CLEANUP_UNCONFIRMED(IoErrorDefinitions.CONNECTOR_ARTIFACT_CLEANUP_UNCONFIRMED),

    /** An input value cannot be represented by the store without changing its value. */
    DOCUMENT_INVALID(IoErrorDefinitions.DOCUMENT_INVALID),

    /**
     * Re-discovery replaced a connection's schema during every permitted read attempt. The store
     * remains available, but the caller must retry after publication settles to read a coherent model.
     */
    SCHEMA_READ_CONTENTION(IoErrorDefinitions.SCHEMA_READ_CONTENTION),

    /** Publication lost its writer lease or exhausted its attempts under concurrent discovery. */
    SCHEMA_WRITE_CONTENTION(IoErrorDefinitions.SCHEMA_WRITE_CONTENTION),

    /** A side-effect write carried an expired, replaced, or otherwise non-current workload claim. */
    WORKLOAD_CLAIM_FENCED(IoErrorDefinitions.WORKLOAD_CLAIM_FENCED),

    /**
     * A pipeline has aggregate sink progress from before progress was recorded per writer, and now names
     * multiple writers. The aggregate may have been advanced by only the fastest writer, so it cannot be
     * promoted into evidence that every writer reached that position. {@code pipeline} identifies the
     * consumer whose retained state must be cleared before a full resync or an explicitly accepted new
     * incremental baseline.
     */
    SINK_WRITER_PROGRESS_AMBIGUOUS(IoErrorDefinitions.SINK_WRITER_PROGRESS_AMBIGUOUS),

    /** A note command lost its captured key revision and must not overwrite the current value. */
    NOTE_WRITE_CONFLICT(IoErrorDefinitions.NOTE_WRITE_CONFLICT),

    /** A diagnostic command no longer owns its original configuration binding or state generation. */
    REQUIRED_DEAD_LETTER_FENCED(IoErrorDefinitions.REQUIRED_DEAD_LETTER_FENCED),

    /** One source event cannot describe two different retained row, deletion or token facts. */
    REQUIRED_DEAD_LETTER_CONFLICT(IoErrorDefinitions.REQUIRED_DEAD_LETTER_CONFLICT),

    /** The captured cache authentication generation is no longer current or ready. */
    CACHE_AUTH_FENCED(IoErrorDefinitions.CACHE_AUTH_FENCED),

    /** A lifecycle intent no longer owns its captured storage binding or stable Pipeline state generation. */
    PIPELINE_INTENT_FENCED(IoErrorDefinitions.PIPELINE_INTENT_FENCED),

    /** Issued first-state authority has incomplete or inconsistent CACHE history; no counters may be recreated. */
    PIPELINE_STATE_REBUILD_REQUIRED(IoErrorDefinitions.PIPELINE_STATE_REBUILD_REQUIRED),

    /** Missing canonical capture history requires explicitly confirmed Cluster cache recovery. */
    CAPTURE_STATE_REBUILD_REQUIRED(IoErrorDefinitions.CAPTURE_STATE_REBUILD_REQUIRED),

    /** The captured business incarnation is unavailable or no longer current. */
    CACHE_BUSINESS_FENCED(IoErrorDefinitions.CACHE_BUSINESS_FENCED),

    /** An event cannot prove its exact ring, capture epoch and model version. */
    SRS_EVENT_MODEL_UNAVAILABLE(IoErrorDefinitions.SRS_EVENT_MODEL_UNAVAILABLE),

    /** A retained source consumer cannot prove its membership, stable state identity and cursor. */
    SRS_CONSUMER_UNAVAILABLE(IoErrorDefinitions.SRS_CONSUMER_UNAVAILABLE),

    /** A source checkpoint cannot prove its original chain, table frontiers and exact model closure. */
    SRS_CHECKPOINT_UNPROVEN(IoErrorDefinitions.SRS_CHECKPOINT_UNPROVEN),

    /** An exact actual model is absent or cannot be used for the captured stream context. */
    SRS_MODEL_UNAVAILABLE(IoErrorDefinitions.SRS_MODEL_UNAVAILABLE),

    /** Required retained models cannot safely fit in the existing chain history budget. */
    SRS_MODEL_CAPACITY(IoErrorDefinitions.SRS_MODEL_CAPACITY),

    /** A pending full baseline exceeds its bounded control-record budget. */
    SRS_FULL_PREPARATION_CAPACITY(IoErrorDefinitions.SRS_FULL_PREPARATION_CAPACITY),

    /** The original explicit full preparation or its exact baseline cannot be proved. */
    SRS_FULL_PREPARATION_UNPROVEN(IoErrorDefinitions.SRS_FULL_PREPARATION_UNPROVEN),

    /** An arbitrary source token cannot prove compatibility with the retained model history. */
    SRS_POSITION_MODEL_UNPROVEN(IoErrorDefinitions.SRS_POSITION_MODEL_UNPROVEN),

    /** An immutable model identity already names different structural content. */
    SRS_MODEL_CONFLICT(IoErrorDefinitions.SRS_MODEL_CONFLICT),

    /** A connector must not reuse retained notes with another connection or artifact. */
    NOTE_ORIGIN_MISMATCH(IoErrorDefinitions.NOTE_ORIGIN_MISMATCH),

    /** A retained shared capture still depends on this private note scope. */
    NOTE_REFERENCED(IoErrorDefinitions.NOTE_REFERENCED),

    /** Necessary connector notes cannot prove the captured namespace, owner or clear generation. */
    NOTE_SCOPE_UNAVAILABLE(IoErrorDefinitions.NOTE_SCOPE_UNAVAILABLE),

    /** Legacy pipeline-level progress cannot establish this source node's confirmed recovery cursor. */
    SRS_PROGRESS_UNPROVEN(IoErrorDefinitions.SRS_PROGRESS_UNPROVEN),

    /** The private direct source cannot prove the retained progress and ordered pending batches. */
    DIRECT_CHANNEL_UNAVAILABLE(IoErrorDefinitions.DIRECT_CHANNEL_UNAVAILABLE),

    /** The requested private source/read contract cannot reuse the prepared physical state. */
    PIPELINE_CAPTURE_IDENTITY_UNPROVEN(IoErrorDefinitions.PIPELINE_CAPTURE_IDENTITY_UNPROVEN),

    /** Added tables cannot join an already prepared Pipeline while its prior state is retained. */
    PIPELINE_TABLE_SELECTION_REBUILD_REQUIRED(IoErrorDefinitions.PIPELINE_TABLE_SELECTION_REBUILD_REQUIRED),

    /** A configuration import cannot satisfy its original scope, input or publication conditions. */
    CONFIGURATION_IMPORT_REFUSED(IoErrorDefinitions.CONFIGURATION_IMPORT_REFUSED),

    /** A bounded import budget is exhausted without discarding retained outcomes. */
    CONFIGURATION_IMPORT_LIMIT(IoErrorDefinitions.CONFIGURATION_IMPORT_LIMIT),

    /**
     * A document a store operation had to write is larger than the store will accept. {@code id} is
     * the document's id, or {@code unknown} where the failing call did not name one.
     *
     * <p>Distinct from {@link #STORE_UNAVAILABLE} because nothing is wrong with the store and
     * retrying cannot help: what has to change is how much is being put in one document. The two were
     * one code until a size failure was found reaching callers as "a store operation could not
     * complete", which sends whoever reads it to check a store that is healthy.
     */
    DOCUMENT_TOO_LARGE(IoErrorDefinitions.DOCUMENT_TOO_LARGE),

    /** Original prepared target bodies must fit their row and complete-scope budgets. */
    SINK_MODEL_CAPACITY(IoErrorDefinitions.SINK_MODEL_CAPACITY),

    /** Upload recovery could not establish publication or confirm removal of this attempt's chunks. */
    CONNECTOR_UPLOAD_UNSETTLED(IoErrorDefinitions.CONNECTOR_UPLOAD_UNSETTLED),

    /** A Source exists but cannot yet provide its required original lifetime/binding facts. */
    SOURCE_IDENTITY_UNAVAILABLE(IoErrorDefinitions.SOURCE_IDENTITY_UNAVAILABLE),

    /** A necessary Source lifecycle revision cannot advance without wrapping or losing ordering. */
    SOURCE_IDENTITY_EXHAUSTED(IoErrorDefinitions.SOURCE_IDENTITY_EXHAUSTED);

    private final ProductErrorDefinition definition;

    IoError(ProductErrorDefinition definition) { this.definition = definition; }

    @Override public String code() { return definition.code(); }
    @Override public Severity severity() { return definition.severity(); }
    @Override public Set<String> placeholders() { return definition.parameters(); }
}
