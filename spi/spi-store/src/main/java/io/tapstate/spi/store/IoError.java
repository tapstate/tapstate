package io.tapstate.spi.store;

import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.core.common.Severity;

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
    STORE_UNAVAILABLE("io.store-unavailable", Set.of("detail")),

    /**
     * The store rejected the credentials for the operation. Carries no placeholder on purpose: echoing
     * anything back risks leaking a credential.
     */
    STORE_UNAUTHORIZED("io.store-unauthorized", Set.of()),

    /**
     * A stored document could not be read back into its model — a corrupt body, or one written by a
     * newer grammar, or a document missing a field this version requires. {@code id} is the stored
     * document's id.
     */
    DOCUMENT_UNREADABLE("io.document-unreadable", Set.of("id", "field")),

    /** A failed or losing upload cannot confirm cleanup of its own unpublished GridFS resource. */
    CONNECTOR_ARTIFACT_CLEANUP_UNCONFIRMED("io.connector-artifact-cleanup-unconfirmed", Set.of("hash", "upload")),

    /** An input value cannot be represented by the store without changing its value. */
    DOCUMENT_INVALID("io.document-invalid", Set.of("id", "field")),

    /**
     * Re-discovery replaced a connection's schema during every permitted read attempt. The store
     * remains available, but the caller must retry after publication settles to read a coherent model.
     */
    SCHEMA_READ_CONTENTION("io.schema-read-contention", Set.of("connectionId")),

    /** Publication lost its writer lease or exhausted its attempts under concurrent discovery. */
    SCHEMA_WRITE_CONTENTION("io.schema-write-contention", Set.of("connectionId")),

    /** A side-effect write carried an expired, replaced, or otherwise non-current workload claim. */
    WORKLOAD_CLAIM_FENCED("io.workload-claim-fenced", Set.of()),

    /**
     * A pipeline has aggregate sink progress from before progress was recorded per writer, and now names
     * multiple writers. The aggregate may have been advanced by only the fastest writer, so it cannot be
     * promoted into evidence that every writer reached that position. {@code pipeline} identifies the
     * consumer whose retained state must be cleared before a full resync or an explicitly accepted new
     * incremental baseline.
     */
    SINK_WRITER_PROGRESS_AMBIGUOUS("io.sink-writer-progress-ambiguous", Set.of("pipeline")),

    /** A note command lost its captured key revision and must not overwrite the current value. */
    NOTE_WRITE_CONFLICT("io.note-write-conflict", Set.of()),

    /** A diagnostic command no longer owns its original configuration binding or state generation. */
    REQUIRED_DEAD_LETTER_FENCED("io.required-dead-letter-fenced", Set.of()),

    /** One source event cannot describe two different retained row, deletion or token facts. */
    REQUIRED_DEAD_LETTER_CONFLICT("io.required-dead-letter-conflict", Set.of()),

    /** The captured cache authentication generation is no longer current or ready. */
    CACHE_AUTH_FENCED("io.cache-auth-fenced", Set.of()),

    /** A lifecycle intent no longer owns its captured storage binding or stable Pipeline state generation. */
    PIPELINE_INTENT_FENCED("io.pipeline-intent-fenced", Set.of()),

    /** Issued first-state authority has incomplete or inconsistent CACHE history; no counters may be recreated. */
    PIPELINE_STATE_REBUILD_REQUIRED("io.pipeline-state-rebuild-required", Set.of("pipeline")),

    /** The captured business incarnation is unavailable or no longer current. */
    CACHE_BUSINESS_FENCED("io.cache-business-fenced", Set.of()),

    /** An event cannot prove its exact ring, capture epoch and model version. */
    SRS_EVENT_MODEL_UNAVAILABLE("io.srs-event-model-unavailable", Set.of("ring", "epoch", "version")),

    /** A retained source consumer cannot prove its membership, stable state identity and cursor. */
    SRS_CONSUMER_UNAVAILABLE("io.srs-consumer-unavailable", Set.of("chain")),

    /** A source checkpoint cannot prove its original chain, table frontiers and exact model closure. */
    SRS_CHECKPOINT_UNPROVEN("io.srs-checkpoint-unproven", Set.of("chain")),

    /** An exact actual model is absent or cannot be used for the captured stream context. */
    SRS_MODEL_UNAVAILABLE("io.srs-model-unavailable", Set.of("chain", "table", "version")),

    /** An immutable model identity already names different structural content. */
    SRS_MODEL_CONFLICT("io.srs-model-conflict", Set.of("chain", "table", "version")),

    /** A connector must not reuse retained notes with another connection or artifact. */
    NOTE_ORIGIN_MISMATCH("io.note-origin-mismatch", Set.of()),

    /** A retained shared capture still depends on this private note scope. */
    NOTE_REFERENCED("io.note-referenced", Set.of()),

    /** Necessary connector notes cannot prove the captured namespace, owner or clear generation. */
    NOTE_SCOPE_UNAVAILABLE("io.note-scope-unavailable", Set.of()),

    /** Legacy pipeline-level progress cannot establish this source node's confirmed recovery cursor. */
    SRS_PROGRESS_UNPROVEN("io.srs-progress-unproven", Set.of("pipeline")),

    /** The private direct source cannot prove the retained progress and ordered pending batches. */
    DIRECT_CHANNEL_UNAVAILABLE("io.direct-channel-unavailable", Set.of("channel")),

    /**
     * A document a store operation had to write is larger than the store will accept. {@code id} is
     * the document's id, or {@code unknown} where the failing call did not name one.
     *
     * <p>Distinct from {@link #STORE_UNAVAILABLE} because nothing is wrong with the store and
     * retrying cannot help: what has to change is how much is being put in one document. The two were
     * one code until a size failure was found reaching callers as "a store operation could not
     * complete", which sends whoever reads it to check a store that is healthy.
     */
    DOCUMENT_TOO_LARGE("io.document-too-large", Set.of("id"));

    private final String code;
    private final Set<String> placeholders;

    IoError(String code, Set<String> placeholders) {
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
