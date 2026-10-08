package io.tapstate.spi.store;

import io.tapstate.core.lifecycle.Observation;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * The per-pipeline observation store: persists one logical latest read-only projection of state,
 * metrics and snapshot progress. A pure interface over the observation model in the core ring (rule
 * R2); physical manifests and payload chunks remain an adapter concern.
 *
 * <p>An observation is a latest-state projection, not a time series. Scoped writes conditionally
 * replace its one current pointer only for a newer execution or observation time. A control reader
 * compares its stored owner with the current artifact and execution before returning it. The plain
 * save/read methods retain the pre-identity compatibility path.
 */
public interface ObservationStore {

    /** Upper bound on one cold-path orphan scan, independent of a caller's batch setting. */
    int MAX_LATEST_SCAN_BATCH = 256;

    /** Internal owner of a published observation; neither field is part of the public observation. */
    sealed interface Owner permits Scope, PreExecutionFailure.Owner {
        String pipelineIncarnationId();
    }

    record Scope(String pipelineIncarnationId, long executionGeneration) implements Owner {
        public Scope {
            Objects.requireNonNull(pipelineIncarnationId, "pipelineIncarnationId");
            if (pipelineIncarnationId.isBlank() || executionGeneration <= 0) {
                throw new IllegalArgumentException("an observation scope needs an incarnation and positive generation");
            }
        }
    }

    /** Stored projection and its optional internal owner; absence means a legacy unscoped document. */
    record Stored(Observation observation, Optional<Scope> scope, Optional<PreExecutionFailure.Owner> refusal) {
        public Stored {
            Objects.requireNonNull(observation, "observation");
            Objects.requireNonNull(scope, "scope");
            Objects.requireNonNull(refusal, "refusal");
            if (scope.isPresent() && refusal.isPresent()) {
                throw new IllegalArgumentException("one current observation has exactly one private owner");
            }
            if (refusal.filter(owner -> !owner.pipelineId().equals(observation.pipelineId())).isPresent()) {
                throw new IllegalArgumentException("a refusal frame belongs to its actual pipeline owner");
            }
        }
        public Stored(Observation observation, Optional<Scope> scope) {
            this(observation, scope, Optional.empty());
        }
    }

    /** Receipt for the actual committed private carrier; payload integrity is independent of DTO equality. */
    record ContinuationReceipt(String pipelineId, String revision, String digest, int encodingVersion,
            String token, Scope sourceScope, Optional<ObservationContinuation.Target> target,
            Optional<ObservationContinuation.Target> baselineOrigin, boolean knownBaseline) {
        public ContinuationReceipt {
            Objects.requireNonNull(pipelineId, "pipelineId");
            Objects.requireNonNull(revision, "revision");
            Objects.requireNonNull(digest, "digest");
            Objects.requireNonNull(token, "token");
            target = Objects.requireNonNull(target, "target");
            baselineOrigin = Objects.requireNonNull(baselineOrigin, "baselineOrigin");
            if (pipelineId.isBlank() || revision.isBlank() || token.isBlank() || encodingVersion < 1
                    || !digest.matches("[0-9a-f]{64}")) {
                throw new IllegalArgumentException("a private continuation receipt needs complete identity and integrity");
            }
        }

        public boolean matches(HandoffIdentity expected) {
            Objects.requireNonNull(expected, "expected");
            return expected.counterPolicy() == StopReservation.CounterPolicy.CONTINUE
                    && pipelineId.equals(expected.pipelineId()) && token.equals(expected.token())
                    && Objects.equals(sourceScope, expected.sourceScope())
                    && target.filter(bound -> bound.scope().equals(expected.targetScope())
                            && bound.realJob().filter(expected.targetJob()::equals).isPresent()).isPresent();
        }
    }

    record StoredContinuation(ObservationContinuation continuation, ContinuationReceipt receipt) {
        public StoredContinuation {
            Objects.requireNonNull(continuation, "continuation");
            Objects.requireNonNull(receipt, "receipt");
            if (!continuation.token().equals(receipt.token())
                    || !Objects.equals(continuation.sourceScope(), receipt.sourceScope())
                    || !continuation.target().equals(receipt.target())
                    || !continuation.baselineOrigin().equals(receipt.baselineOrigin())
                    || continuation.knownBaseline() != receipt.knownBaseline()) {
                throw new IllegalArgumentException("a continuation receipt belongs to its actual stored carrier");
            }
        }
    }

    /** Current and private checkpoint are one atomic publication when STORE is requested. */
    record PublicationResult(boolean committed, Optional<ContinuationReceipt> continuationReceipt) {
        public PublicationResult {
            continuationReceipt = Objects.requireNonNull(continuationReceipt, "continuationReceipt");
            if (!committed && continuationReceipt.isPresent()) {
                throw new IllegalArgumentException("a refused publication cannot claim a committed private receipt");
            }
        }
    }

    sealed interface ContinuationWrite permits ContinuationWrite.Keep, ContinuationWrite.Store,
            ContinuationWrite.Reset {
        record Keep() implements ContinuationWrite { }
        record Store(ObservationContinuation next, Optional<ContinuationReceipt> expectedReceipt)
                implements ContinuationWrite {
            public Store {
                Objects.requireNonNull(next, "next");
                expectedReceipt = Objects.requireNonNull(expectedReceipt, "expectedReceipt");
            }
        }
        record Reset(HandoffIdentity currentHandoff, Optional<ContinuationReceipt> expectedReceipt)
                implements ContinuationWrite {
            public Reset {
                Objects.requireNonNull(currentHandoff, "currentHandoff");
                expectedReceipt = Objects.requireNonNull(expectedReceipt, "expectedReceipt");
                if (currentHandoff.counterPolicy() != StopReservation.CounterPolicy.RESET) {
                    throw new IllegalArgumentException("clearing a private continuation requires reset authorization");
                }
            }
        }

        static ContinuationWrite keep() { return new Keep(); }
        static ContinuationWrite store(ObservationContinuation next, Optional<ContinuationReceipt> expected) {
            return new Store(next, expected);
        }
        static ContinuationWrite reset(HandoffIdentity current, Optional<ContinuationReceipt> expected) {
            return new Reset(current, expected);
        }
    }

    /** The exact owner and time read during a bounded latest-document scan. */
    record LatestSnapshot(String pipelineId, Optional<Scope> scope, Optional<Instant> observedAt) {
        public LatestSnapshot {
            Objects.requireNonNull(pipelineId, "pipelineId");
            Objects.requireNonNull(scope, "scope");
            Objects.requireNonNull(observedAt, "observedAt");
            if (pipelineId.isBlank()) {
                throw new IllegalArgumentException("an observation snapshot needs a pipeline id");
            }
        }
    }

    /** Digest-keyed manifest snapshot for bounded cold cleanup; cursor and revision are opaque. */
    record ManifestSnapshot(String cursor, String revision, List<Scope> scopes, List<PreExecutionFailure.Owner> refusals) {
        public ManifestSnapshot {
            Objects.requireNonNull(cursor, "cursor");
            Objects.requireNonNull(revision, "revision");
            scopes = List.copyOf(Objects.requireNonNull(scopes, "scopes"));
            refusals = List.copyOf(Objects.requireNonNull(refusals, "refusals"));
            if (cursor.isBlank() || revision.isBlank()) {
                throw new IllegalArgumentException("a manifest snapshot needs cursor and revision");
            }
        }
        public ManifestSnapshot(String cursor, String revision, List<Scope> scopes) {
            this(cursor, revision, scopes, List.of());
        }
        public java.util.stream.Stream<String> incarnations() {
            return java.util.stream.Stream.concat(scopes.stream().map(Scope::pipelineIncarnationId),
                    refusals.stream().map(PreExecutionFailure.Owner::pipelineIncarnationId)).distinct();
        }
    }

    /** Work performed by one bounded physical-chunk cleanup batch. */
    record ReclaimResult(long scanned, long deleted) {
        public ReclaimResult {
            if (scanned < 0 || deleted < 0 || deleted > scanned) {
                throw new IllegalArgumentException("reclaim counts are non-negative and deleted is scanned");
            }
        }
    }

    /** Legacy unscoped upsert; new executions use the conditional scoped write. */
    void save(Observation observation);

    /**
     * Conditionally stores the latest observation for one execution. Returns false for an older
     * generation, a different incarnation at the same generation, or non-advancing observation time.
     * A newer generation may replace an older one after a resource is recreated. Implementations that do
     * not support scoped writes must fail closed rather than silently publish without a fence.
     */
    default boolean saveScoped(Observation observation, Scope scope) {
        throw new UnsupportedOperationException("scoped observation writes are unavailable");
    }

    /** Cold publication of a factual refusal under its original complete lifecycle receipt. */
    default boolean savePreExecutionFailure(Observation observation, PreExecutionFailure.Receipt receipt) {
        throw new UnsupportedOperationException("pre-execution refusal publication is unavailable");
    }

    /** Cold read proof over one consistent view of the actual checkpoint, intent, artifacts and authority. */
    default boolean isCurrentPreExecutionFailure(PreExecutionFailure.Owner owner) {
        return false;
    }

    /** Reobserves an already published, still-qualified refusal without creating or rebinding its owner. */
    default boolean refreshPreExecutionFailure(String pipelineId, Instant observedAt) { return false; }

    /** Distinguishes a still-live original input proof awaiting floor transfer from an obsolete request. */
    default boolean preExecutionFailureInputsCurrent(PreExecutionFailure.Owner owner) { return false; }

    /** Publishes one prepared public frame and, when requested, its matching private producer checkpoint. */
    default PublicationResult saveScoped(Observation observation, Scope scope, ContinuationWrite write) {
        Objects.requireNonNull(write, "write");
        if (!(write instanceof ContinuationWrite.Keep)) {
            throw new UnsupportedOperationException("private continuation publication is unavailable");
        }
        return new PublicationResult(saveScoped(observation, scope), Optional.empty());
    }

    /** Reads the private carrier even when the logical latest has no committed public observation. */
    default Optional<StoredContinuation> readContinuation(String pipelineId) {
        return Optional.empty();
    }

    /** Cold attach or bind is conditional on the exact live handoff and previous private receipt. */
    default Optional<ContinuationReceipt> saveContinuation(String pipelineId, StopReservation expectedLiveMarker,
            Optional<ContinuationReceipt> expectedReceipt, ObservationContinuation next) {
        throw new UnsupportedOperationException("private continuation persistence is unavailable");
    }

    /** An explicit reset clears only the matched carrier under the exact live reset handoff. */
    default boolean clearContinuation(String pipelineId, StopReservation expectedLiveReset,
            ContinuationReceipt expectedReceipt) {
        throw new UnsupportedOperationException("private continuation clearing is unavailable");
    }

    /** Returns the current observation for a pipeline, or empty if none has been published. */
    Optional<Observation> read(String pipelineId);

    /**
     * Reads the stored owner without deciding whether legacy data belongs to the current artifact.
     * Callers compare the scope with authoritative artifact and execution identity before projection.
     */
    default Optional<Stored> readStored(String pipelineId) {
        return read(pipelineId).map(observation -> new Stored(observation, Optional.empty()));
    }

    /** Unconditional legacy removal; artifact cleanup must use an owner-matched method below. */
    void delete(String pipelineId);

    /** Removes only the deleted incarnation's latest document, leaving a recreated resource untouched. */
    default void deleteIncarnation(String pipelineId, String incarnationId) {
        throw new UnsupportedOperationException("scoped observation cleanup is unavailable");
    }

    /** Removes only an observation written before an internal execution owner existed. */
    default void deleteLegacy(String pipelineId) {
        throw new UnsupportedOperationException("legacy observation cleanup is unavailable");
    }

    /**
     * Scans at most {@code limit} latest documents after an exclusive pipeline-id cursor, in id order.
     * Only the internal owner and observed time are read; an empty cursor begins at the first id.
     * Implementations without a bounded keyset scan must fail closed.
     */
    default List<LatestSnapshot> scanLatestAfter(Optional<String> afterPipelineId, int limit) {
        throw new UnsupportedOperationException("bounded observation scans are unavailable");
    }

    /**
     * Removes the scanned document only if its id, complete owner envelope and observed time still match.
     * In particular, a legacy snapshot only matches a document with both owner fields absent. A missing
     * observed time matches only a document where that field remains absent. Returns false on any race.
     */
    default boolean deleteIfUnchanged(LatestSnapshot snapshot) {
        throw new UnsupportedOperationException("conditional observation cleanup is unavailable");
    }

    /** Exact cleanup after the caller has proved the artifact is absent or belongs to another incarnation. */
    default boolean deleteOrphanIfUnchanged(LatestSnapshot snapshot) {
        return deleteIfUnchanged(snapshot);
    }

    /** Whether a committed digest-keyed current makes the legacy string document cold residue. */
    default boolean hasCommittedManifest(String pipelineId) {
        return false;
    }

    /** Whether this implementation owns the digest-keyed manifest/chunk physical format. */
    default boolean supportsManifestStorage() {
        return false;
    }

    /** Bounded keyset scan of digest-keyed manifests, independent of legacy string ids. */
    default List<ManifestSnapshot> scanManifestsAfter(Optional<String> afterCursor, int limit) {
        throw new UnsupportedOperationException("bounded manifest scans are unavailable");
    }

    /** Removes only the manifest revision observed by a cold scan. */
    default boolean deleteManifestIfUnchanged(ManifestSnapshot snapshot) {
        throw new UnsupportedOperationException("conditional manifest cleanup is unavailable");
    }

    /** Cold cleanup also guards a live resource's continuation reservation before retiring its manifest. */
    default boolean deleteManifestIfUnchanged(ManifestSnapshot snapshot, String pipelineId) {
        throw new UnsupportedOperationException("handoff-qualified manifest cleanup is unavailable");
    }

    /** Expires pending leases and retires/deletes at most {@code limit} physical chunk rows. */
    default ReclaimResult reclaimChunks(int limit) {
        return new ReclaimResult(0, 0);
    }
}
