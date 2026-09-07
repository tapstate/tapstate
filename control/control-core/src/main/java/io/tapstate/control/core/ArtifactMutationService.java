package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.ReferenceGraph;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.DerivedSchemaStore;
import io.tapstate.spi.store.DesiredStore;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.SrsMetaStore;
import io.tapstate.spi.store.StateStore;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Removal of an applied resource, one path for every kind. Deletion is real: the document leaves the
 * store, so no read path has to learn to filter a tombstone, and the id is free to be applied again
 * immediately.
 *
 * <p>Two grounds refuse a deletion, and both are judged <em>before</em> anything is written, so a
 * refusal leaves the store byte-for-byte as it was:
 *
 * <ul>
 *   <li><b>Still referenced</b> — another stored resource points at the id. The referrers travel back
 *       in the failure so the caller can act without a second query. Nothing cascades: removing the
 *       referrers is the caller's decision, not a side effect of this one.</li>
 *   <li><b>Running pipeline</b> — the id is a pipeline that is running or is about to. Both halves of
 *       the lifecycle are read, because either one alone lets a live pipeline through: the desired
 *       state alone passes a pipeline whose stop has been requested but not yet reached, which is
 *       still executing; the actual state alone passes one whose start has been requested but not yet
 *       reached, which the next convergence would then raise from an artifact that no longer
 *       exists.</li>
 * </ul>
 *
 * <p>The removal itself is the store's atomic conditional delete, so the content hash the caller read
 * is checked and the document removed as one indivisible step — a writer holding a stale version can
 * never remove what it did not see. The precondition is mandatory here, unlike on apply: a delete
 * carries an id and nothing else, so without it the caller is discarding a version it never looked at.
 *
 * <p>Removing a pipeline reclaims the bookkeeping that belongs to that pipeline alone — its desired
 * intent, its checkpoint, its observation — and detaches its cursor from every shared mining chain that
 * carries one. Nothing that another pipeline may share is touched: the chains themselves, the nest
 * state and the target data all outlive the artifact. Detaching is not tidiness — a departed consumer's
 * cursor is folded into two independent minimums that would otherwise pin the shared chain's durable
 * frontier and cdc write headroom permanently, stalling every other pipeline on it without an error.
 */
public final class ArtifactMutationService {

    /** Actual states a pipeline is at rest in; any other means it is still executing. */
    private static final Set<PipelineState> RESTING = Set.of(
            PipelineState.NEW, PipelineState.STOPPED, PipelineState.COMPLETED, PipelineState.FAILED);

    /** Desired states that will drive a pipeline back up, whatever it is doing right now. */
    private static final Set<PipelineState> HEADED_UP = Set.of(
            PipelineState.RUNNING, PipelineState.PAUSED);

    private final ArtifactStore store;
    private final DesiredStore desired;
    private final StateStore state;
    private final ObservationStore observations;
    private final SrsMetaStore srsMeta;
    private final DerivedSchemaStore derivedSchemas;
    private final AuditGate auditGate;

    private final DataBrowserFollows follows;

    public ArtifactMutationService(
            ArtifactStore store,
            DesiredStore desired,
            StateStore state,
            ObservationStore observations,
            SrsMetaStore srsMeta,
            DerivedSchemaStore derivedSchemas,
            AuditGate auditGate,
            DataBrowserFollows follows) {
        this.store = Objects.requireNonNull(store, "store");
        this.desired = Objects.requireNonNull(desired, "desired");
        this.state = Objects.requireNonNull(state, "state");
        this.observations = Objects.requireNonNull(observations, "observations");
        this.srsMeta = Objects.requireNonNull(srsMeta, "srsMeta");
        this.derivedSchemas = Objects.requireNonNull(derivedSchemas, "derivedSchemas");
        this.auditGate = Objects.requireNonNull(auditGate, "auditGate");
        this.follows = Objects.requireNonNull(follows, "follows");
    }

    /**
     * Removes the stored artifact {@code id}, provided {@code expectedContentHash} is the version the
     * caller read and neither refusal ground holds, then reclaims a pipeline's dependent bookkeeping.
     * {@code principal} is the identity the removal is attributed to.
     *
     * <p>A refusal happens before anything is written. A failure to reclaim happens after the artifact is
     * already gone and is reported rather than swallowed, so the residue is visible to whoever has to
     * clear it; it never puts the artifact back, because a removal the caller was told succeeded must not
     * silently undo itself.
     *
     * <p>The two are told apart by their code, not only by their text. A refusal means nothing happened
     * and the caller may retry; {@code artifact.reclaim-incomplete} means the artifact is gone and
     * retrying can only answer {@code artifact.not-found}, which is what a caller handed a refusal for a
     * partly-executed removal would do next — learning nothing about the residue it now owns.
     */
    public void delete(String principal, String id, String expectedContentHash) {
        Objects.requireNonNull(principal, "principal");
        Objects.requireNonNull(id, "id");
        if (expectedContentHash == null) {
            throw error(ArtifactError.PRECONDITION_REQUIRED, Map.of("id", id));
        }

        // The target is fetched by id rather than picked out of the whole store. Reconstructing every
        // stored resource to find one of them makes an unrelated document this version cannot parse fail
        // the lookup — so a single unreadable sibling would answer every removal in the store with the
        // same io diagnostic, including the removal of the document that cannot be read.
        Resource target = store.get(id)
                .orElseThrow(() -> error(ArtifactError.NOT_FOUND, Map.of("id", id)));

        refuseWhenReferenced(id, store.list());
        if (target instanceof PipelineResource) {
            refuseWhenNotStopped(id);
        }

        // Both grounds are judged above, outside the gate, so a refusal writes no audit record and leaves
        // the store untouched. What the gate wraps is the destruction itself: the record lands before the
        // document does, so an audit backend that is down refuses the removal rather than destroying a
        // resource that leaves no trace behind.
        // The declared version travels with the record, not the one the store turns out to hold: this
        // runs before the compare below, and on the branch where that compare refuses, the version the
        // caller offered is the only one that describes the attempt.
        AuditContext audit = new AuditContext(principal, id, expectedContentHash);
        auditGate.dispatch(ControlOperations.ARTIFACT_DELETE, audit, () -> {
            switch (store.delete(id, expectedContentHash)) {
                case DELETED -> {
                }
                case NOT_FOUND -> throw error(ArtifactError.NOT_FOUND, Map.of("id", id));
                case VERSION_CONFLICT -> throw error(ArtifactError.VERSION_CONFLICT, Map.of("id", id));
                default -> throw new IllegalStateException("unexpected artifact mutation outcome for delete");
            }

            if (target instanceof PipelineResource) {
                reclaim(id);
            }
            if (target instanceof SourceResource) {
                // A follow is not in the reference graph, so neither refusal above ever sees one: a
                // source read by nothing but a pair of eyes deletes cleanly while those eyes keep
                // being handed rows from it. The kind-specific door does this already; a removal has
                // to mean the same thing through both, or which endpoint the caller happened to use
                // decides whether the readers are told.
                follows.closeFollowsOf(id);
            }
            return null;
        });
    }

    /**
     * The removal stands; some of what it owns did not come with it. This is reported as its own code
     * rather than as the reclaim step's raw failure, because the raw failure travels the same channel a
     * refusal does and would be read as one: the caller is told the removal was rejected, retries, and
     * gets {@code artifact.not-found} — with no way to learn that the first attempt destroyed the
     * artifact and left the rest. Naming the residue is the whole point of the code; the underlying
     * failures ride along as cause and suppressed so nothing is swallowed.
     */
    private static TapstateException reclaimIncomplete(String id, String reason, List<String> residue,
            List<RuntimeException> failures) {
        TapstateException incomplete = new TapstateException(
                ArtifactError.RECLAIM_INCOMPLETE,
                Map.of("id", id, "reason", reason, "residue", residue),
                failures.isEmpty() ? null : failures.get(0));
        failures.stream().skip(1).forEach(incomplete::addSuppressed);
        return incomplete;
    }

    /**
     * Reclaims everything a removed pipeline owns. Every step is attempted even after one fails, and the
     * failures are reported together at the end: aborting at the first would leave the untouched steps'
     * residue behind on top of the failure, and the artifact is already gone by now so no caller can
     * simply run the removal again to finish the job.
     *
     * <p>The shared chains are detached first because theirs is the only residue that harms a
     * <em>different</em> pipeline; if the process dies mid-reclaim, the damage that has been contained is
     * the one that was not this pipeline's alone to suffer.
     *
     * <p>The lifecycle is read once more first, because the refusal that judged it ran before the
     * artifact was removed and nothing held it still in between: a start that landed inside that window
     * leaves a pipeline whose job is now executing. Reclaiming one of those does not merely lose
     * bookkeeping — deleting the checkpoint discards the fencing epoch, so a pipeline later applied
     * under the same id shares a fencing sequence with the job still running under the old one. The
     * bookkeeping is left alone and reported instead. This narrows the window rather than closing it;
     * closing it needs a lifecycle-aware conditional delete or a lock spanning the check and the write,
     * neither of which this store port offers.
     */
    private void reclaim(String id) {
        if (!isAtRest(id)) {
            throw reclaimIncomplete(id, "pipeline-live",
                    List.of("mining-chain-consumer", "desired", "state", "observation", "derived-schema"),
                    List.of());
        }
        List<RuntimeException> failures = new ArrayList<>();
        List<String> residue = new ArrayList<>();
        attempt(failures, residue, "mining-chain-consumer", () -> detachFromEveryChain(id));
        attempt(failures, residue, "desired", () -> desired.delete(id));
        attempt(failures, residue, "state", () -> state.delete(id));
        attempt(failures, residue, "observation", () -> observations.delete(id));
        // Left behind, this record would be read as the derivation history of whatever is applied under
        // the id next, and would refuse to start it over a difference against a schema belonging to
        // something that no longer exists.
        attempt(failures, residue, "derived-schema", () -> derivedSchemas.delete(id));
        if (!failures.isEmpty()) {
            throw reclaimIncomplete(id, "step-failed", residue, failures);
        }
    }

    /**
     * Detaches the pipeline's cursor from every chain that carries one. The chains are asked which of
     * them hold it, rather than derived from what the pipeline reads: chain identity is resolved where
     * captures are built, and a cursor left behind by an earlier shape of the pipeline would be invisible
     * to any derivation from its current one.
     *
     * <p>Each chain is detached independently, for the same reason the reclaim steps are: one chain's
     * failure must not leave the departing consumer attached to the chains that come after it. That
     * residue is the one this class's own contract calls out as harming a <em>different</em> pipeline,
     * and it cannot be cleared afterwards — the artifact is gone by now, so the removal cannot be run
     * again to finish the job. Detaching is idempotent, so a chain already detached costs nothing.
     */
    private void detachFromEveryChain(String id) {
        List<RuntimeException> failures = new ArrayList<>();
        for (String miningChainId : srsMeta.miningChainIdsWithConsumer(id)) {
            attempt(failures, () -> srsMeta.detachConsumer(miningChainId, id));
        }
        rethrowTogether(failures);
    }

    /**
     * Throws the first collected failure with the rest attached as suppressed, or returns quietly when
     * nothing failed. Reporting them together is what lets every step be attempted: the caller still
     * fails, and still sees each residue it has to clear.
     */
    private static void rethrowTogether(List<RuntimeException> failures) {
        if (failures.isEmpty()) {
            return;
        }
        RuntimeException first = failures.get(0);
        failures.stream().skip(1).forEach(first::addSuppressed);
        throw first;
    }

    /** Runs one reclaim step, collecting a coded or runtime failure instead of ending the reclaim. */
    private static void attempt(List<RuntimeException> failures, Runnable step) {
        try {
            step.run();
        } catch (RuntimeException e) {
            failures.add(e);
        }
    }

    /**
     * The same, naming what the step would have reclaimed. A failure here is reported to someone who
     * has to go and clear it by hand, and "the reclaim failed" does not say what is left — the names
     * are what turn the report into a task.
     */
    private static void attempt(
            List<RuntimeException> failures, List<String> residue, String name, Runnable step) {
        int before = failures.size();
        attempt(failures, step);
        if (failures.size() > before) {
            residue.add(name);
        }
    }

    private void refuseWhenReferenced(String id, List<Resource> stored) {
        List<String> referrers = ReferenceGraph.of(stored).referencedBy(id).stream()
                .map(ReferenceGraph.Edge::id)
                .sorted()
                .toList();
        if (!referrers.isEmpty()) {
            throw error(ArtifactError.IN_USE, Map.of("id", id, "referrers", referrers));
        }
    }

    /**
     * Refuses a pipeline that is executing or is headed back up. A pipeline with neither document has
     * never run, which is the clean case rather than an unknown one.
     */
    private void refuseWhenNotStopped(String id) {
        PipelineState actual = actualStateOf(id);
        PipelineState intent = intentOf(id);
        if (!isAtRest(actual, intent)) {
            throw error(
                    ArtifactError.PIPELINE_NOT_STOPPED,
                    Map.of("id", id, "actual", actual.name(), "desired", intent.name()));
        }
    }

    /**
     * Whether the pipeline is at rest right now, by the same reading the refusal uses. Sharing one
     * definition is the point: a guard that judged "still stopped" differently from the refusal would
     * pass exactly the pipelines the refusal exists to catch.
     */
    private boolean isAtRest(String id) {
        return isAtRest(actualStateOf(id), intentOf(id));
    }

    private static boolean isAtRest(PipelineState actual, PipelineState intent) {
        return RESTING.contains(actual) && !HEADED_UP.contains(intent);
    }

    private PipelineState actualStateOf(String id) {
        return state.read(id)
                .map(checkpoint -> StateJson.parse(checkpoint.stateJson()))
                .orElse(PipelineState.NEW);
    }

    private PipelineState intentOf(String id) {
        return desired.read(id).map(DesiredState::targetState).orElse(PipelineState.NEW);
    }

    private static TapstateException error(ArtifactError code, Map<String, Object> args) {
        return new TapstateException(code, args, null);
    }
}
