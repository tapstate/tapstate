package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.LifecycleError;
import io.tapstate.core.lifecycle.LifecycleMachine;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.DesiredStore;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The pipeline lifecycle write side: the four user verbs start / stop / pause / resume, each turning an
 * intent into a desired-state write. A verb reads the pipeline's current state, validates the transition
 * against the four-verb state machine, applies a minimal revision-compatibility check, and persists the
 * resulting desired intent — the whole write gated by the audit gate so an audited verb that cannot be
 * recorded does not run. The converge side (a separate concern) later reconciles actual state toward this.
 *
 * <p>Current state comes from the pipeline's last desired intent: a pipeline with no desired doc is
 * {@link PipelineState#NEW}, and each verb advances the intent. This lets the four verbs be issued before
 * the converge side writes any actual state; once it does, current state is read from there instead.
 *
 * <p>A pipeline's revision is the content hash of its applied artifact's canonical form. The minimal
 * check is that a run verb (start / resume) runs at the latest applied revision: start always adopts the
 * latest, so it is compatible by construction; resume continues at the revision it was paused against, so
 * a re-apply in the meantime makes it {@code incompatible-revision}. Per-field revision rules are not
 * decided here. There is no rewind verb — a re-dig is stop then start composed by the caller, and the
 * stop half of that composition is the one that clears what the pipeline has, which is why {@link #stop}
 * takes the answer rather than assuming one.
 */
public final class PipelineLifecycleService {

    /** The audited control operation each verb is recorded under. */
    private static final Map<LifecycleVerb, Operation> OPERATIONS = Map.of(
            LifecycleVerb.START, ControlOperations.PIPELINE_START,
            LifecycleVerb.STOP, ControlOperations.PIPELINE_STOP,
            LifecycleVerb.PAUSE, ControlOperations.PIPELINE_PAUSE,
            LifecycleVerb.RESUME, ControlOperations.PIPELINE_RESUME);

    private final ArtifactQueryService artifacts;
    private final DesiredStore desired;
    private final AuditGate auditGate;

    public PipelineLifecycleService(ArtifactQueryService artifacts, DesiredStore desired, AuditGate auditGate) {
        this.artifacts = Objects.requireNonNull(artifacts, "artifacts");
        this.desired = Objects.requireNonNull(desired, "desired");
        this.auditGate = Objects.requireNonNull(auditGate, "auditGate");
    }

    /** Starts the pipeline (from NEW / STOPPED / COMPLETED), running it at the latest applied revision. */
    public DesiredState start(String principal, String pipelineId) {
        return apply(principal, pipelineId, LifecycleVerb.START, false);
    }

    /**
     * Stops the pipeline (from RUNNING / PAUSED). {@code purgeState} says whether stopping also clears
     * what this pipeline has accumulated — its resume position and its operators' state — and it is a
     * parameter rather than a policy because the two answers are not variations of one act: keeping
     * leaves a pipeline that carries on where it left off, clearing leaves one whose next run reads its
     * whole source again. The surface that takes the verb refuses a stop that did not state it.
     */
    public DesiredState stop(String principal, String pipelineId, boolean purgeState) {
        return apply(principal, pipelineId, LifecycleVerb.STOP, purgeState);
    }

    /** Pauses the running pipeline, retaining the revision it was running at. */
    public DesiredState pause(String principal, String pipelineId) {
        return apply(principal, pipelineId, LifecycleVerb.PAUSE, false);
    }

    /**
     * Resumes the paused pipeline. When its paused revision is still the latest applied one it carries on
     * as it was. When the definition was edited while it was paused, and everything that changed is safe
     * to build the run again against, it carries on from its recorded position with the run assembled
     * afresh; anything else is still {@code incompatible-revision}.
     */
    public DesiredState resume(String principal, String pipelineId) {
        return apply(principal, pipelineId, LifecycleVerb.RESUME, false);
    }

    private DesiredState apply(String principal, String pipelineId, LifecycleVerb verb, boolean purgeState) {
        Objects.requireNonNull(principal, "principal");
        Objects.requireNonNull(pipelineId, "pipelineId");

        String latest = artifacts.get(pipelineId)
                .map(a -> CanonicalHash.of(a.canonicalForm()))
                .orElseThrow(() -> new TapstateException(
                        LifecycleError.UNKNOWN_PIPELINE, Map.of("pipeline", pipelineId), null));

        Optional<DesiredState> prior = desired.read(pipelineId);
        PipelineState current = prior.map(DesiredState::targetState).orElse(PipelineState.NEW);
        PipelineState target = LifecycleMachine.transition(current, verb);

        // What the latest artifact would be assembled from, alongside what it hashes to. Both are read off
        // the same canonical text, so a field that moves one moves the other and the two cannot drift into
        // disagreeing about what changed.
        String latestAssembly = artifacts.assemblyIdentityOf(pipelineId).orElse(null);

        // The revision the verb runs at: a fresh start adopts the latest; the other verbs carry forward the
        // revision the pipeline is already at. A run verb (start / resume) must run at the latest applied.
        String runRevision = verb == LifecycleVerb.START ? latest : prior.map(DesiredState::revision).orElse(latest);

        // A resume over an edit made while the pipeline was paused. The refusal is right whenever carrying
        // on would run the pipeline as it was against a definition that has moved; it is wrong only when
        // everything that moved is re-read by building the run again, and then the answer is to build it
        // again rather than to refuse -- the author asked to carry on from the position, and re-reading the
        // whole source is not that.
        boolean reassemble = verb == LifecycleVerb.RESUME
                && !runRevision.equals(latest)
                && latestAssembly != null
                && latestAssembly.equals(prior.map(DesiredState::assemblyRevision).orElse(null));

        if ((verb == LifecycleVerb.START || verb == LifecycleVerb.RESUME)
                && !runRevision.equals(latest) && !reassemble) {
            throw new TapstateException(
                    LifecycleError.INCOMPATIBLE_REVISION, Map.of("requested", runRevision, "latest", latest), null);
        }
        // Having decided to re-assemble, the intent runs at the definition it will be built from.
        if (reassemble) {
            runRevision = latest;
        }

        // The assembly this intent describes follows the revision it runs at: a run verb adopts what it is
        // about to be built from, and the others carry forward what the pipeline is already running, so a
        // pause does not quietly re-point a paused pipeline at an edit made after it.
        //
        // An intent that recorded no assembly carries forward nothing rather than adopting the latest.
        // Adopting it would be a claim about a run this process never saw start: the pipeline is running
        // whatever it was built from, the stored definition may have moved since -- not every edit to a
        // running pipeline is refused, only one that moves a buffering switch -- and writing the current
        // artifact's assembly here would state that the run matches a definition nobody checked it
        // against. The next resume would then find the two equal and rebuild over a change that is not
        // safe to rebuild over. Unknown stays unknown, which costs a refusal that a start clears.
        String runAssembly = verb == LifecycleVerb.START || reassemble
                ? latestAssembly
                : prior.map(DesiredState::assemblyRevision).orElse(null);

        DesiredState next =
                new DesiredState(pipelineId, target, runRevision, purgeState, runAssembly, reassemble);
        return auditGate.dispatch(OPERATIONS.get(verb), new AuditContext(principal, pipelineId), () -> {
            desired.save(next);
            return next;
        });
    }
}
