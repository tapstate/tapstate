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

    /** Resumes the paused pipeline, provided its paused revision is still the latest applied one. */
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

        // The revision the verb runs at: a fresh start adopts the latest; the other verbs carry forward the
        // revision the pipeline is already at. A run verb (start / resume) must run at the latest applied.
        String runRevision = verb == LifecycleVerb.START ? latest : prior.map(DesiredState::revision).orElse(latest);
        if ((verb == LifecycleVerb.START || verb == LifecycleVerb.RESUME) && !runRevision.equals(latest)) {
            throw new TapstateException(
                    LifecycleError.INCOMPATIBLE_REVISION, Map.of("requested", runRevision, "latest", latest), null);
        }

        DesiredState next = new DesiredState(pipelineId, target, runRevision, purgeState);
        return auditGate.dispatch(OPERATIONS.get(verb), new AuditContext(principal, pipelineId), () -> {
            desired.save(next);
            return next;
        });
    }
}
