package io.tapstate.core.lifecycle;

import java.util.Objects;

/**
 * The per-pipeline desired intent: one doc per pipeline stating the target state a user asked the
 * pipeline to reach, at the artifact revision that intent was expressed against. This is the desired
 * half of the split; the actual half is the epoch-fenced {@link CheckpointDoc}. Desired is plain
 * intent, written by the control side and never epoch-fenced; the converge side reads it and drives
 * the actual state toward it. The shape is an external contract — adding a field is backward
 * compatible, changing or removing one is a breaking change. The real Mongo serialization lives in an
 * adapter; this record is the shape.
 *
 * <ul>
 *   <li>{@code pipelineId} — the primary key, one desired doc per pipeline.</li>
 *   <li>{@code targetState} — the state the user wants the pipeline to reach.</li>
 *   <li>{@code revision} — the artifact revision the intent was expressed against.</li>
 *   <li>{@code purgeState} — whether reaching {@link PipelineState#STOPPED} also clears the state this
 *       pipeline accumulated. Only a stop expresses it; every other verb writes {@code false}, and so
 *       does a stored intent written before this field existed. It rides on the intent rather than
 *       being decided where the stop is carried out, because those are two moments in two processes:
 *       the user says it here, and the converge side is what eventually does it.</li>
 *   <li>{@code assemblyRevision} — what the run this intent describes is assembled from: the same
 *       canonical text {@code revision} is taken over, with the fields that only decide how a run is
 *       wired erased first. Two intents sharing it differ only in ways that building the run again
 *       re-reads. Null on an intent stored before this field existed, which reads as "unknown" and
 *       never as "unchanged" — the safe direction, since the only thing that turns on it is whether a
 *       refusal can be skipped.</li>
 *   <li>{@code reassemble} — whether reaching {@link PipelineState#RUNNING} must build the run afresh
 *       rather than continue the one being held. Only a resume over an edit that was safe to
 *       re-assemble expresses it; every other verb writes {@code false}. It rides on the intent for
 *       the same reason {@code purgeState} does: the decision is made here, and the converge side is
 *       what eventually acts on it — and it cannot be a second intent written behind the first,
 *       because the converge side samples the latest intent rather than consuming a queue of them, so
 *       a stop written and immediately overwritten by a start is never seen at all.</li>
 * </ul>
 */
public record DesiredState(
        String pipelineId, PipelineState targetState, String revision, boolean purgeState,
        String assemblyRevision, boolean reassemble) {

    public DesiredState {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(targetState, "targetState");
        Objects.requireNonNull(revision, "revision");
    }

    /**
     * An intent that builds nothing afresh and knows nothing about what its run was assembled from —
     * every verb but the resume that re-assembles, and every intent stored before those fields existed.
     */
    public DesiredState(String pipelineId, PipelineState targetState, String revision, boolean purgeState) {
        this(pipelineId, targetState, revision, purgeState, null, false);
    }

    /**
     * An intent that clears nothing — every verb but a stop, and every stop that was asked to keep what
     * the pipeline has. Keeping is the answer this shorthand is allowed to assume because it is the one
     * that destroys nothing: a caller who meant to clear has to say so, and the surface that takes the
     * verb refuses a stop that did not.
     */
    public DesiredState(String pipelineId, PipelineState targetState, String revision) {
        this(pipelineId, targetState, revision, false, null, false);
    }
}
