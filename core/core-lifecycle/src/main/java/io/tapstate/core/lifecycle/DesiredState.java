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
 *       pipeline accumulated. A stop expresses it, and so does the start that supersedes a stop the
 *       converge side has not read yet -- there it says what the tear-down half of that rebuild
 *       clears, which is the only place the user's answer to it survives being overwritten. Every
 *       other verb writes {@code false}, and so does a stored intent written before this field existed. It rides on the intent rather than
 *       being decided where the stop is carried out, because those are two moments in two processes:
 *       the user says it here, and the converge side is what eventually does it.</li>
 *   <li>{@code assemblyRevision} — what the run this intent describes is assembled from: the same
 *       canonical text {@code revision} is taken over, with the fields that only decide how a run is
 *       wired erased first. Two intents sharing it differ only in ways that building the run again
 *       re-reads. Null on an intent stored before this field existed, which reads as "unknown" and
 *       never as "unchanged" — the safe direction, since the only thing that turns on it is whether a
 *       refusal can be skipped.</li>
 *   <li>{@code rebuiltAtStateEpoch} — the fencing epoch the pipeline's actual state stood at when this
 *       intent was written, on the one intent that has to be acted on once rather than held true:
 *       a start superseding a stop the converge side has not read yet. The instruction is spent by
 *       being carried out, because carrying it out advances that epoch, so nothing has to go back and
 *       take it off the intent -- and nothing may, since intent is the control layer's to write. It
 *       doubles as the check on whether the stop really was lost: an epoch that has moved on means the
 *       stop was converged after all, and then an ordinary start is exactly right. Null on every other
 *       intent, and on every intent stored before this field existed.</li>
 *   <li>{@code reassemble} — whether reaching {@link PipelineState#RUNNING} must build the run afresh
 *       rather than continue the one being held. Two verbs express it: a resume over an edit that was
 *       safe to re-assemble, and a start written while a stop is still the pipeline's intent -- the
 *       pair a restart is, which cannot arrive as two intents for the reason given right below. Every
 *       other verb writes {@code false}. It rides on the intent for
 *       the same reason {@code purgeState} does: the decision is made here, and the converge side is
 *       what eventually acts on it — and it cannot be a second intent written behind the first,
 *       because the converge side samples the latest intent rather than consuming a queue of them, so
 *       a stop written and immediately overwritten by a start is never seen at all.</li>
 * </ul>
 */
public record DesiredState(
        String pipelineId, PipelineState targetState, String revision, boolean purgeState,
        String assemblyRevision, boolean reassemble, Long rebuiltAtStateEpoch) {

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
        this(pipelineId, targetState, revision, purgeState, null, false, null);
    }

    /** An intent that is held true for as long as it stands, rather than carried out once. */
    public DesiredState(
            String pipelineId, PipelineState targetState, String revision, boolean purgeState,
            String assemblyRevision, boolean reassemble) {
        this(pipelineId, targetState, revision, purgeState, assemblyRevision, reassemble, null);
    }

    /**
     * An intent that clears nothing — every verb but a stop, and every stop that was asked to keep what
     * the pipeline has. Keeping is the answer this shorthand is allowed to assume because it is the one
     * that destroys nothing: a caller who meant to clear has to say so, and the surface that takes the
     * verb refuses a stop that did not.
     */
    public DesiredState(String pipelineId, PipelineState targetState, String revision) {
        this(pipelineId, targetState, revision, false, null, false, null);
    }
}
