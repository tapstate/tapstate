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
 * </ul>
 */
public record DesiredState(String pipelineId, PipelineState targetState, String revision, boolean purgeState) {

    public DesiredState {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(targetState, "targetState");
        Objects.requireNonNull(revision, "revision");
    }

    /**
     * An intent that clears nothing — every verb but a stop, and every stop that was asked to keep what
     * the pipeline has. Keeping is the answer this shorthand is allowed to assume because it is the one
     * that destroys nothing: a caller who meant to clear has to say so, and the surface that takes the
     * verb refuses a stop that did not.
     */
    public DesiredState(String pipelineId, PipelineState targetState, String revision) {
        this(pipelineId, targetState, revision, false);
    }
}
