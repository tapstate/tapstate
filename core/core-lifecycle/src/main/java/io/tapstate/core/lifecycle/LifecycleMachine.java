package io.tapstate.core.lifecycle;

import io.tapstate.core.common.TapstateException;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 * The pipeline lifecycle state machine: which verb is legal from which state, and the state a legal
 * verb lands in. Pure functions over {@link PipelineState} and {@link LifecycleVerb}, single-sourced
 * so intent validation, converge-side driving, and command-surface rendering all read one table. A
 * rejected verb raises the {@code lifecycle.illegal-transition} coded error.
 *
 * <p>This is the intent state machine only — the four user verbs. The system-driven transitions
 * (a bounded source finishing into {@link PipelineState#COMPLETED}, a scheduled re-run leaving it)
 * belong to the converge side and are not modelled here.
 */
public final class LifecycleMachine {

    /** The states each verb may be issued from. */
    private static final Map<LifecycleVerb, Set<PipelineState>> LEGAL_FROM = new EnumMap<>(LifecycleVerb.class);

    /** The state each verb lands in (a function of the verb alone; the origin only gates legality). */
    private static final Map<LifecycleVerb, PipelineState> TARGET = new EnumMap<>(LifecycleVerb.class);

    static {
        LEGAL_FROM.put(LifecycleVerb.START, EnumSet.of(PipelineState.NEW, PipelineState.STOPPED, PipelineState.COMPLETED));
        LEGAL_FROM.put(LifecycleVerb.PAUSE, EnumSet.of(PipelineState.RUNNING));
        LEGAL_FROM.put(LifecycleVerb.RESUME, EnumSet.of(PipelineState.PAUSED));
        // STOP is legal from FAILED so a failed run can be cleared to STOPPED and started fresh from there;
        // START is not legal directly from FAILED — recovery is a deliberate stop, then a start.
        // A completed run may also be explicitly stopped to preserve or clear its retained state.
        LEGAL_FROM.put(LifecycleVerb.STOP, EnumSet.of(
                PipelineState.RUNNING, PipelineState.PAUSED, PipelineState.FAILED, PipelineState.COMPLETED));

        TARGET.put(LifecycleVerb.START, PipelineState.RUNNING);
        TARGET.put(LifecycleVerb.PAUSE, PipelineState.PAUSED);
        TARGET.put(LifecycleVerb.RESUME, PipelineState.RUNNING);
        TARGET.put(LifecycleVerb.STOP, PipelineState.STOPPED);
    }

    private LifecycleMachine() {
    }

    /**
     * Applies {@code verb} to {@code from} and returns the resulting state, or raises the
     * {@code lifecycle.illegal-transition} coded error when the verb is not legal from that state.
     */
    public static PipelineState transition(PipelineState from, LifecycleVerb verb) {
        if (!isLegal(from, verb)) {
            throw new TapstateException(
                    LifecycleError.ILLEGAL_TRANSITION,
                    Map.of("from", from, "verb", verb.id()),
                    null);
        }
        return TARGET.get(verb);
    }

    /** Whether {@code verb} may be issued from {@code from}. */
    public static boolean isLegal(PipelineState from, LifecycleVerb verb) {
        return LEGAL_FROM.get(verb).contains(from);
    }

    /** The verbs available from {@code from} — the actions a command surface should offer there. */
    public static Set<LifecycleVerb> legalVerbs(PipelineState from) {
        Set<LifecycleVerb> verbs = EnumSet.noneOf(LifecycleVerb.class);
        for (LifecycleVerb verb : LifecycleVerb.values()) {
            if (isLegal(from, verb)) {
                verbs.add(verb);
            }
        }
        return verbs;
    }
}
