package io.tapstate.runtime.engine.nest;

import io.tapstate.core.common.TapstateException;
import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Which paths the last run of a nest filed its state under, and where this run writes down its own. It is
 * the one thing a starting job can compare itself against: the state layer answers for the keys it is
 * asked about and never for the ones it is not, so a job addressing entirely the wrong paths is
 * indistinguishable from a job whose state is simply cold.
 *
 * <p>The engine does not decide where this is kept, for the same reason it does not decide where the state
 * itself is kept - that is a choice between a heap and a disk, and it belongs to whoever assembled the job.
 * What the engine does own is what is compared and what happens when it differs, which is why
 * {@link #reconcile} is static: an implementation supplies the memory, never the verdict.
 */
public interface NestStateLedger extends Serializable {

    /**
     * A ledger that remembers nothing, for a job assembled without one. Nothing is compared and nothing
     * is refused, which is the right answer where the state does not outlive the job either - a heap-held
     * nest starts empty every time, so there is nothing for a renamed path to abandon.
     */
    NestStateLedger NONE = new NestStateLedger() {

        private static final long serialVersionUID = 1L;

        @Override
        public Set<String> recall(String pipelineId, String nodeId) {
            return Set.of();
        }

        @Override
        public void record(String pipelineId, String nodeId, Set<String> paths) {
        }
    };

    /**
     * The paths this nest last filed its state under, empty if it has never run. Empty is also the answer
     * for a nest that ran and kept nothing, and the two do not have to be told apart: a nest with nothing
     * stored has nothing to abandon either way.
     */
    Set<String> recall(String pipelineId, String nodeId);

    /** Writes down {@code paths} as what this nest's state is filed under from here on. */
    void record(String pipelineId, String nodeId, Set<String> paths);

    /**
     * Refuses to start a nest whose state was written under other names than the ones it now compiles to.
     *
     * <p>State is addressed by the path its embed sits at - a namespace of that name where the embed has
     * children, and an entry of that name inside the parent's state where it does not. So renaming a path
     * or inserting a level renames the place the state is kept without moving anything into it: what was
     * written stays where nothing reads it, the new path answers nothing, and the tree rebuilds from empty
     * while the pipeline reports a resume. Every part of that is silent - no key is missing, no read
     * fails, and the only symptom is documents that lost their elements. Comparing the two sets before the
     * job is built is the one place the difference is visible at all.
     *
     * <p>The comparison is equality rather than "nothing went missing". A path that appears is as much a
     * divergence as one that disappears: it starts empty beside documents that are not, so the elements it
     * should hold are only the ones that happen to arrive from here on. Both directions are fixed the same
     * way and by the same words, so both are told the same way.
     *
     * <p>A first run has nothing to compare against and writes down what it compiled to. A refused one
     * writes down nothing: the record keeps naming where the state actually is, or the next attempt would
     * find its own paths already recorded and start onto the state this one refused to touch.
     */
    static void reconcile(NestStateLedger ledger, String pipelineId, String nodeId, Set<String> paths) {
        Set<String> recorded = ledger.recall(pipelineId, nodeId);
        if (recorded.isEmpty()) {
            ledger.record(pipelineId, nodeId, paths);
            return;
        }
        if (!recorded.equals(paths)) {
            throw new TapstateException(NestError.STATE_PATHS_CHANGED, Map.of(
                    "stepId", nodeId,
                    "recorded", render(recorded),
                    "compiled", render(paths)), null);
        }
    }

    /** The paths as one string, in an order that does not depend on how the set was built. */
    private static String render(Set<String> paths) {
        return paths.isEmpty() ? "none" : String.join(", ", new TreeSet<>(paths));
    }
}
