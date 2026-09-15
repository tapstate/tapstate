package io.tapstate.runtime.engine.join;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The two ends of the contract a rebuild's progress rides on: what it is named among a run's statistics
 * and what a name is read back as.
 *
 * <p>The two ends are tested together because a break in either is spelled the same way at the other end
 * - a pipeline that reports no rebuild at all. Nothing fails, no job dies, and the one reading that says
 * a large fan-out is under way is simply absent, which is also what a pipeline with no large fan-out
 * looks like.
 *
 * <p>Both halves of what a reading is about carry separators of their own: the namespace is a map name
 * built from a pipeline, a step and a source, and the dimension key is a value out of the user's own
 * table. So the cases below put a separator in each half - a parse that split anywhere but where the
 * name was joined would pass on the simple names and lose the key on real ones.
 */
class JoinRecomputeMetricNamesTest {

    /** A real namespace shape: the reverse index of one source of one step of one pipeline. */
    private static final String NAMESPACE = "join.order_pipeline.widen.index.customers";

    /** A dimension key that is a value out of a user's table, and carries a dot of its own. */
    private static final String DIMENSION_KEY = "acme.co/17";

    @Test
    @DisplayName("a rows-done name reads back as the whole subject it was built from")
    void aRowsDoneNameRoundTrips() {
        String name = JoinRecomputeMetricNames.doneNameOf(NAMESPACE, DIMENSION_KEY);

        assertThat(JoinRecomputeMetricNames.doneSubjectOf(name))
                .as("both halves carry separators, so only splitting where the name was joined survives")
                .isEqualTo(JoinRecomputeMetricNames.subjectOf(NAMESPACE, DIMENSION_KEY));
    }

    @Test
    @DisplayName("a rows-expected name reads back as the same subject")
    void aRowsExpectedNameRoundTrips() {
        String name = JoinRecomputeMetricNames.expectedNameOf(NAMESPACE, DIMENSION_KEY);

        assertThat(JoinRecomputeMetricNames.expectedSubjectOf(name))
                .isEqualTo(JoinRecomputeMetricNames.subjectOf(NAMESPACE, DIMENSION_KEY));
    }

    /**
     * The two readings are about one rebuild and are published together, so a parse that took both would
     * read every rebuild's progress as its size and never say the two had diverged - which is the whole
     * of what "how far along is it" means.
     */
    @Test
    @DisplayName("neither reading answers to the other one's name")
    void theTwoReadingsDoNotAnswerToEachOther() {
        String done = JoinRecomputeMetricNames.doneNameOf(NAMESPACE, DIMENSION_KEY);
        String expected = JoinRecomputeMetricNames.expectedNameOf(NAMESPACE, DIMENSION_KEY);

        assertThat(JoinRecomputeMetricNames.expectedSubjectOf(done)).isNull();
        assertThat(JoinRecomputeMetricNames.doneSubjectOf(expected)).isNull();
    }

    /**
     * A run's statistics carry every other reading the product publishes on the same collection. One of
     * these picked out of that stream by a loose prefix would report a nest namespace as a join rebuild.
     */
    @Test
    @DisplayName("a reading that is not one of these is not read as one")
    void anUnrelatedMetricIsNotOneOfThese() {
        assertThat(JoinRecomputeMetricNames.doneSubjectOf("nestState.entries.nest.p.doc.items")).isNull();
        assertThat(JoinRecomputeMetricNames.expectedSubjectOf("executionStartTime")).isNull();
    }

    /**
     * The threshold is the whole difference between a number an operator watches and a stream they learn
     * to ignore. Its own boundary is asserted rather than a value comfortably past it, because an
     * off-by-one here shows up as nothing at all: a rebuild exactly at the size worth reporting simply
     * never appears, and no other reading contradicts it.
     */
    @Test
    @DisplayName("a fan-out at the reporting size is reported and one below it is not")
    void theThresholdIsAtItsOwnBoundary() {
        assertThat(JoinRecomputeMetricNames.worthReporting(
                JoinRecomputeMetricNames.REPORT_FANOUT_ABOVE - 1)).isFalse();
        assertThat(JoinRecomputeMetricNames.worthReporting(
                JoinRecomputeMetricNames.REPORT_FANOUT_ABOVE)).isTrue();
        assertThat(JoinRecomputeMetricNames.worthReporting(
                JoinRecomputeMetricNames.REPORT_FANOUT_ABOVE * 10)).isTrue();
    }
}
