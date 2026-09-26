package io.tapstate.core.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * A run's plan, written after an earlier run of the same pipeline, names the run it replaced and says of each node
 * whose width moved what it was and which inputs of the working-out moved it: the member count, the node's target,
 * or what bounds it. The three are named apart because each is answered by someone else - a lost member by
 * whoever runs the machines, a target by the pipeline's author, a bound by whoever owns the connector or the
 * server's budgets - and one reason covering all three would send every reader to all three.
 */
class AWidthThatMovedSaysWhatMovedItTest {

    private static final Instant BEFORE = Instant.parse("2026-09-26T10:00:00Z");
    private static final Instant NOW = Instant.parse("2026-09-26T11:00:00Z");

    @Test
    void aRunRebuiltOnFewerMembersSaysTheMemberCountMovedTheWidthAndNamesTheRunItReplaced() {
        ExecutionPlan before = plan(6L, List.of("m1", "m2", "m3"), node("sink", 8, 3, 3, 9, List.of("rounded-up")));
        ExecutionPlan after = plan(7L, List.of("m1", "m2"), node("sink", 8, 2, 4, 8, List.of()));

        ExecutionPlan replacing = after.replacing(before);

        assertThat(replacing.replaces()).isEqualTo(new ExecutionPlan.Replaced(6L, List.of("m1", "m2", "m3"), BEFORE));
        // The reasons moved too - nine was a rounding up, eight is exact - but only because the member count
        // did: the reasons are the working-out's own account of the members it was given.
        assertThat(replacing.nodes().get(0).change()).isEqualTo(new ExecutionPlan.Change(9, List.of(
                ExecutionPlan.Change.MEMBERS_CHANGED, ExecutionPlan.Change.CAPABILITY_CHANGED)));
    }

    @Test
    void aNewTargetOnTheSameMembersSaysTheTargetMovedTheWidth() {
        ExecutionPlan before = plan(6L, List.of("m1", "m2", "m3"), node("sink", 3, 3, 1, 3, List.of()));
        ExecutionPlan after = plan(7L, List.of("m1", "m2", "m3"), node("sink", 6, 3, 2, 6, List.of()));

        assertThat(after.replacing(before).nodes().get(0).change())
                .isEqualTo(new ExecutionPlan.Change(3, List.of(ExecutionPlan.Change.TARGET_CHANGED)));
    }

    @Test
    void aBoundThatMovedOnTheSameMembersAndTargetSaysTheCapabilityMovedTheWidth() {
        // The same target on the same members, held to one this time: every row now lands in one table with no
        // key, which is what bounds the node, not anything its author or the cluster did.
        ExecutionPlan before = plan(6L, List.of("m1", "m2"), node("sink", 4, 2, 2, 4, List.of()));
        ExecutionPlan after = plan(7L, List.of("m1", "m2"), totalOne("sink", 4, 2, List.of("single-target-keyless")));

        assertThat(after.replacing(before).nodes().get(0).change())
                .isEqualTo(new ExecutionPlan.Change(4, List.of(ExecutionPlan.Change.CAPABILITY_CHANGED)));
    }

    @Test
    void aNodeWhoseWidthDidNotMoveSaysNothingAboutTheRunBeforeAndANewNodeHadNone() {
        ExecutionPlan before = plan(6L, List.of("m1", "m2"), node("sink", 4, 2, 2, 4, List.of()));
        ExecutionPlan after = plan(7L, List.of("m1", "m2"), node("sink", 4, 2, 2, 4, List.of()),
                node("step", 1, 2, 1, 2, List.of()));

        ExecutionPlan replacing = after.replacing(before);

        assertThat(replacing.nodes()).extracting(ExecutionPlan.Node::change).containsExactly(null, null);
        assertThat(replacing.replaces()).as("the run it replaced is named whether or not a width moved").isNotNull();
    }

    @Test
    void theFirstPlanOfAPipelineReplacesNothing() {
        ExecutionPlan first = plan(1L, List.of("m1"), node("sink", 4, 1, 4, 4, List.of()));

        assertThat(first.replacing(null)).isSameAs(first);
    }

    @Test
    void membersThatJoinedAfterARunWasPlannedAreTheOnesItWasNotWorkedOutFor() {
        ExecutionPlan planned = plan(7L, List.of("m1", "m2", "m3"), node("sink", 8, 3, 3, 9, List.of()));

        assertThat(planned.notPlannedFor(List.of("m1", "m2", "m3", "m4"))).containsExactly("m4");
        assertThat(planned.notPlannedFor(List.of("m1", "m3")))
                .as("a member the plan has and the cluster lost is not one awaiting anything")
                .isEmpty();
    }

    private static ExecutionPlan plan(Long execution, List<String> members, ExecutionPlan.Node... nodes) {
        return new ExecutionPlan("p", 1L, execution, 3L, members, List.of(nodes), execution == 6L ? BEFORE : NOW);
    }

    private static ExecutionPlan.Node node(String id, int requested, int members, int local, int effective,
            List<String> reasons) {
        return new ExecutionPlan.Node(id, requested, "explicit", "native", members, local, effective, reasons, 1024,
                0L, List.of(id));
    }

    private static ExecutionPlan.Node totalOne(String id, int requested, int members, List<String> reasons) {
        return new ExecutionPlan.Node(id, requested, "explicit", "total-one", members, null, 1, reasons, 1024, 0L,
                List.of(id));
    }
}
