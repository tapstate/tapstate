package io.tapstate.core.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.lifecycle.NodeParallelism.Scope;
import io.tapstate.core.lifecycle.ParallelismPlanner.Planned;
import io.tapstate.core.lifecycle.ParallelismRequest.Kind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.api.Test;

/**
 * A cluster-wide target lands on the nearest width the members can represent: every member runs the same
 * number of processors, so only multiples of the member count are reachable, and of the two per-member counts
 * either side of an exact split the one whose total is closer wins - the smaller total when both are as close.
 *
 * <p>Each row pins one arithmetic answer so that an implementation that always rounds up, always rounds
 * down, or reads the target as a per-member count reddens on at least one of them: 8 on three members
 * separates nearest from floor (6), 7 separates nearest from ceiling (9), 6 is the exact split, 2 is the
 * target below the member count whose only candidate is one per member, and the four-member rows cover a
 * true tie.
 */
class TotalParallelismRoundsToTheNearestSafeNativeShapeTest {

    private static NodeParallelism plan(int target, int members) {
        ParallelismRequest request = new ParallelismRequest("enrich", Kind.TRANSFORM, target, null, false, 1024, 0);
        return ((Planned) ParallelismPlanner.plan(request, members, ParallelismBudget.DEFAULTS)).parallelism();
    }

    @ParameterizedTest(name = "{0} on {1} members -> {2} per member, {3} in total")
    @CsvSource({
            "8, 3, 3, 9, rounded-up",
            "7, 3, 2, 6, rounded-down",
            "6, 3, 2, 6, ",
            "2, 3, 1, 3, rounded-up",
            "4, 3, 1, 3, rounded-down",
            // 6 on four members: 1 per member gives 4, 2 gives 8 - both two away, so the smaller total.
            "6, 4, 1, 4, rounded-down",
            "10, 4, 2, 8, rounded-down",
            "8, 4, 2, 8, ",
            "5, 1, 5, 5, ",
    })
    void aTargetLandsOnTheNearestWidthTheMembersCanRepresent(
            int target, int members, int local, int effective, String rounding) {
        NodeParallelism shape = plan(target, members);

        assertThat(shape.scope()).isEqualTo(Scope.NATIVE);
        assertThat(shape.computedLocal()).isEqualTo(local);
        assertThat(shape.effective()).isEqualTo(effective);
        assertThat(shape.requested()).isEqualTo(target);
        assertThat(shape.memberCount()).isEqualTo(members);
        if (rounding == null) {
            assertThat(shape.reasons()).isEmpty();
        } else {
            assertThat(shape.reasons()).containsExactly(rounding);
        }
    }

    @Test
    void aTargetOfOneIsOneProcessorForTheWholeClusterNotOnePerMember() {
        // One per member on three members would be three processors, each with its own share of the
        // state; the engine's named exception is one processor in total, and it carries no per-member
        // count at all.
        NodeParallelism shape = plan(1, 3);

        assertThat(shape.scope()).isEqualTo(Scope.TOTAL_ONE);
        assertThat(shape.effective()).isEqualTo(1);
        assertThat(shape.computedLocal()).isNull();
        assertThat(shape.reasons()).containsExactly(NodeParallelism.REQUESTED_ONE);
    }

    @Test
    void theSameTargetIsWorkedOutAgainForADifferentMemberCount() {
        // After a member leaves, the next execution asks again: eight on two members is four each.
        assertThat(plan(8, 3).computedLocal()).isEqualTo(3);
        assertThat(plan(8, 2).computedLocal()).isEqualTo(4);
        assertThat(plan(8, 2).effective()).isEqualTo(8);
    }
}
