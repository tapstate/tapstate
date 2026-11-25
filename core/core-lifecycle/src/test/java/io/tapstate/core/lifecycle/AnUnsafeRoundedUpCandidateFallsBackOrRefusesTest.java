package io.tapstate.core.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.lifecycle.ParallelismPlanner.Decision;
import io.tapstate.core.lifecycle.ParallelismPlanner.Planned;
import io.tapstate.core.lifecycle.ParallelismPlanner.Refused;
import io.tapstate.core.lifecycle.ParallelismRequest.Kind;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * A candidate width that breaks a limit is dropped before the nearest one is chosen, and a node whose every
 * candidate breaks one is refused rather than run over its limits. Choosing first and checking after would
 * pick the closer candidate and then either run it over budget or fail a start the other candidate would
 * have passed.
 */
class AnUnsafeRoundedUpCandidateFallsBackOrRefusesTest {

    private static Decision plan(ParallelismRequest request, int members, ParallelismBudget budget) {
        return ParallelismPlanner.plan(request, members, budget);
    }

    private static ParallelismRequest sink(int target) {
        return new ParallelismRequest("serve.out", Kind.SINK, target, null, false, 1024, 0);
    }

    @Test
    void theNearerCandidateOverTheConnectorBudgetGivesWayToTheSafeOne() {
        // Eight on three members is nearest at three each; with at most two connector instances per member,
        // three is ruled out and two each - six in total - is what the sink runs at.
        ParallelismBudget budget = new ParallelismBudget(16, 2, 262_144L, 128);

        Decision decision = plan(sink(8), 3, budget);

        assertThat(decision).isInstanceOfSatisfying(Planned.class, planned -> {
            assertThat(planned.parallelism().computedLocal()).isEqualTo(2);
            assertThat(planned.parallelism().effective()).isEqualTo(6);
            assertThat(planned.parallelism().reasons()).containsExactly(
                    NodeParallelism.ROUNDED_DOWN,
                    NodeParallelism.BUDGET_PREFIX + ParallelismBudget.MAX_CONNECTOR_INSTANCES_PER_MEMBER);
        });
    }

    @Test
    void aSinkCertifiedToShareItsConnectorIsNotHeldToTheInstanceBudgetByItsWidth() {
        // One shared instance per member, however many writers: the instance budget cannot rule it out.
        ParallelismBudget budget = new ParallelismBudget(16, 2, 262_144L, 128);
        ParallelismRequest shared = new ParallelismRequest("serve.out", Kind.SINK, 8, null, true, 1024, 0);

        assertThat(plan(shared, 3, budget)).isInstanceOfSatisfying(Planned.class,
                planned -> assertThat(planned.parallelism().computedLocal()).isEqualTo(3));
    }

    @Test
    void whenBothCandidatesBreakALimitTheNodeIsRefusedNamingEach() {
        ParallelismBudget budget = new ParallelismBudget(16, 1, 262_144L, 128);

        Decision decision = plan(sink(8), 3, budget);

        assertThat(decision).isInstanceOfSatisfying(Refused.class, refused -> {
            assertThat(refused.node()).isEqualTo("serve.out");
            assertThat(refused.requested()).isEqualTo(8);
            assertThat(refused.memberCount()).isEqualTo(3);
            assertThat(refused.singleton()).isNull();
            assertThat(refused.brokenLimits()).isEqualTo(Map.of(
                    2, ParallelismBudget.MAX_CONNECTOR_INSTANCES_PER_MEMBER,
                    3, ParallelismBudget.MAX_CONNECTOR_INSTANCES_PER_MEMBER));
        });
    }

    @Test
    void aTargetFarBeyondOneMemberIsRefusedAtTheProcessorLimit() {
        ParallelismRequest wide = new ParallelismRequest("enrich", Kind.TRANSFORM, 100, null, false, 1024, 0);

        assertThat(plan(wide, 1, ParallelismBudget.DEFAULTS)).isInstanceOfSatisfying(Refused.class,
                refused -> assertThat(refused.brokenLimits())
                        .isEqualTo(Map.of(100, ParallelismBudget.MAX_LOCAL_PARALLELISM)));
    }

    @Test
    void whatASinkBuffersCountsTheBatchInFlightAsWell() {
        // Four writers of 65536 rows each, two batches apiece, is 524288 rows on one member - over the
        // default of 262144 - while two writers fit exactly. A buffer count of one batch per writer would let
        // four through.
        ParallelismRequest wideBatches = new ParallelismRequest("serve.out", Kind.SINK, 4, null, false, 65_536, 0);

        assertThat(plan(wideBatches, 1, ParallelismBudget.DEFAULTS)).isInstanceOfSatisfying(Refused.class,
                refused -> assertThat(refused.brokenLimits())
                        .isEqualTo(Map.of(4, ParallelismBudget.MAX_BUFFERED_RECORDS_PER_MEMBER)));
        assertThat(plan(wideBatches, 2, ParallelismBudget.DEFAULTS)).isInstanceOf(Planned.class);
    }

    @Test
    void aNestIsHeldToTheThreadsItsVerticesHoldTogether() {
        // Thirty-three vertices that each hold a thread, at four per member, is 132 threads on each member.
        ParallelismRequest nest = new ParallelismRequest("doc", Kind.TRANSFORM, 4, null, false, 1024, 33);

        assertThat(plan(nest, 1, ParallelismBudget.DEFAULTS)).isInstanceOfSatisfying(Refused.class,
                refused -> assertThat(refused.brokenLimits())
                        .isEqualTo(Map.of(4, ParallelismBudget.MAX_BLOCKING_PROCESSORS_PER_MEMBER)));
        assertThat(plan(nest, 2, ParallelismBudget.DEFAULTS)).isInstanceOf(Planned.class);
    }
}
