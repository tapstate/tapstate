package io.tapstate.runtime.engine.nest;

import static io.tapstate.runtime.engine.nest.NestFixtures.at;
import static io.tapstate.runtime.engine.nest.NestFixtures.noPositions;
import static io.tapstate.runtime.engine.nest.NestFixtures.row;
import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.model.Embed;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.RollupField;
import io.tapstate.core.model.RollupFunction;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * How heavy state is to keep is counted where it is already known: a commit group reports each key's state as
 * it was written, and a render reports how long each level's rollup fields took to fold.
 */
class HowHeavyStateIsToKeepIsCountedTest {

    @Test
    void aCommitReportsWhatItWroteAndTheStatsKeepEachKeyTheGroupAndTheLargest() {
        HazelcastInstance member = Hazelcast.newHazelcastInstance(new Config().setClusterName("weight-" + System.nanoTime()));
        try {
            NestStateStats stats = NestStateStats.of(member);
            AtomicReference<List<byte[]>> reported = new AtomicReference<>();
            NestStateIo.Committed committed = new NestStateIo.Committed() {
                @Override
                public void group(long keys, long roundTrips, long nanos) {
                }

                @Override
                public void sizes(java.util.Collection<byte[]> written) {
                    reported.set(new ArrayList<>(written));
                    stats.wrote("ns", written);
                }
            };
            NestStateIo.commit(committed, List.of(new byte[300], new byte[1_500_000]), () -> { });
            NestStateIo.commit(committed, List.of(new byte[2_000]), () -> { });
            stats.folded("ns", 250_000);

            assertThat(reported.get()).hasSize(1);
            NestStateStats.Weight weight = stats.weight("ns");
            assertThat(weight.documents()).isEqualTo(3);
            assertThat(weight.documentBytes()).isEqualTo(1_502_300);
            assertThat(weight.largestDocument()).isEqualTo(1_500_000);
            assertThat(weight.groups()).isEqualTo(2);
            assertThat(weight.groupBytes()).isEqualTo(1_502_300);
            assertThat(weight.folds()).isEqualTo(1);
            assertThat(weight.foldNanos()).isEqualTo(250_000);
            assertThat(java.util.Arrays.stream(weight.documentBuckets()).sum()).isEqualTo(3);
        } finally {
            member.shutdown();
        }
    }

    @Test
    void aRenderReportsOneFoldPerRollupLevelAndNoneWithoutRollups() {
        Embed policies = new Embed("policy", Map.of("customer_id", "customer_id"), EmbedAs.ARRAY, "policies", null,
                List.of("policy_no"), null, null, null,
                Map.of("n", RollupField.of(RollupFunction.COUNT, true, null, null, null)));
        assertThat(folds(policies)).isEqualTo(1);
        Embed plain = new Embed("policy", Map.of("customer_id", "customer_id"), EmbedAs.ARRAY, "policies", null,
                List.of("policy_no"), null, null, null, null, null);
        assertThat(folds(plain)).isZero();
    }

    private static int folds(Embed embed) {
        NestTopology topology = NestTopology.compile("p", "doc", NestTreeFixtures.nest("customer",
                List.of("customer_id"), embed), NestTreeFixtures.tables());
        RootAssembly root = new RootAssembly();
        root.applyRoot(row("customer_id", "C1"), at(1));
        root.applyElement(new ElementRef(List.of("policies"), null, List.of("P1"), "P1"),
                row("policy_no", "P1", "customer_id", "C1"), at(2), noPositions());
        int[] count = {0};
        root.render(topology.slots(), Map.of(), "C1", nanos -> count[0]++);
        return count[0];
    }
}
