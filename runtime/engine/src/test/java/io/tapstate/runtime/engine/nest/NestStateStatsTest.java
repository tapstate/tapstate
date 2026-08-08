package io.tapstate.runtime.engine.nest;

import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The counters that let a hit be told from a miss. The two are counted on different threads and in
 * different classes - a read is made by the processor, the trip behind it by the substrate - so what these
 * pin is that both sides reach the same counters and that a namespace's counts stay its own.
 */
class NestStateStatsTest {

    private HazelcastInstance member;

    @BeforeEach
    void startMember() {
        Config config = new Config();
        config.setClusterName("nest-state-stats-test-" + System.nanoTime());
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        member = Hazelcast.newHazelcastInstance(config);
    }

    @AfterEach
    void stopMember() {
        if (member != null) {
            member.shutdown();
        }
    }

    @Test
    @DisplayName("both sides of the member reach the same counters")
    void whatOneSideCountsTheOtherSideReads() {
        // Resolved twice, as the two sides do: neither holds a reference across the boundary between them,
        // so a second instance here would mean the processor's reads and the store's trips never meet.
        NestStateStats.of(member).access("nest.p.step.$root");
        NestStateStats.of(member).backfill("nest.p.step.$root", 5_000_000L);

        NestStateStats.Counted counted = NestStateStats.of(member).counted("nest.p.step.$root");
        assertThat(counted.accesses()).isOne();
        assertThat(counted.backfills()).isOne();
        assertThat(counted.backfillNanos()).isEqualTo(5_000_000L);
    }

    @Test
    void countsOneNamespaceApartFromAnother() {
        NestStateStats stats = NestStateStats.of(member);
        stats.access("nest.p.step.items");
        stats.access("nest.p.step.items");
        stats.access("nest.p.step.$root");

        // A resolver thrashing against its cold layer and an assembler that is not must stay tellable
        // apart; one number over both would read as neither.
        assertThat(stats.counted("nest.p.step.items").accesses()).isEqualTo(2);
        assertThat(stats.counted("nest.p.step.$root").accesses()).isOne();
    }

    @Test
    void anUncountedNamespaceIsAbsentRatherThanPresentAtZero() {
        NestStateStats stats = NestStateStats.of(member);

        assertThat(stats.knows("nest.p.step.$root"))
                .describedAs("a namespace nothing has read is not a namespace serving zero reads")
                .isFalse();
        assertThat(stats.counted("nest.p.step.$root").accesses()).isZero();
    }

    @Test
    void forgettingANamespaceTakesItsCountsWithIt() {
        NestStateStats stats = NestStateStats.of(member);
        stats.access("nest.p.step.$root");
        stats.backfill("nest.p.step.$root", 1_000L);

        stats.forget("nest.p.step.$root");

        // Kept, the counts would go on describing a run that is over, and a reader cannot tell a count
        // standing still from one that is merely quiet.
        assertThat(stats.knows("nest.p.step.$root")).isFalse();
        assertThat(stats.counted("nest.p.step.$root").backfills()).isZero();
    }
}
