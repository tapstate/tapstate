package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * What a surviving member is left holding when another member of its cluster goes away, and what the
 * engine will and will not tell it about why.
 *
 * <p>Two members, because one cannot show either fact: a single member losing a member is the member
 * going away, and there is nobody left to ask.
 *
 * <p>The first half is the consequence of submitting runs that the engine may not re-plan by itself. A
 * member leaving ends the run instead of quietly moving it, which is what leaves the product free to
 * decide who rebuilds it, under which claim, and with which execution generation -- decisions that a
 * re-plan underneath would have made already, invisibly and without any of them.
 *
 * <p>The second half is the more useful one, and it is written as a standing check on somebody else's
 * behaviour. A run ended by a member leaving and a run ended by a connector giving up arrive here as the
 * same class with no cause behind it; the only thing that differs is text inside the message. So the
 * product does not ask the engine why a run ended -- it asks its own committed membership whether the
 * cluster changed under that run. If this case ever goes red because the two became distinguishable,
 * that is worth knowing: it would mean the question could be asked of the engine after all.
 */
class ARunEndsWhenAMemberLeavesRatherThanBeingRePlannedTest {

    private static final String PIPELINE = "orders-pipe";

    private HazelcastInstance first;
    private HazelcastInstance second;

    @BeforeEach
    void startTwoMembers() {
        String cluster = "member-loss-" + System.nanoTime();
        first = Hazelcast.newHazelcastInstance(clustered(cluster));
        second = Hazelcast.newHazelcastInstance(clustered(cluster));
    }

    @AfterEach
    void stopMembers() {
        for (HazelcastInstance member : List.of(first, second)) {
            try {
                member.getLifecycleService().terminate();
            } catch (RuntimeException alreadyGone) {
                // A case that killed this member itself has nothing left to shut down.
            }
        }
    }

    @Test
    @DisplayName("a member leaving ends the run instead of the engine re-planning it onto what is left")
    void aMemberLeavingEndsTheRun() {
        Engine engine = new Engine(first);
        engine.submit(PIPELINE, foreverDag());
        awaitStatus(JobStatus.RUNNING);
        assertThat(first.getCluster().getMembers())
                .as("both members have to be in before one of them can leave")
                .hasSize(2);

        second.getLifecycleService().terminate();

        assertThat(awaitOver())
                .as("the run ends rather than being moved onto the surviving member by the engine, which "
                        + "would take no claim and allocate no execution generation for it")
                .isEqualTo(JobStatus.FAILED);
        assertThat(engine.hasLiveJob(PIPELINE)).isFalse();
        assertThat(engine.failureOf(PIPELINE)).isPresent();
    }

    @Test
    @DisplayName("the engine does not say why a run ended in any way that can be branched on")
    void whyARunEndedIsNotSomethingTheEngineAnswers() {
        Engine onSurvivor = new Engine(first);
        onSurvivor.submit(PIPELINE, foreverDag());
        awaitStatus(JobStatus.RUNNING);
        second.getLifecycleService().terminate();
        awaitOver();
        Throwable afterMemberLoss = onSurvivor.failureOf(PIPELINE).orElseThrow();

        Engine ordinary = new Engine(first);
        ordinary.submit("other-pipe", failingDag());
        awaitOver("other-pipe");
        Throwable afterAConnectorGaveUp = ordinary.failureOf("other-pipe").orElseThrow();

        assertThat(afterMemberLoss.getClass())
                .as("same class for both, so nothing here can be told apart by type")
                .isEqualTo(afterAConnectorGaveUp.getClass());
        assertThat(afterMemberLoss.getCause())
                .as("and no cause behind it either, so nothing can be told apart by walking the chain")
                .isNull();
        assertThat(afterAConnectorGaveUp.getCause()).isNull();
        assertThat(String.valueOf(afterMemberLoss.getMessage()))
                .as("the difference exists only as text inside the message, which is why the product asks "
                        + "its own committed membership instead of asking the engine")
                .contains("TopologyChangedException");
        assertThat(String.valueOf(afterAConnectorGaveUp.getMessage()))
                .doesNotContain("TopologyChangedException");
    }

    private JobStatus awaitOver() {
        return awaitOver(PIPELINE);
    }

    private JobStatus awaitOver(String pipelineId) {
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        JobStatus last = null;
        while (System.nanoTime() < deadline) {
            last = statusOf(pipelineId);
            if (last == JobStatus.FAILED || last == JobStatus.COMPLETED) {
                return last;
            }
            sleep(25);
        }
        throw new AssertionError("the run never ended; its last status was " + last);
    }

    private void awaitStatus(JobStatus expected) {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        JobStatus last = null;
        while (System.nanoTime() < deadline) {
            last = statusOf(PIPELINE);
            if (last == expected) {
                return;
            }
            sleep(25);
        }
        throw new AssertionError("the run never reached " + expected + "; its last status was " + last);
    }

    private JobStatus statusOf(String pipelineId) {
        try {
            return first.getJet().getJob(pipelineId) == null
                    ? null : first.getJet().getJob(pipelineId).getStatus();
        } catch (RuntimeException notYet) {
            return null;
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    /** A member configured as the app configures one, joining others of the same cluster name. */
    private static Config clustered(String cluster) {
        Config config = new Config();
        config.setClusterName(cluster);
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(2);
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.getNetworkConfig().setPort(15801).setPortAutoIncrement(true).setPortCount(2);
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(true).setMembers(List.of("127.0.0.1:15801", "127.0.0.1:15802"));
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        return config;
    }

    private static DAG foreverDag() {
        DAG dag = new DAG();
        dag.newVertex("forever", ProcessorMetaSupplier.forceTotalParallelismOne(
                ProcessorSupplier.of((SupplierEx<Processor>) ForeverSource::new)));
        return dag;
    }

    /** A run that ends for a reason of its own, so the two endings can be compared. */
    private static DAG failingDag() {
        DAG dag = new DAG();
        dag.newVertex("boom", ProcessorMetaSupplier.forceTotalParallelismOne(
                ProcessorSupplier.of((SupplierEx<Processor>) GivingUpSource::new)));
        return dag;
    }

    private static final class ForeverSource extends AbstractProcessor {
        @Override
        public boolean isCooperative() {
            return false;
        }

        @Override
        public boolean complete() {
            sleep(5);
            return false;
        }
    }

    private static final class GivingUpSource extends AbstractProcessor {
        @Override
        public boolean isCooperative() {
            return false;
        }

        @Override
        public boolean complete() {
            throw new IllegalStateException("the connector gave up");
        }
    }
}
