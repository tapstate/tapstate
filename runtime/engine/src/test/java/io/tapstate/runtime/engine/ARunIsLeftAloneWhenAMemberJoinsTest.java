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
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * A member joining does not disturb a run that is already going. The run keeps the members it was
 * planned over, and the new one is given nothing until somebody asks for a rebalance -- which is a
 * decision with a plan behind it, not something that should happen because a process started.
 *
 * <p>What is measured is how many times the run's source was started, not what the job configuration
 * says. A configuration value proves what was set; it does not prove what the engine does with it, and
 * that is the whole of what this case is for. Re-planning restarts the run, so a second start is
 * visible from inside the source itself.
 *
 * <p>The counter is also why this case can assert that nothing happened: the same case with the engine
 * allowed to re-plan reaches two starts well inside the window below, so "still one" is a measurement
 * rather than a wait that was not long enough to see anything.
 */
class ARunIsLeftAloneWhenAMemberJoinsTest {

    private static final String PIPELINE = "orders-pipe";

    /** How many times the run's single source has been started, across both members of this JVM. */
    private static final AtomicInteger STARTS = new AtomicInteger();

    private String cluster;
    private HazelcastInstance first;
    private HazelcastInstance second;

    @BeforeEach
    void startOneMember() {
        STARTS.set(0);
        cluster = "member-join-" + System.nanoTime();
        first = Hazelcast.newHazelcastInstance(clustered(cluster));
    }

    @AfterEach
    void stopMembers() {
        for (HazelcastInstance member : new HazelcastInstance[] {first, second}) {
            if (member == null) {
                continue;
            }
            try {
                member.getLifecycleService().terminate();
            } catch (RuntimeException alreadyGone) {
                // Nothing left to shut down.
            }
        }
    }

    @Test
    @DisplayName("a member joining leaves the run where it is instead of the engine re-planning it")
    void aMemberJoiningDoesNotRestartTheRun() {
        Engine engine = new Engine(first);
        engine.submit(PIPELINE, foreverDag());
        awaitRunning();
        assertThat(STARTS.get()).as("one run, started once").isEqualTo(1);
        long idBefore = first.getJet().getJob(PIPELINE).getId();

        second = Hazelcast.newHazelcastInstance(clustered(cluster));
        awaitBothMembersSeeEachOther();

        assertThat(startsWithin(Duration.ofSeconds(15)))
                .as("the run is not started a second time: the engine may not re-plan it onto the member "
                        + "that joined, because a run moved that way takes no claim and is allocated no "
                        + "execution generation, so nothing could tell it from the one it replaced")
                .isEqualTo(1);
        assertThat(first.getJet().getJob(PIPELINE).getStatus()).isEqualTo(JobStatus.RUNNING);
        assertThat(first.getJet().getJob(PIPELINE).getId())
                .as("and it is the same run throughout").isEqualTo(idBefore);
        assertThat(second.getJet().getJob(PIPELINE))
                .as("the member that joined can still see the job -- job identity is cluster-wide, and "
                        + "not being given work is not the same as not knowing about it")
                .isNotNull();
    }

    /** The highest start count seen over {@code window}, so a late restart is caught rather than missed. */
    private int startsWithin(Duration window) {
        long deadline = System.nanoTime() + window.toNanos();
        int highest = STARTS.get();
        while (System.nanoTime() < deadline) {
            highest = Math.max(highest, STARTS.get());
            if (highest > 1) {
                return highest;
            }
            sleep(25);
        }
        return highest;
    }

    private void awaitRunning() {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        JobStatus last = null;
        while (System.nanoTime() < deadline) {
            last = first.getJet().getJob(PIPELINE) == null
                    ? null : first.getJet().getJob(PIPELINE).getStatus();
            if (last == JobStatus.RUNNING) {
                return;
            }
            sleep(25);
        }
        throw new AssertionError("the run never reached RUNNING; its last status was " + last);
    }

    private void awaitBothMembersSeeEachOther() {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < deadline) {
            if (first.getCluster().getMembers().size() == 2 && second.getCluster().getMembers().size() == 2) {
                return;
            }
            sleep(25);
        }
        throw new AssertionError("the second member never joined: "
                + first.getCluster().getMembers().size() + " and " + second.getCluster().getMembers().size());
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
        // A range of its own: another case in this module binds the one below, and two clusters sharing
        // a port range find each other's members while a build runs them one after the other.
        config.getNetworkConfig().setPort(15811).setPortAutoIncrement(true).setPortCount(2);
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(true).setMembers(List.of("127.0.0.1:15811", "127.0.0.1:15812"));
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        return config;
    }

    private static DAG foreverDag() {
        DAG dag = new DAG();
        dag.newVertex("forever", ProcessorMetaSupplier.forceTotalParallelismOne(
                ProcessorSupplier.of((SupplierEx<Processor>) CountingForeverSource::new)));
        return dag;
    }

    /** A source that never ends, and that says every time it is started. */
    private static final class CountingForeverSource extends AbstractProcessor {

        @Override
        protected void init(Processor.Context context) {
            STARTS.incrementAndGet();
        }

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
}
