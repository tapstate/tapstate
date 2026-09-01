package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Processor;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * What a case watching a job is told when the job dies underneath it.
 *
 * <p>The budget here is a long one and the assertion is that the wait ends nowhere near it. That is the
 * whole subject: a job that fails and a mechanism that is slow leave the watched collection looking
 * identical, so what separates them is only ever how the wait ends - at the job's death, naming it, or at
 * a deadline, naming the mechanism. A wait that runs its budget out has already lost the distinction, and
 * every reader after it pays for that once more.
 */
class JobWatchTest {

    private static final Duration LONG_BUDGET = Duration.ofSeconds(30);
    private static final String DOOM = "this processor was built to die";

    private HazelcastInstance member;
    private Job job;

    @BeforeEach
    void startMember() {
        Config config = new Config();
        config.getJetConfig().setEnabled(true);
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        member = Hazelcast.newHazelcastInstance(config);
    }

    @AfterEach
    void stopMember() {
        if (job != null) {
            try {
                job.cancel();
            } catch (IllegalStateException alreadyOver) {
                // A job that ended on its own is already gone; cancelling it is not a failure here.
            }
        }
        if (member != null) {
            member.shutdown();
        }
    }

    @Test
    void aJobThatDiesEndsTheWaitInsteadOfLettingItRunItsBudgetOut() {
        job = member.getJet().newJob(dagThat(true));

        long began = System.nanoTime();
        AssertionError told = assertThrows(AssertionError.class,
                () -> JobWatch.until(job, LONG_BUDGET, () -> false, () -> "nothing"));
        Duration waited = Duration.ofNanos(System.nanoTime() - began);

        assertThat(waited)
                .describedAs("the job was dead within a second, so a wait that lasted the budget is one "
                        + "that never asked the job anything")
                .isLessThan(Duration.ofSeconds(10));
        assertThat(told)
                .describedAs("what ended the wait has to be in what it says, or the reader is sent to the "
                        + "mechanism that was fine")
                .hasMessageContaining("job");
        assertThat(told.getCause())
                .describedAs("the job's own failure is the only thing here that says why, and it is "
                        + "already recorded - carrying it costs nothing and finding it again costs a run")
                .isNotNull();
        assertThat(told.getCause()).hasStackTraceContaining(DOOM);
    }

    @Test
    void aRunningJobThatDeliversIsNotDisturbed() {
        AtomicBoolean arrived = new AtomicBoolean();
        job = member.getJet().newJob(dagThat(false));

        JobWatch.until(job, LONG_BUDGET, () -> arrived.getAndSet(true), () -> "nothing");
    }

    /** One vertex: it either throws on its first turn or runs on forever without producing anything. */
    private static DAG dagThat(boolean dies) {
        DAG dag = new DAG();
        dag.newVertex("only", (SupplierEx<Processor>) () -> new AbstractProcessor() {

            @Override
            public boolean isCooperative() {
                return false;
            }

            @Override
            public boolean complete() {
                if (dies) {
                    throw new IllegalStateException(DOOM);
                }
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(20));
                return false;
            }
        });
        return dag;
    }
}
