package io.tapstate.runtime.srs;

import static com.hazelcast.jet.core.Edge.between;
import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.config.SplitBrainProtectionConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionOn;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.spi.capture.SourcePosition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * What a source does when the cluster refuses the very first read of its change ring.
 *
 * <p>Every change ring is guarded by the cluster's split brain protection, and a member answers that
 * protection from a verdict its own library recomputes on membership changes. Just after a cluster forms
 * there is a window in which the verdict has not caught up, and every operation on the ring is refused --
 * including the one that works out where a fresh reader starts.
 *
 * <p>That refusal used to arrive while the processor was still being initialised, where there is nowhere
 * to wait and nothing to retry: the tasklet failed, the job was cancelled within a second of starting,
 * and nothing submitted another. The pipeline then sat with no execution and delivered nothing, which is
 * the same thing an operator sees as a run that is healthy and quiet.
 *
 * <p>The window is reproduced here the way it happens: a two-member cluster whose protection needs both,
 * with one member taken away. Nothing simulates the refusal -- the real protection refuses the real ring.
 */
class ASourceWhoseFirstRingReadIsRefusedWaitsForTheClusterTest {

    private static final String PROTECTION = "needs-both-members";
    private static final String RING = "srs.chain.refused-first-read";
    private static final String SINK = "out-refused-first-read";
    private static final String PIPELINE = "orders_pipeline";
    private static final int CHANGES = 3;

    private final String cluster = "srs-first-read-refusal-" + System.nanoTime();
    private HazelcastInstance first;
    private HazelcastInstance second;

    @AfterEach
    void stopMembers() {
        for (HazelcastInstance member : List.of(first, second)) {
            if (member != null && member.getLifecycleService().isRunning()) {
                member.shutdown();
            }
        }
    }

    @Test
    void aSourceRefusedItsFirstReadWaitsForTheClusterInsteadOfEndingTheRun() throws InterruptedException {
        first = Hazelcast.newHazelcastInstance(member());
        second = Hazelcast.newHazelcastInstance(member());
        // A member's verdict is worked out after it has started, so a write made before both have one is
        // refused for that reason alone - which is not the refusal this case is about.
        awaitProtectionSatisfied(first);
        awaitProtectionSatisfied(second);
        // Written while the protection is satisfied, so what the source later cannot reach is a ring that
        // demonstrably holds these changes rather than one that was never filled.
        fill(first, CHANGES);

        // The cluster no longer qualifies, so every operation on the ring is refused -- the state a member
        // is in between joining and its own verdict agreeing, held still for as long as the case needs.
        second.shutdown();
        Job job = first.getJet().newJob(sourceToList(), new JobConfig()
                .setName(PIPELINE)
                // The production configuration: the engine decides what replaces a run whose cluster
                // changed. Left on, Jet would restart this job when the member below rejoins, and the
                // restart -- not the source -- would be what got the rows across.
                .setProcessingGuarantee(ProcessingGuarantee.NONE)
                .setAutoScaling(false));

        // The discriminating half. A source that carries the refusal out of its initialisation ends the run
        // within about a second of starting; one that waits is still running, with nothing read. Reached in
        // two steps because a job that is about to die is briefly running too: the wait says it got as far
        // as running at all, and the settle after it says it stayed there.
        awaitRunning(job);
        Thread.sleep(2_000);
        assertThat(job.getStatus())
                .as("the run outlived a refusal that clears itself; ending it is what leaves the pipeline "
                        + "with no execution and nothing to deliver")
                .isEqualTo(JobStatus.RUNNING);
        assertThat(first.getList(SINK))
                .as("and it really was refused -- nothing was read while the cluster said no")
                .isEmpty();

        second = Hazelcast.newHazelcastInstance(member());

        awaitSize(first, CHANGES);
        assertThat(first.<String>getList(SINK))
                .as("once the cluster qualifies again the source reads from the start, so the changes "
                        + "buffered before the window are delivered rather than skipped")
                .hasSize(CHANGES);
        job.cancel();
    }

    /** A member of a cluster whose change rings are only readable while both members are present. */
    private Config member() {
        Config config = new Config();
        config.setClusterName(cluster);
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true).addMember("127.0.0.1");
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(2);
        config.addSplitBrainProtectionConfig(new SplitBrainProtectionConfig(PROTECTION, true)
                .setProtectOn(SplitBrainProtectionOn.READ_WRITE)
                .setMinimumClusterSize(2));
        config.addRingBufferConfig(new RingbufferConfig("srs.*")
                .setCapacity(8)
                .setInMemoryFormat(InMemoryFormat.OBJECT)
                .setTimeToLiveSeconds(0)
                // One backup, so the changes survive the member that leaves below. Without it the case
                // could be reading an empty ring rather than a refused one, and the two look alike.
                .setBackupCount(1)
                .setSplitBrainProtectionName(PROTECTION));
        config.getSerializationConfig().addSerializerConfig(
                new SerializerConfig().setImplementation(new SrsItemSerializer()).setTypeClass(SrsItem.class));
        return config;
    }

    /**
     * source -> render -> list. The rendering is not decoration: a change carries an envelope, and the list
     * the case reads has no serializer for one, so writing envelopes straight into it fails the run for a
     * reason that has nothing to do with what is being asked here.
     */
    private static DAG sourceToList() {
        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", SrsSourceProcessor.metaSupplier(
                PIPELINE, RING, "orders", StartFrom.earliest(), 1L, SrsReadCursorPublisherFactory.NONE,
                SourcePlacement.anyMember()));
        Vertex render = dag.newVertex("render",
                Processors.mapP((FunctionEx<Envelope, String>) envelope -> String.valueOf(envelope.after())));
        Vertex sink = dag.newVertex("sink", SinkProcessors.writeListP(SINK)).localParallelism(1);
        dag.edge(between(source, render)).edge(between(render, sink));
        return dag;
    }

    /** Until {@code member}'s protection admits the cluster, which it does once its first verdict is in. */
    private static void awaitProtectionSatisfied(HazelcastInstance member) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (!member.getSplitBrainProtectionService().getSplitBrainProtection(PROTECTION).hasMinimumSize()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("the protection never admitted the cluster it was started on");
            }
            Thread.sleep(25);
        }
    }

    private static void fill(HazelcastInstance member, int count) {
        SrsRingbuffer ring = new SrsRingbuffer(member.getRingbuffer(RING));
        for (int i = 0; i < count; i++) {
            ring.append(new SrsItem(new SourcePosition("w" + i), Op.INSERT, 1L, null, Map.of("id", i), 0L));
        }
    }

    /**
     * Waits for the job to be running, and says so at once if it died on the way instead.
     *
     * <p>A terminal status is not something to keep waiting through: it is the defect this case is about,
     * and waiting the whole budget out for it would report a timeout where there is an answer.
     */
    private static void awaitRunning(Job job) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (job.getStatus() != JobStatus.RUNNING) {
            JobStatus status = job.getStatus();
            if (status == JobStatus.FAILED || status == JobStatus.COMPLETED) {
                throw new AssertionError("the run ended instead of waiting out the refusal: " + status);
            }
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out waiting for the run to start, last saw " + status);
            }
            Thread.sleep(50);
        }
    }

    private static void awaitSize(HazelcastInstance member, int size) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (member.getList(SINK).size() < size) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError(
                        "timed out waiting for " + size + " changes, got " + member.getList(SINK).size());
            }
            Thread.sleep(50);
        }
    }
}
