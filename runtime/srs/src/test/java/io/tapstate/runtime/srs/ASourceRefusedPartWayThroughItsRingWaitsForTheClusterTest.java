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
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.core.Vertex;
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
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * What a source does when the cluster starts refusing its change ring after it has been reading it.
 *
 * <p>A ring lives in one partition, and a partition moves when a member joins. From then on every read of
 * that ring runs on the new owner, whose split brain protection answers from a verdict it has only just
 * begun to compute, so until that verdict agrees each read the source makes is refused -- the tail it reads
 * up to as much as the changes themselves. It is the refusal a forming cluster gives a reader that is
 * still being positioned, and it clears the same way, seconds later.
 *
 * <p>A source that carried it out of its read ended the run there, and the pipeline was recorded as failed
 * for a person: a member joining had stopped a pipeline that was running. That is what a two-member
 * cluster gaining a third found.
 *
 * <p>The protection's answer is set here rather than a member added, so that nothing else about the
 * cluster changes while the source is refused: the engine sees the same member throughout, and the only
 * thing the run can react to is the refusal. The protection doing the refusing is the real one, guarding
 * the real ring.
 */
class ASourceRefusedPartWayThroughItsRingWaitsForTheClusterTest {

    private static final String PROTECTION = "admits-while-told-to";
    private static final String RING = "srs.chain.refused-part-way";
    private static final String SINK = "out-refused-part-way";
    private static final String PIPELINE = "orders_pipeline";
    private static final int BEFORE = 3;
    private static final int AFTER = 2;

    /** The protection's whole answer. Static, because the member holds the function it is read by. */
    private static final AtomicBoolean ADMITS = new AtomicBoolean(true);

    private final String cluster = "srs-part-way-refusal-" + System.nanoTime();
    private HazelcastInstance member;

    @AfterEach
    void stopMember() {
        ADMITS.set(true);
        if (member != null && member.getLifecycleService().isRunning()) {
            member.shutdown();
        }
    }

    @Test
    void aSourceRefusedAfterItHasStartedReadingWaitsForTheClusterInsteadOfEndingTheRun()
            throws InterruptedException {
        member = Hazelcast.newHazelcastInstance(member());
        SrsRingbuffer ring = new SrsRingbuffer(member.getRingbuffer(RING));
        append(ring, 0, BEFORE);
        Job job = member.getJet().newJob(sourceToList(), new JobConfig()
                .setName(PIPELINE)
                // As in production, the engine never re-plans a run on its own; left on, a restart rather
                // than the source could be what carried the run through.
                .setProcessingGuarantee(ProcessingGuarantee.NONE)
                .setAutoScaling(false));
        // Read before the refusal, so what is refused below is a reader already under way rather than one
        // still being positioned.
        awaitSize(BEFORE);

        ADMITS.set(false);
        awaitRingRefuses(ring);
        // A source that carries the refusal out of its read ends the run within a pass; one that waits is
        // still running after this, with nothing more read.
        Thread.sleep(2_000);
        assertThat(job.getStatus())
                .as("the run outlived a refusal that clears itself; ending it is what left a running "
                        + "pipeline failed when a member joined")
                .isEqualTo(JobStatus.RUNNING);

        ADMITS.set(true);
        awaitRingAnswers(ring);
        append(ring, BEFORE, AFTER);
        awaitSize(BEFORE + AFTER);
        // In any order: the rendering between the source and the list runs in more than one instance.
        assertThat(member.<String>getList(SINK))
                .as("once the ring answers again the reader carries on from where it was refused: the "
                        + "later changes arrive, and none of the earlier ones a second time")
                .containsExactlyInAnyOrder("{id=0}", "{id=1}", "{id=2}", "{id=3}", "{id=4}");
        job.cancel();
    }

    /** A member whose change rings are guarded by a protection that answers whatever this case says. */
    private Config member() {
        Config config = new Config();
        config.setClusterName(cluster);
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        // The protection's answer is recomputed on this timer, so a second rather than the default five
        // keeps each flip below from waiting out a long interval.
        config.setProperty("hazelcast.heartbeat.interval.seconds", "1");
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true).addMember("127.0.0.1");
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(2);
        config.addSplitBrainProtectionConfig(new SplitBrainProtectionConfig(PROTECTION, true)
                .setProtectOn(SplitBrainProtectionOn.READ_WRITE)
                .setFunctionImplementation(members -> ADMITS.get()));
        config.addRingBufferConfig(new RingbufferConfig("srs.*")
                .setCapacity(16)
                .setInMemoryFormat(InMemoryFormat.OBJECT)
                .setTimeToLiveSeconds(0)
                .setSplitBrainProtectionName(PROTECTION));
        config.getSerializationConfig().addSerializerConfig(
                new SerializerConfig().setImplementation(new SrsItemSerializer()).setTypeClass(SrsItem.class));
        return config;
    }

    /** source -> render -> list, as the case about a refused first read has it. */
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

    private static void append(SrsRingbuffer ring, int from, int count) {
        for (int i = from; i < from + count; i++) {
            ring.append(new SrsItem(new SourcePosition("w" + i), Op.INSERT, 1L, null, Map.of("id", i), 0L));
        }
    }

    /** Until the protection's verdict has turned, which is the moment the source's reads start failing. */
    private static void awaitRingRefuses(SrsRingbuffer ring) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (true) {
            try {
                ring.tailSequence();
            } catch (RingWriteRefusedException refused) {
                return;
            }
            if (System.nanoTime() > deadline) {
                throw new AssertionError("the ring was never refused, so nothing below would be about a refusal");
            }
            Thread.sleep(50);
        }
    }

    private static void awaitRingAnswers(SrsRingbuffer ring) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (true) {
            try {
                ring.tailSequence();
                return;
            } catch (RingWriteRefusedException stillRefused) {
                if (System.nanoTime() > deadline) {
                    throw new AssertionError("the ring never answered again", stillRefused);
                }
            }
            Thread.sleep(50);
        }
    }

    private void awaitSize(int size) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (member.getList(SINK).size() < size) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out waiting for " + size + " changes, got "
                        + List.copyOf(member.getList(SINK)));
            }
            Thread.sleep(50);
        }
    }
}
