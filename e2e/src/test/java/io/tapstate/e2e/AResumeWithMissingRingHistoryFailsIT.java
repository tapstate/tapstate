package io.tapstate.e2e;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.core.processor.SinkProcessors;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.runtime.srs.CaptureError;
import io.tapstate.runtime.srs.SourcePlacement;
import io.tapstate.runtime.srs.SrsItem;
import io.tapstate.runtime.srs.SrsItemSerializer;
import io.tapstate.runtime.srs.SrsReadCursorPublisherFactory;
import io.tapstate.runtime.srs.SrsRingbuffer;
import io.tapstate.runtime.srs.SrsSourceProcessor;
import io.tapstate.runtime.srs.SrsWriteGate;
import io.tapstate.runtime.srs.StartFrom;
import io.tapstate.spi.capture.SourcePosition;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static com.hazelcast.jet.core.Edge.between;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A real engine job must expose a missing volatile-ring resume interval before writing any later row.
 * The declarative vocabulary cannot seed a read cursor ahead of confirmation on a ring without a log.
 */
class AResumeWithMissingRingHistoryFailsIT {

    @Test
    void theEngineReportsACodedRecoveryFailureBeforeTheSinkReceivesLaterChanges() {
        String ringName = "srs.resume-missing.orders";
        String pipeline = "resume_missing";
        Config config = new Config();
        config.setClusterName("resume-missing-" + System.nanoTime());
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(2);
        config.addRingBufferConfig(new RingbufferConfig("srs.*").setCapacity(8)
                .setInMemoryFormat(InMemoryFormat.OBJECT).setTimeToLiveSeconds(0).setBackupCount(0));
        config.getSerializationConfig().addSerializerConfig(new SerializerConfig()
                .setImplementation(new SrsItemSerializer()).setTypeClass(SrsItem.class));
        HazelcastInstance member = Hazelcast.newHazelcastInstance(config);
        try {
            SrsRingbuffer ring = new SrsRingbuffer(member.getRingbuffer(ringName));
            SrsWriteGate gate = new SrsWriteGate(ring);
            // The stopped consumer read through 3 but confirmed only 1; continued capture can reach 11.
            for (int seq = 0; seq < 12; seq++) {
                assertThat(gate.append(new SrsItem(new SourcePosition("w" + seq), Op.INSERT,
                        1L, null, Map.of("id", seq), 0L), 3L)).hasValue(seq);
            }
            assertThat(ring.headSequence()).isEqualTo(4L);

            DAG dag = new DAG();
            Vertex source = dag.newVertex("source", SrsSourceProcessor.metaSupplier(
                    pipeline, ringName, "orders", StartFrom.earliest(), 1L, 1L,
                    SrsReadCursorPublisherFactory.NONE, null, SourcePlacement.anyMember()));
            Vertex project = dag.newVertex("project", Processors.mapP(
                    (Envelope change) -> change.after().get("id").toString())).localParallelism(1);
            Vertex sink = dag.newVertex("sink", SinkProcessors.writeListP("resume-target"))
                    .localParallelism(1);
            dag.edge(between(source, project)).edge(between(project, sink));
            Engine engine = new Engine(member);
            engine.submit(pipeline, dag);
            var target = member.getList("resume-target");
            Await.until("the resume to fail or expose a skipped interval", Duration.ofSeconds(30),
                    () -> engine.failureOf(pipeline).isPresent() || !target.isEmpty(),
                    () -> "failure=" + engine.failureOf(pipeline) + ", target=" + target);

            assertThat(engine.failureOf(pipeline)).as("the engine must report the missing resume history")
                    .isPresent();
            Throwable failure = engine.failureOf(pipeline).orElseThrow();
            // Jet retains terminal processor failures as diagnostic text after execution teardown.
            assertThat(failure.getMessage()).contains(CaptureError.RECOVERY_LOG_GAP.code(),
                    "ring=" + ringName, "sequence=2");
            assertThat(target).as("no later row may be served after skipping the required interval").isEmpty();
        } finally {
            member.shutdown();
        }
    }
}
