package io.tapstate.runtime.srs;

import com.hazelcast.collection.IList;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.core.processor.SinkProcessors;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.spi.capture.SourcePosition;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.LongConsumer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static com.hazelcast.jet.core.Edge.between;
import static org.assertj.core.api.Assertions.assertThat;

class SrsSourceProcessorResumeTest {

    private static HazelcastInstance member;

    @BeforeAll
    static void startMember() {
        Config config = new Config();
        config.setClusterName("srs-source-resume-" + System.nanoTime());
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
        member = Hazelcast.newHazelcastInstance(config);
    }

    @AfterAll
    static void stopMember() {
        if (member != null) {
            member.shutdown();
        }
    }

    @ParameterizedTest
    @CsvSource(value = {"1,3", "1,2", "null,3", "null,2"}, nullValues = "null")
    void pauseResumeUsesCurrentConfirmationAndRefusesOnlyMissingHistory(
            Long assemblyConfirmation, long currentConfirmation) throws InterruptedException {
        String pipeline = "resume_" + assemblyConfirmation + "_" + currentConfirmation;
        String ringName = "srs." + pipeline + ".orders";
        String targetName = pipeline + ".target";
        AtomicLong read = new AtomicLong(-1L);
        member.getUserContext().put(ringName + ".read", read);
        if (assemblyConfirmation != null) {
            confirm(ringName, assemblyConfirmation);
        }
        SrsRingbuffer ring = new SrsRingbuffer(member.getRingbuffer(ringName));
        for (int sequence = 0; sequence < 4; sequence++) {
            ring.append(change(sequence));
        }
        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", SrsSourceProcessor.metaSupplier(
                pipeline, ringName, "orders", StartFrom.earliest(), assemblyConfirmation, 1L,
                new CursorFactory(ringName), null, SourcePlacement.anyMember()));
        Vertex project = dag.newVertex("project", Processors.mapP(
                (Envelope change) -> change.position().order().seq())).localParallelism(1);
        Vertex sink = dag.newVertex("sink", SinkProcessors.writeListP(targetName)).localParallelism(1);
        dag.edge(between(source, project)).edge(between(project, sink));
        Engine engine = new Engine(member);
        IList<Long> target = member.getList(targetName);
        int initialCount = assemblyConfirmation == null ? 4 : 2;
        try {
            engine.submit(pipeline, dag);
            await(() -> target.size() == initialCount && read.get() == 3L);
            assertThat(target).containsExactlyElementsOf(assemblyConfirmation == null
                    ? java.util.List.of(0L, 1L, 2L, 3L) : java.util.List.of(2L, 3L));
            // Only delivered changes can be confirmed; read progress stays at 3 in both cases.
            confirm(ringName, currentConfirmation);
            engine.suspend(pipeline);
            await(() -> member.getJet().getJob(pipeline).getStatus() == JobStatus.SUSPENDED);
            SrsWriteGate gate = new SrsWriteGate(ring);
            for (int sequence = 4; sequence < 12; sequence++) {
                assertThat(gate.append(change(sequence), read.get())).hasValue(sequence);
            }
            assertThat(ring.headSequence()).isEqualTo(4L);
            assertThat(ring.tailSequence()).isEqualTo(11L);

            engine.resume(pipeline);
            await(() -> engine.failureOf(pipeline).isPresent() || target.size() > initialCount);
            if (currentConfirmation == 3L) {
                assertThat(engine.failureOf(pipeline)).isEmpty();
                await(() -> target.size() >= initialCount + 8);
                assertThat(target.subList(initialCount, target.size()))
                        .containsExactly(4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L);
            } else {
                assertThat(engine.failureOf(pipeline)).isPresent();
                assertThat(engine.failureOf(pipeline).orElseThrow().getMessage())
                        .contains(CaptureError.RECOVERY_LOG_GAP.code(), "ring=" + ringName, "sequence=3");
                assertThat(target).hasSize(initialCount);
                assertThat(read).hasValue(3L);
            }
        } finally {
            engine.cancel(pipeline);
            assertThat(engine.awaitTerminal(pipeline, Duration.ofSeconds(30))).isTrue();
            member.getUserContext().remove(ringName + ".read");
            member.getUserContext().remove(ringName + ".confirmed");
        }
    }

    private static SrsItem change(int sequence) {
        return new SrsItem(new SourcePosition("w" + sequence), Op.INSERT,
                1L, null, Map.of("id", sequence), 0L);
    }

    private static void confirm(String ringName, long sequence) {
        member.getUserContext().put(ringName + ".confirmed",
                new ChainPosition(new SourceOrder(1L, sequence), "w" + sequence));
    }

    private static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out waiting for the engine resume observation");
            }
            Thread.sleep(50);
        }
    }

    private record CursorFactory(String ringName) implements SrsReadCursorPublisherFactory {
        @Override
        public LongConsumer resolve(HazelcastInstance member) {
            return ((AtomicLong) member.getUserContext().get(ringName + ".read"))::set;
        }

        @Override
        public Optional<ChainPosition> confirmedPosition(HazelcastInstance member) {
            return Optional.ofNullable((ChainPosition) member.getUserContext().get(ringName + ".confirmed"));
        }
    }
}
