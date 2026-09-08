package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.adapters.pdk.ConnectorProvisioner;
import io.tapstate.adapters.pdk.ConnectorRef;
import io.tapstate.adapters.pdk.SyntheticSinkJars;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.runtime.engine.nest.NestSettings;
import io.tapstate.spi.sink.DdlPolicy;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.TargetTable;
import io.tapstate.spi.sink.WriteMode;
import io.tapstate.spi.store.KeyedStateStore;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A sink connector opened by the factory that ships onto the graph is handed the durable layer bound on
 * the member it lands on, so what it keeps for itself outlives the open.
 *
 * <p>This is the seam neither side's own cases reach. The bridge's cases construct the sink port with a
 * store directly, which proves the port uses one it is given; the member cases prove the store is bound
 * into the user context. Between them sits the line that actually joins the two - the factory reading the
 * key member-side and passing what it finds - and nothing drove it. A factory that passed null there
 * would keep every sink connector's notes for the life of one open: every row would still be written, no
 * error would be raised, and the only visible trace would be a connector re-minting whatever it mints on
 * its own first run.
 *
 * <p>What the assertions discriminate:
 * <ul>
 *   <li><b>The first open is held to finding nothing.</b> Without it, an open that never reached the
 *       connector would satisfy the second assertion for the wrong reason.</li>
 *   <li><b>The connector reports what it found</b>, through the write result, so the observation is the
 *       contract's own answer rather than the host reading back its own object.</li>
 *   <li><b>A member with no store bound keeps nothing.</b> That is the control: it is the behaviour
 *       before there was anywhere to file the notes, so it is what says the first case passes because of
 *       the store and not because two opens of one factory happen to share something else.</li>
 * </ul>
 *
 * <p>One member, not two: the factory resolves the store from whichever member runs the sink vertex, and
 * a second member would prove the same lookup at several times the cost. What a second member would add -
 * that the store does not ride along with the factory - is already locked by the serialization case next
 * door, which holds the factory to carrying nothing but its coordinates.
 */
class ASinkOpensAgainstTheStoreBoundOnItsMemberTest {

    private static final PipelineNode SINK = new PipelineNode("p1", "to_synthetic");

    @Test
    void aLaterOpenReadsWhatAnEarlierOneFiled(@TempDir Path dir) throws Exception {
        KeyedStateStore store = new InMemoryKeyedStateStore();
        HazelcastInstance member = memberWith(provisioner(dir), store);
        try {
            PdkSinkWriterFactory factory = factory();

            assertThat(driveOneOpen(factory)).isZero();
            assertThat(driveOneOpen(factory)).isEqualTo(1L);
        } finally {
            member.shutdown();
        }
    }

    @Test
    void aMemberWithNoStoreBoundKeepsNothingBetweenOpens(@TempDir Path dir) throws Exception {
        HazelcastInstance member = memberWith(provisioner(dir), null);
        try {
            PdkSinkWriterFactory factory = factory();

            assertThat(driveOneOpen(factory)).isZero();
            assertThat(driveOneOpen(factory)).isZero();
        } finally {
            member.shutdown();
        }
    }

    /** One open of the factory and one write through it, answering what the connector found in its map. */
    private static long driveOneOpen(PdkSinkWriterFactory factory) throws Exception {
        try (SinkWriter writer = factory.getEx()) {
            return writer.write(List.of(Envelope.insert(1L, "t1", Map.of("id", 1), null)))
                    .toCompletableFuture().get(30, TimeUnit.SECONDS).written();
        }
    }

    private static PdkSinkWriterFactory factory() {
        return new PdkSinkWriterFactory(
                "demo", Map.of(), WriteMode.UPSERT, DdlPolicy.APPLY, (TargetTable) null, SINK);
    }

    private static HazelcastInstance memberWith(ConnectorProvisioner provisioner, KeyedStateStore store) {
        return new HazelcastConfiguration().hazelcastMember(
                new HazelcastProperties(), null, provisioner, null, store, NestSettings.defaults(), null, null);
    }

    /** Resolves every connector id to one synthetic sink, compiled into a jar for this case alone. */
    private static ConnectorProvisioner provisioner(Path dir) {
        ConnectorRef ref = new ConnectorRef(
                List.of(SyntheticSinkJars.stateRecordingSink(dir)), "synthetic.StateRecordingSink", "2.0.8", null);
        return connectorId -> ref;
    }
}
