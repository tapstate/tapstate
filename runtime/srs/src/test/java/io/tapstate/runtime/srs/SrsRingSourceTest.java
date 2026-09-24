package io.tapstate.runtime.srs;

import com.hazelcast.collection.IList;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamSource;
import io.tapstate.core.event.Op;
import io.tapstate.spi.capture.SourcePosition;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The self-built Jet source over a per-table change ring: a SourceBuilder stream source that tails the
 * ring and drives its changes into a Jet pipeline in order, respecting Jet backpressure. The source is
 * deliberately not fault-tolerant — it sets no snapshot functions, so its position never enters a Jet
 * snapshot; on an L1 restart the ring is re-mined and the source replays it from the head. Runs over a
 * single embedded Jet-enabled member sized to the L1 hot-buffer shape (capacity 8).
 */
class SrsRingSourceTest {

    private static HazelcastInstance hz;

    @BeforeAll
    static void startMember() {
        Config config = new Config();
        // Isolated, structurally undiscoverable single member — never merge with anything on the LAN.
        config.setClusterName("srs-source-test-" + System.nanoTime());
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        // The Jet execution engine is on: this source runs inside a Jet job.
        config.getJetConfig().setEnabled(true);
        config.addRingBufferConfig(new RingbufferConfig("srs.*")
                .setCapacity(8)
                .setInMemoryFormat(InMemoryFormat.OBJECT)
                .setTimeToLiveSeconds(0)
                .setBackupCount(0));
        config.getSerializationConfig().addSerializerConfig(
                new SerializerConfig().setImplementation(new SrsItemSerializer()).setTypeClass(SrsItem.class));
        hz = Hazelcast.newHazelcastInstance(config);
    }

    @AfterAll
    static void stopMember() {
        if (hz != null) {
            hz.shutdown();
        }
    }

    /** Pre-fills a ring with {@code count} inserts at sequences {@code 0..count-1}. */
    private static void fill(String ringName, int count) {
        SrsRingbuffer ring = new SrsRingbuffer(hz.getRingbuffer(ringName));
        for (int i = 0; i < count; i++) {
            ring.append(new SrsItem(new SourcePosition("w" + i), Op.INSERT, 1L, null, Map.of("id", i), 0L));
        }
    }

    /** Pre-fills a ring with one insert per event time in {@code timestamps}, ids matching sequence. */
    private static void fillWith(String ringName, long... timestamps) {
        SrsRingbuffer ring = new SrsRingbuffer(hz.getRingbuffer(ringName));
        for (int i = 0; i < timestamps.length; i++) {
            ring.append(new SrsItem(new SourcePosition("w" + i), Op.INSERT, timestamps[i], null, Map.of("id", i), 0L));
        }
    }

    /**
     * Runs a job that streams one ring from {@code start} into a fresh list sink, waiting until {@code size}
     * changes arrive.
     */
    private static IList<SrsItem> streamRingToList(String ringName, String sinkName, int size, StartFrom start)
            throws InterruptedException {
        Pipeline p = Pipeline.create();
        p.readFrom(SrsRingSource.create(ringName, start)).withoutTimestamps().writeTo(Sinks.list(sinkName));
        IList<SrsItem> sink = hz.getList(sinkName);
        Job job = hz.getJet().newJob(p);
        try {
            awaitSize(sink, size);
        } finally {
            job.cancel();
        }
        return sink;
    }

    private static void awaitSize(IList<?> list, int size) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (list.size() < size) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out waiting for " + size + " changes, got " + list.size());
            }
            Thread.sleep(50);
        }
    }

    @Test
    void streamsRingChangesToADownstreamJetStageInOrder() throws InterruptedException {
        fill("srs.chain.orders", 5);

        IList<SrsItem> sink = streamRingToList("srs.chain.orders", "srs-sink-orders", 5, StartFrom.earliest());

        assertThat(sink).extracting(i -> i.after().get("id")).containsExactly(0, 1, 2, 3, 4);
    }

    @Test
    void aFreshEarliestJobReplaysTheRingFromTheHead() throws InterruptedException {
        fill("srs.chain.replay", 4);

        // A first job drains the ring; a second, fresh job reads the same ring from the head again. The
        // source keeps no position in Jet state, so a restart replays rather than resuming a persisted
        // offset — the L1 restart=replay semantic.
        streamRingToList("srs.chain.replay", "srs-sink-replay-1", 4, StartFrom.earliest());
        IList<SrsItem> second = streamRingToList("srs.chain.replay", "srs-sink-replay-2", 4, StartFrom.earliest());

        assertThat(second).extracting(i -> i.after().get("id")).containsExactly(0, 1, 2, 3);
    }

    /**
     * Collects read-cursor sequences a source publishes during a job. A {@code static} sink reached by a
     * factory that resolves a static-method-ref sink member-side: the factory is serialized onto the source
     * and, in this single-JVM member, resolves back to this same static — the seam a durable cursor write
     * hangs off in the assembly layer.
     */
    private static final List<Long> PUBLISHED = new CopyOnWriteArrayList<>();

    private static void collect(long lastReadSeq) {
        PUBLISHED.add(lastReadSeq);
    }

    @Test
    void publishesTheReadCursorAsTheSourceDrainsTheRing() throws InterruptedException {
        PUBLISHED.clear();
        fill("srs.chain.cursor", 5);

        SrsReadCursorPublisherFactory factory = member -> SrsRingSourceTest::collect;
        Pipeline p = Pipeline.create();
        p.readFrom(SrsRingSource.create("srs.chain.cursor", StartFrom.earliest(), factory, null))
                .withoutTimestamps().writeTo(Sinks.list("srs-sink-cursor"));
        IList<SrsItem> sink = hz.getList("srs-sink-cursor");
        Job job = hz.getJet().newJob(p);
        try {
            awaitSize(sink, 5);
        } finally {
            job.cancel();
        }

        // The source reports its read progress member-side as it drains: the last sequence it read (4, the
        // 5th change) is published, the signal the write-side headroom gate reads back as this consumer's.
        assertThat(sink).extracting(i -> i.after().get("id")).containsExactly(0, 1, 2, 3, 4);
        assertThat(PUBLISHED).isNotEmpty();
        assertThat(PUBLISHED.get(PUBLISHED.size() - 1)).isEqualTo(4L);
    }

    @Test
    void aTimestampSourceStreamsOnlyChangesAtOrAfterIt() throws InterruptedException {
        // Buffered changes at event times 10, 20, 30, 40 (ids 0..3). start_from resolves to the first change
        // at or after 25, so the source streams only ids 2 and 3 — proving create() honours a non-earliest
        // start point end-to-end through a real Jet job (deterministic: the subset is already buffered).
        fillWith("srs.chain.at", 10, 20, 30, 40);

        IList<SrsItem> sink = streamRingToList(
                "srs.chain.at", "srs-sink-at", 2, StartFrom.at(Instant.ofEpochMilli(25)));

        assertThat(sink).extracting(i -> i.after().get("id")).containsExactly(2, 3);
    }
}
