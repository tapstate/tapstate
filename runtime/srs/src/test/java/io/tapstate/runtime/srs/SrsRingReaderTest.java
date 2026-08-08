package io.tapstate.runtime.srs;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.event.Op;
import io.tapstate.spi.capture.SourcePosition;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The per-consumer ring reader tails one per-table change ring, advancing a run-local cursor by
 * sequence and emitting each change once. A fill drains a bounded batch from the cursor up to the ring
 * tail — bounded so the reader respects the downstream's pull (Jet backpressure), and never blocking for
 * a change that has not been written yet. The cursor is run-local: it is not durable and does not resume
 * from a Jet snapshot; on an L1 restart the ring is gone and the reader replays a freshly re-mined ring
 * from its head.
 */
class SrsRingReaderTest {

    private static HazelcastInstance hz;

    @BeforeAll
    static void startMember() {
        Config config = new Config();
        // Isolated, structurally undiscoverable single member — never merge with anything on the LAN.
        config.setClusterName("srs-reader-test-" + System.nanoTime());
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getJetConfig().setEnabled(false);
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

    private static SrsItem insert(int id) {
        return insertAt(id, 1L);
    }

    private static SrsItem insertAt(int id, long ts) {
        return new SrsItem(new SourcePosition("w" + id), Op.INSERT, ts, null, Map.of("id", id), 0L);
    }

    /** A ring pre-filled with {@code count} inserts at sequences {@code 0..count-1}. */
    private static SrsRingbuffer filled(String ringName, int count) {
        SrsRingbuffer ring = new SrsRingbuffer(hz.getRingbuffer(ringName));
        for (int i = 0; i < count; i++) {
            ring.append(insert(i));
        }
        return ring;
    }

    /** A ring pre-filled with one insert per event time in {@code timestamps}, ids matching sequence. */
    private static SrsRingbuffer filledWith(String ringName, long... timestamps) {
        SrsRingbuffer ring = new SrsRingbuffer(hz.getRingbuffer(ringName));
        for (int i = 0; i < timestamps.length; i++) {
            ring.append(insertAt(i, timestamps[i]));
        }
        return ring;
    }

    @Test
    void readsAllChangesFromTheStartInOrder() {
        SrsRingReader reader = new SrsRingReader(filled("srs.chain.all", 3), 0);
        List<SrsItem> out = new ArrayList<>();

        int n = reader.fill((item, seq) -> out.add(item), 10);

        assertThat(n).isEqualTo(3);
        assertThat(out).extracting(i -> i.after().get("id")).containsExactly(0, 1, 2);
    }

    @Test
    void handsEachChangeTheSequenceTheRingAssignedIt() {
        SrsRingReader reader = new SrsRingReader(filled("srs.chain.seq", 3), 0);
        List<Long> sequences = new ArrayList<>();

        reader.fill((item, seq) -> sequences.add(seq), 10);

        // The ring assigns the sequence on append and keeps it: it is not a field of the item, so a reader
        // that handed over the item alone would leave the only monotonic order the engine has behind.
        assertThat(sequences).containsExactly(0L, 1L, 2L);
    }

    @Test
    void theSequenceItHandsOverIsTheOneItStartedFromNotACountOfThisFill() {
        SrsRingReader reader = new SrsRingReader(filled("srs.chain.seq-offset", 4), 2);
        List<Long> sequences = new ArrayList<>();

        reader.fill((item, seq) -> sequences.add(seq), 10);

        // A reader that begins part way through the ring must report the ring's sequence, not how many
        // changes this fill has emitted. Counting per fill would restart the order at every batch and at
        // every reader, and two changes of one chain would come out claiming the same place in it.
        assertThat(sequences).containsExactly(2L, 3L);
    }

    @Test
    void boundsEachFillToTheRequestedMaxAndResumesWhereItLeftOff() {
        SrsRingReader reader = new SrsRingReader(filled("srs.chain.bounded", 3), 0);
        List<SrsItem> out = new ArrayList<>();

        // A fill drains at most max, so the reader yields to the downstream between batches (backpressure);
        // the next fill picks up exactly where the last stopped — no change re-read, none skipped.
        assertThat(reader.fill((item, seq) -> out.add(item), 2)).isEqualTo(2);
        assertThat(reader.fill((item, seq) -> out.add(item), 2)).isEqualTo(1);
        assertThat(out).extracting(i -> i.after().get("id")).containsExactly(0, 1, 2);
    }

    @Test
    void stopsAtTheTailWithoutBlockingForUnwrittenChanges() {
        SrsRingReader reader = new SrsRingReader(filled("srs.chain.tail", 2), 0);
        List<SrsItem> out = new ArrayList<>();

        // max exceeds what the ring holds: the reader returns what is present rather than blocking for a
        // change past the tail that has not been written yet.
        int n = reader.fill((item, seq) -> out.add(item), 10);

        assertThat(n).isEqualTo(2);
        assertThat(out).hasSize(2);
    }

    @Test
    void startsFromTheGivenStartSequence() {
        SrsRingReader reader = new SrsRingReader(filled("srs.chain.start", 4), 2);
        List<SrsItem> out = new ArrayList<>();

        int n = reader.fill((item, seq) -> out.add(item), 10);

        assertThat(n).isEqualTo(2);
        assertThat(out).extracting(i -> i.after().get("id")).containsExactly(2, 3);
    }

    @Test
    void emitsNothingWhenThereIsNoNewChange() {
        // Start past the tail (sequences 0,1 are present): nothing to read.
        SrsRingReader reader = new SrsRingReader(filled("srs.chain.drained", 2), 2);
        List<SrsItem> out = new ArrayList<>();

        assertThat(reader.fill((item, seq) -> out.add(item), 10)).isEqualTo(0);
        assertThat(out).isEmpty();
    }

    @Test
    void tailsChangesAppendedAfterAnEmptyFill() {
        SrsRingbuffer ring = filled("srs.chain.continue", 2);
        SrsRingReader reader = new SrsRingReader(ring, 0);
        List<SrsItem> out = new ArrayList<>();

        assertThat(reader.fill((item, seq) -> out.add(item), 10)).isEqualTo(2);
        assertThat(reader.fill((item, seq) -> out.add(item), 10)).isEqualTo(0);
        // A change appended after an exhausted fill is picked up by the next fill — the reader tails the ring.
        ring.append(insert(2));
        assertThat(reader.fill((item, seq) -> out.add(item), 10)).isEqualTo(1);
        assertThat(out).extracting(i -> i.after().get("id")).containsExactly(0, 1, 2);
    }

    @Test
    void rejectsANullRing() {
        assertThatThrownBy(() -> new SrsRingReader(null, 0)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void publishesTheLastReadSequenceAfterAdvancingOnANonEmptyFill() {
        List<Long> published = new ArrayList<>();
        SrsRingReader reader = new SrsRingReader(filled("srs.pub.basic", 3), 0, published::add);

        // The reader reports its read cursor as it advances: after draining a batch it publishes the last
        // sequence it read, the progress signal the write-side headroom gate reads back as this consumer's.
        int n = reader.fill((item, seq) -> { }, 10);

        assertThat(n).isEqualTo(3);
        assertThat(published).containsExactly(2L);
    }

    @Test
    void publishesOncePerFillBatchCarryingItsLastSequence() {
        List<Long> published = new ArrayList<>();
        SrsRingReader reader = new SrsRingReader(filled("srs.pub.batched", 3), 0, published::add);

        // One publish per non-empty fill carrying that batch's last sequence — not one per change — so the
        // durable cursor write is amortized over the batch a bounded fill already draws.
        assertThat(reader.fill((item, seq) -> { }, 2)).isEqualTo(2);
        assertThat(reader.fill((item, seq) -> { }, 2)).isEqualTo(1);
        assertThat(published).containsExactly(1L, 2L);
    }

    @Test
    void doesNotPublishWhenAFillEmitsNothing() {
        List<Long> published = new ArrayList<>();
        // Start past the tail: nothing to read, the cursor does not move, so nothing is published.
        SrsRingReader reader = new SrsRingReader(filled("srs.pub.empty", 2), 2, published::add);

        assertThat(reader.fill((item, seq) -> { }, 10)).isEqualTo(0);
        assertThat(published).isEmpty();
    }

    @Test
    void fromResolvesAStartPointAndPublishesFromThere() {
        List<Long> published = new ArrayList<>();
        // from() carries the publish callback too: an earliest reader replays from the head and reports the
        // last sequence it read there.
        SrsRingReader reader = SrsRingReader.from(filled("srs.pub.from", 3), StartFrom.earliest(), published::add);

        assertThat(reader.fill((item, seq) -> { }, 10)).isEqualTo(3);
        assertThat(published).containsExactly(2L);
    }

    @Test
    void fromEarliestReplaysEveryBufferedChange() {
        SrsRingReader reader = SrsRingReader.from(filled("srs.start.earliest", 3), StartFrom.earliest());
        List<SrsItem> out = new ArrayList<>();

        assertThat(reader.fill((item, seq) -> out.add(item), 10)).isEqualTo(3);
        assertThat(out).extracting(i -> i.after().get("id")).containsExactly(0, 1, 2);
    }

    @Test
    void fromLatestSkipsBufferedChangesAndTailsNewOnes() {
        SrsRingbuffer ring = filled("srs.start.latest", 3);
        SrsRingReader reader = SrsRingReader.from(ring, StartFrom.latest());
        List<SrsItem> out = new ArrayList<>();

        // latest starts past the newest buffered change: nothing already there is replayed...
        assertThat(reader.fill((item, seq) -> out.add(item), 10)).isEqualTo(0);
        // ...but a change appended after the start point is tailed.
        ring.append(insert(3));
        assertThat(reader.fill((item, seq) -> out.add(item), 10)).isEqualTo(1);
        assertThat(out).extracting(i -> i.after().get("id")).containsExactly(3);
    }

    @Test
    void fromAnInstantStartsAtTheFirstChangeAtOrAfterIt() {
        SrsRingReader reader = SrsRingReader.from(
                filledWith("srs.start.at", 10, 20, 30, 40), StartFrom.at(Instant.ofEpochMilli(25)));
        List<SrsItem> out = new ArrayList<>();

        assertThat(reader.fill((item, seq) -> out.add(item), 10)).isEqualTo(2);
        assertThat(out).extracting(i -> i.after().get("id")).containsExactly(2, 3);
    }

    @Test
    void fromAnInstantOlderThanEveryBufferedChangeReplaysFromHead() {
        // The ring only goes back so far: an instant older than everything buffered clamps to the head —
        // the earliest change still available — rather than inventing history the buffer no longer holds.
        SrsRingReader reader = SrsRingReader.from(
                filledWith("srs.start.at-old", 100, 200), StartFrom.at(Instant.ofEpochMilli(50)));
        List<SrsItem> out = new ArrayList<>();

        assertThat(reader.fill((item, seq) -> out.add(item), 10)).isEqualTo(2);
        assertThat(out).extracting(i -> i.after().get("id")).containsExactly(0, 1);
    }

    @Test
    void fromAnInstantNewerThanEveryBufferedChangeTailsOnlyFutureChanges() {
        SrsRingbuffer ring = filledWith("srs.start.at-future", 10, 20);
        SrsRingReader reader = SrsRingReader.from(ring, StartFrom.at(Instant.ofEpochMilli(999)));
        List<SrsItem> out = new ArrayList<>();

        assertThat(reader.fill((item, seq) -> out.add(item), 10)).isEqualTo(0);
        ring.append(insertAt(2, 1000));
        assertThat(reader.fill((item, seq) -> out.add(item), 10)).isEqualTo(1);
        assertThat(out).extracting(i -> i.after().get("id")).containsExactly(2);
    }
}
