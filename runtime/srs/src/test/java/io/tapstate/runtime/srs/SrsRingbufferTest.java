package io.tapstate.runtime.srs;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.config.SplitBrainProtectionConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionException;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionOn;
import io.tapstate.core.event.Op;
import io.tapstate.spi.capture.SourcePosition;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The per-table change ring over a single embedded Hazelcast member. Each test uses its own ring name
 * (the {@code srs.*} wildcard config gives them all the same small capacity) so appends never bleed
 * across tests.
 */
class SrsRingbufferTest {

    private static HazelcastInstance hz;

    @BeforeAll
    static void startMember() {
        Config config = new Config();
        // Isolated, structurally undiscoverable single member — never merge with anything on the LAN.
        config.setClusterName("srs-test-" + System.nanoTime());
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getJetConfig().setEnabled(false);
        // Every srs.* ring: small, in-memory (no time eviction, no backup) — the L1 hot buffer shape.
        config.addRingBufferConfig(new RingbufferConfig("srs.*")
                .setCapacity(8)
                .setInMemoryFormat(InMemoryFormat.OBJECT)
                .setTimeToLiveSeconds(0)
                .setBackupCount(0));
        // The ring stores SrsItem via its dedicated serializer (Hazelcast cannot serialize it natively).
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

    private static SrsItem insert(String token) {
        return new SrsItem(new SourcePosition(token), Op.INSERT, 1L, null, Map.of("id", 1), 0L);
    }

    /**
     * A member the cluster has not qualified refuses the ring, and this class says so in its own terms.
     *
     * <p>The refusal is the real one: the member below runs the same split brain protection production
     * installs on every ring -- {@code READ_WRITE}, a function of the cluster's own choosing -- with a
     * function that has not agreed. What comes back is therefore whatever the library actually raises on
     * each of these calls, not a stand-in for it, and the calls are the ones admitting a write makes: the
     * capacity, the tail the headroom precheck compares against, and the append itself. The library raises
     * them differently -- two where they are called, one through a future -- and a translation that missed
     * either shape would leave the caller holding an exception it cannot act on.
     *
     * <p>What the caller needs from it is that the write did not happen. The library's own type says only
     * that some minimum cluster size was not met, and it reaches the capture as a dead change stream.
     */
    @Test
    void aClusterThatHasNotQualifiedThisMemberRefusesTheWriteInTapstatesOwnTerms() {
        Config config = new Config();
        config.setClusterName("srs-refusal-test-" + System.nanoTime());
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getJetConfig().setEnabled(false);
        config.getSerializationConfig().addSerializerConfig(
                new SerializerConfig().setImplementation(new SrsItemSerializer()).setTypeClass(SrsItem.class));
        // The protection production installs, with a verdict that never comes -- the state a member is in
        // between joining and its own library agreeing, held still.
        String protection = "never-qualified";
        config.addSplitBrainProtectionConfig(new SplitBrainProtectionConfig(protection, true)
                .setProtectOn(SplitBrainProtectionOn.READ_WRITE)
                .setFunctionImplementation(members -> false));
        config.addRingBufferConfig(new RingbufferConfig("srs.*")
                .setCapacity(8)
                .setInMemoryFormat(InMemoryFormat.OBJECT)
                .setTimeToLiveSeconds(0)
                .setBackupCount(0)
                .setSplitBrainProtectionName(protection));

        HazelcastInstance refusing = Hazelcast.newHazelcastInstance(config);
        // Taken while the member is up: the proxy cannot be looked up on a member that is gone, and the
        // last reading below needs the one this held on to.
        SrsRingbuffer ring = new SrsRingbuffer(refusing.getRingbuffer("srs.chain-1.refused"));
        try {
            assertThatThrownBy(ring::capacity)
                    .as("the capacity read is checked against the same protection, locally")
                    .isInstanceOf(RingWriteRefusedException.class)
                    .hasCauseInstanceOf(SplitBrainProtectionException.class);
            assertThatThrownBy(ring::tailSequence)
                    .as("the tail read is a partition operation and throws where it is called")
                    .isInstanceOf(RingWriteRefusedException.class)
                    .hasCauseInstanceOf(SplitBrainProtectionException.class);
            assertThatThrownBy(() -> ring.appendAll(java.util.List.of(insert("w1"))))
                    .as("the append reports through its future, and is unwrapped to the same answer")
                    .isInstanceOf(RingWriteRefusedException.class)
                    .hasCauseInstanceOf(SplitBrainProtectionException.class);
        } finally {
            refusing.shutdown();
        }

        // And nothing else is dressed up as a refusal. The translation says "nothing was written and this
        // clears itself", which is true of the protection and of nothing else; a failure wearing that
        // label would be waited on for the whole bound and then reported as a cluster that never came
        // back -- the wrong diagnosis, arrived at slowly. The member is down by now, which is a failure
        // from the very same library and must come through as itself.
        assertThatThrownBy(ring::tailSequence)
                .as("a member that is gone is not a cluster that is about to agree")
                .isNotInstanceOf(RingWriteRefusedException.class);
    }

    @Test
    void ringNameNamespacesByChainAndTable() {
        assertThat(SrsRingbuffer.ringName("chain-1", "orders")).isEqualTo("srs.chain-1.orders");
    }

    @Test
    void appendAssignsMonotonicSequencesFromZero() {
        SrsRingbuffer buffer = new SrsRingbuffer(hz.getRingbuffer("srs.chain-1.mono"));
        assertThat(buffer.append(insert("a"))).isEqualTo(0L);
        assertThat(buffer.append(insert("b"))).isEqualTo(1L);
        assertThat(buffer.append(insert("c"))).isEqualTo(2L);
    }

    @Test
    void tailSequenceAdvancesWithEachAppend() {
        SrsRingbuffer buffer = new SrsRingbuffer(hz.getRingbuffer("srs.chain-1.tail"));
        assertThat(buffer.tailSequence()).isEqualTo(-1L);
        buffer.append(insert("a"));
        assertThat(buffer.tailSequence()).isEqualTo(0L);
        buffer.append(insert("b"));
        assertThat(buffer.tailSequence()).isEqualTo(1L);
    }

    @Test
    void capacityReflectsTheConfiguredBound() {
        SrsRingbuffer buffer = new SrsRingbuffer(hz.getRingbuffer("srs.chain-1.cap"));
        assertThat(buffer.capacity()).isEqualTo(8L);
    }

    @Test
    void appendedItemRoundTripsThroughTheRing() throws InterruptedException {
        Ringbuffer<SrsItem> ring = hz.getRingbuffer("srs.chain-1.roundtrip");
        SrsRingbuffer buffer = new SrsRingbuffer(ring);
        SrsItem original = new SrsItem(new SourcePosition("gtid:aaa-1:9"), Op.UPDATE, 42L,
                Map.of("id", 1, "name", "old"), Map.of("id", 1, "name", "new"), 3L);
        long seq = buffer.append(original);
        SrsItem stored = ring.readOne(seq);
        assertThat(stored).isEqualTo(original);
    }

    @Test
    void insertRoundTripsWithAnAbsentBefore() throws InterruptedException {
        Ringbuffer<SrsItem> ring = hz.getRingbuffer("srs.chain-1.insert");
        SrsRingbuffer buffer = new SrsRingbuffer(ring);
        SrsItem original = new SrsItem(new SourcePosition("gtid:aaa-1:10"), Op.INSERT, 7L,
                null, Map.of("id", 2, "name", "new"), 1L);
        long seq = buffer.append(original);
        SrsItem stored = ring.readOne(seq);
        assertThat(stored).isEqualTo(original);
        assertThat(stored.before()).isNull();
    }

    @Test
    void deleteRoundTripsWithAnAbsentAfter() throws InterruptedException {
        Ringbuffer<SrsItem> ring = hz.getRingbuffer("srs.chain-1.delete");
        SrsRingbuffer buffer = new SrsRingbuffer(ring);
        SrsItem original = new SrsItem(new SourcePosition("gtid:aaa-1:11"), Op.DELETE, 8L,
                Map.of("id", 3), null, 1L);
        long seq = buffer.append(original);
        SrsItem stored = ring.readOne(seq);
        assertThat(stored).isEqualTo(original);
        assertThat(stored.after()).isNull();
    }

    @Test
    void readOneReturnsTheItemAtASequence() {
        SrsRingbuffer buffer = new SrsRingbuffer(hz.getRingbuffer("srs.chain-1.readone"));
        long a = buffer.append(insert("a"));
        long b = buffer.append(insert("b"));
        assertThat(buffer.readOne(a)).isEqualTo(insert("a"));
        assertThat(buffer.readOne(b)).isEqualTo(insert("b"));
    }

    @Test
    void headSequenceMarksTheOldestReadableSequence() {
        SrsRingbuffer buffer = new SrsRingbuffer(hz.getRingbuffer("srs.chain-1.head"));
        // A fresh ring's head is 0 — where a replaying reader starts. Within capacity nothing is evicted,
        // so the head stays put as changes are appended.
        assertThat(buffer.headSequence()).isEqualTo(0L);
        buffer.append(insert("a"));
        buffer.append(insert("b"));
        assertThat(buffer.headSequence()).isEqualTo(0L);
    }

    @Test
    void rejectsANullItem() {
        SrsRingbuffer buffer = new SrsRingbuffer(hz.getRingbuffer("srs.chain-1.nullitem"));
        assertThatThrownBy(() -> buffer.append(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void rejectsANullRingbuffer() {
        assertThatThrownBy(() -> new SrsRingbuffer(null)).isInstanceOf(NullPointerException.class);
    }
}
