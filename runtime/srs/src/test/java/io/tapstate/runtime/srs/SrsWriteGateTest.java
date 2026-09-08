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

import java.util.Map;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The headroom precheck that guards a write into the per-table change ring: a cdc write is admitted only
 * when it would not overwrite a change the slowest consumer has not read yet; otherwise it is
 * backpressured (not written), so the source read pauses rather than silently dropping an unread change.
 * The formula tests are pure; the ring tests run over a single embedded Hazelcast member sized to the L1
 * hot-buffer shape (capacity 8).
 */
class SrsWriteGateTest {

    private static HazelcastInstance hz;

    @BeforeAll
    static void startMember() {
        Config config = new Config();
        // Isolated, structurally undiscoverable single member — never merge with anything on the LAN.
        config.setClusterName("srs-gate-test-" + System.nanoTime());
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

    private static SrsItem insert(String token) {
        return new SrsItem(new SourcePosition(token), Op.INSERT, 1L, null, Map.of("id", 1), 0L);
    }

    // --- the precheck formula (pure) ---

    @Test
    void refusesAWriteThatWouldOverwriteAnUnreadChange() {
        // Worked example: capacity 1000, slowest consumer read up to seq 501, tailSeq 1501. The next
        // write (seq 1502) would evict the still-unread seq 502 -> (1502 - 501) = 1001 > 1000 -> refuse.
        assertThat(SrsWriteGate.hasHeadroom(1501L, 1000L, 501L)).isFalse();
    }

    @Test
    void admitsOnceTheSlowestConsumerAdvances() {
        // Same ring, but the slowest consumer has now read seq 502 -> (1502 - 502) = 1000, not > 1000 -> admit.
        assertThat(SrsWriteGate.hasHeadroom(1501L, 1000L, 502L)).isTrue();
    }

    @Test
    void admitsExactlyAtCapacityAndRefusesOnePast() {
        // Capacity 8, no consumer has read anything (min read seq -1). The ring can hold seq 0..7: writing
        // seq 7 (tailSeq 6) fits exactly; writing seq 8 (tailSeq 7) would evict the unread seq 0.
        assertThat(SrsWriteGate.hasHeadroom(6L, 8L, -1L)).isTrue();
        assertThat(SrsWriteGate.hasHeadroom(7L, 8L, -1L)).isFalse();
    }

    @Test
    void admitsWhenNoConsumerConstrainsTheRing() {
        // No consumer to protect (unconstrained) -> a write is always admitted, even on a full ring.
        assertThat(SrsWriteGate.hasHeadroom(9_999L, 8L, Long.MAX_VALUE)).isTrue();
    }

    // --- the gated append over a real ring ---

    @Test
    void appendAdmitsWhileHeadroomRemains() {
        SrsWriteGate gate = new SrsWriteGate(new SrsRingbuffer(hz.getRingbuffer("srs.gate.admit")));
        for (long expected = 0; expected < 8; expected++) {
            assertThat(gate.append(insert("p" + expected), -1L)).hasValue(expected);
        }
    }

    @Test
    void appendBackpressuresRatherThanOverwriteAnUnreadChange() {
        SrsRingbuffer ring = new SrsRingbuffer(hz.getRingbuffer("srs.gate.backpressure"));
        SrsWriteGate gate = new SrsWriteGate(ring);
        // Fill the ring to capacity with a consumer that has read nothing (min read seq -1).
        for (long i = 0; i < 8; i++) {
            assertThat(gate.append(insert("p" + i), -1L)).isPresent();
        }
        // The next write would overwrite the unread seq 0 -> backpressure, and the ring must not grow.
        assertThat(gate.append(insert("overflow"), -1L)).isEmpty();
        assertThat(ring.tailSequence()).isEqualTo(7L);
    }

    @Test
    void appendResumesOnceTheSlowestConsumerAdvances() {
        SrsRingbuffer ring = new SrsRingbuffer(hz.getRingbuffer("srs.gate.resume"));
        SrsWriteGate gate = new SrsWriteGate(ring);
        for (long i = 0; i < 8; i++) {
            assertThat(gate.append(insert("p" + i), -1L)).isPresent();
        }
        assertThat(gate.append(insert("blocked"), -1L)).isEmpty();
        // The consumer reads seq 0, freeing headroom; the write is now admitted and takes seq 8.
        OptionalLong resumed = gate.append(insert("resumed"), 0L);
        assertThat(resumed).hasValue(8L);
        assertThat(ring.tailSequence()).isEqualTo(8L);
    }

    @Test
    void rejectsANullItem() {
        SrsWriteGate gate = new SrsWriteGate(new SrsRingbuffer(hz.getRingbuffer("srs.gate.nullitem")));
        assertThatThrownBy(() -> gate.append(null, -1L)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void rejectsANullRingbuffer() {
        assertThatThrownBy(() -> new SrsWriteGate(null)).isInstanceOf(NullPointerException.class);
    }
}
