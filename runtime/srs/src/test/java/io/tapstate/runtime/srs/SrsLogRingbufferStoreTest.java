package io.tapstate.runtime.srs;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.RingbufferStoreConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.event.Op;
import io.tapstate.spi.capture.SourcePosition;
import io.tapstate.spi.store.SrsLogRecord;
import io.tapstate.spi.store.SrsLogStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("the change ring backed by the durable change log")
class SrsLogRingbufferStoreTest {

    private static final String RING = "srs.mc-1.orders";

    @Test
    @DisplayName("writes a change down before it is in the ring, so the ring never holds an unwritten one")
    void writesThroughBeforeAdmitting() {
        RecordingLog log = new RecordingLog();
        withMember(log, member -> {
            member.getRingbuffer(RING).add(item("a", 1L));

            assertThat(log.load(RING, 0L).orElseThrow().srcToken())
                    .as("the ring calls the store before it admits the change, so a change that is in the "
                            + "ring is already written down -- that ordering is what removes the need for a "
                            + "reconciliation pass of our own")
                    .isEqualTo("a");
        });
    }

    @Test
    @DisplayName("keeps the changes when the member holding the ring goes away")
    void changesOutliveTheMember() {
        RecordingLog log = new RecordingLog();
        withMember(log, member -> {
            member.getRingbuffer(RING).add(item("a", 1L));
            member.getRingbuffer(RING).add(item("b", 2L));
        });

        // The member is gone. Nothing of the ring is left in this process -- only the log.
        assertThat(log.largestSequence(RING)).isEqualTo(1L);

        withMember(log, member -> {
            // A rebuilt ring resumes above the largest sequence the log has seen rather than from zero, so
            // the sequence a change was written at keeps naming that change.
            long seq = member.getRingbuffer(RING).add(item("c", 3L));

            assertThat(seq)
                    .as("numbering from zero again would give two different changes the same sequence, and "
                            + "a consumer cursor recorded before the restart would then point at the wrong "
                            + "one -- silently, since both are ordinary changes")
                    .isEqualTo(2L);
            assertThat(log.load(RING, 0L).orElseThrow().srcToken()).isEqualTo("a");
            assertThat(log.load(RING, 1L).orElseThrow().srcToken()).isEqualTo("b");
        });
    }

    @Test
    @DisplayName("writes a run of changes in one act rather than one call per change")
    void writesARunInOneAct() {
        RecordingLog log = new RecordingLog();
        withMember(log, member -> {
            member.getRingbuffer(RING).addAllAsync(
                    List.of(item("a", 1L), item("b", 2L), item("c", 3L)),
                    com.hazelcast.ringbuffer.OverflowPolicy.OVERWRITE).toCompletableFuture().join();

            assertThat(log.storeAllCalls)
                    .as("the cost of a durable write is per call and not per byte, so a run written one "
                            + "call at a time costs N times what the same run costs in one")
                    .isEqualTo(1);
            assertThat(log.storeCalls).isZero();
            assertThat(log.load(RING, 2L).orElseThrow().srcToken()).isEqualTo("c");
        });
    }

    @Test
    @DisplayName("carries a change with no position through unchanged")
    void carriesAnAbsentPosition() {
        RecordingLog log = new RecordingLog();
        withMember(log, member -> {
            member.getRingbuffer(RING).add(new SrsItem(null, Op.INSERT, 5L, null, Map.of("id", 1), 0L));

            assertThat(log.load(RING, 0L).orElseThrow().srcToken()).isNull();
        });
    }

    private static SrsItem item(String token, long ts) {
        return new SrsItem(new SourcePosition(token), Op.INSERT, ts, null, Map.of("id", ts), 0L);
    }

    /** Runs {@code body} against a member whose srs rings are backed by {@code log}. */
    private static void withMember(SrsLogStore log, java.util.function.Consumer<HazelcastInstance> body) {
        Config config = new Config();
        config.setClusterName("srs-log-" + System.nanoTime());
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getSerializationConfig().addSerializerConfig(new SerializerConfig()
                .setTypeClass(SrsItem.class)
                .setImplementation(new SrsItemSerializer()));
        config.addRingBufferConfig(new RingbufferConfig("srs.*")
                .setCapacity(16)
                .setInMemoryFormat(InMemoryFormat.OBJECT)
                .setTimeToLiveSeconds(0)
                .setBackupCount(0)
                .setRingbufferStoreConfig(new RingbufferStoreConfig()
                        .setEnabled(true)
                        .setFactoryImplementation(new SrsLogRingbufferStoreFactory(log))));
        HazelcastInstance member = Hazelcast.newHazelcastInstance(config);
        try {
            body.accept(member);
        } finally {
            member.getLifecycleService().terminate();
        }
    }

    /** A change log in memory that also counts which of its two write paths the ring took. */
    private static final class RecordingLog implements SrsLogStore {

        private final Map<String, NavigableMap<Long, SrsLogRecord>> rings = new ConcurrentHashMap<>();
        private int storeCalls;
        private int storeAllCalls;

        @Override
        public void store(String ring, long seq, SrsLogRecord record) {
            storeCalls++;
            rings.computeIfAbsent(ring, name -> new ConcurrentSkipListMap<>()).put(seq, record);
        }

        @Override
        public void storeAll(String ring, long firstSeq, List<SrsLogRecord> records) {
            storeAllCalls++;
            long seq = firstSeq;
            for (SrsLogRecord record : records) {
                rings.computeIfAbsent(ring, name -> new ConcurrentSkipListMap<>()).put(seq++, record);
            }
        }

        @Override
        public Optional<SrsLogRecord> load(String ring, long seq) {
            NavigableMap<Long, SrsLogRecord> entries = rings.get(ring);
            return entries == null ? Optional.empty() : Optional.ofNullable(entries.get(seq));
        }

        @Override
        public long largestSequence(String ring) {
            NavigableMap<Long, SrsLogRecord> entries = rings.get(ring);
            return entries == null || entries.isEmpty() ? -1L : entries.lastKey();
        }

        @Override
        public void trim(String ring, long throughSeq) {
            NavigableMap<Long, SrsLogRecord> entries = rings.get(ring);
            if (entries != null) {
                entries.headMap(throughSeq, true).clear();
            }
        }
    }
}
