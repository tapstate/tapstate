package io.tapstate.runtime.srs;

import io.tapstate.core.event.ChainPosition;
import io.tapstate.spi.store.ConsumerOffset;
import io.tapstate.spi.store.SrsLogRecord;
import io.tapstate.spi.store.SrsLogStore;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class PerTableLogTrimmerTest {

    @Test
    void eachTableUsesOnlyItsSelectedConsumersAndItsOwnCompletion() {
        AtomicReference<Collection<ConsumerOffset>> consumers = new AtomicReference<>(List.of(
                consumer("orders-reader", List.of("orders"), 3L, Map.of()),
                consumer("customers-reader", List.of("customers"), 3L, Map.of("customers", 1L))));
        RecordingLog log = new RecordingLog();
        PerTableLogTrimmer trimmer = new PerTableLogTrimmer(consumers::get, log, 3L);
        trimmer.observed("orders", "ring.orders", 1L);
        trimmer.observed("customers", "ring.customers", 1L);

        trimmer.trim();

        assertThat(log.trims).containsExactly("ring.customers:1:3");
        consumers.set(List.of(
                consumer("orders-reader", List.of("orders"), 3L, Map.of("orders", 0L)),
                consumer("customers-reader", List.of("customers"), 3L, Map.of("customers", 1L))));
        trimmer.trim();
        assertThat(log.trims).containsExactly("ring.customers:1:3", "ring.orders:0:3");
    }

    @Test
    void unknownAndOldGenerationConsumersBlockCuts() {
        AtomicReference<Collection<ConsumerOffset>> consumers = new AtomicReference<>(List.of(
                consumer("known", List.of("orders"), 4L, Map.of("orders", 2L)),
                consumer("unknown", null, null, Map.of())));
        RecordingLog log = new RecordingLog();
        PerTableLogTrimmer trimmer = new PerTableLogTrimmer(consumers::get, log, 4L);
        trimmer.observed("orders", "ring.orders", 2L);

        trimmer.trim();
        assertThat(log.trims).isEmpty();

        consumers.set(List.of(consumer("known", List.of("orders"), 3L, Map.of("orders", 2L))));
        trimmer.trim();
        assertThat(log.trims).isEmpty();
    }

    @Test
    void oneSlowConsumerAndTheObservedRingHeadBoundTheCut() {
        AtomicReference<Collection<ConsumerOffset>> consumers = new AtomicReference<>(List.of(
                consumer("fast", List.of("orders"), 2L, Map.of("orders", 10L)),
                consumer("slow", List.of("orders"), 2L, Map.of("orders", 3L))));
        RecordingLog log = new RecordingLog();
        PerTableLogTrimmer trimmer = new PerTableLogTrimmer(consumers::get, log, 2L);
        trimmer.observed("orders", "ring.orders", 5L);

        trimmer.trim();
        assertThat(log.trims).containsExactly("ring.orders:3:2");

        consumers.set(List.of(
                consumer("fast", List.of("orders"), 2L, Map.of("orders", 10L)),
                consumer("slow", List.of("orders"), 2L, Map.of("orders", 10L))));
        trimmer.trim();
        trimmer.trim();
        assertThat(log.trims).containsExactly("ring.orders:3:2", "ring.orders:5:2");
    }

    private static ConsumerOffset consumer(
            String pipelineId, List<String> tables, Long epoch, Map<String, Long> done) {
        return new ConsumerOffset(pipelineId, Map.of(), null, List.of(), null, 0L,
                tables, epoch, tables == null ? null : "writer-" + pipelineId,
                Map.<String, ChainPosition>of(), done);
    }

    private static final class RecordingLog implements SrsLogStore {
        private final List<String> trims = new ArrayList<>();

        @Override
        public void store(String ring, long seq, SrsLogRecord record) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void storeAll(String ring, long firstSeq, List<SrsLogRecord> records) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<SrsLogRecord> load(String ring, long seq) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long largestSequence(String ring) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void trim(String ring, long throughSeq, long ringEpoch) {
            trims.add(ring + ":" + throughSeq + ":" + ringEpoch);
        }
    }
}
