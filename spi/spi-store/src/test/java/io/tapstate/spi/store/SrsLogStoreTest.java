package io.tapstate.spi.store;

import io.tapstate.core.event.Op;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class SrsLogStoreTest {

    @Test
    void compatibilityReadExposesAHoleWithoutPackingLaterRecordsIntoIt() {
        SrsLogStore store = reading(Map.of(10L, record("low"), 12L, record("high"), 13L, record("mail")));

        SrsLogBatch batch = store.readBatch("root", 10L, 3);

        assertThat(batch.bounds()).isEqualTo(new SrsLogBounds(13L, -1L));
        assertThat(batch.records().keySet()).containsExactly(10L, 12L);
        assertThat(batch.records().get(12L).srcToken()).isEqualTo("high");
        assertThat(batch.records()).doesNotContainKey(11L);
    }

    @Test
    void aReadAtTheLargestLongDoesNotWrapIntoAnotherSequence() {
        SrsLogStore store = reading(Map.of(Long.MAX_VALUE, record("last")));

        assertThat(store.readBatch("root", Long.MAX_VALUE, 256).records().keySet())
                .containsExactly(Long.MAX_VALUE);
    }

    private static SrsLogRecord record(String token) {
        return new SrsLogRecord(token, Op.INSERT, 1L, null, Map.of("id", 1), 0L);
    }

    private static SrsLogStore reading(Map<Long, SrsLogRecord> records) {
        return new SrsLogStore() {
            @Override
            public void store(String ring, long seq, SrsLogRecord record) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void storeAll(String ring, long firstSeq, List<SrsLogRecord> supplied) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Optional<SrsLogRecord> load(String ring, long seq) {
                return Optional.ofNullable(records.get(seq));
            }

            @Override
            public long largestSequence(String ring) {
                return records.keySet().stream().mapToLong(Long::longValue).max().orElse(-1L);
            }

            @Override
            public void trim(String ring, long throughSeq) {
                throw new UnsupportedOperationException();
            }
        };
    }
}
