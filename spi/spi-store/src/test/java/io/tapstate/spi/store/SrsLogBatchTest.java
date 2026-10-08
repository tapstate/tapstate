package io.tapstate.spi.store;

import io.tapstate.core.event.Op;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SrsLogBatchTest {

    @Test
    void exactSequenceKeysAndOriginalGenerationsSurviveCallerMutation() {
        SrsLogRecord first = new SrsLogRecord("low", Op.UPDATE, 1L, null, Map.of("priority", "Low"), 0L, 7L);
        SrsLogRecord later = new SrsLogRecord("mail", Op.INSERT, 2L, null, Map.of("id", 2), 0L, 8L);
        Map<Long, SrsLogRecord> supplied = new LinkedHashMap<>();
        supplied.put(12L, later);
        supplied.put(10L, first);
        SrsLogBatch batch = new SrsLogBatch(new SrsLogBounds(12L, 9L), supplied);

        supplied.clear();

        assertThat(batch.records().keySet()).containsExactly(10L, 12L);
        assertThat(batch.records().get(10L)).isEqualTo(first);
        assertThat(batch.records().get(12L).epoch()).isEqualTo(8L);
        assertThatThrownBy(() -> batch.records().put(11L, first))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
