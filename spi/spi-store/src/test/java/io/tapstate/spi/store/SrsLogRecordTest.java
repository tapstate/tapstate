package io.tapstate.spi.store;

import io.tapstate.core.event.Op;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("SrsLogRecord")
class SrsLogRecordTest {

    @Test
    @DisplayName("keeps a row image the caller mutates afterwards as it was when handed over")
    void copiesRowImagesDefensively() {
        Map<String, Object> after = new LinkedHashMap<>(Map.of("id", 1));

        SrsLogRecord record = new SrsLogRecord("tok", Op.INSERT, 1L, null, after, 3L);
        after.put("id", 2);

        assertThat(record.after())
                .as("a log record is written down and read back by another run, so a caller still holding "
                        + "the map it passed must not be able to change what was recorded")
                .containsEntry("id", 1);
    }

    @Test
    @DisplayName("refuses a snapshot read, which the ring never holds either")
    void refusesSnapshotRead() {
        assertThatThrownBy(() -> new SrsLogRecord(null, Op.READ, 1L, null, Map.of("id", 1), 0L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("snapshot read");
    }

    @Test
    @DisplayName("refuses a negative schema version")
    void refusesNegativeSchemaVersion() {
        assertThatThrownBy(() -> new SrsLogRecord(null, Op.INSERT, 1L, null, Map.of("id", 1), -1L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("schemaVer");
    }

    @Test
    @DisplayName("carries no position for a change the source stated none at")
    void allowsAnAbsentPosition() {
        SrsLogRecord record = new SrsLogRecord(null, Op.UPDATE, 7L, Map.of("id", 1), Map.of("id", 2), 0L);

        assertThat(record.srcToken())
                .as("only the change closing a run of changes carries the source's position; the others "
                        + "carry none, and that absence is the record's meaning rather than a gap")
                .isNull();
    }
}
