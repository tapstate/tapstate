package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.ReservedField;
import io.tapstate.core.event.ReservedField.Owner;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.WriteResult;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ServedRowsTest {

    private static final Map<String, Object> ORDER = Map.of("e", 4, "q", 9L);
    private static final Map<String, Object> MARKER = Map.of("by_day", Map.of("status", "failed"));

    /** A writer that keeps what it was handed, standing in for a target. */
    private static final class Target implements SinkWriter {
        final List<Envelope> received = new ArrayList<>();

        @Override
        public CompletionStage<WriteResult> write(List<Envelope> records) {
            received.addAll(records);
            return CompletableFuture.completedFuture(new WriteResult(records.size()));
        }

        @Override
        public void close() {
        }
    }

    private static Map<String, Object> marked(int id) {
        Map<String, Object> row = ReservedField.with(Map.of("id", id), Owner.SRS, ORDER);
        return ReservedField.with(row, Owner.AGGREGATE, MARKER);
    }

    @Test
    @DisplayName("a target receives the consumer marker and never the internal entry, on both row images")
    void internalDataDoesNotReachTheTarget() {
        Target target = new Target();
        Envelope update = Envelope.update(1L, "s.t", marked(1), marked(1), null);

        ServedRows.over(target, true).write(List.of(update));

        Envelope written = target.received.get(0);
        for (Map<String, Object> image : List.of(written.before(), written.after())) {
            assertThat(ReservedField.read(image, Owner.SRS)).isNull();
            assertThat(ReservedField.read(image, Owner.AGGREGATE)).isEqualTo(MARKER);
            assertThat(image).containsEntry("id", 1);
        }
        assertThat(written.op()).isEqualTo(update.op());
        assertThat(written.src()).isEqualTo(update.src());
    }

    @Test
    @DisplayName("a target that opted out receives no reserved field at all")
    void anOptedOutTargetReceivesNothingReserved() {
        Target target = new Target();

        ServedRows.over(target, false).write(List.of(Envelope.insert(1L, "s.t", marked(2), null)));

        assertThat(target.received.get(0).after()).isEqualTo(Map.of("id", 2));
    }

    @Test
    @DisplayName("a batch carrying nothing reserved is handed on as the same list")
    void anUnmarkedBatchIsNotCopied() {
        List<Envelope> batch = List.of(Envelope.insert(1L, "s.t", Map.of("id", 3), null));
        List<Envelope>[] seen = new List[1];
        SinkWriter target = new SinkWriter() {
            @Override
            public CompletionStage<WriteResult> write(List<Envelope> records) {
                seen[0] = records;
                return CompletableFuture.completedFuture(new WriteResult(records.size()));
            }

            @Override
            public void close() {
            }
        };

        ServedRows.over(target, true).write(batch);

        assertThat(seen[0]).isSameAs(batch);
    }
}
