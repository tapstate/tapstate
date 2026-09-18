package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Envelope;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.WriteResult;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;

class ViewSinkWritersTest {

    @Test
    void a_delete_without_the_alternate_key_is_refused_before_any_row_is_written() {
        RecordingWriter delegate = new RecordingWriter();
        SinkWriter writer = ViewSinkWriters.requireAlternateKeyInBeforeImage(
                delegate, "order_state", "id");

        CompletionStage<WriteResult> result = writer.write(List.of(
                Envelope.insert(1L, "orders", Map.of("id", 16), null),
                Envelope.delete(2L, "orders", Map.of("_id", "internal-1"), null)));

        assertThatThrownBy(() -> result.toCompletableFuture().join())
                .cause()
                .isInstanceOf(TapstateException.class)
                .satisfies(failure -> {
                    TapstateException coded = (TapstateException) failure;
                    assertThat(coded.code().code())
                            .isEqualTo("engine.view-key-missing-from-before-image");
                    assertThat(coded.args()).containsEntry("view", "order_state")
                            .containsEntry("key", "id")
                            .containsEntry("operation", "delete");
                });
        assertThat(delegate.batches).isEmpty();
    }

    @Test
    void an_update_without_the_alternate_key_in_its_before_image_is_refused() {
        RecordingWriter delegate = new RecordingWriter();
        SinkWriter writer = ViewSinkWriters.requireAlternateKeyInBeforeImage(
                delegate, "order_state", "id");

        CompletionStage<WriteResult> result = writer.write(List.of(Envelope.update(
                1L, "orders", Map.of("_id", "internal-1"), Map.of("id", 15), null)));

        assertThatThrownBy(() -> result.toCompletableFuture().join())
                .cause()
                .isInstanceOf(TapstateException.class)
                .satisfies(failure -> assertThat(((TapstateException) failure).args())
                        .containsEntry("operation", "update"));
        assertThat(delegate.batches).isEmpty();
    }

    @Test
    void complete_changes_and_rows_without_a_before_image_reach_the_delegate_unchanged() {
        RecordingWriter delegate = new RecordingWriter();
        SinkWriter writer = ViewSinkWriters.requireAlternateKeyInBeforeImage(
                delegate, "order_state", "id");
        List<Envelope> records = List.of(
                Envelope.read(1L, "orders", Map.of("id", 1), null),
                Envelope.insert(2L, "orders", Map.of("id", 2), null),
                Envelope.update(3L, "orders", Map.of("id", 2), Map.of("id", 3), null),
                Envelope.delete(4L, "orders", Map.of("id", 3), null),
                Envelope.ddl(5L, "orders", Map.of("id", "int")));

        WriteResult result = writer.write(records).toCompletableFuture().join();

        assertThat(result.written()).isEqualTo(records.size());
        assertThat(delegate.batches).containsExactly(records);
    }

    @Test
    void closing_the_guard_closes_its_delegate() {
        RecordingWriter delegate = new RecordingWriter();

        ViewSinkWriters.requireAlternateKeyInBeforeImage(
                delegate, "order_state", "id").close();

        assertThat(delegate.closed).isTrue();
    }

    private static final class RecordingWriter implements SinkWriter {
        private final List<List<Envelope>> batches = new ArrayList<>();
        private boolean closed;

        @Override
        public CompletionStage<WriteResult> write(List<Envelope> records) {
            batches.add(records);
            return CompletableFuture.completedFuture(new WriteResult(records.size()));
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
