package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapstate.e2e.mongowitness.MongoPresentBoundaryWitness;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MongoPresentBoundaryWitnessTest {
    @TempDir Path temporary;

    @Test
    void preserves_the_exact_returned_offset_and_stream_arguments_without_serializing_other_fields() throws Throwable {
        Path journal = temporary.resolve("boundary.tsv");
        ConnectorFunctions functions = new ConnectorFunctions();
        Object sentinel = new Object() {
            @Override public String toString() { throw new AssertionError("must not inspect the token"); }
        };
        Map<String, Object> offset = new LinkedHashMap<>();
        offset.put("cdcOffset", 1_792_251_694);
        offset.put("resumeToken", sentinel);
        AtomicInteger calls = new AtomicInteger();
        List<String> tables = List.of("orders");
        functions.supportTimestampToStreamOffset((context, start) -> {
            assertThat(start).isNull();
            calls.incrementAndGet();
            return offset;
        });
        var consumer = io.tapdata.pdk.apis.consumer.StreamReadConsumer.create((events, point) -> {});
        functions.supportStreamRead((context, received, value, size, callback) -> {
            assertThat(received).isSameAs(tables);
            assertThat(value).isSameAs(offset);
            assertThat(callback).isSameAs(consumer);
            assertThat(size).isEqualTo(7);
            calls.incrementAndGet();
        });
        MongoPresentBoundaryWitness.install(functions, journal);
        assertThat(functions.getTimestampToStreamOffsetFunction().timestampToStreamOffset(null, null)).isSameAs(offset);
        functions.getStreamReadFunction().streamRead(null, tables, offset, 7, consumer);
        assertThat(calls).hasValue(2);
        assertThat(Files.readAllLines(journal)).containsExactly("RESOLVE\ttrue\t1792251694", "STREAM\t1792251694");
        assertThat(offset.get("resumeToken")).isSameAs(sentinel);
    }

    @Test
    void does_not_stringify_an_unknown_offset_and_preserves_unsigned_bson_seconds() throws Throwable {
        Path journal = temporary.resolve("unknown.tsv");
        ConnectorFunctions functions = new ConnectorFunctions();
        Object unknown = new Object() {
            @Override public String toString() { throw new AssertionError("must not stringify unknown offsets"); }
        };
        functions.supportTimestampToStreamOffset((context, start) -> unknown);
        functions.supportStreamRead((context, tables, offset, size, consumer) -> {});
        MongoPresentBoundaryWitness.install(functions, journal);
        assertThat(functions.getTimestampToStreamOffsetFunction().timestampToStreamOffset(null, 0L)).isSameAs(unknown);
        functions.getStreamReadFunction().streamRead(null, List.of(), Map.of("cdcOffset", -1), 1, null);
        assertThat(Files.readAllLines(journal)).containsExactly("RESOLVE\tfalse\tunknown", "STREAM\t4294967295");
    }

    @Test
    void a_failed_journal_cannot_change_successful_returns_or_prevent_stream_entry() throws Throwable {
        Path unavailable = temporary.resolve("a-directory");
        Files.createDirectory(unavailable);
        ConnectorFunctions functions = new ConnectorFunctions();
        Map<String, Object> offset = Map.of("cdcOffset", 1);
        AtomicInteger streams = new AtomicInteger();
        functions.supportTimestampToStreamOffset((context, start) -> offset);
        functions.supportStreamRead((context, tables, received, size, consumer) -> {
            assertThat(received).isSameAs(offset);
            streams.incrementAndGet();
        });
        MongoPresentBoundaryWitness.install(functions, unavailable);
        assertThat(functions.getTimestampToStreamOffsetFunction().timestampToStreamOffset(null, null)).isSameAs(offset);
        functions.getStreamReadFunction().streamRead(null, List.of(), offset, 1, null);
        assertThat(streams).hasValue(1);
    }

    @Test
    void a_failed_journal_does_not_replace_the_original_stream_failure() throws Exception {
        Path unavailable = temporary.resolve("another-directory");
        Files.createDirectory(unavailable);
        ConnectorFunctions functions = new ConnectorFunctions();
        AssertionError original = new AssertionError("stream delegate failure");
        functions.supportStreamRead((context, tables, offset, size, consumer) -> { throw original; });
        MongoPresentBoundaryWitness.install(functions, unavailable);
        assertThatThrownBy(() -> functions.getStreamReadFunction().streamRead(null, List.of(), null, 1, null))
                .isSameAs(original);
    }

    @Test
    void propagates_the_same_delegate_failure_without_fabricating_a_resolved_boundary() {
        Path journal = temporary.resolve("failure.tsv");
        ConnectorFunctions functions = new ConnectorFunctions();
        AssertionError failure = new AssertionError("delegate failure");
        functions.supportTimestampToStreamOffset((context, start) -> { throw failure; });
        MongoPresentBoundaryWitness.install(functions, journal);
        assertThatThrownBy(() -> functions.getTimestampToStreamOffsetFunction().timestampToStreamOffset(null, null))
                .isSameAs(failure);
        assertThat(journal).doesNotExist();
    }
}
