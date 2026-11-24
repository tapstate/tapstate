package io.tapstate.e2e;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapstate.e2e.mongowitness.LatencyWitness;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LatencyWitnessTest {
    @TempDir Path directory;

    @Test
    void recordsOnlyFullyConfirmedSuccessfulCallsAfterActivation() throws Throwable {
        Path source = directory.resolve("source.tsv");
        Path ack = directory.resolve("source.tsv.ack");
        List<TapRecordEvent> batch = List.of(insert(1), insert(2));
        var result = new WriteListResult<TapRecordEvent>(2, 0, 0);
        var received = new ArrayList<WriteListResult<TapRecordEvent>>();
        var complete = LatencyWitness.target(source, (context, events, table, consumer) -> {
            assertSame(batch, events);
            consumer.accept(result);
        });
        complete.writeRecord(null, batch, null, received::add);
        assertEquals(List.of(result), received);
        assertFalse(Files.exists(ack));

        Files.createFile(directory.resolve("source.tsv.active"));
        var partial = new WriteListResult<TapRecordEvent>(1, 0, 0);
        var split = LatencyWitness.target(source, (context, events, table, consumer) -> {
            consumer.accept(partial);
            consumer.accept(partial);
        });
        split.writeRecord(null, batch, null, received::add);
        assertEquals(List.of(result, partial, partial), received);
        assertFalse(Files.exists(ack));

        var failure = new IllegalStateException("connector failed after callback");
        var failed = LatencyWitness.target(source, (context, events, table, consumer) -> {
            consumer.accept(result);
            throw failure;
        });
        assertSame(failure, assertThrows(IllegalStateException.class,
                () -> failed.writeRecord(null, batch, null, received::add)));
        assertFalse(Files.exists(ack));

        complete.writeRecord(null, batch, null, received::add);
        var events = BenchmarkLatencyProbe.read(ack, true);
        assertEquals(List.of(new BenchmarkLatencyProbe.Event(0, 1, 1_000),
                new BenchmarkLatencyProbe.Event(0, 2, 1_000)),
                events.stream().map(BenchmarkLatencyProbe.Confirmation::event).toList());
        assertEquals(events.getFirst().ackMillis(), events.getLast().ackMillis());
    }

    private static TapInsertRecordEvent insert(long key) {
        return TapInsertRecordEvent.create().table("records").referenceTime(1_000L).after(Map.of("_id", key));
    }
}
