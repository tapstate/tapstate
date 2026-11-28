package io.tapstate.e2e;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.tapstate.e2e.BenchmarkBacklogProbe.SourceRecord;
import io.tapstate.e2e.BenchmarkLatencyProbe.Confirmation;
import io.tapstate.e2e.BenchmarkLatencyProbe.Event;
import io.tapstate.e2e.benchmark.MongoWorkloadGenerator.Operations;
import io.tapstate.e2e.benchmark.MongoWorkloadGenerator.PhaseReport;
import io.tapstate.e2e.benchmark.MongoWorkloadGenerator.Position;
import io.tapstate.e2e.benchmark.MongoWorkloadGenerator.StreamReport;
import java.util.List;
import java.util.Map;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.junit.jupiter.api.Test;

class BenchmarkBacklogProbeTest {
    private static final SourceRecord LEFT = record("left", 5, "left-tail", 2_003);
    private static final SourceRecord RIGHT = record("right", 9, "right-tail", 2_010);
    private static final Confirmation LEFT_ACK = new Confirmation(LEFT.event(), 2_018);
    private static final Confirmation RIGHT_ACK = new Confirmation(RIGHT.event(), 2_015);

    @Test
    void stopsAtTheLastAcknowledgementEvenWhenItsSourceTailWasReadFirst() {
        var result = BenchmarkBacklogProbe.verify(batch(), List.of(LEFT, RIGHT), List.of(RIGHT_ACK, LEFT_ACK),
                2_000, 2_100, event -> true);
        assertEquals(18, result.durationMillis());
        assertEquals(2, result.confirmedWrites());
        assertEquals(LEFT.token(), result.sourceTails().get("left").token());
        assertEquals(2_010, result.sourceTails().get("right").readEpochMillis());
    }

    @Test
    void rejectsASameTimestampOutsideTokenAndAnyMissingInputOrAcknowledgement() {
        var outside = record("right", 9, "outside-batch", 2_010);
        assertThrows(IllegalArgumentException.class, () -> BenchmarkBacklogProbe.verify(batch(),
                List.of(LEFT, outside), List.of(LEFT_ACK, RIGHT_ACK), 2_000, 2_100, event -> true));
        assertThrows(IllegalArgumentException.class, () -> BenchmarkBacklogProbe.verify(batch(),
                List.of(LEFT), List.of(LEFT_ACK), 2_000, 2_100, event -> true));
        assertThrows(IllegalArgumentException.class, () -> BenchmarkBacklogProbe.verify(batch(),
                List.of(LEFT, RIGHT), List.of(LEFT_ACK), 2_000, 2_100, event -> true));
        var early = new Confirmation(RIGHT.event(), 2_009);
        assertThrows(IllegalArgumentException.class, () -> BenchmarkBacklogProbe.verify(batch(),
                List.of(LEFT, RIGHT), List.of(LEFT_ACK, early), 2_000, 2_100, event -> true));
    }

    @Test
    void usesALaterFilteredSourceTailAndRejectsUnexpectedOutput() {
        var filtered = new SourceRecord(LEFT.stream(), LEFT.event(), LEFT.token(), 2_020);
        var result = BenchmarkBacklogProbe.verify(batch(), List.of(RIGHT, filtered), List.of(RIGHT_ACK),
                2_000, 2_100, RIGHT.event()::equals);
        assertEquals(20, result.durationMillis());
        assertEquals(2, result.sourceEvents());
        assertEquals(1, result.confirmedWrites());
        assertThrows(IllegalArgumentException.class, () -> BenchmarkBacklogProbe.verify(batch(),
                List.of(RIGHT, filtered), List.of(RIGHT_ACK, new Confirmation(LEFT.event(), 2_025)),
                2_000, 2_100, RIGHT.event()::equals));
    }

    private static SourceRecord record(String stream, long key, String token, long readMillis) {
        return new SourceRecord(stream, new Event(0, key, 1_000),
                new BsonDocument("_data", new BsonString(token)), readMillis);
    }

    private static PhaseReport batch() {
        return new PhaseReport("fixed", null, null, 0, 0, null, null, null, false, 0, 0, null,
                Map.of("left", stream(LEFT), "right", stream(RIGHT)), true);
    }

    private static StreamReport stream(SourceRecord record) {
        var count = new Operations(1, 0, 0);
        var endpoint = new Position(record.token(), new BsonTimestamp(1, 1));
        return new StreamReport(count, count, count, null, endpoint, endpoint, null, 1, 0,
                null, BenchmarkLatencyProbe.digest(List.of(new Confirmation(record.event(), 0))), null);
    }
}
