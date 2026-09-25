package io.tapstate.e2e;

import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Server-wide Mongo command counters are parsed and compared without application instrumentation. */
class BenchmarkMongoCommandSamplerTest {

    @Test
    void deltasKeepRawCommandNamesAndStableFamiliesWithoutCountingSamplerReads() {
        BenchmarkMongoCommandSampler.Snapshot before = BenchmarkMongoCommandSampler.parse(status(100,
                Map.of("find", 10L, "update", 3L, "serverStatus", 5L, "ping", 2L)));
        BenchmarkMongoCommandSampler.Snapshot after = BenchmarkMongoCommandSampler.parse(status(105,
                Map.of("find", 13L, "update", 5L, "createIndexes", 1L,
                        "serverStatus", 6L, "ping", 4L)));

        BenchmarkMongoCommandSampler.Summary delta =
                BenchmarkMongoCommandSampler.difference(before, after, 500);
        assertThat(delta.byCommand()).containsExactlyInAnyOrderEntriesOf(Map.of(
                "find", 3L, "update", 2L, "createIndexes", 1L, "ping", 2L));
        assertThat(delta.byCommand()).doesNotContainKey("serverStatus");
        assertThat(delta.byFamily()).containsEntry(BenchmarkMongoCommandSampler.Family.READ, 3L)
                .containsEntry(BenchmarkMongoCommandSampler.Family.WRITE, 2L)
                .containsEntry(BenchmarkMongoCommandSampler.Family.SCHEMA, 1L)
                .containsEntry(BenchmarkMongoCommandSampler.Family.OTHER, 2L)
                .containsEntry(BenchmarkMongoCommandSampler.Family.TRANSACTION, 0L);
        assertThat(delta.totalCommands()).isEqualTo(8);
        assertThat(delta.elapsedMillis()).isEqualTo(500);
    }

    @Test
    void unavailableMalformedOrResetCountersFailClosed() {
        assertThatThrownBy(() -> BenchmarkMongoCommandSampler.parse(new Document("ok", 1)))
                .isInstanceOf(AssertionError.class).hasMessageContaining("no complete command-counter header");
        Document malformed = status(100, Map.of("find", 1L));
        malformed.get("metrics", Document.class).get("commands", Document.class)
                .put("update", new Document("failed", 0));
        assertThatThrownBy(() -> BenchmarkMongoCommandSampler.parse(malformed))
                .isInstanceOf(AssertionError.class).hasMessageContaining("counter is unavailable: update");

        BenchmarkMongoCommandSampler.Snapshot before = BenchmarkMongoCommandSampler.parse(status(100,
                Map.of("find", 10L, "update", 3L)));
        BenchmarkMongoCommandSampler.Snapshot backward = BenchmarkMongoCommandSampler.parse(status(105,
                Map.of("find", 9L, "update", 4L)));
        assertThatThrownBy(() -> BenchmarkMongoCommandSampler.difference(before, backward, 1))
                .isInstanceOf(AssertionError.class).hasMessageContaining("moved backward: find");

        BenchmarkMongoCommandSampler.Snapshot missing = BenchmarkMongoCommandSampler.parse(status(105,
                Map.of("find", 11L)));
        assertThatThrownBy(() -> BenchmarkMongoCommandSampler.difference(before, missing, 1))
                .isInstanceOf(AssertionError.class).hasMessageContaining("counter disappeared: update");

        BenchmarkMongoCommandSampler.Snapshot restarted = BenchmarkMongoCommandSampler.parse(status(10,
                Map.of("find", 11L, "update", 4L)));
        assertThatThrownBy(() -> BenchmarkMongoCommandSampler.difference(before, restarted, 1))
                .isInstanceOf(AssertionError.class).hasMessageContaining("server restart");
    }

    @Test
    void MongoSevenScalarUnknownCommandIsRetainedButOtherScalarShapesAreRefused() {
        Document status = status(100, Map.of("find", 1L));
        status.get("metrics", Document.class).get("commands", Document.class).put("<UNKNOWN>", 2L);
        assertThat(BenchmarkMongoCommandSampler.parse(status).commandTotals())
                .containsEntry("<UNKNOWN>", 2L);
        status.get("metrics", Document.class).get("commands", Document.class).put("update", 2L);
        assertThatThrownBy(() -> BenchmarkMongoCommandSampler.parse(status))
                .isInstanceOf(AssertionError.class).hasMessageContaining("counter is unavailable: update");
    }

    private static Document status(long uptimeMillis, Map<String, Long> totals) {
        Document commands = new Document();
        totals.forEach((name, total) -> commands.put(name, new Document("total", total).append("failed", 0)));
        return new Document("host", "benchmark-mongo").append("pid", 1234L)
                .append("uptimeMillis", uptimeMillis)
                .append("metrics", new Document("commands", commands));
    }
}
