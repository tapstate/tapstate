package io.tapstate.adapters.pdk;

import io.tapdata.pdk.apis.entity.ExecuteResult;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Envelope;
import io.tapstate.spi.sink.*;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PdkMongoIdentityGuardTest {
    private static ConnectorRef ref;
    @BeforeAll static void fixture(@TempDir Path path) { ref = IdentityConnectorFixture.ref(path); }

    @ParameterizedTest
    @ValueSource(strings = {"unsafe", "missing-flag", "too-many", "no-answer", "non-row", "error-after-rows", "throws", "empty-envelope", "missing-rows"})
    void unsafeIncompleteAndFailedChecksRejectBeforeWriting(String kind) throws Throwable {
        try (var connector = PdkConnector.open("mongodb", ref, Map.of())) {
            connector.functions().supportExecuteCommandFunction((context, command, out) -> {
                switch (kind) {
                    case "unsafe" -> out.accept(new ExecuteResult<>().result(envelope(List.of(Map.of("safe", false)))));
                    case "missing-flag" -> out.accept(new ExecuteResult<>().result(envelope(List.of(Map.of()))));
                    case "too-many" -> out.accept(new ExecuteResult<>().result(envelope(List.of(Map.of("safe", true), Map.of("safe", true)))));
                    case "empty-envelope" -> out.accept(new ExecuteResult<>().result(List.of()));
                    case "missing-rows" -> out.accept(new ExecuteResult<>().result(List.of(Map.of())));
                    case "no-answer" -> out.accept(null);
                    case "non-row" -> out.accept(new ExecuteResult<>().result(List.of("not a document")));
                    case "error-after-rows" -> {
                        out.accept(new ExecuteResult<>().result(envelope(List.of(Map.of("safe", true)))));
                        out.accept(new ExecuteResult<>().error(new IllegalStateException("cursor failed")));
                    }
                    default -> throw new IllegalStateException("query failed");
                }
            });
            List<Map<String, Object>> keys = List.of(Map.of("original", Map.of("$eq", "p"), "k", Map.of("$eq", "a")));
            assertThatThrownBy(() -> PdkMongoIdentityGuard.verify(connector, MongoIdentityKeysTest.PLAN, keys))
                    .isInstanceOf(TapstateException.class);
        }
    }

    @Test
    void allChunksAndStreamsAreCheckedBeforeTheFirstWrite() throws Throwable {
        var connector = PdkConnector.open("mongodb", ref, Map.of("rows", IdentityConnectorFixture.mongoIndexes()));
        List<String> calls = new ArrayList<>();
        var target = new TargetTable("orders", List.of(new TargetField("_id", "string", true),
                new TargetField("position", "long", true)), List.of(new TargetIndex(List.of("_id", "position"), true)),
                new UnwindIdentity(List.of("_id"), "_unwind_source_id"));
        var config = new SinkConfig("mongodb", Map.of(), WriteMode.UPSERT, DdlPolicy.FAIL);
        try (var writer = new PdkSinkWriter(connector, (context, rows, table, out) -> {
            calls.add("write"); out.accept(new WriteListResult<io.tapdata.entity.event.dml.TapRecordEvent>().insertedCount(rows.size()));
        }, config, Map.of("one", target, "two", target), null)) {
            writer.prepareTargets();
            connector.functions().supportExecuteCommandFunction((context, command, out) -> {
                calls.add("check");
                out.accept(new ExecuteResult<>().result(envelope(calls.size() == 3 ? List.of(Map.of("safe", false)) : List.of())));
            });
            List<Envelope> rows = new ArrayList<>();
            for (long i = 0; i < 129; i++) rows.add(Envelope.insert(1L, "one", Map.of("_id", "p", "position", i), null));
            rows.add(Envelope.delete(2L, "two", Map.of("_id", "p", "position", 900L), null));
            assertThatThrownBy(() -> writer.write(rows).toCompletableFuture().get(5, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(TapstateException.class);
            assertThat(calls).containsExactly("check", "check", "check");
            calls.clear();
            rows.set(1, Envelope.insert(1L, "one", Map.of("_id", "p", "position", List.of(1L)), null));
            assertThatThrownBy(() -> writer.write(rows).toCompletableFuture().get(5, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(TapstateException.class);
            assertThat(calls).isEmpty();
        }
    }

    @Test
    void authorityLostInOneChunkPreventsTheNextChunkQuery() throws Throwable {
        var active = new java.util.concurrent.atomic.AtomicBoolean(true);
        var calls = new java.util.concurrent.atomic.AtomicInteger();
        var lost = new TapstateException(io.tapstate.spi.store.IoError.NOTE_SCOPE_UNAVAILABLE, Map.of(), null);
        try (var connector = IdentityConnectorFixture.guarded("mongodb", ref, Map.of(), () -> {
            if (!active.get()) throw lost;
        })) {
            connector.functions().supportExecuteCommandFunction((context, command, out) -> {
                calls.incrementAndGet();
                out.accept(new ExecuteResult<>().result(envelope(List.of())));
                active.set(false);
            });
            List<Map<String, Object>> keys = new ArrayList<>();
            for (int i = 0; i < 129; i++) keys.add(Map.of("original", "p", "k", i));
            assertThatThrownBy(() -> PdkMongoIdentityGuard.verify(connector, MongoIdentityKeysTest.PLAN, keys))
                    .isSameAs(lost);
            assertThat(calls).hasValue(1);
        }
    }

    private static List<Map<String, Object>> envelope(List<?> rows) { return List.of(Map.of("rows", rows)); }

    @Test
    void pointChecksDeduplicateNativePredicatesAndKeepBothUpdateImages() throws Throwable {
        try (var connector = PdkConnector.open("mongodb", ref, Map.of())) {
            List<Object> pipelines = new ArrayList<>();
            connector.functions().supportExecuteCommandFunction((context, command, out) -> {
                pipelines.add(command.getParams().get("pipeline"));
                out.accept(new ExecuteResult<>().result(envelope(List.of(Map.of("safe", true)))));
            });
            var stringKey = MongoIdentityKeys.key(MongoIdentityKeysTest.PLAN, Map.of("original", "p", "k", "012345678901234567890123"));
            var objectKey = MongoIdentityKeys.key(MongoIdentityKeysTest.PLAN, Map.of("original", "p", "k",
                    new io.tapstate.core.event.ConvertedValue("012345678901234567890123", "OBJECT_ID")));
            PdkMongoIdentityGuard.verify(connector, MongoIdentityKeysTest.PLAN, List.of(stringKey, objectKey, stringKey));
            assertThat(pipelines).hasSize(1);
            assertThat(((List<?>) pipelines.getFirst()).getFirst()).isEqualTo(Map.of("$match", Map.of("$or", List.of(stringKey, objectKey))));
        }
    }
}
