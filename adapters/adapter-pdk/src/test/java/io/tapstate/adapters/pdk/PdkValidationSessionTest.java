package io.tapstate.adapters.pdk;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.TapValue;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationValues;
import io.tapstate.spi.store.ValidationRowCursor;
import io.tapstate.spi.store.ValidationReadHandle;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.assertj.core.api.Assertions.*;

class PdkValidationSessionTest {
    private static final ValidationValues.Limits LIMITS = new ValidationValues.Limits(8192, 100, 12);
    @TempDir static Path fixtureDirectory;
    private static ConnectorRef artifact;
    private final ExecutorService decodeIo = Executors.newSingleThreadExecutor(
            runnable -> new Thread(runnable, "validation-decode-fixture"));

    @AfterEach void closeDecoder() { decodeIo.close(); }

    @BeforeAll static void connectorFixture() {
        Path jar = SyntheticJar.compileToJar(fixtureDirectory, "synthetic.ValidationQuery", """
                package synthetic;
                import io.tapdata.pdk.apis.TapConnector;
                import io.tapdata.pdk.apis.functions.ConnectorFunctions;
                import io.tapdata.entity.codec.TapCodecsRegistry;
                import io.tapdata.entity.schema.value.TapStringValue;
                import io.tapdata.pdk.apis.context.TapConnectionContext;
                import io.tapdata.pdk.apis.context.TapConnectorContext;
                import io.tapdata.pdk.apis.entity.ConnectionOptions;
                import io.tapdata.pdk.apis.entity.TestItem;
                import io.tapdata.pdk.apis.entity.FilterResults;
                import io.tapdata.pdk.apis.entity.SortOn;
                import io.tapdata.entity.schema.TapTable;
                import io.tapdata.entity.schema.TapField;
                import io.tapdata.entity.schema.type.TapNumber;
                import java.util.List;
                import java.util.Map;
                import java.util.concurrent.CountDownLatch;
                import java.util.concurrent.TimeUnit;
                import java.util.concurrent.atomic.AtomicInteger;
                import java.util.concurrent.atomic.AtomicReference;
                import java.util.function.Consumer;
                public class ValidationQuery implements TapConnector {
                    private String tag;
                    private void loader() {
                        if (Thread.currentThread().getContextClassLoader() != getClass().getClassLoader())
                            throw new IllegalStateException("wrong connector loader");
                    }
                    private String mode(TapConnectionContext c) { return c.getConnectionConfig().getString("mode"); }
                    private void count(TapConnectionContext c, String key) {
                        ((AtomicInteger) c.getConnectionConfig().get(key)).incrementAndGet();
                    }
                    private void signal(TapConnectionContext c, String key) {
                        ((CountDownLatch) c.getConnectionConfig().get(key)).countDown();
                    }
                    public void registerCapabilities(ConnectorFunctions f, TapCodecsRegistry codecs) {
                        loader();
                        codecs.registerToTapValue(Integer.class, (value, model) -> {
                            loader();
                            if (!(model instanceof TapNumber)) throw new IllegalStateException("missing model");
                            return new TapStringValue(tag + ":" + value);
                        });
                        f.supportQueryByAdvanceFilter((c, filter, table, consumer) -> {
                            loader(); count(c, "queries"); signal(c, "entered");
                            if (filter.getLimit() != null || filter.getSkip() != null
                                    || filter.getBatchSize() != 2 || filter.getSortOnList().size() != 1
                                    || !filter.getSortOnList().get(0).getKey().equals("id")
                                    || filter.getSortOnList().get(0).getSort() != SortOn.ASCENDING)
                                throw new IllegalStateException("wrong scan request");
                            if (mode(c).equals("query-failure")) throw new Exception("query failed first");
                            if (mode(c).equals("projection")) {
                                if (filter.getProjection() == null
                                        || !new java.util.HashSet<>(filter.getProjection().getIncludeFields())
                                            .equals(java.util.Set.of("id", "nullable", "absent")))
                                    throw new IllegalStateException("missing necessary-field projection");
                            } else if (filter.getProjection() != null)
                                throw new IllegalStateException("unexpected full-row projection");
                            FilterResults result = new FilterResults();
                            Map<String, Object> row = new java.util.LinkedHashMap<>();
                            row.put("id", 7);
                            row.put("nullable", null);
                            row.put("opaque", new Object());
                            result.setResults(List.of(row));
                            if (mode(c).equals("callback-thread")) {
                                java.util.concurrent.CompletableFuture.runAsync(() -> consumer.accept(result)).get();
                            } else if ((mode(c).equals("swallow-callback") || mode(c).equals("swallow-then-throw") || mode(c).equals("swallow-then-fatal"))) {
                                try { consumer.accept(result); } catch (RuntimeException ignored) {}
                                if (mode(c).equals("swallow-then-throw")) throw new Exception("later query failure");
                                if (mode(c).equals("swallow-then-fatal")) throw new AssertionError("fatal after callback");
                            } else if (mode(c).equals("many-batches")) {
                                for (int i = 0; i < 64; i++) consumer.accept(result);
                            } else consumer.accept(result);
                            if (mode(c).equals("block-query"))
                                ((CountDownLatch) c.getConnectionConfig().get("release")).await();
                            if (mode(c).equals("abandon")) Thread.currentThread().interrupt();
                        });
                    }
                    public void init(TapConnectionContext c) throws Exception {
                        loader(); count(c, "inits"); tag = c.getConnectionConfig().getString("tag");
                        TapConnectorContext context = (TapConnectorContext)c;
                        if (context.getStateMap().putIfAbsent("private", tag) != null
                                || context.getGlobalStateMap().putIfAbsent("private", tag) != null)
                            throw new IllegalStateException("borrowed notes");
                        if (mode(c).equals("block-init")) {
                            signal(c, "entered");
                            while (!((CountDownLatch)c.getConnectionConfig().get("release")).await(10, TimeUnit.MILLISECONDS)) {}
                        }
                    }
                    public void stop(TapConnectionContext c) {
                        loader(); count(c, "stops"); signal(c, "release");
                        if (Thread.currentThread().isInterrupted()) count(c, "interruptedStops");
                        if (mode(c).equals("query-failure") || mode(c).equals("stop-failure"))
                            throw new IllegalStateException("cleanup failed");
                    }
                    public void discoverSchema(TapConnectionContext c, List<String> names, int limit,
                            Consumer<List<TapTable>> consumer) {
                        loader(); count(c, "discoveries");
                        if (!names.equals(List.of("records")) || limit != 2)
                            throw new IllegalStateException("unbounded discovery");
                        ((AtomicReference<Consumer<List<TapTable>>>)c.getConnectionConfig().get("discovery")).set(consumer);
                        TapTable table = new TapTable("records");
                        table.add(new TapField("id", "integer").tapType(new TapNumber()));
                        table.add(new TapField("nullable", "unknown"));
                        table.add(new TapField("absent", "unknown"));
                        table.add(new TapField("opaque", "unavailable-driver-type"));
                        consumer.accept(List.of(table));
                        if (mode(c).equals("duplicate-model")) {
                            try { consumer.accept(List.of(table)); } catch (RuntimeException ignored) {}
                        }
                    }
                    public ConnectionOptions connectionTest(TapConnectionContext c, Consumer<TestItem> s) {
                        return ConnectionOptions.create();
                    }
                    public int tableCount(TapConnectionContext c) { return 1; }
                }
                """);
        artifact = new ConnectorRef(List.of(jar), "synthetic.ValidationQuery", "2.0.8", null);
    }

    @Test void necessaryFieldProjectionSkipsUnusedModelsAndPayloadsWithoutInventingMissingValues() {
        var probe = new Probe("project", "projection");
        var requested = new java.util.ArrayList<>(List.of("id", "nullable", "absent"));
        var scan = new PdkValidationSession.Scan("records", List.of("id"), requested, 2);
        requested.clear();
        try (var io = Executors.newSingleThreadExecutor();
             var cursor = PdkValidationSession.scan("fixture", artifact, probe.settings(), "binding", scan,
                     (model, codecs) -> {
                         assertThat(model.getNameFieldMap()).containsOnlyKeys("id", "nullable", "absent");
                         return row -> {
                             assertThat(row).containsOnlyKeys("id", "nullable").containsEntry("nullable", null);
                             assertThat(row).doesNotContainKey("absent");
                             assertThatThrownBy(() -> row.put("id", 8)).isInstanceOf(UnsupportedOperationException.class);
                             return ValidationValues.ownRow(row, LIMITS);
                         };
                     }, () -> { }, io, decodeIo, 1, LIMITS)) {
            try (var row = cursor.next().orElseThrow()) {
                assertThat(row.values().fields()).containsOnlyKeys("id", "nullable");
                assertThat(row.values().fields().get("id")).isEqualTo(7);
            }
            assertThat(cursor.next()).isEmpty();
        }
        assertThat(probe.queries).hasValue(1);
    }

    @Test void fullRowReadKeepsExtraFieldsVisibleToAdmissionAndDecoder() {
        var probe = new Probe("full", "normal");
        try (var io = Executors.newSingleThreadExecutor();
             var cursor = scan(probe, io, (model, codecs) -> {
                 assertThat(model.getNameFieldMap()).containsKey("opaque");
                 return row -> {
                     assertThat(row).containsKeys("id", "opaque", "nullable");
                     return ValidationValues.ownRow(row, LIMITS);
                 };
             }, () -> { })) {
            assertCode(cursor, ValidationError.VALUE_TYPE_UNSUPPORTED);
        }
    }

    @Test void missingProjectedModelFieldFailsBeforeAdmissionOrQuery() {
        var probe = new Probe("missing", "normal");
        AtomicInteger admissions = new AtomicInteger();
        try (var io = Executors.newSingleThreadExecutor();
             var cursor = PdkValidationSession.scan("fixture", artifact, probe.settings(), "binding",
                     new PdkValidationSession.Scan("records", List.of("id"), List.of("id", "unknown"), 2),
                     (model, codecs) -> { admissions.incrementAndGet(); return decoder(model, codecs); },
                     () -> { }, io, decodeIo, 1, LIMITS)) {
            assertCode(cursor, ValidationError.READ_PROTOCOL_VIOLATION);
        }
        assertThat(admissions).hasValue(0);
        assertThat(probe.queries).hasValue(0);
    }

    @Test void invalidProjectionCannotDropPhysicalSortKeysOrAdmitDuplicateUnboundedFields() {
        for (List<String> projection : List.of(List.of("nullable"), List.of("id", "id"),
                List.of("id", " "), List.of("id", "x".repeat(513)),
                java.util.stream.IntStream.range(0, 4097).mapToObj(i -> "column" + i).toList())) {
            assertThatThrownBy(() -> new PdkValidationSession.Scan("records", List.of("id"), projection, 2))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test void independentSessionsOwnTheirNotesCodecsModelsAndOnlyTheirCancellation() throws Exception {
        var a = new Probe("a", "block-query");
        var b = new Probe("b", "block-query");
        try (var io = Executors.newFixedThreadPool(2);
             var first = scan(a, io); var second = scan(b, io)) {
            assertThat(value(first)).isEqualTo("a:7");
            assertThat(value(second)).isEqualTo("b:7");
            first.close();
            assertThat(a.release.await(3, TimeUnit.SECONDS)).isTrue();
            assertThat(b.stops).hasValue(0);
            b.release.countDown();
            assertThat(second.next()).isEmpty();
            assertThat(b.stops).hasValue(1);
            assertThat(a.inits).hasValue(1);
            assertThat(b.inits).hasValue(1);
            assertThat(a.discoveries).hasValue(1);
            assertThat(b.discoveries).hasValue(1);
        }
    }

    @Test void joinedConnectorCallbackStillDecodesOnAdmittedIoUnderTheConnectorLoader() {
        var probe = new Probe("off-thread", "callback-thread");
        try (var io = Executors.newSingleThreadExecutor(); var cursor = scan(probe, io)) {
            assertThatCode(() -> {
                assertThat(value(cursor)).isEqualTo("off-thread:7");
                assertThat(cursor.next()).isEmpty();
            }).doesNotThrowAnyException();
        }
    }

    @Test void successfulCompletionStopsItsHandleBeforeEofAndNeverReusesIt() {
        var probe = new Probe("read", "normal");
        try (var io = Executors.newSingleThreadExecutor()) {
            for (int i = 0; i < 2; i++) {
                try (var cursor = scan(probe, io)) {
                    assertThat(value(cursor)).isEqualTo("read:7");
                    assertThat(cursor.next()).isEmpty();
                    assertThat(probe.stops).hasValue(i + 1);
                }
            }
        }
        assertThat(probe.queries).hasValue(2);
        assertThat(probe.inits).hasValue(2);
    }

    @Test void cancellationDuringInitializationCannotStartDiscoveryOrQuery() throws Exception {
        var probe = new Probe("init", "block-init");
        try (var io = Executors.newSingleThreadExecutor(); var cursor = scan(probe, io)) {
            assertThat(probe.entered.await(3, TimeUnit.SECONDS)).isTrue();
            cursor.close();
            io.submit(() -> { }).get(3, TimeUnit.SECONDS);
            assertThat(probe.queries).hasValue(0);
            assertThat(probe.discoveries).hasValue(0);
            assertThat(probe.stops.get()).isGreaterThanOrEqualTo(1);
            assertCode(cursor, ValidationError.READ_CANCELLED);
        }
    }

    @Test void cancelledQueuedReadNeverConstructsOrInitializesAConnector() throws Exception {
        var probe = new Probe("queued", "normal");
        CountDownLatch releaseWorker = new CountDownLatch(1);
        try (var io = Executors.newSingleThreadExecutor()) {
            var occupied = io.submit(() -> { releaseWorker.await(); return null; });
            try (var cursor = scan(probe, io)) { cursor.close(); }
            finally { releaseWorker.countDown(); }
            occupied.get(3, TimeUnit.SECONDS);
            io.submit(() -> { }).get(3, TimeUnit.SECONDS);
            assertThat(probe.inits).hasValue(0);
            assertThat(probe.queries).hasValue(0);
        }
    }

    @Test void rejectedDecodeExecutorIsCodedEvenWhenTheConnectorSwallowsTheCallbackFailure() {
        decodeIo.shutdownNow();
        var probe = new Probe("rejected", "swallow-callback");
        try (var io = Executors.newSingleThreadExecutor(); var cursor = scan(probe, io)) {
            assertCode(cursor, ValidationError.READ_FAILED);
        }
    }

    @Test void laterQueryFailureDoesNotReplaceAnEarlierDispatchFailure() throws Exception {
        decodeIo.shutdownNow();
        var probe = new Probe("first-cause", "swallow-then-throw");
        try (var io = Executors.newSingleThreadExecutor(); var cursor = scan(probe, io)) {
            io.submit(() -> { }).get(3, TimeUnit.SECONDS);
            assertThatThrownBy(cursor::next).isInstanceOfSatisfying(TapstateException.class, failure -> {
                assertThat(failure.code()).isEqualTo(ValidationError.READ_FAILED);
                assertThat(failure.getCause()).isInstanceOf(java.util.concurrent.RejectedExecutionException.class);
                assertThat(failure.getSuppressed()).hasSize(1);
                assertThat(failure.getSuppressed()[0].getCause()).hasMessage("later query failure");
            });
        }
    }

    @Test void ordinaryFailureCannotHideALaterFatalError() throws Exception {
        decodeIo.shutdownNow();
        var probe = new Probe("fatal", "swallow-then-fatal");
        try (var io = Executors.newSingleThreadExecutor(); var cursor = scan(probe, io)) {
            io.submit(() -> { }).get(3, TimeUnit.SECONDS);
            assertThatThrownBy(cursor::next).isInstanceOfSatisfying(AssertionError.class, fatal -> {
                assertThat(fatal).hasMessage("fatal after callback");
                assertThat(fatal.getSuppressed()).isNotEmpty();
                assertThat(fatal.getSuppressed()[0].getCause()).isInstanceOf(java.util.concurrent.RejectedExecutionException.class);
            });
        }
    }

    @Test void closeInterruptsAnActiveDecodeAndKeepsTheReadCancelled() throws Exception {
        var probe = new Probe("decoding", "normal");
        CountDownLatch decoding = new CountDownLatch(1);
        CountDownLatch released = new CountDownLatch(1);
        try (var io = Executors.newSingleThreadExecutor();
             var cursor = scan(probe, io, (model, codecs) -> row -> {
                 decoding.countDown();
                 try { released.await(); }
                 catch (InterruptedException cancelled) { Thread.currentThread().interrupt(); }
                 return ValidationValues.ownRow(row, LIMITS);
             }, () -> { })) {
            try {
                assertThat(decoding.await(3, TimeUnit.SECONDS)).isTrue();
                cursor.close();
                io.submit(() -> { }).get(3, TimeUnit.SECONDS);
                decodeIo.submit(() -> { }).get(3, TimeUnit.SECONDS);
                assertCode(cursor, ValidationError.READ_CANCELLED);
                assertThat(probe.stops.get()).isGreaterThanOrEqualTo(1);
            } finally { released.countDown(); }
        }
    }

    @Test void workerExitWaitsForAnInterruptIgnoringDecoderAndRetainsItsLateFatalError() throws Exception {
        for (boolean fatal : List.of(false, true)) {
            var probe = new Probe("late-decode", "normal");
            var entered = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            var releasedReservations = new AtomicInteger();
            var late = new AssertionError("late decoder failure");
            try (var io = Executors.newSingleThreadExecutor();
                 var cursor = (ValidationReadHandle)scan(probe, io, (model, codecs) -> row -> {
                     entered.countDown(); uninterruptibly(release);
                     if (fatal) throw late;
                     return ValidationValues.ownRow(Map.of("id", 7), LIMITS);
                 }, () -> {})) {
                try {
                    cursor.workersExited().whenComplete((ignored, error) -> releasedReservations.incrementAndGet());
                    assertThat(entered.await(3, TimeUnit.SECONDS)).isTrue();
                    cursor.close();
                    io.submit(() -> {}).get(3, TimeUnit.SECONDS);
                    assertThat(cursor.workersExited().toCompletableFuture().isDone()).isFalse();
                    assertThat(cursor.rowsReleased().toCompletableFuture().isDone()).isFalse();
                    assertThat(releasedReservations).hasValue(0);
                    var detached = cursor.workersExited().toCompletableFuture();
                    detached.complete(null);
                    assertThat(cursor.workersExited().toCompletableFuture().isDone()).isFalse();
                    release.countDown(); decodeIo.submit(() -> {}).get(3, TimeUnit.SECONDS);
                    assertThatThrownBy(() -> cursor.workersExited().toCompletableFuture().get(3, TimeUnit.SECONDS))
                            .isInstanceOfSatisfying(java.util.concurrent.ExecutionException.class, error -> {
                                if (fatal) assertThat(error.getCause()).isSameAs(late);
                                else assertThat(error.getCause()).isInstanceOfSatisfying(TapstateException.class,
                                        stopped -> assertThat(stopped.code()).isEqualTo(ValidationError.READ_CANCELLED));
                            });
                    assertThat(releasedReservations).hasValue(1);
                    assertThat(cursor.rowsReleased().toCompletableFuture().get(3, TimeUnit.SECONDS)).isNull();
                    if (fatal) assertThatThrownBy(cursor::next).isSameAs(late);
                } finally { release.countDown(); }
            }
        }
    }

    @Test void serialBatchesRetireTheirDecodeActivityBeforeAdmittingTheNextBatch() throws Exception {
        var probe = new Probe("batches", "many-batches");
        try (var io = Executors.newSingleThreadExecutor(); var cursor = (ValidationReadHandle)scan(probe, io)) {
            int rows = 0;
            while (true) {
                var next = cursor.next();
                if (next.isEmpty()) break;
                try (var row = next.orElseThrow()) { assertThat(row.ordinal()).isEqualTo(rows++); }
            }
            assertThat(rows).isEqualTo(64);
            cursor.close();
            assertThat(cursor.workersExited().toCompletableFuture().get(3, TimeUnit.SECONDS)).isNull();
        }
    }

    private static void uninterruptibly(CountDownLatch latch) {
        while (true) {
            try { if (!latch.await(3, TimeUnit.SECONDS)) throw new AssertionError("fixture timed out"); return; }
            catch (InterruptedException ignored) { }
        }
    }

    @Test void modelAdmissionRunsBeforeBusinessReadsAndFailureStillStopsTheHandle() {
        var probe = new Probe("admit", "normal");
        var refusal = new TapstateException(ValidationError.VALUE_TYPE_UNSUPPORTED, Map.of("type", "fixture"), null);
        try (var io = Executors.newSingleThreadExecutor();
             var cursor = scan(probe, io, (model, codecs) -> { throw refusal; }, () -> { })) {
            assertThatThrownBy(cursor::next).isSameAs(refusal);
            assertThat(probe.queries).hasValue(0);
            assertThat(probe.stops.get()).isGreaterThanOrEqualTo(1);
        }
    }

    @Test void authorityLossAfterAdmissionPreventsTheQuery() {
        var probe = new Probe("auth", "normal");
        AtomicBoolean authorized = new AtomicBoolean(true);
        try (var io = Executors.newSingleThreadExecutor();
             var cursor = scan(probe, io, (model, codecs) -> {
                 authorized.set(false); return decoder(model, codecs);
             }, () -> {
                 if (!authorized.get()) throw new TapstateException(ValidationError.OWNER_NOT_AUTHORIZED,
                         Map.of("pipeline", "pipeline"), null);
             })) {
            assertCode(cursor, ValidationError.OWNER_NOT_AUTHORIZED);
            assertThat(probe.queries).hasValue(0);
        }
    }

    @Test void swallowedDuplicateModelCallbackCannotBecomeASuccessfulRead() {
        var probe = new Probe("duplicate", "duplicate-model");
        try (var io = Executors.newSingleThreadExecutor(); var cursor = scan(probe, io)) {
            assertCode(cursor, ValidationError.READ_PROTOCOL_VIOLATION);
            assertThat(probe.queries).hasValue(0);
        }
    }

    @Test void lateDiscoveryCallbackPermanentlyInvalidatesTheRead() {
        var probe = new Probe("late", "normal");
        try (var io = Executors.newSingleThreadExecutor(); var cursor = scan(probe, io)) {
            value(cursor);
            assertThat(cursor.next()).isEmpty();
            assertThatThrownBy(() -> probe.discovery.get().accept(List.of(new TapTable("records"))))
                    .isInstanceOf(TapstateException.class);
            assertCode(cursor, ValidationError.READ_PROTOCOL_VIOLATION);
        }
    }

    @Test void queryFailureRemainsPrimaryWhenCleanupAlsoFails() throws Exception {
        var probe = new Probe("fail", "query-failure");
        try (var io = Executors.newSingleThreadExecutor(); var cursor = scan(probe, io)) {
            io.submit(() -> { }).get(3, TimeUnit.SECONDS);
            assertThatThrownBy(cursor::next).isInstanceOfSatisfying(TapstateException.class, failure -> {
                assertThat(failure.code()).isEqualTo(ValidationError.READ_FAILED);
                assertThat(failure.getCause()).hasMessage("query failed first");
                assertThat(failure.getSuppressed()).hasSize(1);
                assertThat(failure.getSuppressed()[0].getCause()).hasMessage("cleanup failed");
            });
        }
    }

    @Test void cleanupFailureAndQuietAbandonmentAreNotNormalEof() {
        try (var io = Executors.newSingleThreadExecutor()) {
            for (String mode : List.of("stop-failure", "abandon")) {
                var probe = new Probe(mode, mode);
                try (var cursor = scan(probe, io)) {
                    assertThatThrownBy(() -> {
                        while (true) {
                            var row = cursor.next();
                            if (row.isEmpty()) break;
                            row.orElseThrow().close();
                        }
                    })
                            .isInstanceOf(TapstateException.class);
                }
            }
        }
    }

    @Test void anInterruptedQueryStillRunsConnectorCleanupWithoutAnInheritedInterrupt() throws Exception {
        var probe = new Probe("interrupt-cleanup", "abandon");
        try (var io = Executors.newSingleThreadExecutor(); var cursor = scan(probe, io)) {
            assertThatThrownBy(() -> {
                while (true) {
                    var next = cursor.next();
                    if (next.isEmpty()) break;
                    next.orElseThrow().close();
                }
            }).isInstanceOf(TapstateException.class);
            io.submit(() -> {}).get(3, TimeUnit.SECONDS);
            assertThat(probe.stops.get()).isGreaterThanOrEqualTo(1);
            assertThat(probe.interruptedStops).hasValue(0);
        }
    }

    private static PdkValidationCursor.Decoder decoder(TapTable model, PdkValidationCodecs codecs) {
        return row -> {
            assertThat(Thread.currentThread().getName()).isEqualTo("validation-decode-fixture");
            return ValidationValues.ownRow(Map.of("id", ((TapValue<?, ?>)codecs.select("id", row.get("id"),
                    model.getNameFieldMap().get("id").getTapType()).value()).getValue()), LIMITS);
        };
    }

    private ValidationRowCursor scan(Probe probe, ExecutorService io) {
        return scan(probe, io, PdkValidationSessionTest::decoder, () -> { });
    }

    private ValidationRowCursor scan(Probe probe, ExecutorService io,
            PdkValidationSession.ReadAdmission admission, Runnable guard) {
        return PdkValidationSession.scan("fixture", artifact, probe.settings(), "fixture-content-hash",
                new PdkValidationSession.Scan("records", List.of("id"), 2), admission, guard, io, decodeIo, 1, LIMITS);
    }

    private static Object value(ValidationRowCursor cursor) {
        try (var row = cursor.next().orElseThrow()) { return row.values().fields().get("id"); }
    }

    private static void assertCode(ValidationRowCursor cursor, ValidationError code) {
        assertThatThrownBy(cursor::next).isInstanceOfSatisfying(TapstateException.class,
                failure -> assertThat(failure.code()).isEqualTo(code));
    }

    private static final class Probe {
        private final String tag;
        private final String mode;
        private final AtomicInteger inits = new AtomicInteger();
        private final AtomicInteger queries = new AtomicInteger();
        private final AtomicInteger discoveries = new AtomicInteger();
        private final AtomicInteger stops = new AtomicInteger();
        private final AtomicInteger interruptedStops = new AtomicInteger();
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private final AtomicReference<Consumer<List<TapTable>>> discovery = new AtomicReference<>();
        private Probe(String tag, String mode) { this.tag = tag; this.mode = mode; }
        private Map<String, Object> settings() {
            return Map.of("tag", tag, "mode", mode, "inits", inits, "queries", queries,
                    "discoveries", discoveries, "stops", stops, "entered", entered,
                    "release", release, "discovery", discovery, "interruptedStops", interruptedStops);
        }
    }
}
