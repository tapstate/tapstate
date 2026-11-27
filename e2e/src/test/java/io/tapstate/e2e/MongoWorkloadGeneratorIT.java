package io.tapstate.e2e;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocumentBeforeChange;
import io.tapstate.e2e.benchmark.MongoWorkloadGenerator;
import io.tapstate.e2e.benchmark.MongoWorkloadGenerator.HotKeys;
import io.tapstate.e2e.benchmark.MongoWorkloadGenerator.LiveWindow;
import io.tapstate.e2e.benchmark.MongoWorkloadGenerator.Operations;
import io.tapstate.e2e.benchmark.WorkloadRecipe;
import io.tapstate.e2e.benchmark.WorkloadRecipe.Mix;
import io.tapstate.testsupport.RequiresDocker;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.bson.BsonDocument;
import org.bson.BsonType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

@RequiresDocker
class MongoWorkloadGeneratorIT {
    private static BsonDocument catalog;

    @BeforeAll
    static void readActualCatalog() throws Exception {
        String file = System.getProperty("tapstate.benchmarks.catalog");
        if (file != null) {
            catalog = BsonDocument.parse(Files.readString(Path.of(file)));
            return;
        }
        Path here = Path.of("").toAbsolutePath();
        while (here != null && !Files.isRegularFile(here.resolve("benchmarks/catalog.py"))) {
            here = here.getParent();
        }
        assertNotNull(here, "Find the benchmark catalog tool in the checkout");
        Path output = Files.createTempFile("perf-catalog-", ".json");
        try {
            var generator = new ProcessBuilder("python3", here.resolve("benchmarks/catalog.py").toString())
                    .redirectOutput(output.toFile()).redirectError(ProcessBuilder.Redirect.INHERIT).start();
            if (!generator.waitFor(30, TimeUnit.SECONDS)) {
                generator.destroyForcibly();
                throw new IOException("Timed out building the test catalog");
            }
            assertEquals(0, generator.exitValue());
            catalog = BsonDocument.parse(Files.readString(output));
        } finally {
            Files.deleteIfExists(output);
        }
    }

    @Test
    void fourProfilesHaveRealDeterministicBsonTypesAndWidths() throws Exception {
        withSource(source -> {
            var widths = Map.of("small", 256, "standard", 1024, "wide", 4096, "large", 16384);
            for (var entry : widths.entrySet()) {
                var generator = generator(source, "copy.snapshot.basic." + entry.getKey(), Map.of("records", 500));
                var preparation = generator.prepare(generator.calibrateStringLengths()).preparation();
                var row = source.getCollection("records", BsonDocument.class).find().sort(Sorts.ascending("_id")).first();
                assertNotNull(row);
                assertEquals(BsonType.INT64, row.get("_id").getBsonType());
                assertEquals(1, row.getInt64("_id").getValue());
                var table = preparation.tables().get("records");
                assertEquals(500, table.rows());
                assertEquals(1, table.firstKey());
                assertEquals(500, table.lastKey());
                assertTrue(table.rowWidthWithinTolerance(), table.logicalBytes().toString());
                assertEquals(entry.getValue(), table.logicalBytes().mean(), entry.getValue() * 0.05);
                System.out.printf("Source profile %s: MongoDB %s, rows %d, logical bytes %.2f, BSON bytes %.2f%n",
                        entry.getKey(), preparation.source().mongoVersion(), table.rows(),
                        table.logicalBytes().mean(), table.bsonBytes().mean());
                assertTrue(table.bsonTypesByField().values().stream().anyMatch(types -> types.containsKey("DECIMAL128")));
                assertTrue(table.bsonTypesByField().values().stream().anyMatch(types -> types.containsKey("DATE_TIME")));
                assertTrue(table.nullValues() > 0);
                assertEquals("copy.snapshot.basic." + entry.getKey(), preparation.source().recipe().caseId());
                source.getCollection("records").drop();
            }
        });
        var first = independentLoad();
        var second = independentLoad();
        assertEquals(first.tables().get("records"), second.tables().get("records"));
        assertEquals(first.source().stringBaseLengths(), second.source().stringBaseLengths());
        assertNotEquals(first.source().namespace(), second.source().namespace());
    }

    @Test
    void arrayMembersAndRelationsAreVerifiedFromTheDatabase() throws Exception {
        withSource(database -> {
            var unwind = generator(database, "unwind.snapshot.basic.small", Map.of("parents", 32));
            var state = unwind.prepare(unwind.calibrateStringLengths()).preparation();
            assertEquals(Map.of(4, 32L), state.tables().get("parents").arrayLengths());
            assertTrue(state.tables().get("parents").rowWidthWithinTolerance(), state::toString);
            for (var row : database.getCollection("parents", BsonDocument.class).find()) {
                var elements = row.getArray("items");
                for (int i = 0; i < 4; i++) {
                    assertEquals(i, elements.get(i).asDocument().getInt32("k").getValue());
                    assertEquals(i, elements.get(i).asDocument().getInt32("p").getValue());
                }
            }

            var nest = generator(database, "nest.snapshot.basic.standard", Map.of("roots", 16, "children", 64));
            var nested = nest.prepare(nest.calibrateStringLengths()).preparation().tables().get("children");
            assertEquals(64, nested.matchedReferences());
            assertEquals(0, nested.missingReferences());
            Map<Long, Integer> children = new HashMap<>();
            for (var row : database.getCollection("children", BsonDocument.class).find()) {
                children.merge(row.getInt64("root_id").getValue(), 1, Integer::sum);
            }
            assertEquals(16, children.size());
            for (var row : database.getCollection("roots", BsonDocument.class).find()) {
                assertEquals(4, children.get(row.getInt64("_id").getValue()));
            }

            var join = generator(database, "join.snapshot.basic.standard", Map.of("dimensions", 100, "facts", 1000));
            var joined = join.prepare(join.calibrateStringLengths()).preparation().tables().get("facts");
            assertEquals(900, joined.matchedReferences());
            assertEquals(100, joined.missingReferences());
        });
    }

    @Test
    void fixedBatchHasIndependentPerStreamHistoryAndRepeatableOrder() throws Exception {
        var first = backlog();
        var second = backlog();
        for (String input : List.of("left", "right")) {
            var stream = first.batch().streams().get(input);
            assertEquals(stream.sourceSequenceSha256(), second.batch().streams().get(input).sourceSequenceSha256());
            assertNotEquals(stream.last().resumeToken(), second.batch().streams().get(input).last().resumeToken());
        }
    }

    @Test
    void manualMixAndGuardrailsApplyBeforeSendingChanges() throws Exception {
        withSource(source -> {
            var generator = generator(source, "copy.backlog.fixed-iud.standard", Map.of("records", 4));
            var lengths = generator.calibrateStringLengths();
            var prepared = generator.prepare(lengths);
            assertThrows(IllegalArgumentException.class, () -> new Mix(10, 10, 10));
            assertThrows(IllegalArgumentException.class, () -> prepared.prewrite(10, new Mix(0, 100, 0), null));
            assertThrows(IllegalArgumentException.class, () -> generator.prepare(lengths));
            assertEquals(4, source.getCollection("records").countDocuments());
            var report = prepared.prewrite(12, new Mix(100, 0, 0), null);
            assertTrue(report.batch().complete(), report::toString);
            assertEquals(new Operations(12, 0, 0), report.batch().streams().get("records").observed());
            assertEquals(16, source.getCollection("records").countDocuments());
            assertThrows(IllegalStateException.class, () -> prepared.prewrite(1, new Mix(100, 0, 0), null));
        });
    }

    @Test
    void liveInjectionSeparatesWarmupAndMeasurementAndPacesIndividualOperations() throws Exception {
        withSource(source -> {
            String id = "copy.live.basic.standard";
            var clock = new AtomicLong();
            var generator = new MongoWorkloadGenerator(source, WorkloadRecipe.fromCatalog(definition(id),
                    Map.of("records", 40), null), id, 17, clock::get, clock::addAndGet);
            var prepared = generator.prepare(generator.calibrateStringLengths());
            AtomicInteger boundary = new AtomicInteger();
            var result = prepared.inject(new LiveWindow(10, Duration.ofMillis(600), Duration.ofMillis(1200)),
                    null, null, () -> boundary.set(Math.toIntExact(source.getCollection("records").countDocuments())));
            assertTrue(result.warmup().complete(), result::toString);
            assertTrue(result.measured().complete(), result::toString);
            assertEquals(45, boundary.get());
            assertEquals(6, result.warmup().sourceRequestLatency().requests());
            assertEquals(12, result.measured().sourceRequestLatency().requests());
            assertEquals(10, result.measured().requestedEventsPerSecond());
            assertTrue(result.measured().actualAcknowledgedEventsPerSecond() > 6, result::toString);
            assertTrue(result.measured().actualAcknowledgedEventsPerSecond() <= 11, result::toString);
            var warm = result.warmup().streams().get("records");
            var scored = result.measured().streams().get("records");
            var warmChanges = readHistory(source, "records", warm);
            var scoreChanges = readHistory(source, "records", scored);
            assertTrue(warm.last().clusterTime().compareTo(scored.first().clusterTime()) < 0);
            System.out.printf("Source live window: requested %d/s, acknowledged %.2f/s, warm-up %d, measured %d%n",
                    result.measured().requestedEventsPerSecond(), result.measured().actualAcknowledgedEventsPerSecond(),
                    warm.acknowledged().total(), scored.acknowledged().total());
            var warmKeys = new HashSet<Long>();
            warmChanges.forEach(event -> warmKeys.add(event.getDocumentKey().getInt64("_id").getValue()));
            assertTrue(scoreChanges.stream().noneMatch(event ->
                    warmKeys.contains(event.getDocumentKey().getInt64("_id").getValue())));
        });
    }

    @ParameterizedTest
    @CsvSource({
            "11, false, 0 21 31 41 51",
            "19, true, 0 29 39 49 59",
            "31, false, 0 41 51"
    })
    void liveWindowCompletesWithTheWritesSubmittedBeforeItsCutoff(
            long overshootMillis, boolean acknowledgedAfterWindow, String expectedWrites)
            throws Exception {
        withSource(source -> {
            String id = "map.live.basic.standard";
            var clock = new AtomicLong();
            var scoredStart = new AtomicLong(-1);
            var overshot = new AtomicBoolean();
            var writes = new ArrayList<Long>();
            var observed = observingPhases(source, () -> {}, () -> {
                if (scoredStart.get() >= 0) {
                    writes.add(TimeUnit.NANOSECONDS.toMillis(clock.get() - scoredStart.get()));
                }
                clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(2));
            });
            var generator = new MongoWorkloadGenerator(observed, WorkloadRecipe.fromCatalog(definition(id),
                    Map.of("records", 40), null), id, 17, clock::get, delay -> {
                        clock.addAndGet(delay);
                        if (scoredStart.get() >= 0 && overshot.compareAndSet(false, true)) {
                            clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(overshootMillis));
                        }
                    });
            var prepared = generator.prepare(generator.calibrateStringLengths());
            var result = prepared.inject(new LiveWindow(100, Duration.ofMillis(30), Duration.ofMillis(60)),
                    null, null, () -> scoredStart.set(clock.get()));

            assertTrue(result.warmup().complete(), result::toString);
            var measured = result.measured();
            var records = measured.streams().get("records");
            assertTrue(measured.complete(), result::toString);
            assertEquals(6, records.planned().total());
            assertEquals(writes.size(), readHistory(source, "records", records).size());
            assertEquals(writes.size(), records.submittedOperations());
            assertEquals(records.submittedOperations(), records.acknowledged().total());
            assertEquals(records.acknowledged(), records.observed());
            assertEquals(java.util.Arrays.stream(expectedWrites.split(" ")).map(Long::valueOf).toList(), writes);
            assertEquals(TimeUnit.MILLISECONDS.toNanos(60), measured.scheduledWindowNanos());
            assertEquals(writes.size() / 0.06, measured.actualSubmittedEventsPerSecond(), 1e-9);
            assertEquals(acknowledgedAfterWindow, measured.acknowledgedAfterWindow());
        });
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void readinessFailureKeepsWarmupEvidenceAndSkipsScoring(boolean assertion) throws Exception {
        withSource(source -> {
            var generator = generator(source, "copy.live.basic.standard", Map.of("records", 10));
            var prepared = generator.prepare(generator.calibrateStringLengths());
            var called = new AtomicBoolean();
            var result = prepared.inject(new LiveWindow(1, Duration.ofMillis(1900), Duration.ofSeconds(5)),
                    new Mix(100, 0, 0), null, () -> {
                        called.set(true);
                        if (assertion) throw new AssertionError("reader not ready");
                        throw new IllegalStateException("reader not ready");
                    });
            assertTrue(called.get());
            assertTrue(result.warmup().complete(), result::toString);
            assertFalse(result.measured().complete());
            assertEquals(11, source.getCollection("records").countDocuments());
            var scoring = result.measured().streams().get("records");
            assertEquals(5, scoring.planned().total());
            assertEquals(0, scoring.submittedOperations());
            assertTrue(scoring.error().contains("reader not ready"));
        });
    }

    @Test
    void aFailedTargetBoundaryKeepsObservedSourceHistory() throws Exception {
        withSource(source -> {
            var generator = generator(source, "copy.live.basic.standard", Map.of("records", 10));
            var prepared = generator.prepare(generator.calibrateStringLengths());
            var result = prepared.inject(new LiveWindow(1, Duration.ofMillis(1900), Duration.ofMillis(1900)),
                    new Mix(100, 0, 0), null, () -> {}, () -> {
                        throw new IllegalStateException("target frontier unavailable");
                    });
            assertTrue(result.warmup().complete(), result::toString);
            assertFalse(result.measured().complete());
            var measured = result.measured().streams().get("records");
            assertEquals(1, measured.submittedOperations());
            assertEquals(new Operations(1, 0, 0), measured.acknowledged());
            assertEquals(measured.acknowledged(), measured.observed());
            assertNotNull(measured.sourceSequenceSha256());
            assertTrue(measured.error().contains("target frontier unavailable"));
        });
    }

    @Test
    void measurementSetupFailureKeepsTheVerifiedWarmup() throws Exception {
        withSource(source -> {
            String id = "copy.live.basic.standard";
            var watches = new AtomicInteger();
            var clock = new AtomicLong();
            var observed = observingPhases(source, () -> {
                if (watches.incrementAndGet() == 2) throw new com.mongodb.MongoException("measurement watch unavailable");
            }, () -> {});
            var generator = new MongoWorkloadGenerator(observed, WorkloadRecipe.fromCatalog(definition(id),
                    Map.of("records", 10), null), id, 17, clock::get, clock::addAndGet);
            var prepared = generator.prepare(generator.calibrateStringLengths());
            var result = prepared.inject(new LiveWindow(1, Duration.ofSeconds(2), Duration.ofSeconds(2)),
                    new Mix(100, 0, 0), null, () -> {});

            assertTrue(result.warmup().complete(), result::toString);
            assertEquals(new Operations(2, 0, 0), result.warmup().streams().get("records").observed());
            assertFalse(result.measured().complete());
            var measured = result.measured().streams().get("records");
            assertEquals(0, measured.submittedOperations());
            assertEquals(0, measured.observed().total());
            assertTrue(measured.error().contains("measurement watch unavailable"));
            assertEquals(12, source.getCollection("records").countDocuments());
        });
    }

    @Test
    void aWriteSubmittedInTheWindowCanBeAcknowledgedAfterItsCutoff() throws Exception {
        withSource(source -> {
            String id = "copy.live.basic.standard";
            var writes = new AtomicInteger();
            var clock = new AtomicLong();
            var observed = observingPhases(source, () -> {}, () -> {
                if (writes.incrementAndGet() == 3) clock.addAndGet(TimeUnit.SECONDS.toNanos(5));
            });
            var generator = new MongoWorkloadGenerator(observed, WorkloadRecipe.fromCatalog(definition(id),
                    Map.of("records", 10), null), id, 17, clock::get, clock::addAndGet);
            var prepared = generator.prepare(generator.calibrateStringLengths());
            var result = prepared.inject(new LiveWindow(1, Duration.ofSeconds(1), Duration.ofSeconds(2)),
                    new Mix(100, 0, 0), null, () -> {});

            assertTrue(result.warmup().complete(), result::toString);
            var phase = result.measured();
            assertTrue(phase.complete(), phase::toString);
            assertTrue(phase.acknowledgedAfterWindow());
            assertEquals(TimeUnit.SECONDS.toNanos(2), phase.scheduledWindowNanos());
            assertEquals(TimeUnit.SECONDS.toNanos(6), phase.writeNanos());
            assertEquals(1.0, phase.actualSubmittedEventsPerSecond());
            assertEquals(1.0, phase.actualAcknowledgedEventsPerSecond());
            var measured = phase.streams().get("records");
            assertEquals(2, measured.submittedOperations());
            assertEquals(new Operations(2, 0, 0), measured.acknowledged());
            assertEquals(measured.acknowledged(), measured.observed());
            assertNull(measured.error());
            assertNotNull(measured.sourceSequenceSha256());
        });
    }

    @Test
    void aFailedHistoryCloseKeepsBothStreamReportsAndClosesTheOtherCursor() throws Exception {
        withSource(source -> {
            String id = "union.backlog.basic.standard";
            var closed = new AtomicInteger();
            var observed = observingPhases(source, () -> {}, () -> {}, () -> {
                if (closed.incrementAndGet() == 1) throw new com.mongodb.MongoException("first cursor close failed");
            });
            var generator = generator(observed, id, Map.of("left", 30, "right", 30));
            var batch = generator.prepare(generator.calibrateStringLengths()).prewrite(20, null, null).batch();

            assertEquals(2, closed.get());
            assertFalse(batch.complete());
            assertEquals(2, batch.streams().size());
            assertEquals(20, batch.streams().values().stream().mapToLong(stream -> stream.observed().total()).sum());
            assertTrue(batch.streams().values().stream().allMatch(stream -> stream.observed().equals(stream.acknowledged())));
            assertEquals(1, batch.streams().values().stream().filter(stream -> stream.error() != null
                    && stream.error().contains("first cursor close failed")).count());
        });
    }

    private static MongoDatabase observingPhases(MongoDatabase database, Runnable beforeWatch, Runnable afterWrite) {
        return observingPhases(database, beforeWatch, afterWrite, () -> {});
    }

    private static MongoDatabase observingPhases(MongoDatabase database, Runnable beforeWatch, Runnable afterWrite,
            Runnable afterClose) {
        return (MongoDatabase) Proxy.newProxyInstance(MongoDatabase.class.getClassLoader(),
                new Class<?>[] {MongoDatabase.class}, (proxy, method, args) -> {
                    if (method.getName().equals("watch")) beforeWatch.run();
                    Object result = invoke(database, method, args);
                    if (result instanceof MongoDatabase configured) {
                        return observingPhases(configured, beforeWatch, afterWrite, afterClose);
                    }
                    if (result instanceof com.mongodb.client.ChangeStreamIterable<?> watch) {
                        return observingClose(watch, afterClose);
                    }
                    if (result instanceof MongoCollection<?> collection) {
                        return Proxy.newProxyInstance(MongoCollection.class.getClassLoader(),
                                new Class<?>[] {MongoCollection.class}, (ignored, operation, parameters) -> {
                                    Object reply = invoke(collection, operation, parameters);
                                    if (operation.getName().equals("bulkWrite")) afterWrite.run();
                                    return reply;
                                });
                    }
                    return result;
                });
    }

    private static Object observingClose(com.mongodb.client.ChangeStreamIterable<?> watch, Runnable afterClose) {
        var changeStream = com.mongodb.client.ChangeStreamIterable.class;
        var cursorType = com.mongodb.client.MongoChangeStreamCursor.class;
        return Proxy.newProxyInstance(changeStream.getClassLoader(), new Class<?>[] {changeStream},
                (ignored, operation, parameters) -> {
                    Object reply = invoke(watch, operation, parameters);
                    if (reply instanceof com.mongodb.client.ChangeStreamIterable<?> configured) {
                        return observingClose(configured, afterClose);
                    }
                    if (cursorType.isInstance(reply)) {
                        return Proxy.newProxyInstance(cursorType.getClassLoader(), new Class<?>[] {cursorType},
                                (cursor, method, args) -> {
                                    Object value = invoke(reply, method, args);
                                    if (method.getName().equals("close")) afterClose.run();
                                    return value;
                                });
                    }
                    return reply;
                });
    }

    @Test
    void aTwoStreamBatchCanAllocateOneUpdateAndOneDelete() throws Exception {
        withSource(source -> {
            var generator = generator(source, "union.backlog.basic.standard", Map.of("left", 1, "right", 1));
            var prepared = generator.prepare(generator.calibrateStringLengths());
            var result = prepared.prewrite(2, new Mix(0, 50, 50), null);
            assertTrue(result.batch().complete(), result::toString);
            assertEquals(new Operations(0, 1, 0), result.batch().streams().get("left").observed());
            assertEquals(new Operations(0, 0, 1), result.batch().streams().get("right").observed());
        });
    }

    @Test
    void aLostWriteAcknowledgementStillShowsTheCommittedSourceEvent() throws Exception {
        withSource(source -> {
            var generator = generator(withLostWriteAcknowledgement(source), "copy.backlog.fixed-iud.standard",
                    Map.of("records", 10));
            var prepared = generator.prepare(generator.calibrateStringLengths());
            var result = prepared.prewrite(1, new Mix(100, 0, 0), null);
            var stream = result.batch().streams().get("records");
            assertFalse(result.batch().complete());
            assertEquals(1, stream.submittedOperations());
            assertEquals(0, stream.acknowledged().total());
            assertEquals(new Operations(1, 0, 0), stream.observed());
            assertNotNull(stream.historyBoundary());
            assertTrue(stream.error().contains("acknowledgement lost after commit"));
            assertEquals(11, source.getCollection("records").countDocuments());
        });
    }

    @Test
    void operatorReadbackRejectsRowsInUnexpectedCollections() throws Exception {
        withSource(database -> {
            var operators = new BenchmarkOperators(definition("filter.snapshot.basic.standard"), 1, false);
            var row = BsonDocument.parse("{'_id': 2, 'value': 1}");
            var expected = BenchmarkOperatorOracle.expected("filter-basic", Map.of("records", List.of(row)));
            database.getCollection("records", BsonDocument.class).insertOne(row);
            assertNull(BenchmarkOperatorOracle.difference(operators, database, expected));
            database.createCollection("unexpected");
            assertNull(BenchmarkOperatorOracle.difference(operators, database, expected));
            database.getCollection("unexpected", BsonDocument.class).insertOne(row);
            assertEquals("Unexpected nonempty target collection unexpected",
                    BenchmarkOperatorOracle.difference(operators, database, expected));
        });
    }

    private static MongoDatabase withLostWriteAcknowledgement(MongoDatabase database) {
        return (MongoDatabase) Proxy.newProxyInstance(MongoDatabase.class.getClassLoader(),
                new Class<?>[] {MongoDatabase.class}, (proxy, method, args) -> {
                    Object result = invoke(database, method, args);
                    if (result instanceof MongoDatabase configured) {
                        return withLostWriteAcknowledgement(configured);
                    }
                    if (result instanceof MongoCollection<?> collection && args != null && "records".equals(args[0])) {
                        return Proxy.newProxyInstance(MongoCollection.class.getClassLoader(),
                                new Class<?>[] {MongoCollection.class}, (ignored, operation, parameters) -> {
                                    Object reply = invoke(collection, operation, parameters);
                                    if (operation.getName().equals("bulkWrite")) {
                                        throw new com.mongodb.MongoException("acknowledgement lost after commit");
                                    }
                                    return reply;
                                });
                    }
                    return result;
                });
    }

    private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException failure) {
            throw failure.getCause();
        }
    }

    @Test
    void hotVariantsTouchOnlyExpectedKeysAndSteadyUpdatesKeepTheKeyset() throws Exception {
        var baselineOrder = new ArrayList<String>();
        var mix = new Mix(0, 70, 30);
        withSource(source -> {
            var base = generator(source, "join.live.hot-update.standard", Map.of("dimensions", 100, "facts", 200));
            var report = base.prepare(base.calibrateStringLengths()).prewrite(20, mix, null);
            assertTrue(report.batch().complete(), report::toString);
            readHistory(source, "dimensions", report.batch().streams().get("dimensions"))
                    .forEach(event -> baselineOrder.add(event.getOperationType().getValue()));
        });
        withSource(source -> {
            var generator = generator(source, "join.live.hot-update.standard", Map.of("dimensions", 100, "facts", 200));
            var prepared = generator.prepare(generator.calibrateStringLengths());
            var hot = prepared.prewrite(20, mix, new HotKeys(20, 100));
            var dimensions = hot.batch().streams().get("dimensions");
            assertTrue(hot.batch().complete(), hot::toString);
            assertEquals(20, dimensions.hotKeySelections());
            assertEquals(94, source.getCollection("dimensions").countDocuments());
            var events = readHistory(source, "dimensions", dimensions);
            assertEquals(baselineOrder, events.stream().map(event -> event.getOperationType().getValue()).toList());
            assertTrue(events.stream().allMatch(event -> event.getDocumentKey().getInt64("_id").getValue() <= 20));
            source.getCollection("dimensions").drop();
            source.getCollection("facts").drop();

            var steady = generator(source, "join.live.steady-state.standard", Map.of("dimensions", 2, "facts", 4));
            var fixed = steady.prepare(steady.calibrateStringLengths()).prewrite(30, null, null);
            assertTrue(fixed.batch().complete(), fixed::toString);
            assertEquals(new Operations(0, 30, 0), fixed.batch().streams().get("facts").observed());
            assertEquals(4, source.getCollection("facts").countDocuments());
        });
    }

    private static MongoWorkloadGenerator.Backlog backlog() throws Exception {
        var reports = new ArrayList<MongoWorkloadGenerator.Backlog>();
        withSource(source -> {
            var generator = generator(source, "union.backlog.basic.standard", Map.of("left", 30, "right", 30));
            var prepared = generator.prepare(generator.calibrateStringLengths());
            var result = prepared.prewrite(100, null, null);
            assertTrue(result.batch().complete(), result::toString);
            assertEquals("majority", result.source().writeConcern());
            assertTrue(result.source().preImages());
            int inserts = 0;
            int updates = 0;
            int deletes = 0;
            var allKeys = new HashSet<Long>();
            for (String table : List.of("left", "right")) {
                var stream = result.batch().streams().get(table);
                var history = readHistory(source, table, stream);
                assertEquals(stream.first().resumeToken(), history.getFirst().getResumeToken());
                assertEquals(stream.last().resumeToken(), history.getLast().getResumeToken());
                var insertedKeys = new HashSet<Long>();
                var deletedKeys = new HashSet<Long>();
                for (var event : history) {
                    long key = event.getDocumentKey().getInt64("_id").getValue();
                    switch (event.getOperationType()) {
                        case INSERT -> { inserts++; assertTrue(insertedKeys.add(key)); }
                        case UPDATE -> {
                            updates++;
                            assertTrue(insertedKeys.contains(key));
                            assertNotNull(event.getFullDocumentBeforeChange());
                            assertEquals(2, event.getUpdateDescription().getUpdatedFields().size());
                        }
                        case DELETE -> {
                            deletes++;
                            assertTrue(deletedKeys.add(key));
                            assertFalse(insertedKeys.contains(key));
                            assertNotNull(event.getFullDocumentBeforeChange());
                        }
                        default -> throw new AssertionError(event);
                    }
                }
                for (long key : insertedKeys) {
                    assertTrue(allKeys.add(key));
                }
                assertEquals(30 + insertedKeys.size() - deletedKeys.size(), source.getCollection(table).countDocuments());
            }
            assertEquals(new Operations(80, 15, 5), new Operations(inserts, updates, deletes));
            reports.add(result);
        });
        return reports.getFirst();
    }

    private static List<ChangeStreamDocument<BsonDocument>> readHistory(
            MongoDatabase source, String table, MongoWorkloadGenerator.StreamReport report) {
        var events = new ArrayList<ChangeStreamDocument<BsonDocument>>();
        long deadline = System.nanoTime() + Duration.ofSeconds(8).toNanos();
        try (var cursor = source.getCollection(table, BsonDocument.class).watch()
                .resumeAfter(report.startExclusive().resumeToken())
                .fullDocumentBeforeChange(FullDocumentBeforeChange.REQUIRED)
                .maxAwaitTime(100, TimeUnit.MILLISECONDS).cursor()) {
            while (events.size() < report.observed().total() && System.nanoTime() < deadline) {
                var next = cursor.tryNext();
                if (next != null) {
                    events.add(next);
                }
            }
        }
        assertEquals(report.observed().total(), events.size());
        return events;
    }

    private static MongoWorkloadGenerator.Preparation independentLoad() throws Exception {
        var result = new ArrayList<MongoWorkloadGenerator.Preparation>();
        withSource(source -> {
            var generator = generator(source, "copy.snapshot.basic.standard", Map.of("records", 100));
            result.add(generator.prepare(generator.calibrateStringLengths()).preparation());
        });
        return result.getFirst();
    }

    private static MongoWorkloadGenerator generator(MongoDatabase source, String id, Map<String, Integer> rows) {
        return new MongoWorkloadGenerator(source, WorkloadRecipe.fromCatalog(definition(id), rows, null), id, 17);
    }

    private static BsonDocument definition(String id) {
        return catalog.getArray("cases").stream().map(value -> value.asDocument())
                .filter(entry -> id.equals(entry.getString("id").getValue())).findFirst().orElseThrow();
    }

    @FunctionalInterface
    private interface SourceCheck {
        void run(MongoDatabase database) throws Exception;
    }

    private static void withSource(SourceCheck check) throws Exception {
        String name = "perf_" + UUID.randomUUID().toString().replace("-", "");
        try (var client = MongoClients.create(SharedMongo.replicaSetUrl(name))) {
            var source = client.getDatabase(name).withTimeout(12, TimeUnit.SECONDS);
            try {
                check.run(source);
            } finally {
                source.drop();
            }
        }
    }
}
