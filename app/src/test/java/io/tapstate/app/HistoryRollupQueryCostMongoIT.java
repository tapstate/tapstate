package io.tapstate.app;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;
import com.mongodb.ExplainVerbosity;
import io.tapstate.adapters.mongostore.MongoHistoryRollupStore;
import io.tapstate.adapters.mongostore.MongoRateHistoryStore;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.control.core.ArtifactQueryService;
import io.tapstate.control.core.HistoryAggregator;
import io.tapstate.control.core.HistoryCursorCodec;
import io.tapstate.control.core.HistoryResolution;
import io.tapstate.control.core.PipelineHistoryQuery;
import io.tapstate.control.core.PipelineHistoryQueryService;
import io.tapstate.control.core.PipelineMetricsHistory;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.lifecycle.RateSample;
import io.tapstate.core.model.Resource;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.HistoryRollupStore;
import io.tapstate.spi.store.RateHistoryStore;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.BsonArray;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real Mongo query-contract and same-process cost witness for persisted history buckets. */
@RequiresDocker
class HistoryRollupQueryCostMongoIT {

    private static final DockerImageName MONGO_IMAGE = DockerImageName.parse("mongo:7.0");
    private static final Duration RETENTION = Duration.ofDays(15);
    private static final Duration SAMPLE_INTERVAL = Duration.ofMinutes(1);
    private static final int MEASURED_RUNS = 7;

    @Container
    private static final MongoDBContainer MONGO = new MongoDBContainer(MONGO_IMAGE);

    @Test
    void fullAndMissingBucketsHaveBoundedMongoCostAndRawParity() {
        Instant now = nextSixHourBoundary(Instant.now());
        Instant first = now.minus(RETENTION);
        Clock clock = Clock.fixed(now, ZoneOffset.UTC);
        CommandTrace trace = new CommandTrace();
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl()))
                .addCommandListener(trace).build();
        try (MongoClient client = MongoClients.create(settings)) {
            MongoDatabase database = client.getDatabase("history_rollup_query_cost_it");
            database.drop();
            MongoCollection<Document> rawCollection = SystemCollections.PIPELINE_RATE_HISTORY.on(database);
            MongoCollection<Document> rollupCollection = SystemCollections.PIPELINE_HISTORY_ROLLUPS.on(database);
            MongoRateHistoryStore raw = new MongoRateHistoryStore(database, rawCollection, RETENTION);
            MongoHistoryRollupStore rollups = new MongoHistoryRollupStore(database, rollupCollection, RETENTION);
            NavigableMap<Instant, RateHistoryStore.Entry> entries = seedRaw(rawCollection, first, now);
            seedRollups(rollups, entries, first, now);
            seedUnrelatedRollups(rollupCollection);

            ArtifactQueryService artifacts = artifacts();
            HistoryCursorCodec codec = new HistoryCursorCodec("mongo-cost-secret".getBytes(StandardCharsets.UTF_8), clock);
            PipelineHistoryQueryService forcedRaw = new PipelineHistoryQueryService(
                    artifacts, raw, SAMPLE_INTERVAL, clock, codec);
            PipelineHistoryQueryService cached = new PipelineHistoryQueryService(
                    artifacts, raw, rollups, SAMPLE_INTERVAL, clock, codec);

            compareWindow(forcedRaw, cached, trace, rollupCollection, now,
                    Duration.ofHours(1), null);
            compareWindow(forcedRaw, cached, trace, rollupCollection, now,
                    Duration.ofDays(1), HistoryRollupStore.Resolution.PT30M);
            compareWindow(forcedRaw, cached, trace, rollupCollection, now,
                    RETENTION, HistoryRollupStore.Resolution.PT6H);

            // The same explain gate must turn red if the scoped range index disappears.
            PipelineHistoryQuery indexProbe = new PipelineHistoryQuery("orders", now.minus(Duration.ofDays(1)), now,
                    HistoryResolution.AUTO, PipelineHistoryQuery.DEFAULT_LIMIT, List.of("orders"), null);
            Commands captured = measure(cached, indexProbe, trace).commands();
            rollupCollection.dropIndex("pipelineId_scopeKey_resolution_bucketStart_idx");
            assertThatThrownBy(() -> assertIndexBounds(rollupCollection, captured,
                    Duration.ofDays(1), HistoryRollupStore.Resolution.PT30M))
                    .isInstanceOf(AssertionError.class);
        }
    }

    private static void compareWindow(PipelineHistoryQueryService forcedRaw,
            PipelineHistoryQueryService cached, CommandTrace trace,
            MongoCollection<Document> rollupCollection, Instant now,
            Duration span, HistoryRollupStore.Resolution resolution) {
        Instant from = now.minus(span);
        PipelineHistoryQuery query = new PipelineHistoryQuery("orders", from, now,
                HistoryResolution.AUTO, PipelineHistoryQuery.DEFAULT_LIMIT, List.of("orders"), null);
        Measurement coldRaw = measure(forcedRaw, query, trace);
        Measurement coldCached = measure(cached, query, trace);
        sameOutput(coldRaw.response(), coldCached.response());
        if (resolution != null) {
            assertThat(coldCached.commands().rawReads()).isZero();
            assertThat(coldCached.commands().rollupReads()).isBetween(1L, 128L);
            assertIndexBounds(rollupCollection, coldCached.commands(), span, resolution);
        }
        for (int i = 0; i < 2; i++) {
            sameOutput(measure(forcedRaw, query, trace).response(),
                    measure(cached, query, trace).response());
        }

        long[] rawNanos = new long[MEASURED_RUNS];
        long[] cachedNanos = new long[MEASURED_RUNS];
        for (int i = 0; i < MEASURED_RUNS; i++) {
            Measurement rawRun = measure(forcedRaw, query, trace);
            Measurement cachedRun = measure(cached, query, trace);
            sameOutput(rawRun.response(), cachedRun.response());
            rawNanos[i] = rawRun.nanos();
            cachedNanos[i] = cachedRun.nanos();
            if (resolution != null) {
                assertThat(cachedRun.commands().rawReads()).isZero();
            }
        }
        report("raw", span, coldRaw, rawNanos);
        report("cached", span, coldCached, cachedNanos);

        if (resolution == null) {
            return;
        }
        Instant missing = from.plus(resolution.duration().multipliedBy(
                span.dividedBy(resolution.duration()) / 2));
        rollupCollection.deleteOne(Filters.and(
                Filters.eq("pipelineId", "orders"), Filters.eq("scopeKey", "legacy"),
                Filters.eq("resolution", resolution.name()),
                Filters.eq("bucketStart", Date.from(missing))));
        Measurement fallback = measure(cached, query, trace);
        sameOutput(coldRaw.response(), fallback.response());
        assertThat(fallback.commands().rawReads()).isBetween(1L, 4L);
        assertThat(fallback.commands().rollupReads()).isBetween(1L, 128L);
        assertOnlyMissingRawRange(fallback.commands(), missing, missing.plus(resolution.duration()));
        report("missing-bucket", span, fallback, new long[] {fallback.nanos()});
    }

    private static void sameOutput(PipelineMetricsHistory expected, PipelineMetricsHistory actual) {
        assertThat(actual.segments()).isEqualTo(expected.segments());
        assertThat(actual.gaps()).isEqualTo(expected.gaps());
        assertThat(actual.unavailable()).isEqualTo(expected.unavailable());
        assertThat(actual.status()).isEqualTo(expected.status());
        assertThat(actual.effectiveResolution()).isEqualTo(expected.effectiveResolution());
        assertThat(actual.nextCursor()).isEqualTo(expected.nextCursor());
    }

    private static Measurement measure(PipelineHistoryQueryService service,
            PipelineHistoryQuery query, CommandTrace trace) {
        trace.start();
        long started = System.nanoTime();
        try {
            PipelineMetricsHistory response = service.query(query);
            return new Measurement(response, System.nanoTime() - started, trace.stop());
        } catch (RuntimeException | Error failure) {
            trace.stop();
            throw failure;
        }
    }

    private static void assertOnlyMissingRawRange(Commands commands, Instant from, Instant to) {
        List<BsonDocument> ranged = commands.finds(SystemCollections.PIPELINE_RATE_HISTORY.collectionName())
                .stream().map(Command::filter)
                .filter(filter -> bounds(filter) != null).toList();
        assertThat(ranged).hasSize(1);
        assertThat(bounds(ranged.getFirst())).containsExactly(Date.from(from).getTime(), Date.from(to).getTime());
        for (Command command : commands.finds(SystemCollections.PIPELINE_RATE_HISTORY.collectionName())) {
            assertThat(command.filter().toJson()).contains("pipelineId", "orders");
            if (bounds(command.filter()) == null) {
                assertThat(command.limit()).isEqualTo(1);
            }
        }
    }

    private static long[] bounds(BsonDocument filter) {
        BsonArray terms = filter.getArray("$and", new BsonArray());
        Long lower = null;
        Long upper = null;
        for (BsonValue term : terms) {
            BsonDocument at = term.asDocument().getDocument("observedAt", new BsonDocument());
            BsonDateTime gte = at.getDateTime("$gte", null);
            BsonDateTime lt = at.getDateTime("$lt", null);
            if (gte != null) {
                lower = gte.getValue();
            }
            if (lt != null) {
                upper = lt.getValue();
            }
        }
        return lower == null || upper == null ? null : new long[] {lower, upper};
    }

    private static void assertIndexBounds(MongoCollection<Document> collection, Commands commands,
            Duration span, HistoryRollupStore.Resolution resolution) {
        List<Command> rollupFinds = commands.finds(SystemCollections.PIPELINE_HISTORY_ROLLUPS.collectionName());
        assertThat(rollupFinds).isNotEmpty();
        int requestedBuckets = Math.toIntExact(span.dividedBy(resolution.duration()));
        int examined = 0;
        int keys = 0;
        for (Command find : rollupFinds) {
            assertThat(find.filter().toJson()).contains("pipelineId", "scopeKey", "resolution", "bucketStart");
            Document explain = collection.find(find.filter())
                    .sort(Sorts.ascending("bucketStart"))
                    .limit(find.limit())
                    .explain(ExplainVerbosity.EXECUTION_STATS);
            Document stats = explain.get("executionStats", Document.class);
            examined += stats.getInteger("totalDocsExamined");
            keys += stats.getInteger("totalKeysExamined");
            assertThat(explain.get("queryPlanner", Document.class)
                    .get("winningPlan", Document.class).toJson())
                    .contains("IXSCAN", "pipelineId_scopeKey_resolution_bucketStart_idx");
        }
        assertThat(examined).isLessThanOrEqualTo(requestedBuckets);
        assertThat(keys).isLessThanOrEqualTo(requestedBuckets + rollupFinds.size());
        System.out.printf("history-rollup-explain resolution=%s requestedBuckets=%d keys=%d docs=%d"
                        + " index=pipelineId_scopeKey_resolution_bucketStart_idx%n",
                resolution, requestedBuckets, keys, examined);
    }

    private static void report(String mode, Duration span, Measurement cold, long[] samples) {
        long[] sorted = Arrays.copyOf(samples, samples.length);
        Arrays.sort(sorted);
        Runtime runtime = Runtime.getRuntime();
        // Record text size is a comparison proxy; only an HTTP response can prove serialized API bytes.
        System.out.printf("history-rollup-query mode=%s span=%s p50Ms=%.3f p95Ms=%.3f"
                        + " coldMs=%.3f mongoCommands=%d rawReads=%d rollupReads=%d"
                        + " mongoReplyBytes=%d dtoTextProxyUtf8Bytes=%d outputPoints=%d"
                        + " os=%s/%s java=%s processors=%d maxHeapBytes=%d processHeapUsedBytes=%d%n",
                mode, span, percentile(sorted, 0.50) / 1_000_000d,
                percentile(sorted, 0.95) / 1_000_000d, cold.nanos() / 1_000_000d,
                cold.commands().commands().size(), cold.commands().rawReads(),
                cold.commands().rollupReads(), cold.commands().replyBytes(),
                cold.response().toString().getBytes(StandardCharsets.UTF_8).length,
                cold.response().segments().stream().mapToInt(segment -> segment.points().size()).sum(),
                System.getProperty("os.name"), System.getProperty("os.arch"),
                System.getProperty("java.version"), runtime.availableProcessors(),
                runtime.maxMemory(), runtime.totalMemory() - runtime.freeMemory());
    }

    private static long percentile(long[] sorted, double fraction) {
        return sorted[Math.max(0, (int) Math.ceil(sorted.length * fraction) - 1)];
    }

    private static NavigableMap<Instant, RateHistoryStore.Entry> seedRaw(
            MongoCollection<Document> collection, Instant first, Instant now) {
        List<Document> documents = new ArrayList<>();
        NavigableMap<Instant, RateHistoryStore.Entry> entries = new TreeMap<>();
        long minutes = Duration.between(first, now).toMinutes();
        long resetMinute = Duration.ofDays(7).toMinutes();
        long gapMinute = Duration.ofDays(9).toMinutes() + 75;
        for (long minute = 0; minute <= minutes; minute++) {
            if (minute == gapMinute) {
                continue;
            }
            Instant at = first.plus(Duration.ofMinutes(minute));
            boolean reset = minute >= resetMinute;
            long counter = reset ? minute - resetMinute : minute;
            Instant since = reset ? first.plus(Duration.ofMinutes(resetMinute)) : first.minusSeconds(60);
            long lag = minute == gapMinute + 20 ? 10_000 : minute % 11;
            RateSample sample = new RateSample("orders", at,
                    Map.of("records.out", counter, "bytes.out", counter * 10),
                    Map.of("orders", lag), since);
            ObjectId id = new ObjectId();
            documents.add(MongoRateHistoryStore.toDocument(sample).append("_id", id));
            entries.put(at, new RateHistoryStore.Entry(
                    new RateHistoryStore.Key(at, id.toHexString()), sample));
        }
        collection.insertMany(documents);
        return entries;
    }

    private static void seedRollups(MongoHistoryRollupStore rollups,
            NavigableMap<Instant, RateHistoryStore.Entry> entries, Instant first, Instant now) {
        for (HistoryRollupStore.Resolution resolution : List.of(
                HistoryRollupStore.Resolution.PT30M, HistoryRollupStore.Resolution.PT6H)) {
            for (Instant at = first; at.isBefore(now); at = at.plus(resolution.duration())) {
                Instant end = at.plus(resolution.duration());
                HistoryAggregator aggregator = new HistoryAggregator(at, end, at,
                        resolution.duration(), SAMPLE_INTERVAL, List.of("orders"),
                        at.equals(first) ? PipelineMetricsHistory.StartReason.WINDOW_START
                                : PipelineMetricsHistory.StartReason.CONTINUATION,
                        HistoryRollupStore.MAX_FRAGMENTS + 1);
                aggregator.begin(Optional.ofNullable(entries.lowerEntry(at))
                        .map(Map.Entry::getValue).orElse(null));
                List<RateHistoryStore.Entry> window = new ArrayList<>(entries.subMap(at, true, end, false).values());
                window.forEach(aggregator::add);
                RateHistoryStore.Entry successor = Optional.ofNullable(entries.ceilingEntry(end))
                        .map(Map.Entry::getValue).orElse(null);
                HistoryAggregator.Projection projection = aggregator.finish(successor);
                assertThat(projection.points().size()).isLessThanOrEqualTo(HistoryRollupStore.MAX_FRAGMENTS);
                HistoryRollupStore.Key key = new HistoryRollupStore.Key("orders",
                        HistoryRollupStore.Scope.legacy(), resolution, at);
                rollups.upsert(new HistoryRollupStore.Bucket(key, now, now,
                        now.plus(HistoryRollupStore.MAX_CACHE_AGE), false,
                        projection.points().stream().map(HistoryRollupQueryCostMongoIT::fragment).toList(),
                        projection.gaps().stream().map(HistoryRollupQueryCostMongoIT::gap).toList(),
                        window.size()));
            }
        }
    }

    private static void seedUnrelatedRollups(MongoCollection<Document> collection) {
        List<Document> others = new ArrayList<>();
        for (Document source : collection.find(Filters.eq("pipelineId", "orders"))) {
            Document sourceId = source.get("_id", Document.class);
            for (int index = 0; index < 8; index++) {
                String pipelineId = "unrelated-" + index;
                Document copy = new Document(source);
                copy.put("pipelineId", pipelineId);
                copy.put("_id", new Document(sourceId).append("pipelineId", pipelineId));
                others.add(copy);
            }
        }
        collection.insertMany(others);
    }

    private static HistoryRollupStore.Fragment fragment(HistoryAggregator.Emitted emitted) {
        PipelineMetricsHistory.Point point = emitted.point();
        return new HistoryRollupStore.Fragment(emitted.segment(),
                HistoryRollupStore.StartReason.valueOf(emitted.startReason().name()),
                point.intervalStart(), point.intervalEnd(), rate(point.recordsOut()),
                rate(point.bytesOut()), point.lag().stream().map(lag ->
                        new HistoryRollupStore.Lag(lag.table(), lag.observedAt(), lag.last(), lag.max())).toList(),
                stats(emitted.recordsOutStats()), stats(emitted.bytesOutStats()),
                emitted.resumeAfter(), emitted.resumeAt());
    }

    private static HistoryRollupStore.Rate rate(PipelineMetricsHistory.Rate rate) {
        return rate == null ? null : new HistoryRollupStore.Rate(
                rate.delta(), rate.averageRate(), rate.maxRate());
    }

    private static HistoryRollupStore.CounterStats stats(HistoryAggregator.CounterStats stats) {
        return stats == null ? null : new HistoryRollupStore.CounterStats(
                stats.delta(), stats.coveredNanos(), stats.maxRate());
    }

    private static HistoryRollupStore.Gap gap(HistoryAggregator.EmittedGap emitted) {
        return new HistoryRollupStore.Gap(emitted.segment(),
                emitted.gap().intervalStart(), emitted.gap().intervalEnd(),
                HistoryRollupStore.GapReason.valueOf(emitted.gap().reason().name()));
    }

    private static Instant nextSixHourBoundary(Instant at) {
        long width = Duration.ofHours(6).toSeconds();
        return Instant.ofEpochSecond((Math.floorDiv(at.getEpochSecond(), width) + 1) * width);
    }

    private static ArtifactQueryService artifacts() {
        Resource pipeline = new DslParser().parse("""
                version: tapstate/v1
                kind: pipeline
                id: orders
                source: source
                serve:
                  from: /.*/
                  sync:
                    - id: sink
                      source: target
                      write_mode: upsert
                      ddl: apply
                """);
        return new ArtifactQueryService(new ArtifactStore() {
            @Override
            public void saveAll(List<Resource> artifacts) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Optional<Resource> get(String id) {
                return pipeline.id().equals(id) ? Optional.of(pipeline) : Optional.empty();
            }

            @Override
            public List<Resource> list() {
                return List.of(pipeline);
            }
        });
    }

    private record Measurement(PipelineMetricsHistory response, long nanos, Commands commands) { }

    private record Command(String name, String collection, BsonDocument filter, int limit) { }

    private record Commands(List<Command> commands, long replyBytes) {
        long rawReads() {
            return reads(SystemCollections.PIPELINE_RATE_HISTORY.collectionName());
        }

        long rollupReads() {
            return reads(SystemCollections.PIPELINE_HISTORY_ROLLUPS.collectionName());
        }

        long reads(String collection) {
            return commands.stream().filter(command -> collection.equals(command.collection())).count();
        }

        List<Command> finds(String collection) {
            return commands.stream().filter(command -> "find".equals(command.name())
                    && collection.equals(command.collection())).toList();
        }
    }

    private static final class CommandTrace implements CommandListener {
        private final List<Command> commands = new ArrayList<>();
        private boolean active;
        private long replyBytes;

        synchronized void start() {
            commands.clear();
            replyBytes = 0;
            active = true;
        }

        synchronized Commands stop() {
            active = false;
            return new Commands(List.copyOf(commands), replyBytes);
        }

        @Override
        public synchronized void commandStarted(CommandStartedEvent event) {
            if (!active) {
                return;
            }
            BsonDocument command = event.getCommand();
            String name = event.getCommandName();
            if ("find".equals(name)) {
                commands.add(new Command(name, command.getString("find").getValue(),
                        BsonDocument.parse(command.getDocument("filter", new BsonDocument()).toJson()),
                        command.getInt32("limit", new org.bson.BsonInt32(0)).getValue()));
            } else if ("getMore".equals(name)) {
                commands.add(new Command(name, command.getString("collection").getValue(),
                        new BsonDocument(), 0));
            }
        }

        @Override
        public synchronized void commandSucceeded(CommandSucceededEvent event) {
            if (active) {
                replyBytes += new RawBsonDocument(event.getResponse(), new BsonDocumentCodec())
                        .getByteBuffer().remaining();
            }
        }
    }
}
