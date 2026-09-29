package io.tapstate.e2e;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.MongoHistoryRollupStore;
import io.tapstate.adapters.mongostore.MongoRateHistoryStore;
import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.lifecycle.RateSample;
import io.tapstate.control.core.HistoryAggregator;
import io.tapstate.control.core.PipelineMetricsHistory;
import io.tapstate.spi.store.HistoryRollupStore;
import io.tapstate.spi.store.RateHistoryStore;
import io.tapstate.testsupport.DockerGate;
import org.bson.BsonArray;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.HexFormat;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Repeatable real-process HTTP and Mongo scan fixture for history query comparisons.
 *
 * <p>Run the same compiled test with {@code -Dtapstate.e2e.history-benchmark.jar=/path/to/app-boot.jar}
 * to launch another build, and supply the same {@code -Dtapstate.e2e.history-benchmark.anchor=...}
 * to keep the seeded timestamps and request windows identical between runs. Set
 * {@code -Dtapstate.e2e.history-benchmark.raw-only=true} when replaying the original fixture
 * against a reference build without rollup support. Optional warm-up and hot-read counts apply
 * equally to both arms; the five-read default preserves the original small fixture.
 */
class HistoryQueryBenchmarkIT {

    private static final String DATABASE = "history_query_benchmark";
    private static final String PIPELINE = "history_benchmark";
    private static final String BOOT_JAR_PROPERTY = "tapstate.e2e.history-benchmark.jar";
    private static final String ANCHOR_PROPERTY = "tapstate.e2e.history-benchmark.anchor";
    private static final String RAW_ONLY_PROPERTY = "tapstate.e2e.history-benchmark.raw-only";
    private static final String CACHED_FIRST_PROPERTY = "tapstate.e2e.history-benchmark.cached-first";
    private static final String REACTOR_JAR_PROPERTY = "tapstate.e2e.boot-jar";
    private static final int HOT_READS = Integer.getInteger(
            "tapstate.e2e.history-benchmark.hot-reads", 5);
    private static final int WARMUP_READS = Integer.getInteger(
            "tapstate.e2e.history-benchmark.warmup-reads", 0);
    private static final Duration RETENTION = Duration.ofDays(15);
    private static final Duration SAMPLE_INTERVAL = Duration.ofMinutes(1);
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    private static final String SOURCE = """
            version: tapstate/v1
            kind: source
            id: history_source
            connector: mongodb
            config: { uri: "mongodb://127.0.0.1:27017/unused" }
            mode: cdc
            tables: [ orders ]
            """;
    private static final String TARGET = """
            version: tapstate/v1
            kind: source
            id: history_target
            connector: mongodb
            config: { uri: "mongodb://127.0.0.1:27017/unused" }
            """;
    private static final String PIPELINE_DSL = """
            version: tapstate/v1
            kind: pipeline
            id: history_benchmark
            source: history_source
            serve:
              from: /.*/
              sync:
                - id: sink
                  source: history_target
                  write_mode: upsert
                  ddl: apply
            """;

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void theSameFifteenDayFixtureMeasuresOneHourOneDayAndFifteenDays() throws Exception {
        assertThat(HOT_READS).isBetween(5, 100);
        assertThat(WARMUP_READS).isBetween(0, 50);
        String storeUri = SharedMongo.replicaSetUrl(DATABASE);
        try (MongoClient mongo = MongoClients.create(storeUri)) {
            MongoDatabase database = mongo.getDatabase(DATABASE);
            database.drop();
            String configuredJar = System.getProperty(BOOT_JAR_PROPERTY);
            String jarSetting = configuredJar == null || configuredJar.isBlank()
                    ? System.getProperty(REACTOR_JAR_PROPERTY) : configuredJar;
            Path jar = jarSetting == null || jarSetting.isBlank() ? null : Path.of(jarSetting);
            List<Window> frozen = List.of(
                    new Window("1h", Duration.ofHours(1), "raw", "PT1M"),
                    new Window("1d", Duration.ofDays(1), "PT30M", "PT30M"),
                    new Window("15d", RETENTION, "PT6H", "PT6H"));
            Instant to;
            Instant missingBucket;
            boolean rawOnly = Boolean.getBoolean(RAW_ONLY_PROPERTY);
            try (RealProcessServer setupServer = startServer(storeUri, jar, true)) {
                ControlPlane setupControl = new ControlPlane(setupServer.baseUrl());
                setupControl.bootstrapAndLogin("history-benchmark", "history-benchmark-password");
                setupControl.apply(Map.of("source.tap.yml", SOURCE, "target.tap.yml", TARGET,
                        "pipeline.tap.yml", PIPELINE_DSL));

                // Keep the upper edge off the minute boundary and three minutes behind wall time. The
                // latter leaves room for the live server's retention cutoff to move while tests run.
                String configuredAnchor = System.getProperty(ANCHOR_PROPERTY);
                to = configuredAnchor == null || configuredAnchor.isBlank()
                        ? Instant.now().truncatedTo(ChronoUnit.MINUTES)
                                .minus(Duration.ofMinutes(3)).plusSeconds(17)
                        : Instant.parse(configuredAnchor);
                missingBucket = Instant.ofEpochSecond(
                        Math.floorDiv(to.minus(Duration.ofMinutes(30)).getEpochSecond(), 1_800) * 1_800);
                Document artifact = database.getCollection(MongoStorePort.ARTIFACTS)
                        .find(new Document("_id", PIPELINE)).first();
                assertThat(artifact).as("the applied pipeline artifact").isNotNull();
                String incarnation = artifact.getString("pipelineIncarnationId");
                NavigableMap<Instant, RateHistoryStore.Entry> entries = seed(
                        database, to, missingBucket, incarnation);
                System.out.printf("history-query-fixture jar=%s jarSha256=%s os=%s/%s java=%s mongo=7.0"
                                + " anchor=%s missingBucket=%s seededSamples=%d historyScope=%s"
                                + " concurrency=1 warmupReads=%d hotReads=%d%n",
                        jar == null ? "reactor" : jar, jar == null ? "reactor" : sha256(jar),
                        System.getProperty("os.name"), System.getProperty("os.arch"),
                        System.getProperty("java.version"), to, missingBucket, entries.size(),
                        incarnation == null ? "legacy" : "incarnation", WARMUP_READS, HOT_READS);
                if (rawOnly) {
                    readWindows(database, setupServer, setupControl.credential(), frozen, to,
                            missingBucket, true, "raw-frozen");
                    return;
                }
                int buckets = seedRollups(database, entries, to, incarnation);
                System.out.printf("history-query-rollup-seed buckets=%d resolutions=PT30M,PT6H%n", buckets);
            }

            Instant alignedTo = floor(to, Duration.ofHours(6));
            List<Window> aligned = List.of(
                    new Window("1d-aligned", Duration.ofDays(1), "PT30M", "PT30M"),
                    // A full 15-day range is clipped at the moving retention cutoff. This is
                    // the longest six-hour-aligned window strictly inside that cutoff.
                    new Window("15d-tier-aligned-14d18h", RETENTION.minus(Duration.ofHours(6)),
                            "PT6H", "PT6H"));
            boolean cachedFirst = Boolean.getBoolean(CACHED_FIRST_PROPERTY);
            System.out.printf("history-query-arm-order first=%s second=%s%n",
                    cachedFirst ? "cached" : "raw", cachedFirst ? "raw" : "cached");
            ArmRuns first = runArm(database, storeUri, jar, cachedFirst,
                    frozen, aligned, to, alignedTo, missingBucket);
            ArmRuns second = runArm(database, storeUri, jar, !cachedFirst,
                    frozen, aligned, to, alignedTo, missingBucket);
            ArmRuns raw = cachedFirst ? second : first;
            ArmRuns cached = cachedFirst ? first : second;
            compare(raw.frozen(), cached.frozen());
            compare(raw.aligned(), cached.aligned());
            compare(raw.aligned(), cached.missing());
            for (WindowRun run : cached.frozen()) {
                reportPair("requested", matching(raw.frozen(), run.window()), run);
            }
            for (WindowRun run : cached.aligned()) {
                reportPair("full-hit", matching(raw.aligned(), run.window()), run);
            }
            for (WindowRun run : cached.missing()) {
                reportPair("missing-rollup", matching(raw.aligned(), run.window()), run);
            }
        }
    }

    private static RealProcessServer startServer(String storeUri, Path jar, boolean forcedRaw) {
        List<String> settings = forcedRaw
                ? List.of("--tapstate.metrics.history.rollup-read-enabled=false") : List.of();
        if (jar == null) {
            return forcedRaw ? RealProcessServer.start(storeUri, settings)
                    : RealProcessServer.start(storeUri);
        }
        return forcedRaw ? RealProcessServer.start(storeUri, jar, settings)
                : RealProcessServer.start(storeUri, jar);
    }

    private static ArmRuns runArm(MongoDatabase database, String storeUri, Path jar, boolean cached,
            List<Window> frozen, List<Window> aligned, Instant to, Instant alignedTo,
            Instant missingBucket) throws Exception {
        try (RealProcessServer server = startServer(storeUri, jar, !cached)) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.login("history-benchmark", "history-benchmark-password");
            String credential = control.credential();
            List<WindowRun> frozenRuns = readWindows(database, server, credential,
                    frozen, to, missingBucket, true, cached ? "cached-frozen" : "raw-frozen");
            List<WindowRun> alignedRuns = readWindows(database, server, credential,
                    aligned, alignedTo, missingBucket, false,
                    cached ? "cached-full-hit" : "raw-aligned");
            if (!cached) {
                return new ArmRuns(frozenRuns, alignedRuns, List.of());
            }
            for (WindowRun run : alignedRuns) {
                assertFullHit(run);
            }
            deleteMiddleRollup(database, alignedTo, aligned);
            List<WindowRun> missingRuns = readWindows(database, server, credential,
                    aligned, alignedTo, missingBucket, false, "cached-missing-rollup");
            for (WindowRun run : missingRuns) {
                assertOnlyMissingBucketReadsRaw(run, alignedTo);
            }
            return new ArmRuns(frozenRuns, alignedRuns, missingRuns);
        }
    }

    private static NavigableMap<Instant, RateHistoryStore.Entry> seed(
            MongoDatabase database, Instant to, Instant missingBucket, String incarnation) {
        MongoCollection<Document> history = database.getCollection(MongoStorePort.PIPELINE_RATE_HISTORY);
        Instant first = to.truncatedTo(ChronoUnit.MINUTES).minus(Duration.ofDays(15))
                .plus(Duration.ofMinutes(10));
        Instant last = to.truncatedTo(ChronoUnit.MINUTES);
        List<Document> samples = new ArrayList<>((int) Duration.between(first, last).toMinutes() + 1);
        NavigableMap<Instant, RateHistoryStore.Entry> entries = new TreeMap<>();
        for (Instant at = first; !at.isAfter(last); at = at.plus(Duration.ofMinutes(1))) {
            if (!at.isBefore(missingBucket) && at.isBefore(missingBucket.plus(Duration.ofMinutes(30)))) {
                continue;
            }
            long minutes = Duration.between(first, at).toMinutes();
            RateSample value = new RateSample(PIPELINE, at,
                    Map.of("records.out", minutes * 60, "bytes.out", minutes * 600),
                    Map.of("orders", minutes % 11), first);
            ObjectId id = new ObjectId();
            Document sample = MongoRateHistoryStore.toDocument(value).append("_id", id);
            // The fixture keeps the same logical samples on both builds. New runs carry their current
            // internal owner; a pre-identity reference build reads the same samples as legacy history.
            if (incarnation != null) {
                sample.append("pipelineIncarnationId", incarnation).append("executionGeneration", 1L);
            }
            samples.add(sample);
            entries.put(at, new RateHistoryStore.Entry(
                    new RateHistoryStore.Key(at, id.toHexString()), value));
        }
        history.insertMany(samples);
        assertThat(history.countDocuments()).isEqualTo(samples.size());
        return entries;
    }

    private static int seedRollups(MongoDatabase database,
            NavigableMap<Instant, RateHistoryStore.Entry> entries, Instant to, String incarnation) {
        MongoHistoryRollupStore store = new MongoHistoryRollupStore(database,
                database.getCollection(MongoStorePort.PIPELINE_HISTORY_ROLLUPS), RETENTION);
        HistoryRollupStore.Scope scope = incarnation == null
                ? HistoryRollupStore.Scope.legacy() : HistoryRollupStore.Scope.incarnation(incarnation);
        Instant computedAt = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        int count = 0;
        for (Window window : List.of(
                new Window("1d", Duration.ofDays(1), "PT30M", "PT30M"),
                new Window("15d", RETENTION, "PT6H", "PT6H"))) {
            HistoryRollupStore.Resolution resolution = HistoryRollupStore.Resolution.valueOf(window.resolution());
            Duration width = resolution.duration();
            Instant first = floor(to.minus(window.span()), width);
            Instant alignedFirst = floor(to, Duration.ofHours(6)).minus(window.span());
            if (alignedFirst.isBefore(first)) {
                first = alignedFirst;
            }
            for (Instant at = first; !at.plus(width).isAfter(computedAt); at = at.plus(width)) {
                Instant end = at.plus(width);
                HistoryAggregator aggregator = new HistoryAggregator(at, end, at,
                        width, SAMPLE_INTERVAL, List.of("orders"),
                        at.equals(first) ? PipelineMetricsHistory.StartReason.WINDOW_START
                                : PipelineMetricsHistory.StartReason.CONTINUATION,
                        HistoryRollupStore.MAX_FRAGMENTS + 1);
                aggregator.begin(Optional.ofNullable(entries.lowerEntry(at))
                        .map(Map.Entry::getValue).orElse(null));
                List<RateHistoryStore.Entry> windowEntries = new ArrayList<>(
                        entries.subMap(at, true, end, false).values());
                windowEntries.forEach(aggregator::add);
                RateHistoryStore.Entry successor = Optional.ofNullable(entries.ceilingEntry(end))
                        .map(Map.Entry::getValue).orElse(null);
                HistoryAggregator.Projection projection = aggregator.finish(successor);
                assertThat(projection.points().size()).isLessThanOrEqualTo(HistoryRollupStore.MAX_FRAGMENTS);
                HistoryRollupStore.Key key = new HistoryRollupStore.Key(PIPELINE, scope, resolution, at);
                store.upsert(new HistoryRollupStore.Bucket(key, computedAt, computedAt,
                        computedAt.plus(HistoryRollupStore.MAX_CACHE_AGE), false,
                        projection.points().stream().map(HistoryQueryBenchmarkIT::fragment).toList(),
                        projection.gaps().stream().map(HistoryQueryBenchmarkIT::gap).toList(),
                        windowEntries.size()));
                count++;
            }
        }
        return count;
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

    private static void deleteMiddleRollup(MongoDatabase database, Instant to, List<Window> windows) {
        MongoCollection<Document> rollups = database.getCollection(MongoStorePort.PIPELINE_HISTORY_ROLLUPS);
        for (Window window : windows) {
            HistoryRollupStore.Resolution resolution = HistoryRollupStore.Resolution.valueOf(window.resolution());
            Instant middle = middleBucket(to, window);
            long removed = rollups.deleteOne(new Document("pipelineId", PIPELINE)
                    .append("resolution", resolution.name())
                    .append("bucketStart", Date.from(middle))).getDeletedCount();
            assertThat(removed).as("one persisted middle bucket was removed").isEqualTo(1);
            System.out.printf("history-query-missing-rollup resolution=%s bucketStart=%s%n",
                    resolution, middle);
        }
    }

    private static Instant middleBucket(Instant to, Window window) {
        Duration width = HistoryRollupStore.Resolution.valueOf(window.resolution()).duration();
        return to.minus(window.span()).plus(width.multipliedBy(window.span().dividedBy(width) / 2));
    }

    private static void assertFullHit(WindowRun run) {
        int buckets = Math.toIntExact(run.window().span().dividedBy(
                HistoryRollupStore.Resolution.valueOf(run.window().resolution()).duration()));
        for (Reading reading : allReadings(run)) {
            assertThat(reading.raw().commands()).as("complete cached hit reads no raw history").isZero();
            assertThat(reading.rollup().commands()).as("complete cached hit reads rollups")
                    .isBetween(1, 2);
            assertThat(reading.rollup().docsExamined()).as("rollup docs track requested buckets")
                    .isLessThanOrEqualTo(buckets);
            assertThat(reading.rollup().keysExamined()).as("rollup keys track requested buckets")
                    .isLessThanOrEqualTo(buckets + reading.rollup().commands());
        }
    }

    private static void assertOnlyMissingBucketReadsRaw(WindowRun run, Instant to) {
        Duration width = HistoryRollupStore.Resolution.valueOf(run.window().resolution()).duration();
        Instant missing = middleBucket(to, run.window());
        int buckets = Math.toIntExact(run.window().span().dividedBy(width));
        for (Reading reading : allReadings(run)) {
            assertThat(reading.raw().commands()).as("a missing bucket descends to raw").isPositive();
            assertThat(reading.rollup().commands()).as("other buckets still use rollups").isPositive();
            assertThat(reading.rollup().docsExamined()).as("only one rollup bucket is missing")
                    .isLessThanOrEqualTo(buckets - 1);
            assertThat(reading.raw().docsExamined()).as("raw fallback remains one bucket plus boundary samples")
                    .isLessThanOrEqualTo(width.toMinutes() + 3);
            List<long[]> ranged = reading.raw().operations().stream()
                    .map(HistoryQueryBenchmarkIT::rangeBounds).filter(bounds -> bounds != null).toList();
            assertThat(ranged).as("one raw range for the missing bucket").hasSize(1);
            assertThat(ranged.getFirst()).containsExactly(
                    missing.toEpochMilli(), missing.plus(width).toEpochMilli());
            for (Document operation : reading.raw().operations()) {
                Document command = operation.get("command", Document.class);
                assertThat(command).as("a profiled raw command").isNotNull();
                if (command.containsKey("getMore")) {
                    assertThat(command.getString("collection"))
                            .as("the cursor continues the bounded raw history read")
                            .isEqualTo(MongoStorePort.PIPELINE_RATE_HISTORY);
                    continue;
                }
                assertThat(command).as("fallback uses bounded find commands").containsKey("find");
                if (rangeBounds(operation) == null) {
                    assertThat(command.getInteger("limit"))
                            .as("each non-range raw boundary lookup reads at most one document")
                            .isEqualTo(1);
                }
            }
        }
    }

    private static long[] rangeBounds(Document profile) {
        Document command = profile.get("command", Document.class);
        if (command == null || !command.containsKey("find")) {
            return null;
        }
        Document filter = command.get("filter", Document.class);
        if (filter == null) {
            return null;
        }
        BsonArray terms = BsonDocument.parse(filter.toJson()).getArray("$and", new BsonArray());
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

    private static List<Reading> allReadings(WindowRun run) {
        List<Reading> readings = new ArrayList<>(1 + run.hot().size());
        readings.add(run.cold());
        readings.addAll(run.hot());
        return readings;
    }

    private static Instant floor(Instant at, Duration width) {
        long seconds = width.toSeconds();
        return Instant.ofEpochSecond(Math.floorDiv(at.getEpochSecond(), seconds) * seconds);
    }

    private static String sha256(Path jar) throws Exception {
        MessageDigest hash = MessageDigest.getInstance("SHA-256");
        try (var input = Files.newInputStream(jar)) {
            byte[] buffer = new byte[64 * 1024];
            for (int read; (read = input.read(buffer)) != -1;) {
                hash.update(buffer, 0, read);
            }
        }
        return HexFormat.of().formatHex(hash.digest());
    }

    private static URI query(URI base, Instant from, Instant to, String resolution) {
        String path = "/api/pipelines/" + PIPELINE + "/metrics/history?from=" + encoded(from)
                + "&to=" + encoded(to) + "&resolution=" + resolution + "&limit=1000&table=orders";
        return base.resolve(path);
    }

    private static String encoded(Instant time) {
        return URLEncoder.encode(time.toString(), StandardCharsets.UTF_8);
    }

    private static List<WindowRun> readWindows(MongoDatabase database, RealProcessServer server,
            String credential, List<Window> windows, Instant to, Instant missingBucket,
            boolean frozen, String mode) throws Exception {
        List<WindowRun> runs = new ArrayList<>();
        for (Window window : windows) {
            URI uri = query(server.baseUrl(), to.minus(window.span()), to, window.resolution());
            WindowRun run;
            var samplingAnchor = BenchmarkForkEnvironment.ClockAnchor.capture();
            try (BenchmarkResourceSampler sampler = BenchmarkResourceSampler.open(
                    server.pid(), Duration.ofMillis(5))) {
                sampler.start();
                Reading cold = profiledGet(database, uri, credential);
                assertResponse(cold, window, missingBucket, frozen);
                for (int i = 0; i < WARMUP_READS; i++) {
                    assertResponse(profiledGet(database, uri, credential), window, missingBucket, frozen);
                }
                List<Reading> hot = new ArrayList<>();
                for (int i = 0; i < HOT_READS; i++) {
                    Reading reading = profiledGet(database, uri, credential);
                    assertResponse(reading, window, missingBucket, frozen);
                    hot.add(reading);
                }
                run = new WindowRun(window, cold, List.copyOf(hot), sampler.finish());
            } catch (BenchmarkResourceSampler.SamplingFailure failure) {
                throw failure.inPhase(mode + "/" + window.name());
            }
            runs.add(run);
            if (frozen && "raw-frozen".equals(mode)) {
                reportFrozen(window, run.cold(), run.hot());
            }
            reportPath(mode, run);
            System.out.println("history-query-evidence=" + JsonWriter.write(
                    windowEvidence(mode, run, to, samplingAnchor)));
        }
        return List.copyOf(runs);
    }

    @SuppressWarnings("unchecked")
    private static Reading profiledGet(MongoDatabase database, URI uri, String credential) throws Exception {
        database.runCommand(new Document("profile", 0));
        database.getCollection("system.profile").drop();
        database.runCommand(new Document("profile", 2).append("slowms", 0));
        HttpRequest request = HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(40))
                .header("Authorization", "Bearer " + credential).GET().build();
        long start = System.nanoTime();
        HttpResponse<byte[]> answer;
        long elapsedNanos;
        try {
            answer = HTTP.send(request, HttpResponse.BodyHandlers.ofByteArray());
            elapsedNanos = System.nanoTime() - start;
        } finally {
            database.runCommand(new Document("profile", 0));
        }
        assertThat(answer.statusCode()).as("history HTTP status and body: %s",
                new String(answer.body(), StandardCharsets.UTF_8)).isEqualTo(200);
        Object parsed = JsonReader.parse(new String(answer.body(), StandardCharsets.UTF_8));
        assertThat(parsed).isInstanceOf(Map.class);
        CollectionCost raw = cost(database, MongoStorePort.PIPELINE_RATE_HISTORY);
        CollectionCost rollup = cost(database, MongoStorePort.PIPELINE_HISTORY_ROLLUPS);
        assertThat(raw.commands() + rollup.commands()).as("profiled Mongo history reads").isPositive();
        assertThat(raw.docsExamined()).as("bounded raw history documents examined")
                .isLessThanOrEqualTo(25_000);
        return new Reading((Map<String, Object>) parsed, elapsedNanos, answer.body().length,
                raw, rollup);
    }

    private static CollectionCost cost(MongoDatabase database, String collection) {
        List<Document> operations = database.getCollection("system.profile")
                .find(new Document("ns", DATABASE + "." + collection)).into(new ArrayList<>())
                .stream().filter(operation -> {
                    Document command = operation.get("command", Document.class);
                    return command != null && (command.containsKey("find") || command.containsKey("getMore")
                            || command.containsKey("aggregate") || command.containsKey("count")
                            || command.containsKey("distinct"));
                }).toList();
        return new CollectionCost(operations.size(),
                operations.stream().mapToLong(operation -> count(operation, "keysExamined")).sum(),
                operations.stream().mapToLong(operation -> count(operation, "docsExamined")).sum(),
                operations.stream().mapToLong(operation -> count(operation, "responseLength")).sum(),
                operations);
    }

    static long count(Document document, String field) {
        Object value = document.get(field);
        assertThat(value).as("profiled read field %s must be an integral count", field)
                .isInstanceOfAny(Integer.class, Long.class);
        long count = ((Number) value).longValue();
        assertThat(count).as("profiled read field %s must be nonnegative", field)
                .isGreaterThanOrEqualTo(0);
        return count;
    }

    private static Map<String, Object> windowEvidence(String mode, WindowRun run, Instant to,
            BenchmarkForkEnvironment.ClockAnchor samplingAnchor) {
        Map<String, Object> evidence = new LinkedHashMap<>();
        evidence.put("mode", mode); evidence.put("window", run.window().name());
        evidence.put("requestedFrom", to.minus(run.window().span()).toString());
        evidence.put("requestedTo", to.toString());
        evidence.put("mongoCostSource", "PROFILER_READ_OPERATIONS");
        evidence.put("mongoReplyBytesSource", "PROFILER_RESPONSE_LENGTH");
        evidence.put("coldScope", "FIRST_QUERY_FOR_WINDOW");
        evidence.put("memoryScope", "WINDOW_COLD_WARMUP_HOT_AND_PROFILE_SETUP");
        evidence.put("cold", readingEvidence(run.cold()));
        evidence.put("hot", readingsEvidence(run.hot()));
        evidence.put("clock", Map.of("utc", samplingAnchor.utc().toString(),
                "monotonicBeforeNanos", samplingAnchor.beforeNanos(),
                "monotonicAfterNanos", samplingAnchor.afterNanos()));
        evidence.put("resources", PipelineBenchmarkLiveRunIT.resourceEvidence(run.resources(), samplingAnchor));
        return evidence;
    }

    static List<Map<String, Object>> readingsEvidence(List<Reading> readings) {
        return readings.stream().map(HistoryQueryBenchmarkIT::readingEvidence).toList();
    }

    private static Map<String, Object> readingEvidence(Reading reading) {
        Map<String, Object> evidence = new LinkedHashMap<>();
        evidence.put("elapsedNanos", reading.elapsedNanos());
        evidence.put("httpBytes", reading.responseBytes());
        evidence.put("httpBytesSource", "HTTP_RESPONSE_ENTITY_BODY");
        evidence.put("httpBytesScope", "PAYLOAD_ONLY");
        for (String field : List.of("effectiveFrom", "effectiveTo", "retentionCutoff")) {
            assertThat(reading.response().get(field)).as("history response field %s", field).isInstanceOf(String.class);
            evidence.put(field, reading.response().get(field));
        }
        evidence.put("raw", collectionEvidence(reading.raw()));
        evidence.put("rollup", collectionEvidence(reading.rollup()));
        return evidence;
    }

    private static Map<String, Object> collectionEvidence(CollectionCost cost) {
        return Map.of("readOperations", cost.commands(), "keysExamined", cost.keysExamined(),
                "docsExamined", cost.docsExamined(), "replyBytes", cost.replyBytes());
    }

    @SuppressWarnings("unchecked")
    private static void assertResponse(Reading reading, Window window,
            Instant missingBucket, boolean frozen) {
        Map<String, Object> response = reading.response();
        assertThat(response).containsEntry("pipelineId", PIPELINE)
                .containsEntry("effectiveResolution", window.effectiveResolution())
                .containsEntry("status", "OK")
                .containsEntry("consistency", "EVENTUAL")
                .containsEntry("nextCursor", null);
        List<Map<String, Object>> segments = (List<Map<String, Object>>) response.get("segments");
        assertThat(segments).isNotEmpty();
        if (!frozen) {
            return;
        }
        List<Map<String, Object>> gaps = (List<Map<String, Object>>) response.get("gaps");
        assertThat(gaps).as("the 30-minute missing bucket must remain visible").anySatisfy(gap -> {
            assertThat(gap).containsEntry("reason", "SAMPLE_GAP");
            assertThat(Instant.parse(String.valueOf(gap.get("intervalStart"))))
                    .isBefore(missingBucket.plusSeconds(60));
            assertThat(Instant.parse(String.valueOf(gap.get("intervalEnd")))).isAfter(missingBucket);
        });
    }

    private static void compare(List<WindowRun> raw, List<WindowRun> cached) {
        assertThat(cached).hasSameSizeAs(raw);
        for (int i = 0; i < raw.size(); i++) {
            Reading expected = raw.get(i).cold();
            Reading actual = cached.get(i).cold();
            assertThat(cached.get(i).window()).isEqualTo(raw.get(i).window());
            for (String field : List.of("pipelineId", "effectiveResolution", "status", "consistency",
                    "segments", "gaps", "unavailable", "nextCursor")) {
                assertThat(actual.response().get(field)).as("raw/cache response field %s", field)
                        .isEqualTo(expected.response().get(field));
            }
        }
    }

    private static WindowRun matching(List<WindowRun> runs, Window window) {
        return runs.stream().filter(run -> run.window().equals(window)).findFirst().orElseThrow();
    }

    private static void reportFrozen(Window window, Reading cold, List<Reading> hot) {
        List<Long> latencies = hot.stream().map(Reading::elapsedNanos).sorted().toList();
        System.out.printf("history-query-window=%s resolution=%s coldMs=%.3f hotP50Ms=%.3f hotP95Ms=%.3f"
                        + " coldCommands=%d coldKeys=%d coldDocs=%d coldResponseBytes=%d"
                        + " hotCommands=%d hotKeys=%d hotDocs=%d hotResponseBytes=%d%n",
                window.name(), window.effectiveResolution(), millis(cold.elapsedNanos()),
                millis(percentile(latencies, 0.50)), millis(percentile(latencies, 0.95)),
                cold.raw().commands(), cold.raw().keysExamined(), cold.raw().docsExamined(), cold.responseBytes(),
                median(hot.stream().map(reading -> reading.raw().commands()).toList()),
                median(hot.stream().map(reading -> reading.raw().keysExamined()).toList()),
                median(hot.stream().map(reading -> reading.raw().docsExamined()).toList()),
                median(hot.stream().map(Reading::responseBytes).toList()));
    }

    private static void reportPath(String mode, WindowRun run) {
        Reading cold = run.cold();
        List<Reading> hot = run.hot();
        List<Long> latencies = hot.stream().map(Reading::elapsedNanos).sorted().toList();
        System.out.printf("history-query-path mode=%s window=%s span=%s resolution=%s"
                        + " coldMs=%.3f hotP50Ms=%.3f hotP95Ms=%.3f"
                        + " coldRawCommands=%d coldRawKeys=%d coldRawDocs=%d coldRawReplyBytes=%d"
                        + " coldRollupCommands=%d coldRollupKeys=%d coldRollupDocs=%d coldRollupReplyBytes=%d"
                        + " coldHttpBytes=%d hotRawCommands=%d hotRawKeys=%d hotRawDocs=%d hotRawReplyBytes=%d"
                        + " hotRollupCommands=%d hotRollupKeys=%d hotRollupDocs=%d hotRollupReplyBytes=%d hotHttpBytes=%d"
                        + " processPeakHeapBytes=%d processPeakRssBytes=%d resourceSamples=%d%n",
                mode, run.window().name(), run.window().span(), run.window().effectiveResolution(),
                millis(cold.elapsedNanos()), millis(percentile(latencies, 0.50)),
                millis(percentile(latencies, 0.95)),
                cold.raw().commands(), cold.raw().keysExamined(), cold.raw().docsExamined(),
                cold.raw().replyBytes(), cold.rollup().commands(), cold.rollup().keysExamined(),
                cold.rollup().docsExamined(), cold.rollup().replyBytes(), cold.responseBytes(),
                median(hot.stream().map(reading -> reading.raw().commands()).toList()),
                median(hot.stream().map(reading -> reading.raw().keysExamined()).toList()),
                median(hot.stream().map(reading -> reading.raw().docsExamined()).toList()),
                median(hot.stream().map(reading -> reading.raw().replyBytes()).toList()),
                median(hot.stream().map(reading -> reading.rollup().commands()).toList()),
                median(hot.stream().map(reading -> reading.rollup().keysExamined()).toList()),
                median(hot.stream().map(reading -> reading.rollup().docsExamined()).toList()),
                median(hot.stream().map(reading -> reading.rollup().replyBytes()).toList()),
                median(hot.stream().map(Reading::responseBytes).toList()),
                run.resources().peakHeapBytes(), run.resources().peakRssBytes(),
                run.resources().sampleCount());
    }

    private static void reportPair(String phase, WindowRun raw, WindowRun cached) {
        long rawP95 = percentile(raw.hot().stream().map(Reading::elapsedNanos).sorted().toList(), 0.95);
        long cachedP95 = percentile(cached.hot().stream().map(Reading::elapsedNanos).sorted().toList(), 0.95);
        System.out.printf("history-query-pair phase=%s window=%s rawHotP95Ms=%.3f"
                        + " cachedHotP95Ms=%.3f changePercent=%.2f rawHttpBytes=%d cachedHttpBytes=%d%n",
                phase, raw.window().name(), millis(rawP95), millis(cachedP95),
                (cachedP95 - rawP95) * 100.0 / rawP95,
                raw.cold().responseBytes(), cached.cold().responseBytes());
    }

    private static double millis(long nanos) {
        return nanos / 1_000_000.0;
    }

    private static long percentile(List<Long> sorted, double fraction) {
        return sorted.get(Math.max(0, (int) Math.ceil(sorted.size() * fraction) - 1));
    }

    private static long median(List<? extends Number> values) {
        return values.stream().map(Number::longValue).sorted(Comparator.naturalOrder())
                .toList().get(values.size() / 2);
    }

    private record Window(String name, Duration span, String resolution, String effectiveResolution) {}

    private record ArmRuns(List<WindowRun> frozen, List<WindowRun> aligned, List<WindowRun> missing) {}

    private record WindowRun(Window window, Reading cold, List<Reading> hot,
            BenchmarkResourceSampler.Summary resources) {}

    record CollectionCost(int commands, long keysExamined, long docsExamined,
            long replyBytes, List<Document> operations) {}

    record Reading(Map<String, Object> response, long elapsedNanos, int responseBytes,
            CollectionCost raw, CollectionCost rollup) {}
}
