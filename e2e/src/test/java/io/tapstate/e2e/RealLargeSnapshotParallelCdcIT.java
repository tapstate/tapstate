package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.adapters.mongostore.MongoObservationStore;
import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.core.lifecycle.MetricAttributes;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.testsupport.RequiresDocker;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import static org.assertj.core.api.Assertions.assertThat;

/** A real-process host witness: a large MySQL load must leave another CDC pipeline moving. */
@RequiresDocker
@EnabledIfSystemProperty(named = "tapstate.e2e.large-snapshot.jar", matches = ".+")
class RealLargeSnapshotParallelCdcIT {

    private static final String BOOT_JAR_PROPERTY = "tapstate.e2e.large-snapshot.jar";
    private static final String BULK_PIPELINE = "large_snapshot";
    private static final String FAST_PIPELINE = "parallel_cdc";
    private static final long BULK_ROWS = 524_288L;
    private static final Duration WAIT = Duration.ofMinutes(12);

    @BeforeAll
    static void requireConnectorsAndJar() {
        RealConnectorGate.require("mysql", "mongodb");
        assertThat(Files.isRegularFile(Path.of(System.getProperty(BOOT_JAR_PROPERTY))))
                .as("the explicitly requested boot JAR exists").isTrue();
    }

    @Test
    void aLargeSnapshotLeavesParallelCdcAndObservationsMoving() throws Exception {
        Map<String, Object> bulkMysql = SharedMySql.settings("large_snapshot_source");
        Map<String, Object> fastMysql = SharedMySql.settings("parallel_cdc_source");
        seedBulk(bulkMysql);
        seedFast(fastMysql);
        String storeUri = SharedMongo.replicaSetUrl("large_snapshot_store");
        String targetUri = SharedMongo.replicaSetUrl("large_snapshot_target");
        EndpointAddress target = EndpointAddress.uri(targetUri);
        Path jar = Path.of(System.getProperty(BOOT_JAR_PROPERTY));

        try (MongoEndpoints mongo = new MongoEndpoints();
                MongoClient targetClient = MongoClients.create(targetUri);
                RealProcessServer server = RealProcessServer.start(storeUri, jar)) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("large-snapshot", "large-snapshot-password");
            control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
            control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
            control.apply(resources(bulkMysql, fastMysql, targetUri));
            control.discoverSchema("bulk_source", "mysql", bulkMysql);
            control.discoverSchema("fast_source", "mysql", fastMysql);

            control.lifecycle(FAST_PIPELINE, LifecycleVerb.START);
            Await.until("parallel CDC baseline", Duration.ofMinutes(2),
                    () -> "before".equals(fastStatus(mongo, target)),
                    () -> "state=" + control.state(FAST_PIPELINE) + ", target=" + fastStatus(mongo, target));
            assertThat(control.state(FAST_PIPELINE)).contains(PipelineState.RUNNING);
            long firstFastRecords = Await.answered("parallel CDC records-out baseline",
                    () -> control.recordsOut(FAST_PIPELINE).filter(count -> count > 0));
            Instant firstFastObserved = control.statusObservedAt(FAST_PIPELINE);

            control.lifecycle(BULK_PIPELINE, LifecycleVerb.START);
            Await.until("bulk snapshot to be visibly in flight", Duration.ofMinutes(3),
                    () -> inFlightRows(control) > 1_000 && inFlightRows(control) < BULK_ROWS,
                    () -> "state=" + control.state(BULK_PIPELINE)
                            + ", rows=" + control.snapshotRowsRead(BULK_PIPELINE));
            long firstRows = inFlightRows(control);
            Instant firstObserved = control.statusObservedAt(BULK_PIPELINE);

            long issued = System.nanoTime();
            updateFast(fastMysql);
            Await.until("parallel CDC while the other snapshot is still running", Duration.ofMinutes(2),
                    () -> "after".equals(fastStatus(mongo, target)),
                    () -> "fast=" + control.state(FAST_PIPELINE)
                            + ", target=" + fastStatus(mongo, target)
                            + ", bulkRows=" + inFlightRows(control));
            long deliveredNanos = System.nanoTime() - issued;
            long rowsAtFastDelivery = inFlightRows(control);
            assertThat(rowsAtFastDelivery)
                    .as("the other pipeline delivered CDC before the bulk load finished")
                    .isBetween(1L, BULK_ROWS - 1L);
            awaitFastProgress(control, firstFastRecords, firstFastObserved);
            long secondFastRecords = control.recordsOut(FAST_PIPELINE).orElseThrow();
            Instant secondFastObserved = control.statusObservedAt(FAST_PIPELINE);
            updateFast(fastMysql, "after_again");
            Await.until("second CDC change during the other snapshot", Duration.ofMinutes(2),
                    () -> "after_again".equals(fastStatus(mongo, target)),
                    () -> "fast=" + control.state(FAST_PIPELINE)
                            + ", target=" + fastStatus(mongo, target)
                            + ", bulkRows=" + inFlightRows(control));
            awaitFastProgress(control, secondFastRecords, secondFastObserved);
            long finalFastRecords = control.recordsOut(FAST_PIPELINE).orElseThrow();
            Instant finalFastObserved = control.statusObservedAt(FAST_PIPELINE);
            Await.until("snapshot rows and freshness to progress while CDC stays live", Duration.ofSeconds(20),
                    () -> inFlightRows(control) > firstRows && inFlightRows(control) < BULK_ROWS
                            && control.statusObservedAt(BULK_PIPELINE).isAfter(firstObserved),
                    () -> "firstRows=" + firstRows + ", now=" + inFlightRows(control)
                            + ", firstObserved=" + firstObserved);

            Await.until("bulk target terminal coverage", WAIT,
                    () -> control.recordCount(BULK_PIPELINE).orElse(0L) >= BULK_ROWS,
                    () -> "recordsOut=" + control.recordCount(BULK_PIPELINE)
                            + ", snapshot=" + control.snapshotRowsRead(BULK_PIPELINE));
            String targetDatabase = new ConnectionString(targetUri).getDatabase();
            long physicallyDelivered = targetClient.getDatabase(targetDatabase)
                    .getCollection("bulk_orders").countDocuments();
            assertThat(physicallyDelivered).isEqualTo(BULK_ROWS);
            assertThat(control.errorCount(BULK_PIPELINE)).contains(0L);
            assertThat(control.errorCount(FAST_PIPELINE)).contains(0L);
            System.out.printf("large-snapshot-live jar=%s bulkRows=%d firstObservedRows=%d"
                            + " rowsWhenParallelCdcDelivered=%d parallelCdcMs=%.3f"
                            + " fastRecordsFrom=%d fastRecordsMid=%d fastRecordsTo=%d"
                            + " fastObservedFrom=%s fastObservedMid=%s fastObservedTo=%s"
                            + " physicallyDelivered=%d%n",
                    jar, BULK_ROWS, firstRows, rowsAtFastDelivery,
                    deliveredNanos / 1_000_000.0, firstFastRecords, secondFastRecords, finalFastRecords,
                    firstFastObserved, secondFastObserved, finalFastObserved, physicallyDelivered);
        }
    }

    @Test
    void rebuildingAPausedSnapshotKeepsCumulativeFactsAndParallelCdcMoving() throws Exception {
        Path jar = Path.of(System.getProperty(BOOT_JAR_PROPERTY));
        boolean directCounts = Boolean.getBoolean("tapstate.e2e.large-snapshot.direct-counts");
        BenchmarkLiveReport report = directCounts ? admissionReport() : null;
        var admissions = new ExecutionAdmissionStages(report, directCounts);
        try {
            if (report != null) {
                report.begin(Map.of("purpose", "REAL_PARTIAL_SNAPSHOT_REBUILD_ADMISSION_COUNTS",
                                "application", PipelineBenchmarkLiveRunIT.artifact(jar), "discoveryMode", "none"),
                        PipelineBenchmarkLiveRunIT.environment(), List.of());
            }
            runSnapshotRebuildWitness(jar, admissions, report, directCounts);
            if (report != null) {
                report.completeDiagnostic(Map.of("correctness", "REAL_PARTIAL_SNAPSHOT_REBUILT_ONCE_WITH_CONTINUOUS_TOTALS",
                        "performanceAcceptanceEligible", false,
                        "unverified", List.of("PHYSICAL_MONGO_COMMAND_COUNTS", "ALL_TELEMETRY_SURFACE_IDENTITIES")));
            }
        } catch (Exception | Error failure) {
            if (report != null) {
                try { report.fail(failure); }
                catch (RuntimeException writeFailure) { if (writeFailure != failure) { failure.addSuppressed(writeFailure); } }
            }
            throw failure;
        }
    }

    private static BenchmarkLiveReport admissionReport() throws Exception {
        String requested = System.getProperty("tapstate.e2e.large-snapshot.output");
        if (requested == null || requested.isBlank()) {
            throw new IllegalArgumentException("direct snapshot admission counts require an output path");
        }
        Path output = Path.of(requested).toAbsolutePath().normalize();
        PipelineBenchmarkLiveRunIT.requireSafeOutput(output, PipelineBenchmarkLiveRunIT.harnessRoot());
        return new BenchmarkLiveReport(output);
    }

    private static void runSnapshotRebuildWitness(Path jar, ExecutionAdmissionStages admissions,
            BenchmarkLiveReport report, boolean directCounts) throws Exception {
        Map<String, Object> bulkMysql = SharedMySql.settings("snapshot_resume_source");
        Map<String, Object> fastMysql = SharedMySql.settings("snapshot_resume_cdc_source");
        seedBulk(bulkMysql);
        seedFast(fastMysql);
        String storeUri = SharedMongo.replicaSetUrl("snapshot_resume_store");
        String targetUri = SharedMongo.replicaSetUrl("snapshot_resume_target");
        EndpointAddress target = EndpointAddress.uri(targetUri);
        String jarSha = PipelineBenchmarkLiveRunIT.sha256(jar);

        try (MongoEndpoints mongo = new MongoEndpoints();
                MongoClient targetClient = MongoClients.create(targetUri);
                MongoClient storeClient = MongoClients.create(storeUri);
                BenchmarkForkEnvironment.OwnedBoot boot = snapshotBoot(storeUri, jar, admissions, directCounts)) {
            MongoDatabase database = storeClient.getDatabase(new ConnectionString(storeUri).getDatabase());
            var latest = new MongoObservationStore(storeClient,
                    database.getCollection(MongoStorePort.PIPELINE_OBSERVATION),
                    database.getCollection(MongoStorePort.PIPELINE_OBSERVATION_CHUNKS));
            MongoDatabase targetDatabase = targetClient.getDatabase(new ConnectionString(targetUri).getDatabase());
            ControlPlane control = new ControlPlane(boot.server().baseUrl());
            control.bootstrapAndLogin("snapshot-resume", "snapshot-resume-password");
            control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
            control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
            control.apply(resources(bulkMysql, fastMysql, targetUri));
            control.discoverSchema("bulk_source", "mysql", bulkMysql);
            control.discoverSchema("fast_source", "mysql", fastMysql);
            control.lifecycle(FAST_PIPELINE, LifecycleVerb.START);
            Await.until("real CDC target baseline", Duration.ofMinutes(2),
                    () -> "before".equals(fastStatus(mongo, target)),
                    () -> "target=" + fastStatus(mongo, target));
            long fastBefore = Await.answered("known CDC output before snapshot pause",
                    () -> control.recordsOut(FAST_PIPELINE).filter(count -> count > 0));
            Instant fastObservedBefore = control.statusObservedAt(FAST_PIPELINE);
            admissions.stage("before-bulk-start", 0, 0);
            control.lifecycle(BULK_PIPELINE, LifecycleVerb.START);
            ObservationStore.Stored before = Await.answered("actual partial snapshot delivery with all counters",
                    Duration.ofMinutes(3), () -> latest.readStored(BULK_PIPELINE).filter(value ->
                            value.scope().isPresent() && hasCumulativeDelivery(value.observation())
                            && inFlightRows(control) > 0 && inFlightRows(control) < BULK_ROWS
                            && bulkTargetRows(targetDatabase) > 0 && bulkTargetRows(targetDatabase) < BULK_ROWS));
            ObservationStore.Scope oldScope = before.scope().orElseThrow();
            assertThat(claimGeneration(database, BULK_PIPELINE)).isEqualTo(oldScope.executionGeneration());
            Document oldSample = actualHistorySample(database, oldScope);
            admissions.stage("partial-snapshot-start", 1, 1);
            control.lifecycle(BULK_PIPELINE, LifecycleVerb.PAUSE);
            Await.until("the unfinished snapshot to be actually paused", Duration.ofMinutes(2),
                    () -> control.state(BULK_PIPELINE).filter(PipelineState.PAUSED::equals).isPresent(),
                    () -> "state=" + control.state(BULK_PIPELINE));
            long pausedRows = bulkTargetRows(targetDatabase);
            assertThat(pausedRows).as("the pause reached the data plane before snapshot completion")
                    .isBetween(1L, BULK_ROWS - 1);
            var paused = Await.answered("scoped paused observation", () -> latest.readStored(BULK_PIPELINE)
                    .filter(value -> value.observation().state() == PipelineState.PAUSED));
            assertThat(paused.scope()).contains(oldScope);
            assertThat(claimGeneration(database, BULK_PIPELINE)).isEqualTo(oldScope.executionGeneration());
            admissions.stage("snapshot-pause", 0, 0);

            updateFast(fastMysql, "while_paused");
            awaitFastTargetAndObservation(control, mongo, target, "while_paused", fastBefore, fastObservedBefore);
            long fastPaused = control.recordsOut(FAST_PIPELINE).orElseThrow();
            Instant fastObservedPaused = control.statusObservedAt(FAST_PIPELINE);
            control.lifecycle(BULK_PIPELINE, LifecycleVerb.RESUME);
            ObservationStore.Scope resumedScope = new ObservationStore.Scope(oldScope.pipelineIncarnationId(),
                    oldScope.executionGeneration() + 1);
            ObservationStore.Stored resumed = Await.answered("new scoped snapshot to make real target progress",
                    Duration.ofMinutes(3), () -> latest.readStored(BULK_PIPELINE).filter(value ->
                            value.scope().filter(resumedScope::equals).isPresent()
                            && value.observation().state() == PipelineState.RUNNING
                            && hasCumulativeDelivery(value.observation())
                            && cumulativeDeliveryAdvanced(before.observation(), value.observation())
                            && bulkTargetRows(targetDatabase) > pausedRows
                            && bulkTargetRows(targetDatabase) < BULK_ROWS));
            assertThat(claimGeneration(database, BULK_PIPELINE)).isEqualTo(resumedScope.executionGeneration());
            assertCumulativeDeliveryContinued(before.observation(), resumed.observation());
            Document resumedSample = actualHistorySample(database, resumedScope);
            assertThat(resumedSample.getDate("countingSince")).isEqualTo(oldSample.getDate("countingSince"));
            admissions.stage("partial-snapshot-rebuild-resume", 1, 1);

            updateFast(fastMysql, "after_resume");
            awaitFastTargetAndObservation(control, mongo, target, "after_resume", fastPaused, fastObservedPaused);
            assertThat(bulkTargetRows(targetDatabase)).as("CDC advanced during the resumed partial load")
                    .isBetween(1L, BULK_ROWS - 1);
            Map<?, ?> history = control.history(BULK_PIPELINE,
                    oldSample.getDate("observedAt").toInstant().minusSeconds(1), Instant.now());
            List<String> rawTrace = database.getCollection(MongoStorePort.PIPELINE_RATE_HISTORY)
                    .find(new Document("pipelineId", BULK_PIPELINE))
                    .sort(new Document("observedAt", 1).append("_id", 1)).limit(64)
                    .into(new java.util.ArrayList<>()).stream().map(Document::toJson).toList();
            System.out.println("snapshot-resume-history=" + JsonWriter.write(Map.of(
                    "jarSha", jarSha, "rawSamples", rawTrace, "response", history)));
            assertThat(history.get("segments")).isInstanceOf(List.class);
            List<?> segments = (List<?>) history.get("segments");
            long points = segments.stream().map(Map.class::cast)
                    .mapToLong(segment -> ((List<?>) segment.get("points")).size()).sum();
            assertThat(points).as("history contains actual samples on both sides of the rebuild")
                    .isGreaterThanOrEqualTo(2);
            assertThat(segments.stream().map(Map.class::cast).map(segment -> segment.get("startReason")))
                    .doesNotContain("COUNTER_RESET");
            Await.until("the resumed snapshot to reach its complete physical target", WAIT,
                    () -> bulkTargetRows(targetDatabase) == BULK_ROWS,
                    () -> "targetRows=" + bulkTargetRows(targetDatabase));
            Await.answered("final cumulative delivery records and histogram to cover the full snapshot", () ->
                    latest.readStored(BULK_PIPELINE).filter(value -> value.scope().filter(resumedScope::equals).isPresent()
                            && deliveryPoints(value.observation(), "tapstate.pipeline.records").stream()
                                    .mapToLong(MetricPoint::value).sum() >= BULK_ROWS
                            && deliveryPoints(value.observation(), "tapstate.pipeline.record.delivery.duration").stream()
                                    .mapToLong(point -> point.histogram().count()).sum() >= BULK_ROWS));
            assertThat(control.errorCount(BULK_PIPELINE)).contains(0L);
            assertThat(control.errorCount(FAST_PIPELINE)).contains(0L);
            admissions.stage("snapshot-completed-with-cdc-ticks", 0, 0);
            control.stop(BULK_PIPELINE, false);
            control.stop(FAST_PIPELINE, false);
            admissions.stage("both-stopped", 0, 0);
            System.out.printf("snapshot-resume-live jarSha=%s generationFrom=%d generationTo=%d"
                            + " pausedTargetRows=%d recordsBefore=%d recordsAfter=%d targetRows=%d"
                            + " publicHistoryPoints=%d counterStart=%s%n", jarSha,
                    oldScope.executionGeneration(), resumedScope.executionGeneration(), pausedRows,
                    deliveryPoints(before.observation(), "tapstate.pipeline.records").stream()
                            .mapToLong(MetricPoint::value).sum(),
                    deliveryPoints(resumed.observation(), "tapstate.pipeline.records").stream()
                            .mapToLong(MetricPoint::value).sum(), bulkTargetRows(targetDatabase), points,
                    deliveryPoints(before.observation(), "tapstate.pipeline.records").getFirst().startTime());
            if (report != null) {
                report.addFork(Map.of("action", "partial-snapshot-correctness", "pipelineId", BULK_PIPELINE,
                        "incarnation", oldScope.pipelineIncarnationId(), "generationFrom", oldScope.executionGeneration(),
                        "generationTo", resumedScope.executionGeneration(), "pausedTargetRows", pausedRows,
                        "finalTargetRows", bulkTargetRows(targetDatabase), "publicHistoryPoints", points,
                        "counterStart", deliveryPoints(before.observation(), "tapstate.pipeline.records")
                                .getFirst().startTime().toString()));
            }
            admissions.shutdown("snapshot-process-shutdown");
        }
        assertThat(PipelineBenchmarkLiveRunIT.sha256(jar)).isEqualTo(jarSha);
    }

    private static BenchmarkForkEnvironment.OwnedBoot snapshotBoot(String storeUri, Path jar,
            ExecutionAdmissionStages admissions, boolean directCounts) throws Exception {
        List<String> arguments = List.of("--tapstate.metrics.history.sample-interval=PT2S");
        if (!directCounts) {
            return new BenchmarkForkEnvironment.OwnedBoot(
                    RealProcessServer.start(storeUri, "snapshot_resume_operator", jar, arguments), null);
        }
        var observer = ExecutionAdmissionJdiSession.startWithLeaseObservation(storeUri,
                "snapshot_resume_operator", jar, BULK_PIPELINE, arguments);
        admissions.bind(observer);
        return new BenchmarkForkEnvironment.OwnedBoot(observer.server(), null, observer);
    }

    private static long bulkTargetRows(MongoDatabase target) {
        return target.getCollection("bulk_orders").countDocuments();
    }

    private static long claimGeneration(MongoDatabase database, String pipeline) {
        Document claim = database.getCollection(MongoStorePort.WORKLOAD_CLAIMS)
                .find(new Document("resourceType", "PIPELINE_ACTUATION").append("resourceId", pipeline)).first();
        assertThat(claim).as("the real submission has a durable generation").isNotNull();
        assertThat(claim.get("executionGeneration")).isInstanceOf(Number.class);
        return ((Number) claim.get("executionGeneration")).longValue();
    }

    private static Document actualHistorySample(MongoDatabase database, ObservationStore.Scope scope) {
        List<Document> samples = Await.answered("two real increasing history samples for scope " + scope, () -> {
            List<Document> positive = database.getCollection(MongoStorePort.PIPELINE_RATE_HISTORY)
                        .find(new Document("pipelineId", BULK_PIPELINE)
                                .append("pipelineIncarnationId", scope.pipelineIncarnationId())
                                .append("executionGeneration", scope.executionGeneration()))
                        .sort(new Document("observedAt", 1).append("_id", 1)).limit(64)
                        .into(new java.util.ArrayList<>()).stream().filter(value -> {
                            Object counters = value.get("counters");
                            return counters instanceof Document values
                                    && values.get("records.out") instanceof Number count && count.longValue() > 0
                                    && values.get("bytes.out") instanceof Number bytes && bytes.longValue() > 0
                                    && value.getDate("countingSince") != null;
                        }).toList();
            if (positive.size() < 2) { return Optional.empty(); }
            Document first = positive.getFirst();
            return positive.stream().skip(1).filter(next ->
                    first.getDate("countingSince").equals(next.getDate("countingSince"))
                            && List.of("records.out", "bytes.out").stream().allMatch(counter ->
                            ((Number) next.get("counters", Document.class).get(counter)).longValue()
                                    > ((Number) first.get("counters", Document.class).get(counter)).longValue()))
                    .findFirst().map(next -> List.of(first, next));
        });
        Document first = samples.getFirst();
        Document second = samples.get(1);
        assertThat(second.getDate("observedAt")).isAfter(first.getDate("observedAt"));
        assertThat(second.getDate("countingSince")).isEqualTo(first.getDate("countingSince"));
        for (String counter : List.of("records.out", "bytes.out")) {
            assertThat(((Number) second.get("counters", Document.class).get(counter)).longValue())
                    .isGreaterThan(((Number) first.get("counters", Document.class).get(counter)).longValue());
        }
        return first;
    }

    private static boolean cumulativeDeliveryAdvanced(Observation before, Observation after) {
        for (String name : List.of("tapstate.pipeline.records", "tapstate.pipeline.bytes",
                "tapstate.pipeline.record.delivery.duration")) {
            List<MetricPoint> fresh = deliveryPoints(after, name);
            for (MetricPoint old : deliveryPoints(before, name)) {
                Optional<MetricPoint> now = fresh.stream()
                        .filter(point -> point.attributes().equals(old.attributes())).findFirst();
                if (now.isEmpty() || (old.histogram() == null ? now.get().value() <= old.value()
                        : now.get().histogram().count() <= old.histogram().count())) { return false; }
            }
        }
        return true;
    }

    private static boolean hasCumulativeDelivery(Observation observation) {
        return List.of("tapstate.pipeline.records", "tapstate.pipeline.bytes",
                "tapstate.pipeline.record.delivery.duration").stream().allMatch(name ->
                deliveryPoints(observation, name).stream().anyMatch(point -> point.startTime() != null
                        && (point.histogram() == null ? point.value() > 0 : point.histogram().count() > 0)));
    }

    private static List<MetricPoint> deliveryPoints(Observation observation, String name) {
        return observation.facts().stream().filter(fact -> fact.name().equals(name))
                .flatMap(fact -> fact.points().stream()).filter(point ->
                        "bulk_orders".equals(point.attributes().get(MetricAttributes.TABLE_ID))
                                && (name.endsWith(".duration")
                                || "out".equals(point.attributes().get(MetricAttributes.DIRECTION)))).toList();
    }

    private static void assertCumulativeDeliveryContinued(Observation before, Observation after) {
        for (String name : List.of("tapstate.pipeline.records", "tapstate.pipeline.bytes",
                "tapstate.pipeline.record.delivery.duration")) {
            List<MetricPoint> fresh = deliveryPoints(after, name);
            for (MetricPoint old : deliveryPoints(before, name)) {
                MetricPoint continued = fresh.stream().filter(point -> point.attributes().equals(old.attributes()))
                        .findFirst().orElseThrow(() -> new AssertionError("known delivery point disappeared: " + name));
                assertThat(continued.startTime()).isEqualTo(old.startTime());
                if (old.histogram() == null) { assertThat(continued.value()).isGreaterThanOrEqualTo(old.value()); }
                else {
                    assertThat(continued.histogram().count()).isGreaterThanOrEqualTo(old.histogram().count());
                    assertThat(continued.histogram().sum()).isGreaterThanOrEqualTo(old.histogram().sum());
                    assertThat(continued.histogram().bounds()).isEqualTo(old.histogram().bounds());
                    for (int bucket = 0; bucket < old.histogram().bucketCounts().size(); bucket++) {
                        assertThat(continued.histogram().bucketCounts().get(bucket))
                                .isGreaterThanOrEqualTo(old.histogram().bucketCounts().get(bucket));
                    }
                }
            }
        }
    }

    private static void awaitFastTargetAndObservation(ControlPlane control, MongoEndpoints mongo,
            EndpointAddress target, String status, long records, Instant observed) {
        Await.until("the real parallel CDC target and observation to advance", Duration.ofSeconds(30),
                () -> status.equals(fastStatus(mongo, target))
                        && control.recordsOut(FAST_PIPELINE).filter(count -> count > records).isPresent()
                        && control.statusObservedAt(FAST_PIPELINE).isAfter(observed),
                () -> "target=" + fastStatus(mongo, target) + ", output=" + control.recordsOut(FAST_PIPELINE));
    }

    private static long inFlightRows(ControlPlane control) {
        return control.snapshotRowsRead(BULK_PIPELINE).getOrDefault("bulk_orders", 0L);
    }

    private static void awaitFastProgress(ControlPlane control, long previousRecords, Instant previousObserved) {
        Await.until("parallel CDC freshness and records-out during the other snapshot", Duration.ofSeconds(20),
                () -> inFlightRows(control) > 0
                        && inFlightRows(control) < BULK_ROWS
                        && control.recordsOut(FAST_PIPELINE).orElse(0L) > previousRecords
                        && control.statusObservedAt(FAST_PIPELINE).isAfter(previousObserved),
                () -> "recordsOut=" + control.recordsOut(FAST_PIPELINE)
                        + ", observedAt=" + control.statusObservedAt(FAST_PIPELINE)
                        + ", bulkRows=" + inFlightRows(control));
    }

    private static String fastStatus(MongoEndpoints mongo, EndpointAddress target) {
        List<Document> rows = mongo.documents(target, "fast_orders");
        return rows.isEmpty() ? null : rows.getFirst().getString("status");
    }

    private static void seedBulk(Map<String, Object> mysql) throws Exception {
        try (Connection connection = SharedMySql.connect(mysql);
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE bulk_orders (id BIGINT PRIMARY KEY, payload VARCHAR(64))");
            statement.execute("INSERT INTO bulk_orders VALUES (1, REPEAT('x', 64))");
            for (long count = 1; count < BULK_ROWS; count *= 2) {
                statement.execute("INSERT INTO bulk_orders (id, payload)"
                        + " SELECT id + " + count + ", payload FROM bulk_orders");
            }
        }
    }

    private static void seedFast(Map<String, Object> mysql) throws Exception {
        try (Connection connection = SharedMySql.connect(mysql);
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE fast_orders (id BIGINT PRIMARY KEY, status VARCHAR(16))");
            statement.execute("INSERT INTO fast_orders VALUES (1, 'before')");
        }
    }

    private static void updateFast(Map<String, Object> mysql) throws Exception {
        updateFast(mysql, "after");
    }

    private static void updateFast(Map<String, Object> mysql, String status) throws Exception {
        try (Connection connection = SharedMySql.connect(mysql);
                var statement = connection.prepareStatement("UPDATE fast_orders SET status=? WHERE id=1")) {
            statement.setString(1, status);
            statement.executeUpdate();
        }
    }

    private static Map<String, String> resources(
            Map<String, Object> bulkMysql, Map<String, Object> fastMysql, String targetUri) {
        Map<String, String> resources = new LinkedHashMap<>();
        resources.put("bulk-source.tap.yml", source("bulk_source", "bulk_orders", bulkMysql));
        resources.put("fast-source.tap.yml", source("fast_source", "fast_orders", fastMysql));
        resources.put("target.tap.yml", """
                version: tapstate/v1
                kind: source
                id: parallel_target
                connector: mongodb
                config: { uri: "%s" }
                """.formatted(targetUri));
        resources.put("bulk-pipeline.tap.yml",
                pipeline(BULK_PIPELINE, "bulk_source", "bulk_orders"));
        resources.put("fast-pipeline.tap.yml",
                pipeline(FAST_PIPELINE, "fast_source", "fast_orders"));
        return resources;
    }

    private static String source(String id, String table, Map<String, Object> mysql) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mysql
                config: { host: %s, port: %s, database: %s, username: %s, password: %s }
                mode: cdc
                tables: [ %s ]
                """.formatted(id, mysql.get("host"), mysql.get("port"), mysql.get("database"),
                mysql.get("username"), mysql.get("password"), table);
    }

    private static String pipeline(String id, String source, String table) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: %s
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - { id: all_rows, from: [%s], type: filter, expr: "true" }
                serve:
                  from: all_rows
                  sync:
                    - source: parallel_target
                """.formatted(id, source, table);
    }
}
