package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.MongoDesiredStore;
import io.tapstate.adapters.mongostore.MongoObservationStore;
import io.tapstate.adapters.mongostore.MongoStateStore;
import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.core.lifecycle.HistogramValue;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricAttributes;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.MetricType;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.spi.store.HandoffIdentity;
import io.tapstate.spi.store.ObservationContinuation;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.StopReservation;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Offset.offset;

/** Real standalone process crashes preserve a paused snapshot's qualified counter and histogram floor. */
@RequiresDocker
@EnabledIfSystemProperty(named = "tapstate.e2e.rebuild-crash.jar", matches = ".+")
class RealRebuildHandoffCrashIT {
    private static final String PREFIX = "tapstate.e2e.rebuild-crash.";
    private static final String PIPELINE = "handoff_bulk";
    private static final String TABLE = "bulk_orders";
    private static final String USER = "rebuild-crash", PASSWORD = "rebuild-crash-password";
    private static final int ROWS = 524_288, UPDATED = 7, DELETED = 11, INSERTED = ROWS + 1;
    private static final String PAYLOAD = "x".repeat(64);
    private static final Duration SETUP_WAIT = Duration.ofMinutes(3), DELIVERY_WAIT = Duration.ofMinutes(12);
    private record Series(String name, Map<String, String> attributes) { }
    private record Matched(ObservationStore.Stored publicValue, ObservationStore.StoredContinuation privateValue,
            RebuildHandoffJdiSession.Raw raw) { }

    @BeforeAll
    static void requireInputs() {
        RealConnectorGate.require("mysql", "mongodb");
        assertThat(Path.of(required("jar"))).isRegularFile();
        assertThat(Path.of(required("output")).isAbsolute()).as("diagnostic output is explicitly owned").isTrue();
    }

    @ParameterizedTest(name = "a real OS crash at {0} preserves continuation")
    @MethodSource("crashCuts")
    @Timeout(value = 20, unit = TimeUnit.MINUTES)
    void aRealCrashInsideReplacementRestoresKnownCumulativeFacts(RebuildHandoffJdiSession.Cut cut) throws Exception {
        Path jar = Path.of(required("jar")).toRealPath();
        Path requested = Path.of(required("output")).toAbsolutePath().normalize();
        Path output = requested.resolveSibling(requested.getFileName() + "." + cut.name().toLowerCase(Locale.ROOT) + ".json");
        PipelineBenchmarkLiveRunIT.requireSafeOutput(output, PipelineBenchmarkLiveRunIT.harnessRoot());
        Path logDirectory = output.resolveSibling(output.getFileName() + ".server-logs");
        Map<String, Object> application = PipelineBenchmarkLiveRunIT.artifact(jar);
        Map<String, Object> connectors = Map.of("mysql", PipelineBenchmarkLiveRunIT.artifact(ConnectorJars.pathFor("mysql")),
                "mongodb", PipelineBenchmarkLiveRunIT.artifact(ConnectorJars.pathFor("mongodb")));
        BenchmarkLiveReport report = new BenchmarkLiveReport(output);
        report.begin(Map.of("purpose", "REAL_STANDALONE_REBUILD_CRASH", "cut", cut.name(),
                        "application", application, "connectors", connectors, "rows", ROWS,
                        "serverLogDirectory", logDirectory.toString()),
                Map.of("kind", "correctness-only", "discoveryMode", "none"), List.of());
        try {
            String suffix = UUID.randomUUID().toString().replace("-", "").substring(0, 12);
            Map<String, Object> source = SharedMySql.settings("rebuild_crash_" + suffix);
            seed(source);
            String storeUri = SharedMongo.replicaSetUrl("rebuild_crash_" + suffix + "_store");
            String targetUri = SharedMongo.replicaSetUrl("rebuild_crash_" + suffix + "_target");
            String operatorDatabase = "rebuild_crash_" + suffix + "_operator";
            try (MongoClient storeClient = MongoClients.create(storeUri); MongoClient targetClient = MongoClients.create(targetUri)) {
                MongoDatabase database = storeClient.getDatabase(new ConnectionString(storeUri).getDatabase());
                MongoDatabase target = targetClient.getDatabase(new ConnectionString(targetUri).getDatabase());
                var actual = new MongoStateStore(database.getCollection(MongoStorePort.PIPELINE_STATE));
                var desired = new MongoDesiredStore(database.getCollection(MongoStorePort.PIPELINE_DESIRED));
                var latest = new MongoObservationStore(storeClient, database.getCollection(MongoStorePort.PIPELINE_OBSERVATION),
                        database.getCollection(MongoStorePort.PIPELINE_OBSERVATION_CHUNKS));
                ObservationStore.Scope oldScope;
                StopReservation marker;
                ObservationContinuation savedFloor;
                long killedPid;
                try (RebuildHandoffJdiSession first = RebuildHandoffJdiSession.start(
                        storeUri, operatorDatabase, jar, PIPELINE, TABLE, cut, logDirectory, "first")) {
                    report.addFork(Map.of("action", "owned-first-process-ready", "pid", first.server().pid(),
                            "retainedServerOutput", first.retainedOutput().toString()));
                    ControlPlane control = new ControlPlane(first.server().baseUrl());
                    control.bootstrapAndLogin(USER, PASSWORD);
                    control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
                    control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
                    control.apply(resources(source, targetUri));
                    control.discoverSchema("bulk_source", "mysql", source);
                    control.lifecycle(PIPELINE, LifecycleVerb.START);
                    var beforePause = Await.answered("real partial snapshot delivery with known counters and histogram", SETUP_WAIT,
                            () -> latest.readStored(PIPELINE).filter(value -> value.scope().isPresent()
                                    && hasKnownDelivery(value.observation().facts())
                                    && target.getCollection(TABLE).countDocuments() > 0
                                    && target.getCollection(TABLE).countDocuments() < ROWS
                                    && target.getCollection(TABLE).find(new Document("id", (long) DELETED)).first() != null));
                    control.lifecycle(PIPELINE, LifecycleVerb.PAUSE);
                    Await.until("the real native snapshot to be paused before delivery completes", SETUP_WAIT,
                            () -> actual.read(PIPELINE).filter(value -> StateJson.parse(value.stateJson()) == PipelineState.PAUSED).isPresent()
                                    && control.state(PIPELINE).filter(PipelineState.PAUSED::equals).isPresent(),
                            () -> "actual=" + actual.read(PIPELINE) + ", reported=" + control.state(PIPELINE));
                    long pausedRows = target.getCollection(TABLE).countDocuments();
                    assertThat(pausedRows).as("a genuine partial load was paused").isBetween(1L, ROWS - 1L);
                    var paused = Await.answered("known scoped PAUSED snapshot", SETUP_WAIT,
                            () -> latest.readStored(PIPELINE).filter(value -> value.observation().state() == PipelineState.PAUSED
                                    && value.scope().isPresent()));
                    oldScope = paused.scope().orElseThrow();
                    assertThat(paused.scope()).isEqualTo(beforePause.scope());
                    assertThat(generation(database)).isEqualTo(oldScope.executionGeneration());
                    assertThat(desired.read(PIPELINE).orElseThrow().targetState()).isEqualTo(PipelineState.PAUSED);
                    first.arm(oldScope);
                    control.lifecycle(PIPELINE, LifecycleVerb.RESUME);
                    RebuildHandoffJdiSession.Held held = first.awaitHeld(SETUP_WAIT);
                    // Ordinary first-start need not collect a private continuation packet. The real lookup
                    // used by lifecycle source binding is an independent native Job return before teardown.
                    var oldJobProof = first.observedJob(oldScope).orElseThrow(
                            () -> new AssertionError("no genuine Engine.executionJob return for the paused source scope"));
                    marker = actual.readStopReservation(PIPELINE).orElseThrow();
                    assertCrashBoundary(cut, marker, held, oldScope, generation(database));
                    assertThat(oldJobProof.scope()).isEqualTo(marker.source().scope());
                    assertThat(oldJobProof.job()).as("the marker pins the Job that the real native lookup returned")
                            .isEqualTo(marker.source().oldJob());
                    assertThat(oldJobProof.returnedAtNanos()).isLessThanOrEqualTo(held.atNanos());
                    assertThat(desired.read(PIPELINE)).contains(marker.originalDesired());
                    assertThat(actual.read(PIPELINE).map(value -> StateJson.parse(value.stateJson())))
                            .as("the held replacement has not acknowledged RUNNING").contains(PipelineState.STOPPED);
                    var carrier = Await.answered("a qualified known floor readable before the OS crash", SETUP_WAIT,
                            () -> latest.readContinuation(PIPELINE).filter(saved -> saved.continuation().token().equals(marker.token())
                                    && oldScope.equals(saved.continuation().sourceScope())
                                    && saved.receipt().knownBaseline() && hasKnownDelivery(saved.continuation().baselineFacts())));
                    savedFloor = carrier.continuation();
                    assertFloorAtLeast(delivery(beforePause.observation().facts()), delivery(savedFloor.baselineFacts()));
                    killedPid = first.server().pid();
                    report.addFork(Map.of("action", "qualified-crash-boundary", "held", held.evidence(),
                            "pid", killedPid, "serverOutput", first.retainedOutput().toString(), "pausedTargetRows", pausedRows,
                            "marker", markerEvidence(marker), "floor", factsEvidence(savedFloor.baselineFacts()),
                            "sourceCheckpoint", sourceCheckpoint(database), "oldJobProof", oldJobProof.evidence()));
                    first.killHeldProcess();
                    assertThat(first.server().isAlive()).isFalse();
                    assertThat(actual.readStopReservation(PIPELINE)).contains(marker);
                    assertThat(desired.read(PIPELINE)).contains(marker.originalDesired());
                    assertThat(generation(database)).isEqualTo(marker.writerAuthority().executionGeneration());
                }

                // These changes happen while the process is absent, against the same physical source log.
                mutateDuringCrash(source);
                try (RebuildHandoffJdiSession restarted = RebuildHandoffJdiSession.start(
                        storeUri, operatorDatabase, jar, PIPELINE, TABLE, null, logDirectory, "restarted")) {
                    report.addFork(Map.of("action", "owned-restarted-process-ready", "pid", restarted.server().pid(),
                            "retainedServerOutput", restarted.retainedOutput().toString()));
                    assertThat(restarted.server().pid()).isNotEqualTo(killedPid);
                    ControlPlane control = new ControlPlane(restarted.server().baseUrl());
                    control.login(USER, PASSWORD);
                    long expectedGeneration = marker.phase() == StopReservation.Phase.REPLACEMENT_PENDING
                            ? Math.incrementExact(oldScope.executionGeneration())
                            : Math.incrementExact(marker.successor().scope().executionGeneration());
                    ObservationStore.Scope expected = new ObservationStore.Scope(oldScope.pipelineIncarnationId(), expectedGeneration);
                    Matched firstKnown = Await.answered("same-store restart to publish floor plus its actual raw native facts", SETUP_WAIT,
                            () -> matched(latest, restarted, expected, savedFloor, null));
                    assertCumulativeExactly(firstKnown, savedFloor);
                    assertThat(generation(database)).isEqualTo(expectedGeneration);
                    assertThat(firstKnown.raw().job().bootId()).isNotEqualTo(marker.source().oldJob().bootId());
                    if (marker.successor() != null) {
                        assertThat(firstKnown.raw().job().bootId()).isNotEqualTo(marker.successor().submissionBootId());
                    }
                    Await.until("the actual handoff receipt to retire its matching marker", SETUP_WAIT,
                            () -> actual.readStopReservation(PIPELINE).isEmpty()
                                    && actual.read(PIPELINE).map(value -> StateJson.parse(value.stateJson()))
                                            .filter(PipelineState.RUNNING::equals).isPresent(),
                            () -> "marker=" + actual.readStopReservation(PIPELINE) + ", actual=" + actual.read(PIPELINE));
                    Await.until("snapshot and crash-time CDC to reach the physical target", DELIVERY_WAIT,
                            () -> target.getCollection(TABLE).countDocuments() == ROWS
                                    && row(target, INSERTED, "inserted-during-crash")
                                    && row(target, UPDATED, "updated-during-crash")
                                    && target.getCollection(TABLE).find(new Document("id", (long) DELETED)).first() == null
                                    && snapshotConfirmed(database),
                            () -> "targetRows=" + target.getCollection(TABLE).countDocuments()
                                    + ", actual=" + actual.read(PIPELINE) + ", offsets=" + sourceCheckpoint(database));
                    String coverageSha = assertFullTargetContent(target);
                    Matched quiet = Await.answered("known final native delivery frame", SETUP_WAIT,
                            () -> matched(latest, restarted, expected, savedFloor, firstKnown.publicValue().observation().observedAt()));
                    assertCumulativeExactly(quiet, savedFloor);
                    Matched repeated = Await.answered("a later unchanged native sample to retain totals without adding the floor again", SETUP_WAIT,
                            () -> matched(latest, restarted, expected, savedFloor, quiet.publicValue().observation().observedAt())
                                    .filter(value -> sameNativeTotals(quiet.raw(), value.raw())));
                    assertCumulativeExactly(repeated, savedFloor);
                    assertSameTotals(delivery(quiet.publicValue().observation().facts()),
                            delivery(repeated.publicValue().observation().facts()));
                    assertThat(generation(database)).as("freshness ticks do not admit further executions").isEqualTo(expectedGeneration);
                    assertThat(desired.read(PIPELINE)).contains(marker.originalDesired());
                    assertThat(control.errorCount(PIPELINE)).contains(0L);
                    report.addFork(Map.of("action", "real-process-recovered", "pid", restarted.server().pid(),
                            "serverOutput", restarted.retainedOutput().toString(), "scope", RebuildHandoffJdiSession.scopeEvidence(expected),
                            "job", RebuildHandoffJdiSession.jobEvidence(repeated.raw().job()),
                            "rawBinding", restarted.rawBinding().evidence(), "rawNative", factsEvidence(repeated.raw().facts()),
                            "cumulative", factsEvidence(repeated.publicValue().observation().facts()),
                            "targetIdCoverageSha256", coverageSha, "sourceCheckpoint", sourceCheckpoint(database)));
                    control.stop(PIPELINE, false);
                    Await.until("the recovered real execution to finish its stop", SETUP_WAIT,
                            () -> actual.read(PIPELINE).map(value -> StateJson.parse(value.stateJson()))
                                    .filter(PipelineState.STOPPED::equals).isPresent(), () -> "actual=" + actual.read(PIPELINE));
                }
            }
            assertThat(PipelineBenchmarkLiveRunIT.artifact(jar)).isEqualTo(application);
            assertThat(PipelineBenchmarkLiveRunIT.artifact(ConnectorJars.pathFor("mysql"))).isEqualTo(connectors.get("mysql"));
            assertThat(PipelineBenchmarkLiveRunIT.artifact(ConnectorJars.pathFor("mongodb"))).isEqualTo(connectors.get("mongodb"));
            report.completeDiagnostic(Map.of("correctness", "STANDALONE_PHASED_REBUILD_CRASH_CONTINUATION",
                    "performanceAcceptanceEligible", false,
                    "unverified", List.of("SURVIVING_REMOTE_JOB_ADOPTION", "OWNER_TAKEOVER", "FULL_LIFECYCLE_MATRIX", "PERFORMANCE_ACCEPTANCE")));
        } catch (Exception | Error failure) {
            try { report.fail(failure); } catch (RuntimeException recording) { failure.addSuppressed(recording); }
            throw failure;
        }
    }

    private static Stream<RebuildHandoffJdiSession.Cut> crashCuts() {
        String selected = System.getProperty(PREFIX + "cuts");
        if (selected == null) { return Arrays.stream(RebuildHandoffJdiSession.Cut.values()); }
        if (selected.isBlank() || selected.length() > 256) { throw new AssertionError("rebuild crash cuts must be a bounded enum selection"); }
        Set<RebuildHandoffJdiSession.Cut> cuts = new LinkedHashSet<>();
        for (String entry : selected.split(",", -1)) {
            RebuildHandoffJdiSession.Cut cut;
            try { cut = RebuildHandoffJdiSession.Cut.valueOf(entry.trim()); }
            catch (IllegalArgumentException invalid) { throw new AssertionError("unknown rebuild crash cut: " + entry, invalid); }
            if (!cuts.add(cut)) { throw new AssertionError("duplicate rebuild crash cut: " + entry); }
        }
        return cuts.stream();
    }

    private static void assertCrashBoundary(RebuildHandoffJdiSession.Cut cut, StopReservation marker,
            RebuildHandoffJdiSession.Held held, ObservationStore.Scope source, long generation) {
        assertThat(marker.legacy()).isFalse(); assertThat(marker.counterPolicy()).isEqualTo(StopReservation.CounterPolicy.CONTINUE);
        assertThat(marker.source().scope()).isEqualTo(source); assertThat(marker.source().oldJob()).isNotNull();
        assertThat(marker.writerAuthority().standalone()).isTrue();
        assertThat(marker.originalDesired().targetState()).isEqualTo(PipelineState.RUNNING);
        assertThat(marker.originalDesired().rebuiltAtStateEpoch()).isNull(); assertThat(marker.originalDesired().purgeState()).isFalse();
        assertThat(held.cut()).isEqualTo(cut); assertThat(held.pipelineId()).isEqualTo(PIPELINE);
        if (held.token() != null) { assertThat(held.token()).isEqualTo(marker.token()); }
        if (cut == RebuildHandoffJdiSession.Cut.PRE_ADMISSION) {
            assertThat(marker.phase()).isEqualTo(StopReservation.Phase.REPLACEMENT_PENDING); assertThat(marker.successor()).isNull();
            assertThat(generation).isEqualTo(source.executionGeneration()); assertThat(held.scope()).isEqualTo(source);
        } else {
            assertThat(marker.phase()).isEqualTo(StopReservation.Phase.SUCCESSOR_ADMITTED);
            assertThat(marker.successor().scope()).isEqualTo(held.scope()); assertThat(marker.successor().job()).isNull();
            assertThat(generation).isEqualTo(Math.incrementExact(source.executionGeneration()));
            assertThat(marker.writerAuthority().executionGeneration()).isEqualTo(generation);
            if (cut == RebuildHandoffJdiSession.Cut.SUBMIT_PRE_BIND) {
                assertThat(held.submittedJob()).isNotNull();
                assertThat(held.submittedJob().bootId()).isEqualTo(marker.successor().submissionBootId());
                assertThat(held.submittedJob().clusterId()).isEqualTo(marker.source().clusterId());
            } else { assertThat(held.submittedJob()).isNull(); }
        }
    }

    private static Optional<Matched> matched(MongoObservationStore latest, RebuildHandoffJdiSession observer,
            ObservationStore.Scope expected, ObservationContinuation floor, Instant after) {
        var current = latest.readStored(PIPELINE).filter(value -> value.scope().filter(expected::equals).isPresent()
                && value.observation().state() == PipelineState.RUNNING && hasKnownDelivery(value.observation().facts())
                && (after == null || value.observation().observedAt().isAfter(after)));
        if (current.isEmpty()) { return Optional.empty(); }
        ObservationStore.Stored publicValue = current.orElseThrow();
        var raw = observer.rawAt(expected, publicValue.observation().observedAt()).filter(value -> hasKnownDelivery(value.facts()));
        var privateValue = latest.readContinuation(PIPELINE);
        if (raw.isEmpty() || privateValue.isEmpty()) { return Optional.empty(); }
        var measured = raw.orElseThrow(); var saved = privateValue.orElseThrow();
        HandoffIdentity identity = new HandoffIdentity(PIPELINE, floor.token(), StopReservation.CounterPolicy.CONTINUE,
                floor.sourceScope(), expected, measured.job());
        if (!saved.receipt().knownBaseline() || !saved.receipt().matches(identity)
                || !latest.readStored(PIPELINE).filter(publicValue::equals).isPresent()) { return Optional.empty(); }
        return Optional.of(new Matched(publicValue, saved, measured));
    }

    private static void assertCumulativeExactly(Matched reading, ObservationContinuation floor) {
        Map<Series, MetricPoint> base = points(delivery(floor.baselineFacts()));
        Map<Series, MetricPoint> raw = points(reading.raw().facts());
        Map<Series, MetricPoint> published = points(delivery(reading.publicValue().observation().facts()));
        assertSameTotals(delivery(floor.baselineFacts()), delivery(reading.privateValue().continuation().baselineFacts()));
        assertThat(raw.keySet()).containsAll(base.keySet());
        assertThat(published.keySet()).isEqualTo(raw.keySet());
        for (var entry : raw.entrySet()) {
            Series series = entry.getKey(); MetricPoint old = base.get(series);
            MetricPoint nativePoint = entry.getValue(); MetricPoint cumulative = published.get(series);
            assertThat(cumulative).as("the same measured series is publicly present").isNotNull();
            if (old == null) {
                assertThat(cumulative.startTime().toEpochMilli()).isEqualTo(nativePoint.startTime().toEpochMilli());
                assertThat(cumulative.value()).isEqualTo(nativePoint.value());
                assertThat(cumulative.histogram()).isEqualTo(nativePoint.histogram());
                continue;
            }
            assertThat(cumulative.startTime()).as("a rebuilt physical execution does not reset logical counting").isEqualTo(old.startTime());
            assertThat(nativePoint.startTime()).isNotNull().isAfter(old.startTime());
            if (old.histogram() == null) {
                assertThat(cumulative.value()).as("known floor plus actual native value, added once")
                        .isEqualTo(Math.addExact(old.value(), nativePoint.value()));
            } else {
                HistogramValue before = old.histogram(), measured = nativePoint.histogram(), after = cumulative.histogram();
                assertThat(after.bounds()).isEqualTo(before.bounds()).isEqualTo(measured.bounds());
                assertThat(after.count()).isEqualTo(Math.addExact(before.count(), measured.count()));
                double sum = before.sum() + measured.sum();
                assertThat(after.sum()).isCloseTo(sum, offset(Math.max(1e-8, Math.abs(sum) * 1e-9)));
                for (int i = 0; i < before.bucketCounts().size(); i++) {
                    assertThat(after.bucketCounts().get(i)).isEqualTo(Math.addExact(before.bucketCounts().get(i), measured.bucketCounts().get(i)));
                }
            }
        }
    }

    private static boolean sameNativeTotals(RebuildHandoffJdiSession.Raw before, RebuildHandoffJdiSession.Raw after) {
        return before.scope().equals(after.scope()) && before.job().equals(after.job())
                && points(before.facts()).keySet().equals(points(after.facts()).keySet())
                && points(before.facts()).entrySet().stream().allMatch(entry -> {
                    MetricPoint next = points(after.facts()).get(entry.getKey()); MetricPoint old = entry.getValue();
                    return Objects.equals(old.startTime(), next.startTime()) && Objects.equals(old.value(), next.value())
                            && Objects.equals(old.histogram(), next.histogram());
                });
    }

    private static List<MetricFact> delivery(List<MetricFact> facts) {
        return facts.stream().filter(fact -> RebuildHandoffJdiSession.INSTRUMENTS.contains(fact.name()))
                .map(fact -> new MetricFact(fact.name(), fact.type(), fact.unit(), fact.points().stream()
                        .filter(point -> TABLE.equals(point.attributes().get(MetricAttributes.TABLE_ID))
                                && (fact.type() == MetricType.HISTOGRAM || "out".equals(point.attributes().get(MetricAttributes.DIRECTION)))).toList()))
                .filter(fact -> !fact.points().isEmpty()).sorted(Comparator.comparing(MetricFact::name)).toList();
    }
    private static boolean hasKnownDelivery(List<MetricFact> facts) {
        var selected = delivery(facts);
        return selected.size() == RebuildHandoffJdiSession.INSTRUMENTS.size() && selected.stream().allMatch(fact ->
                fact.points().stream().allMatch(point -> point.startTime() != null
                        && (point.histogram() == null ? point.value() > 0 : point.histogram().count() > 0)));
    }
    private static Map<Series, MetricPoint> points(List<MetricFact> facts) {
        Map<Series, MetricPoint> points = new HashMap<>();
        facts.forEach(fact -> fact.points().forEach(point -> points.put(new Series(fact.name(), point.attributes()), point)));
        return points;
    }
    private static void assertSameTotals(List<MetricFact> before, List<MetricFact> after) {
        Map<Series, MetricPoint> old = points(before), next = points(after);
        assertThat(next.keySet()).isEqualTo(old.keySet());
        old.forEach((series, point) -> {
            assertThat(next.get(series).startTime()).isEqualTo(point.startTime());
            assertThat(next.get(series).value()).isEqualTo(point.value());
            assertThat(next.get(series).histogram()).isEqualTo(point.histogram());
        });
    }
    private static void assertFloorAtLeast(List<MetricFact> before, List<MetricFact> after) {
        Map<Series, MetricPoint> old = points(before), next = points(after);
        assertThat(next.keySet()).containsAll(old.keySet());
        old.forEach((series, point) -> {
            MetricPoint kept = next.get(series); assertThat(kept.startTime()).isEqualTo(point.startTime());
            if (point.histogram() == null) { assertThat(kept.value()).isGreaterThanOrEqualTo(point.value()); }
            else {
                HistogramValue oldHistogram = point.histogram(), keptHistogram = kept.histogram();
                assertThat(keptHistogram.bounds()).isEqualTo(oldHistogram.bounds());
                assertThat(keptHistogram.count()).isGreaterThanOrEqualTo(oldHistogram.count());
                double tolerance = Math.max(1e-8, Math.abs(oldHistogram.sum()) * 1e-9);
                assertThat(keptHistogram.sum()).isGreaterThanOrEqualTo(oldHistogram.sum() - tolerance);
                for (int i = 0; i < oldHistogram.bucketCounts().size(); i++) {
                    assertThat(keptHistogram.bucketCounts().get(i)).isGreaterThanOrEqualTo(oldHistogram.bucketCounts().get(i));
                }
            }
        });
    }

    private static long generation(MongoDatabase database) {
        Document current = database.getCollection(MongoStorePort.WORKLOAD_CLAIMS)
                .find(new Document("resourceType", "PIPELINE_ACTUATION").append("resourceId", PIPELINE)).first();
        assertThat(current).as("the unique durable generation document exists").isNotNull();
        return ((Number) current.get("executionGeneration")).longValue();
    }
    private static boolean snapshotConfirmed(MongoDatabase database) {
        return database.getCollection(MongoStorePort.SRS_CONSUMER_OFFSETS).find().limit(16).into(new ArrayList<>()).stream()
                .anyMatch(document -> document.get("snapshotCompletedTables") instanceof List<?> tables && tables.contains(TABLE));
    }
    private static Map<String, Object> sourceCheckpoint(MongoDatabase database) {
        return Map.of("meta", database.getCollection(MongoStorePort.SRS_META).find().limit(16).into(new ArrayList<>()).stream()
                        .map(Document::toJson).toList(),
                "consumers", database.getCollection(MongoStorePort.SRS_CONSUMER_OFFSETS).find().limit(16).into(new ArrayList<>()).stream()
                        .map(Document::toJson).toList());
    }
    private static boolean row(MongoDatabase database, long id, String payload) {
        Document row = database.getCollection(TABLE).find(new Document("id", id)).first();
        return row != null && payload.equals(row.getString("payload"));
    }
    private static String assertFullTargetContent(MongoDatabase database) throws Exception {
        BitSet actual = new BitSet(INSERTED + 1); int count = 0;
        try (var rows = database.getCollection(TABLE).find().batchSize(1_024).iterator()) {
            while (rows.hasNext()) {
                Document row = rows.next();
                assertThat(++count).as("the real target cursor is bounded by the complete source set").isLessThanOrEqualTo(ROWS);
                assertThat(row.get("id")).isInstanceOf(Number.class);
                long id = ((Number) row.get("id")).longValue();
                assertThat(id).isBetween(1L, (long) INSERTED).isNotEqualTo(DELETED);
                assertThat(actual.get((int) id)).as("one physical row per source primary key").isFalse();
                actual.set((int) id);
                assertThat(row.getString("payload")).isEqualTo(id == UPDATED ? "updated-during-crash"
                        : id == INSERTED ? "inserted-during-crash" : PAYLOAD);
            }
        }
        BitSet expected = new BitSet(INSERTED + 1); expected.set(1, INSERTED + 1); expected.clear(DELETED);
        assertThat(count).isEqualTo(ROWS); assertThat(actual).isEqualTo(expected);
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(actual.toByteArray()));
    }

    private static Map<String, Object> markerEvidence(StopReservation marker) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("token", marker.token()); result.put("phase", marker.phase().name()); result.put("policy", marker.counterPolicy().name());
        result.put("sourceEpoch", marker.sourceEpoch()); result.put("reservedEpoch", marker.reservedEpoch());
        result.put("sourceScope", RebuildHandoffJdiSession.scopeEvidence(marker.source().scope()));
        result.put("oldJob", RebuildHandoffJdiSession.jobEvidence(marker.source().oldJob()));
        result.put("writerGeneration", marker.writerAuthority().executionGeneration());
        if (marker.successor() != null) {
            result.put("admittedScope", RebuildHandoffJdiSession.scopeEvidence(marker.successor().scope()));
            result.put("submissionBootId", marker.successor().submissionBootId());
        }
        return result;
    }
    private static List<Map<String, Object>> factsEvidence(List<MetricFact> facts) {
        return delivery(facts).stream().map(fact -> {
            Map<String, Object> value = new LinkedHashMap<>(); value.put("name", fact.name()); value.put("type", fact.type().name());
            value.put("unit", fact.unit()); value.put("points", fact.points().stream().map(point -> {
                Map<String, Object> sample = new LinkedHashMap<>(); sample.put("attributes", point.attributes());
                sample.put("startTime", point.startTime().toString()); sample.put("observedAt", point.observedAt().toString());
                if (point.histogram() == null) { sample.put("value", point.value()); }
                else { sample.put("histogram", Map.of("count", point.histogram().count(), "sum", point.histogram().sum(),
                        "bounds", point.histogram().bounds(), "bucketCounts", point.histogram().bucketCounts())); }
                return sample;
            }).toList()); return value;
        }).toList();
    }

    private static void seed(Map<String, Object> source) throws Exception {
        try (Connection connection = SharedMySql.connect(source); Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE bulk_orders (id BIGINT PRIMARY KEY, payload VARCHAR(64))");
            statement.execute("INSERT INTO bulk_orders VALUES (1, REPEAT('x', 64))");
            for (int count = 1; count < ROWS; count *= 2) {
                statement.execute("INSERT INTO bulk_orders (id, payload) SELECT id + " + count + ", payload FROM bulk_orders");
            }
        }
    }
    private static void mutateDuringCrash(Map<String, Object> source) throws Exception {
        try (Connection connection = SharedMySql.connect(source); Statement statement = connection.createStatement()) {
            assertThat(statement.executeUpdate("UPDATE bulk_orders SET payload='updated-during-crash' WHERE id=" + UPDATED)).isEqualTo(1);
            assertThat(statement.executeUpdate("DELETE FROM bulk_orders WHERE id=" + DELETED)).isEqualTo(1);
            assertThat(statement.executeUpdate("INSERT INTO bulk_orders VALUES (" + INSERTED + ", 'inserted-during-crash')")).isEqualTo(1);
        }
    }
    private static Map<String, String> resources(Map<String, Object> source, String targetUri) {
        return Map.of("source.tap.yml", """
                version: tapstate/v1
                kind: source
                id: bulk_source
                connector: mysql
                config: { host: %s, port: %s, database: %s, username: %s, password: %s }
                mode: cdc
                tables: [ bulk_orders ]
                """.formatted(source.get("host"), source.get("port"), source.get("database"), source.get("username"), source.get("password")),
                "target.tap.yml", """
                version: tapstate/v1
                kind: source
                id: bulk_target
                connector: mongodb
                config: { uri: "%s" }
                """.formatted(targetUri), "pipeline.tap.yml", """
                version: tapstate/v1
                kind: pipeline
                id: handoff_bulk
                source: bulk_source
                settings: { read_mode: snapshot_and_cdc }
                serve:
                  from: bulk_orders
                  sync:
                    - source: bulk_target
                """);
    }
    private static String required(String name) {
        String value = System.getProperty(PREFIX + name);
        if (value == null || value.isBlank()) { throw new AssertionError("real rebuild crash witness requires -D" + PREFIX + name); }
        return value;
    }
}
