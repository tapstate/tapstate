package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.MongoArtifactStore;
import io.tapstate.adapters.mongostore.MongoDesiredStore;
import io.tapstate.adapters.mongostore.MongoAuthStores;
import io.tapstate.adapters.mongostore.MongoClusterIdentityStore;
import io.tapstate.adapters.mongostore.MongoObservationStore;
import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.adapters.mongostore.MongoWorkloadClaimStore;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.model.SourceResource;
import io.tapstate.runtime.srs.CaptureId;
import io.tapstate.runtime.srs.MiningChainId;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.SrsConsumerId;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static io.tapstate.e2e.NativeTelemetryPositiveCalibrationIT.flag;
import static io.tapstate.e2e.NativeTelemetryPositiveCalibrationIT.matchingProduced;
import static io.tapstate.e2e.NativeTelemetryPositiveCalibrationIT.positiveProduced;
import static io.tapstate.e2e.NativeTelemetryPositiveCalibrationIT.positiveScrape;

/** Real shared-source takeover while both owned members and the surviving Job remain alive. */
@RequiresDocker
class NativeJoinedTakeoverLogIT {
    private static final String PREFIX = "tapstate.e2e.native-joined-takeover.";
    private static final String P = "native_joined_holder", Q = "native_joined_survivor";
    private static final String SOURCE = "native_joined_source", TABLE = "native_joined_orders";
    private static final Duration WAIT = Duration.ofMinutes(2);
    private static final int MAX_RECORDS = 512, MAX_BYTES = 2 * 1024 * 1024;

    @Test
    void aRealJoinedReaderTakesOverAndLogsUnderItsUnchangedSubmittedOwner() throws Exception {
        Assumptions.assumeTrue(List.of("jar", "sha256", "output").stream()
                .anyMatch(name -> System.getProperty(PREFIX + name) != null), "named immutable inputs are required");
        Path jar = Path.of(required("jar")).toAbsolutePath().normalize();
        String sha = required("sha256");
        assertThat(PipelineBenchmarkLiveRunIT.sha256(jar)).isEqualTo(sha);
        BenchmarkCaptureCalibrationLiveRunIT.requireConnectors();
        Path harness = PipelineBenchmarkLiveRunIT.harnessRoot();
        Path output = Path.of(required("output")).toAbsolutePath().normalize();
        PipelineBenchmarkLiveRunIT.requireSafeOutput(output, harness);
        BenchmarkLiveReport report = new BenchmarkLiveReport(output);
        Map<String, Object> inputs = hashes(harness);
        Map<String, Map<String, Object>> connectors = new LinkedHashMap<>();
        for (String id : List.of("mysql", "mongodb", "postgres")) {
            connectors.put(id, PipelineBenchmarkLiveRunIT.artifact(ConnectorJars.pathFor(id)));
        }
        String namespace = "joined_native_" + UUID.randomUUID().toString().replace("-", "");
        String storeUri = SharedMongo.replicaSetUrl(namespace + "_control");
        String targetP = SharedMongo.replicaSetUrl(namespace + "_target_p");
        String targetQ = SharedMongo.replicaSetUrl(namespace + "_target_q");
        String operator = namespace + "_operator";
        Map<String, Object> settings = SharedMySql.settings(namespace + "_source");
        Map<String, NativeTelemetryIdentityJdiSession> sessions = new LinkedHashMap<>();
        Map<String, Integer> ports = new LinkedHashMap<>();
        Map<String, List<Map<String, Object>>> records = new LinkedHashMap<>();
        Map<String, NativeTelemetryIdentityJdiSession.Boundary> last = new LinkedHashMap<>();
        TwoMemberCluster cluster = null;
        Throwable primary = null;
        try (var sqlConnection = SharedMySql.connect(settings); var mongo = MongoClients.create(storeUri);
                var tp = MongoClients.create(targetP); var tq = MongoClients.create(targetQ)) {
            try (var sql = sqlConnection.createStatement()) {
                sql.execute("CREATE TABLE " + TABLE + " (id BIGINT PRIMARY KEY, amount BIGINT NOT NULL, payload VARCHAR(32) NOT NULL)");
                sql.execute("INSERT INTO " + TABLE + " VALUES (1,100,'native-1'),(2,200,'native-2'),(3,300,'native-3')");
            }
            MongoDatabase database = mongo.getDatabase(new ConnectionString(storeUri).getDatabase());
            MongoDatabase pTarget = tp.getDatabase(new ConnectionString(targetP).getDatabase());
            MongoDatabase qTarget = tq.getDatabase(new ConnectionString(targetQ).getDatabase());
            Map<String, String> resources = resources(settings, targetP, targetQ);
            report.begin(Map.of("purpose", "REAL_JOINED_TAKEOVER_SOURCE_LOG", "application", PipelineBenchmarkLiveRunIT.artifact(jar),
                    "harness", inputs, "connectors", connectors, "clusterMembers", 2,
                    "fixtureResourceSha256", digest(JsonWriter.write(resources).getBytes(StandardCharsets.UTF_8)),
                    "performanceAcceptanceEligible", false), PipelineBenchmarkLiveRunIT.environment(), List.of());
            String clusterId;
            try (var setup = RealProcessServer.start(storeUri, operator, jar)) {
                ControlPlane control = new ControlPlane(setup.baseUrl());
                control.bootstrapAndLogin("benchmark", "benchmark-password");
                control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
                control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
                control.apply(Map.of(SOURCE + ".tap.yml", resources.get(SOURCE + ".tap.yml")));
                var connection = new LinkedHashMap<String, Object>(settings); connection.put("highPerformance", false);
                control.discoverSchema(SOURCE, "mysql", connection);
                control.apply(resources);
                clusterId = new MongoClusterIdentityStore(database.getCollection(MongoAuthStores.CLUSTER_IDENTITY))
                        .find().orElseThrow().clusterId();
                assertThat(database.getCollection(MongoStorePort.WORKLOAD_CLAIMS).countDocuments(
                        new Document("clusterId", clusterId).append("resourceType", "PIPELINE_ACTUATION"))).isZero();
            }
            for (String node : List.of(TwoMemberCluster.NODE_A, TwoMemberCluster.NODE_B)) {
                ports.put(node, RealProcessServer.reservePort()); records.put(node, new ArrayList<>());
            }
            cluster = TwoMemberCluster.start(storeUri, operator, jar, clusterId, "benchmark", "benchmark-password",
                    List.of(), List.of(), (node, address, arguments, jvm) -> {
                        try {
                            var observer = NativeTelemetryIdentityJdiSession.startWithJoinedTakeoverObservation(jar, sha, Q,
                                    (artifact, debug) -> {
                                        List<String> options = new ArrayList<>(jvm); options.addAll(debug);
                                        return RealProcessServer.launchingWithJvmArguments(storeUri, operator, artifact, address,
                                                port -> {
                                                    List<String> application = new ArrayList<>(arguments.apply(port));
                                                    application.add("--tapstate.metrics.export.prometheus.host=127.0.0.1");
                                                    application.add("--tapstate.metrics.export.prometheus.port=" + ports.get(node));
                                                    application.add("--tapstate.metrics.history.sample-interval=PT2S");
                                                    return List.copyOf(application);
                                                }, List.copyOf(options));
                                    });
                            sessions.put(node, observer); return observer.server();
                        } catch (RuntimeException | Error failure) { throw failure; }
                        catch (Exception failure) { throw new AssertionError("the owned observed member could not start", failure); }
                    });
            var claims = new MongoWorkloadClaimStore(database.getCollection(MongoStorePort.WORKLOAD_CLAIMS));
            var artifacts = new MongoArtifactStore(mongo, database.getCollection(MongoStorePort.ARTIFACTS));
            SourceResource source = (SourceResource) artifacts.get(SOURCE).orElseThrow();
            CaptureConfig config = new CaptureConfig(source.connector(), source.config(), List.of(TABLE));
            String srsKey = source.srs() == null ? null : source.srs().key();
            String captureId = CaptureId.of(config, srsKey).value(), chainId = MiningChainId.resolve(config, srsKey).value();
            WorkloadClaimKey pKey = new WorkloadClaimKey(clusterId, WorkloadClaimType.PIPELINE_ACTUATION, P);
            WorkloadClaimKey qKey = new WorkloadClaimKey(clusterId, WorkloadClaimType.PIPELINE_ACTUATION, Q);
            WorkloadClaimKey cKey = new WorkloadClaimKey(clusterId, WorkloadClaimType.CAPTURE, captureId);
            var latest = new MongoObservationStore(mongo, database.getCollection(MongoStorePort.PIPELINE_OBSERVATION),
                    database.getCollection(MongoStorePort.PIPELINE_OBSERVATION_CHUNKS));
            long deadline = System.nanoTime() + WAIT.toNanos();
            cluster.first().lifecycle(P, LifecycleVerb.START);
            WorkloadClaim holder = Await.answered("the actual first physical capture holder", remaining(deadline),
                    () -> live(claims, cKey));
            Await.until("holder snapshot is physically written", remaining(deadline),
                    () -> pTarget.getCollection(TABLE).countDocuments() == 3,
                    () -> "holder target rows=" + pTarget.getCollection(TABLE).countDocuments());
            WorkloadClaim p = live(claims, pKey).orElseThrow();
            assertThat(p.owner()).isEqualTo(holder.owner());
            ControlPlane other = holder.owner().nodeId().equals(TwoMemberCluster.NODE_A) ? cluster.second() : cluster.first();
            var desired = new MongoDesiredStore(database.getCollection(MongoStorePort.PIPELINE_DESIRED));
            if (desired.read(Q).isPresent() || claims.read(qKey).isPresent()) {
                report.addFork(Map.of("action", "UNSELECTED_Q_ALREADY_DESIRED_OR_CLAIMED",
                        "desiredPresent", desired.read(Q).isPresent(), "claimPresent", claims.read(qKey).isPresent()));
                throw new AssertionError("UNSELECTED: Q must have no actual desired or claim before its first START");
            }
            String otherNode = holder.owner().nodeId().equals(TwoMemberCluster.NODE_A) ? TwoMemberCluster.NODE_B : TwoMemberCluster.NODE_A;
            WorkloadClaim otherSession = Await.answered("the other member's actual node-session lease", remaining(deadline),
                    () -> live(claims, new WorkloadClaimKey(clusterId, WorkloadClaimType.NODE_SESSION, otherNode)));
            var readiness = sessions.get(otherNode).observePublisherSweep(P,
                    otherSession.owner().nodeId(), otherSession.owner().bootId(), deadline);
            try (readiness) {
                Map<String, Object> entry = readiness.awaitEntry();
                assertThat(readiness.evidence().get("releaseReason")).isEqualTo("OBSERVATION_RELEASE");
                assertThat(((Number) readiness.evidence().get("releasedAtNanos")).longValue() - deadline).isLessThanOrEqualTo(0L);
                assertBoot(claims, clusterId, otherSession);
                assertThat(desired.read(Q)).isEmpty(); assertThat(claims.read(qKey)).isEmpty();
                report.addFork(Map.of("action", "actual-other-member-P-only-eligible-convergence-entry", "entry", entry,
                        "release", readiness.evidence(), "nodeSession", tuple(otherSession)));
            }
            Duration renewBound = Duration.ofSeconds(10);
            WorkloadClaim freshP = live(claims, pKey).orElseThrow();
            assertThat(tuple(freshP)).isEqualTo(tuple(p));
            if (!freshP.leaseUntil().isAfter(Instant.now().plus(renewBound))) {
                throw new AssertionError("UNSELECTED: holder pipeline lease lacks the existing renewal-bound margin");
            }
            long parkDeadline = System.nanoTime() + Math.min(remaining(deadline).toNanos(), renewBound.toNanos());
            WorkloadClaim leasedQ;
            var parked = sessions.get(holder.owner().nodeId()).armPublisherSweepPark(P,
                    holder.owner().nodeId(), holder.owner().bootId(), parkDeadline);
            try (parked) {
                Map<String, Object> entry = parked.awaitEntry();
                if (!parked.held() || desired.read(Q).isPresent() || claims.read(qKey).isPresent()) {
                    throw new AssertionError("UNSELECTED: Q absence or the exact held sweep changed before START");
                }
                assertThat(tuple(live(claims, pKey).orElseThrow())).isEqualTo(tuple(freshP));
                assertThat(tuple(live(claims, cKey).orElseThrow())).isEqualTo(tuple(holder));
                report.addFork(Map.of("action", "actual-P-only-sweep-before-Q-START", "entry", entry,
                        "QDesired", "ABSENT", "QClaim", "ABSENT", "pipelineClaim", tuple(freshP),
                        "captureClaim", tuple(holder), "parkBudget", renewBound.toString()));
                other.lifecycle(Q, LifecycleVerb.START);
                leasedQ = Await.answered("the actual surviving leased pipeline owner", remaining(parkDeadline),
                        () -> {
                            sessions.get(otherNode).check(); sessions.get(holder.owner().nodeId()).check();
                            if (!parked.held()) { throw new AssertionError("UNSELECTED: held sweep was released before Q issuance"); }
                            var issued = live(claims, qKey).filter(value -> value.owner().nodeId().equals(otherNode));
                            if (issued.isPresent()) { assertBoot(claims, clusterId, issued.orElseThrow()); }
                            if (!parked.held()) { throw new AssertionError("UNSELECTED: Q lease read exceeded the held sweep bound"); }
                            return issued;
                        });
            } catch (Exception | Error failed) {
                try {
                    report.addFork(Map.of("action", "UNSELECTED_Q_ISSUANCE", "park", parked.evidence(),
                            "actualQClaimAtFailure", live(claims, qKey).map(NativeJoinedTakeoverLogIT::tuple).orElse(Map.of())));
                    for (var session : sessions.entrySet()) {
                        try {
                            report.addFork(Map.of("action", "actual-observer-after-unselected-Q", "node", session.getKey(),
                                    "boundary", session.getValue().boundary("joined-issuer-failure").evidence()));
                        } catch (Exception | Error unavailable) {
                            report.addFork(Map.of("action", "unavailable-observer-after-unselected-Q", "node", session.getKey(),
                                    "failureType", unavailable.getClass().getName(),
                                    "failureMessage", String.valueOf(unavailable.getMessage()),
                                    "ownedProcessAlive", session.getValue().server().isAlive()));
                            if (unavailable != failed) { failed.addSuppressed(unavailable); }
                        }
                    }
                } catch (Exception | Error reporting) { if (reporting != failed) { failed.addSuppressed(reporting); } }
                throw new AssertionError("UNSELECTED: one-shot real Q issuance failed while the holder sweep was parked", failed);
            }
            report.addFork(Map.of("action", "actual-publisher-sweep-release", "park", parked.evidence()));
            report.addFork(Map.of("action", "actual-leased-Q-before-admission-check", "lease", tuple(leasedQ),
                    "leaseUntil", leasedQ.leaseUntil().toString()));
            assertThat(parked.evidence().get("releaseReason")).isEqualTo("CALLER_RELEASE");
            assertThat(((Number) parked.evidence().get("releasedAtNanos")).longValue()
                    - ((Number) parked.evidence().get("deadlineNanos")).longValue())
                    .as("the actual release completed within the original park deadline").isLessThanOrEqualTo(0L);
            WorkloadClaim q = Await.answered("the same surviving lease actually admits its first execution", remaining(deadline),
                    () -> live(claims, qKey).filter(value -> value.key().equals(leasedQ.key())
                            && value.owner().equals(leasedQ.owner())
                            && value.claimGeneration() == leasedQ.claimGeneration()
                            && value.topologyRevision() == leasedQ.topologyRevision()
                            && value.executionGeneration() == (leasedQ.executionGeneration() > 0
                                    ? leasedQ.executionGeneration() : 1)
                            && value.contextExecutionGeneration() == value.executionGeneration()
                            && value.executionClaimGeneration() == value.claimGeneration()
                            && value.failureClaimGeneration() == 0 && !value.failureAfterMemberLoss()
                            && value.executionNodeIds().equals(java.util.Set.of(TwoMemberCluster.NODE_A, TwoMemberCluster.NODE_B))));
            assertBoot(claims, clusterId, q);
            report.addFork(Map.of("action", "actual-same-Q-lease-admitted", "claim", tuple(q)));
            if (q.owner().nodeId().equals(holder.owner().nodeId())) {
                report.addFork(Map.of("action", "UNSELECTED_REMOTE_JOINED_FIXTURE", "holder", tuple(holder), "survivor", tuple(q)));
                throw new AssertionError("UNSELECTED: both actual owners are on one member; this does not select a remote JoinedCapture");
            }
            Await.until("the joined consumer snapshot is physically written", remaining(deadline), () -> {
                capture(sessions, records, last, report, "joined-before-stop");
                return qTarget.getCollection(TABLE).countDocuments() == 3 && snapshotDone(database, chainId, Q);
            }, () -> "survivor target rows=" + qTarget.getCollection(TABLE).countDocuments());
            assertThat(tuple(live(claims, cKey).orElseThrow())).isEqualTo(tuple(holder));
            var current = Await.answered("the surviving current scope", remaining(deadline), () -> latest.readStored(Q)
                    .filter(value -> value.scope().isPresent() && value.observation().state() == PipelineState.RUNNING
                            && value.scope().orElseThrow().executionGeneration() == q.executionGeneration()));
            ObservationStore.Scope scope = current.scope().orElseThrow();
            assertThat(artifacts.pipelineIncarnationId(Q)).contains(scope.pipelineIncarnationId());
            assertThat(claims.currentGeneration(clusterId, Q)).hasValue(scope.executionGeneration());
            Document qDocument = database.getCollection(MongoStorePort.WORKLOAD_CLAIMS).find(claimKey(qKey)).first();
            var authority = new NativeTelemetryIdentityJdiSession.AuthorityReceipt(Q, clusterId,
                    scope.pipelineIncarnationId(), scope.executionGeneration(), JsonWriter.write(qDocument.get("_id")), Instant.now().toString());
            sessions.values().forEach(session -> session.recordAuthority(authority));
            var owner = sessions.get(q.owner().nodeId());
            var submitted = Await.answered("the actual surviving admitted native Job", remaining(deadline), () -> {
                capture(sessions, records, last, report, "joined-job-before-stop");
                return owner.claimedSubmission(authority);
            });
            assertThat(withoutLease(submitted.claim())).isEqualTo(tuple(q));
            assertThat(submitted.members()).containsExactlyInAnyOrder(TwoMemberCluster.NODE_A, TwoMemberCluster.NODE_B);
            assertBoot(claims, clusterId, q); assertBoot(claims, clusterId, holder);
            try (var sql = sqlConnection.createStatement()) { sql.execute("INSERT INTO " + TABLE + " VALUES (4,400,'native-4')"); }
            Await.until("real CDC reaches both live consumers before releasing the holder", remaining(deadline), () -> {
                capture(sessions, records, last, report, "joined-both-targets");
                return pTarget.getCollection(TABLE).countDocuments() == 4 && qTarget.getCollection(TABLE).countDocuments() == 4;
            }, () -> "target rows: holder=" + pTarget.getCollection(TABLE).countDocuments()
                    + ", survivor=" + qTarget.getCollection(TABLE).countDocuments());
            requireRows(pTarget, 4); requireRows(qTarget, 4);
            var baseline = Await.answered("actual known survivor output before takeover", remaining(deadline),
                    () -> latest.readStored(Q).filter(value -> value.scope().filter(scope::equals).isPresent()
                            && recordsOut(value.observation()).filter(count -> count >= 4).isPresent()));
            var beforeRoot = database.getCollection(MongoStorePort.SRS_META).find(new Document("_id", chainId)).first();
            assertThat(beforeRoot).isNotNull(); assertThat(beforeRoot.getBoolean("sourceReadDurable")).isTrue();
            assertThat(beforeRoot.getString("sourceReadOffset")).isNotBlank();
            long oldEpoch = ((Number) beforeRoot.get("epoch")).longValue();
            Map<String, Object> intent = new Document(database.getCollection(MongoStorePort.PIPELINE_DESIRED)
                    .find(new Document("_id", Q)).first());
            Map<String, Boolean> preNegative = new LinkedHashMap<>();
            for (String node : sessions.keySet()) { preNegative.put(node, logSubset(last.get(node), authority, records.get(node))); }
            assertThat(records.values().stream().flatMap(List::stream).noneMatch(record -> sourceLog(record, authority))).isTrue();
            assertThat(records.values().stream().flatMap(List::stream)
                    .noneMatch(record -> "JOINED_TAKEOVER".equals(record.get("target")))).isTrue();
            report.addFork(Map.of("action", "actual-remote-joined-before-holder-stop", "holder", tuple(holder),
                    "survivor", tuple(q), "scope", authority.scope(), "job", submitted.job().job(),
                    "chainId", chainId, "captureId", captureId, "sourceEpoch", oldEpoch,
                    "sourceLogNegativeSubsetInitialized", preNegative));
            ControlPlane holderControl = holder.owner().nodeId().equals(TwoMemberCluster.NODE_A) ? cluster.first() : cluster.second();
            holderControl.stop(P, false);
            WorkloadClaim successor = Await.answered("the joined member actually acquires the physical capture", remaining(deadline), () -> {
                capture(sessions, records, last, report, "joined-takeover");
                return live(claims, cKey).filter(value -> value.owner().equals(q.owner())
                        && value.claimGeneration() > holder.claimGeneration());
            });
            var receipt = Await.answered("the actual caller-qualified joined reader and returned tail", remaining(deadline), () -> {
                capture(sessions, records, last, report, "joined-cold-receipt");
                return records.get(q.owner().nodeId()).stream().filter(record -> "JOINED_TAKEOVER".equals(record.get("target"))
                        && Boolean.TRUE.equals(record.get("normalReturn")) && !record.containsKey("decoderStatus")
                        && Boolean.TRUE.equals(record.get("callerStable")) && authority.scope().equals(record.get("scope"))
                        && captureId.equals(record.get("captureId")) && chainId.equals(record.get("returnedChainId"))
                        && tuple(successor).equals(withoutLease(cast(record.get("captureClaim"))))).findFirst();
            });
            assertThat(receipt.get("consumerId")).isEqualTo(SrsConsumerId.of(Q, SOURCE).value());
            assertThat(receipt.get("sourceId")).isEqualTo(SOURCE);
            assertThat(((Number) receipt.get("returnedRunObject")).longValue())
                    .isNotEqualTo(((Number) receipt.get("logicalRunObject")).longValue());
            long takeoverReturned = ((Number) receipt.get("returnOrder")).longValue();
            requireSameSurvivor(database, claims, qKey, q, latest, scope, intent, owner, authority, submitted);
            var newRoot = database.getCollection(MongoStorePort.SRS_META).find(new Document("_id", chainId)).first();
            assertThat(((Number) newRoot.get("epoch")).longValue()).isGreaterThan(oldEpoch);
            assertBoot(claims, clusterId, successor);
            report.addFork(Map.of("action", "actual-cold-joined-tail-selected", "receipt", receipt,
                    "captureClaim", tuple(successor), "sourceEpoch", newRoot.get("epoch"), "scope", authority.scope()));
            try (var sql = sqlConnection.createStatement()) { sql.execute("INSERT INTO " + TABLE + " VALUES (5,500,'native-5')"); }
            HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(20)).build();
            URI scrape = URI.create("http://127.0.0.1:" + ports.get(q.owner().nodeId()) + "/metrics");
            ControlPlane survivorControl = q.owner().nodeId().equals(TwoMemberCluster.NODE_A) ? cluster.first() : cluster.second();
            var positive = Await.answered("takeover source log, actual row and current SDK scrape", remaining(deadline), () -> {
                try {
                    capture(sessions, records, last, report, "joined-positive");
                    String body = http.send(java.net.http.HttpRequest.newBuilder(scrape).timeout(remaining(deadline)).GET().build(),
                            java.net.http.HttpResponse.BodyHandlers.ofString()).body();
                    var publicValue = latest.readStored(Q).filter(value -> value.scope().filter(scope::equals).isPresent()
                            && value.observation().state() == PipelineState.RUNNING
                            && value.observation().observedAt().isAfter(baseline.observation().observedAt())
                            && recordsOut(value.observation()).filter(count -> count > recordsOut(baseline.observation()).orElseThrow()).isPresent());
                    var observed = records.get(q.owner().nodeId());
                    boolean ready = qTarget.getCollection(TABLE).countDocuments() == 5 && publicValue.isPresent()
                            && observed.stream().anyMatch(record -> sourceLog(record, authority))
                            && flag(observed, "OFFER", "accepted", authority) && flag(observed, "VISIBLE", "included", authority)
                            && observed.stream().anyMatch(record -> producedOutAtLeast(record, 5))
                            && positiveScrape(body, Q) && scrapeOutAtLeast(body, 5)
                            && matchingProduced(observed.stream().filter(record -> !"PRODUCE".equals(record.get("target"))
                                    || producedOutAtLeast(record, 5)).toList(), body, authority);
                    if (!ready) { return Optional.<String>empty(); }
                    return Optional.of(body);
                } catch (Exception failure) { throw new AssertionError("the bounded positive read failed", failure); }
            });
            requireRows(qTarget, 5);
            requireSameSurvivor(database, claims, qKey, q, latest, scope, intent, owner, authority, submitted);
            assertThat(tuple(live(claims, cKey).orElseThrow())).isEqualTo(tuple(successor));
            List<Map<String, Object>> sourceLogs = records.get(q.owner().nodeId()).stream()
                    .filter(record -> sourceLog(record, authority)).toList();
            report.addFork(NativeTelemetryPositiveCalibrationIT.assertScopedLogRead(sourceLogs, http, survivorControl,
                    owner.server().baseUrl(), authority));
            String oldMember = holder.owner().nodeId();
            boolean postNegative = logSubset(last.get(oldMember), authority, records.get(oldMember))
                    && records.get(oldMember).stream().noneMatch(record -> sourceLog(record, authority));
            report.addFork(Map.of("action", "paired-source-log-positive-and-narrow-negative", "positiveNode", q.owner().nodeId(),
                    "negativeNode", oldMember, "negativeSubsetQualified", postNegative, "positiveBodySha256", digest(positive.getBytes(StandardCharsets.UTF_8))));
            assertThat(preNegative.values()).containsOnly(true);
            assertThat(postNegative).as("only the bound, drained LOG family can qualify the nonemitting source subset").isTrue();
            capture(sessions, records, last, report, "joined-before-owned-Q-stop");
            long beforeQStopOrder = last.get(q.owner().nodeId()).events();
            report.addFork(Map.of("action", "actual-running-Q-through-row5-before-owned-cleanup", "claim", tuple(q),
                    "scope", authority.scope(), "submittedJobHistory", submitted.job().job(),
                    "preQStopEventOrder", beforeQStopOrder));
            // Ordinary stop reads the current native Job before cancellation; stable polling reads live metrics instead.
            survivorControl.stop(Q, false);
            var currentJob = Await.answered("a fresh actual Q Job lookup during its owned normal-stop cleanup", remaining(deadline),
                    () -> {
                        capture(sessions, records, last, report, "joined-current-job-at-Q-stop");
                        return records.get(q.owner().nodeId()).stream().filter(record -> currentJobAfter(record,
                                authority, submitted.job().job(), Math.max(takeoverReturned, beforeQStopOrder))).findFirst();
                    });
            report.addFork(Map.of("action", "actual-current-native-job-after-pre-Q-stop-cutoff", "receipt", currentJob,
                    "preQStopEventOrder", beforeQStopOrder));
            Await.until("Q's owned stop is actually published for the same execution", remaining(deadline), () -> {
                capture(sessions, records, last, report, "joined-Q-stopped");
                return latest.readStored(Q).filter(value -> value.scope().filter(scope::equals).isPresent()
                        && value.observation().state() == PipelineState.STOPPED).isPresent();
            }, () -> "the same Q execution has no final STOPPED publication yet");
            assertThat(claims.currentGeneration(clusterId, Q)).hasValue(scope.executionGeneration());
            assertThat(artifacts.pipelineIncarnationId(Q)).contains(scope.pipelineIncarnationId());
            assertThat(last.get(q.owner().nodeId()).decodedAndAuthorityBound()).isTrue();
            for (var entry : sessions.entrySet()) {
                var terminal = entry.getValue().shutdownAndFinish(); report.addFork(terminal.evidence());
                records.get(entry.getKey()).addAll(terminal.records()); requireBudget(records.get(entry.getKey()));
                assertThat(terminal.invocationDrainComplete()).isTrue();
                assertThat(terminal.ownedVmDeath()).isTrue(); assertThat(terminal.ownedVmDisconnected()).isTrue();
                assertThat(retainedLogDecoded(terminal, records.get(entry.getKey())))
                        .as("all retained LOG receipts remain decoded after final drain").isTrue();
                if (entry.getKey().equals(oldMember)) {
                    assertThat(records.get(entry.getKey()).stream().noneMatch(record -> sourceLog(record, authority)))
                            .as("late old-holder Q source logs remain part of the narrow negative").isTrue();
                }
            }
            report.addFork(Map.of("action", "terminal-drained-old-holder-log-subset-negative", "node", oldMember,
                    "qualified", true, "retainedRecords", records.get(oldMember).size()));
            assertThat(PipelineBenchmarkLiveRunIT.sha256(jar)).isEqualTo(sha);
            assertThat(hashes(harness)).isEqualTo(inputs);
            for (var entry : connectors.entrySet()) {
                assertThat(PipelineBenchmarkLiveRunIT.artifact(ConnectorJars.pathFor(entry.getKey()))).isEqualTo(entry.getValue());
            }
            report.completeDiagnostic(Map.of("correctness", "REAL_NORMAL_STOP_JOINED_TAKEOVER_SOURCE_LOG",
                    "nativeJobLookupBoundary", "AFTER_ROW5_AND_PRE_Q_STOP_EVENT_CUTOFF",
                    "unverified", List.of("OLD_HOLDER_P_NATIVE_JOB_AND_LOG", "UNAVAILABLE_NONEMITTING_OBSERVER_FAMILIES",
                            "OS_KILL_MEMBER_LOSS", "FULL_T3_T4_MATRIX", "FORMAL_PERFORMANCE")));
        } catch (Exception | Error failure) {
            primary = failure;
            try { report.fail(failure); } catch (RuntimeException reporting) { if (reporting != failure) { failure.addSuppressed(reporting); } }
            throw failure;
        } finally {
            Throwable cleanup = null;
            for (String node : List.of(TwoMemberCluster.NODE_B, TwoMemberCluster.NODE_A)) {
                if (sessions.get(node) != null) { try { sessions.get(node).close(); } catch (Exception | Error failure) { cleanup = add(cleanup, failure); } }
            }
            if (cluster != null) { try { cluster.close(); } catch (Exception | Error failure) { cleanup = add(cleanup, failure); } }
            ports.values().forEach(RealProcessServer::releasePort);
            if (cleanup != null) {
                if (primary != null) { if (primary != cleanup) { primary.addSuppressed(cleanup); } }
                else { if (cleanup instanceof Exception exception) { throw exception; } else { throw (Error) cleanup; } }
            }
        }
    }

    private static void requireSameSurvivor(MongoDatabase database, MongoWorkloadClaimStore claims,
            WorkloadClaimKey key, WorkloadClaim expected, MongoObservationStore latest, ObservationStore.Scope scope,
            Map<String, Object> intent, NativeTelemetryIdentityJdiSession session,
            NativeTelemetryIdentityJdiSession.AuthorityReceipt authority,
            NativeTelemetryIdentityJdiSession.ClaimedSubmission submitted) {
        assertThat(tuple(live(claims, key).orElseThrow())).isEqualTo(tuple(expected));
        assertThat(claims.currentGeneration(key.clusterId(), Q)).hasValue(scope.executionGeneration());
        Document artifact = database.getCollection(MongoStorePort.ARTIFACTS).find(new Document("_id", Q)).first();
        assertThat(artifact).isNotNull();
        assertThat(artifact.getString("pipelineIncarnationId")).isEqualTo(scope.pipelineIncarnationId());
        var current = latest.readStored(Q).orElseThrow();
        assertThat(current.scope()).contains(scope); assertThat(current.observation().state()).isEqualTo(PipelineState.RUNNING);
        assertThat(new Document(database.getCollection(MongoStorePort.PIPELINE_DESIRED).find(new Document("_id", Q)).first()))
                .isEqualTo(intent);
        var actual = session.claimedSubmission(authority).orElseThrow();
        assertThat(actual.job().job()).isEqualTo(submitted.job().job());
        assertThat(actual.job().executionObjectId()).isEqualTo(submitted.job().executionObjectId());
        assertThat(actual.admissionObjectId()).isEqualTo(submitted.admissionObjectId());
    }

    private static boolean snapshotDone(MongoDatabase database, String chain, String pipeline) {
        String consumer = SrsConsumerId.of(pipeline, SOURCE).value();
        Document row = database.getCollection(MongoStorePort.SRS_CONSUMER_OFFSETS)
                .find(new Document("_id", new Document("chain", chain).append("pipeline", consumer))).first();
        if (row == null) { return false; }
        assertThat(row.getString("ownerPipelineId")).isEqualTo(pipeline);
        assertThat(row.getString("sourceNodeId")).isEqualTo(SOURCE);
        Object completed = row.get("snapshotCompletedTables");
        return completed instanceof List<?> tables && tables.contains(TABLE);
    }

    private static Optional<WorkloadClaim> live(MongoWorkloadClaimStore claims, WorkloadClaimKey key) {
        return claims.read(key).filter(value -> value.leased()).map(value -> value.claim());
    }

    private static Document claimKey(WorkloadClaimKey key) {
        return new Document("_id", new Document("clusterId", key.clusterId())
                .append("resourceType", key.type().name()).append("resourceId", key.resourceId()));
    }

    private static void assertBoot(MongoWorkloadClaimStore claims, String clusterId, WorkloadClaim claim) {
        var boot = live(claims, new WorkloadClaimKey(clusterId, WorkloadClaimType.NODE_SESSION, claim.owner().nodeId())).orElseThrow();
        assertThat(boot.owner()).isEqualTo(claim.owner());
    }

    private static Map<String, Object> tuple(WorkloadClaim claim) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("key", Map.of("clusterId", claim.key().clusterId(), "type", claim.key().type().name(), "resourceId", claim.key().resourceId()));
        result.put("owner", Map.of("nodeId", claim.owner().nodeId(), "bootId", claim.owner().bootId()));
        result.put("claimGeneration", claim.claimGeneration()); result.put("executionGeneration", claim.executionGeneration());
        result.put("topologyRevision", claim.topologyRevision()); result.put("contextExecutionGeneration", claim.contextExecutionGeneration());
        result.put("executionClaimGeneration", claim.executionClaimGeneration());
        result.put("executionNodeIds", claim.executionNodeIds().stream().sorted().toList());
        result.put("failureClaimGeneration", claim.failureClaimGeneration()); result.put("failureAfterMemberLoss", claim.failureAfterMemberLoss());
        return Map.copyOf(result);
    }

    private static Map<String, Object> withoutLease(Map<String, Object> claim) {
        Map<String, Object> copy = new LinkedHashMap<>(claim); copy.remove("leaseUntil"); return Map.copyOf(copy);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> cast(Object value) {
        assertThat(value).isInstanceOf(Map.class); return (Map<String, Object>) value;
    }

    private static boolean producedOutAtLeast(Map<String, Object> record, long minimum) {
        if (!positiveProduced(record) || !(record.get("metrics") instanceof List<?> metrics)) { return false; }
        long total = 0; boolean present = false;
        for (Object value : metrics) {
            if (!(value instanceof Map<?, ?> metric) || !"tapstate.pipeline.records".equals(metric.get("name"))
                    || !(metric.get("points") instanceof List<?> points)) { continue; }
            for (Object pointValue : points) {
                if (pointValue instanceof Map<?, ?> point && point.get("attributes") instanceof Map<?, ?> attributes
                        && Q.equals(attributes.get("tapstate.pipeline.id")) && "out".equals(attributes.get("direction"))
                        && point.get("value") instanceof Number count) {
                    present = true; total = Math.addExact(total, count.longValue());
                }
            }
        }
        return present && total >= minimum;
    }

    private static boolean scrapeOutAtLeast(String body, long minimum) {
        java.math.BigDecimal total = java.math.BigDecimal.ZERO; boolean present = false;
        for (String line : body.lines().toList()) {
            if (!line.startsWith("tapstate_pipeline_records_total{") || !line.contains("tapstate_pipeline_id=\"" + Q + "\"")
                    || !line.contains("direction=\"out\"")) { continue; }
            int end = line.indexOf('}'); if (end < 0) { return false; }
            total = total.add(new java.math.BigDecimal(line.substring(end + 1).trim().split("\\s+")[0])); present = true;
        }
        return present && total.compareTo(java.math.BigDecimal.valueOf(minimum)) >= 0;
    }

    private static Optional<Long> recordsOut(Observation observation) {
        var points = observation.facts().stream().filter(fact -> fact.name().equals("tapstate.pipeline.records"))
                .flatMap(fact -> fact.points().stream()).filter(point -> "out".equals(point.attributes().get("direction"))).toList();
        if (points.isEmpty()) { return Optional.empty(); }
        long count = 0;
        for (var point : points) { assertThat(point.value()).isNotNull(); assertThat(point.startTime()).isNotNull(); count = Math.addExact(count, point.value()); }
        return Optional.of(count);
    }

    private static boolean sourceLog(Map<String, Object> record, NativeTelemetryIdentityJdiSession.AuthorityReceipt authority) {
        return "LOG".equals(record.get("target")) && authority.scope().equals(record.get("scope"))
                && Boolean.TRUE.equals(record.get("normalReturn")) && !record.containsKey("decoderStatus") && streamCaller(record);
    }

    private static boolean streamCaller(Map<String, Object> record) {
        if (!(record.get("callers") instanceof List<?> encoded) || encoded.size() != 2
                || !(encoded.get(0) instanceof List<?> names) || !(encoded.get(1) instanceof List<?> rows) || rows.size() % 9 != 0) { return false; }
        for (int at = 0; at < rows.size(); at += 9) {
            if ("io.tapstate.adapters.pdk.PdkCapturePort".equals(name(names, rows.get(at)))
                    && "streamLoop".equals(name(names, rows.get(at + 1)))
                    && "EXACT_ARTIFACT_METHOD".equals(name(names, rows.get(at + 7)))) { return true; }
        }
        return false;
    }

    private static Object name(List<?> names, Object index) {
        return index instanceof Integer value && value >= 0 && value < names.size() ? names.get(value) : null;
    }

    private static boolean logSubset(NativeTelemetryIdentityJdiSession.Boundary boundary,
            NativeTelemetryIdentityJdiSession.AuthorityReceipt authority, List<Map<String, Object>> records) {
        if (boundary == null || boundary.ownedVmDeath() || boundary.ownedVmDisconnected() || !boundary.queueDrained()
                || !boundary.bindings().containsKey(NativeTelemetryIdentityJdiSession.Target.LOG)
                || boundary.authorityReceipts().stream().noneMatch(value -> value.scope().equals(authority.scope()))) { return false; }
        var counts = boundary.counts().get(NativeTelemetryIdentityJdiSession.Target.LOG);
        return counts != null && logFamilyReady(boundary) && counts.inFlight() == 0
                && counts.entries() == counts.normalReturns() + counts.exceptionalExits()
                && boundary.unverified().stream().noneMatch(value -> value.contains(":LOG:") || value.equals("LIVE_BINDING_UNAVAILABLE:LOG"))
                && records.stream().noneMatch(record -> "LOG".equals(record.get("target"))
                        && (record.containsKey("decoderStatus") || streamCaller(record) && record.get("scope") == null));
    }

    private static boolean retainedLogDecoded(NativeTelemetryIdentityJdiSession.Boundary boundary,
            List<Map<String, Object>> records) {
        var counts = boundary.counts().get(NativeTelemetryIdentityJdiSession.Target.LOG);
        return boundary.bindings().containsKey(NativeTelemetryIdentityJdiSession.Target.LOG) && counts != null
                && logFamilyReady(boundary)
                && counts.inFlight() == 0 && counts.entries() == counts.normalReturns() + counts.exceptionalExits()
                && boundary.unverified().stream().noneMatch(value -> value.contains(":LOG:") || value.equals("LIVE_BINDING_UNAVAILABLE:LOG"))
                && records.stream().noneMatch(record -> "LOG".equals(record.get("target"))
                        && (record.containsKey("decoderStatus") || streamCaller(record) && record.get("scope") == null));
    }

    private static boolean logFamilyReady(NativeTelemetryIdentityJdiSession.Boundary boundary) {
        var counts = boundary.counts().get(NativeTelemetryIdentityJdiSession.Target.LOG);
        if (counts != null && counts.entries() > 0) { return true; }
        var calibration = boundary.logFamilyCalibration();
        var binding = boundary.bindings().get(NativeTelemetryIdentityJdiSession.Target.LOG);
        return binding != null && Boolean.TRUE.equals(calibration.get("calibrationComplete"))
                && Boolean.TRUE.equals(calibration.get("normalReturn")) && !calibration.containsKey("decoderStatus")
                && P.equals(calibration.get("foreignPipelineId")) && Q.equals(calibration.get("queryPipelineId"))
                && binding.codeSha256().equals(calibration.get("bindingCodeSha256"))
                && Long.valueOf(binding.loaderId()).equals(calibration.get("bindingLoaderId"))
                && calibration.get("entryOrder") instanceof Number entered
                && calibration.get("returnOrder") instanceof Number returned && returned.longValue() > entered.longValue()
                && calibration.get("calibrationScope") instanceof Map<?, ?> scope
                && scope.get("incarnation") instanceof String incarnation && !incarnation.isBlank()
                && scope.get("generation") instanceof Number generation && generation.longValue() > 0;
    }

    private static boolean currentJobAfter(Map<String, Object> record,
            NativeTelemetryIdentityJdiSession.AuthorityReceipt authority, Map<String, Object> expectedJob, long after) {
        return "JOB".equals(record.get("target")) && Boolean.TRUE.equals(record.get("normalReturn"))
                && !record.containsKey("decoderStatus") && authority.scope().equals(record.get("scope"))
                && expectedJob.equals(record.get("job")) && record.get("entryOrder") instanceof Number entered
                && entered.longValue() > after && record.get("returnOrder") instanceof Number returned
                && returned.longValue() > entered.longValue();
    }

    private static void capture(Map<String, NativeTelemetryIdentityJdiSession> sessions,
            Map<String, List<Map<String, Object>>> records, Map<String, NativeTelemetryIdentityJdiSession.Boundary> last,
            BenchmarkLiveReport report, String phase) {
        try {
            for (var entry : sessions.entrySet()) {
                var boundary = entry.getValue().boundary(phase); last.put(entry.getKey(), boundary);
                report.addFork(Map.of("observedNodeId", entry.getKey(), "boundary", boundary.evidence()));
                records.get(entry.getKey()).addAll(boundary.records()); requireBudget(records.get(entry.getKey()));
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt(); throw new AssertionError("the bounded native evidence read was interrupted", interrupted);
        } catch (Exception failure) { throw new AssertionError("the bounded native evidence read failed", failure); }
    }

    private static void requireBudget(List<Map<String, Object>> records) {
        assertThat(records.size()).isLessThanOrEqualTo(MAX_RECORDS);
        assertThat(JsonWriter.write(records).getBytes(StandardCharsets.UTF_8).length).isLessThanOrEqualTo(MAX_BYTES);
    }

    private static void requireRows(MongoDatabase target, int expected) {
        var rows = target.getCollection(TABLE).find().sort(new Document("id", 1)).limit(6).into(new ArrayList<>());
        assertThat(rows).hasSize(expected);
        for (int index = 0; index < expected; index++) {
            long id = index + 1L;
            assertThat(((Number) rows.get(index).get("id")).longValue()).isEqualTo(id);
            assertThat(((Number) rows.get(index).get("amount")).longValue()).isEqualTo(id * 100);
            assertThat(rows.get(index).getString("payload")).isEqualTo("native-" + id);
        }
    }

    private static Map<String, String> resources(Map<String, Object> settings, String targetP, String targetQ) {
        Map<String, String> files = new LinkedHashMap<>();
        files.put(SOURCE + ".tap.yml", """
                version: tapstate/v1
                kind: source
                id: native_joined_source
                connector: mysql
                config: { host: %s, port: %s, database: %s, username: %s, password: %s, highPerformance: false }
                mode: cdc
                tables: [ native_joined_orders ]
                """.formatted(settings.get("host"), settings.get("port"), settings.get("database"), settings.get("username"), settings.get("password")));
        for (String pipeline : List.of(P, Q)) {
            String target = pipeline + "_target", uri = pipeline.equals(P) ? targetP : targetQ;
            files.put(target + ".tap.yml", """
                    version: tapstate/v1
                    kind: source
                    id: %s
                    connector: mongodb
                    config: { uri: "%s" }
                    """.formatted(target, uri));
            files.put(pipeline + ".tap.yml", """
                    version: tapstate/v1
                    kind: pipeline
                    id: %s
                    source: native_joined_source
                    settings: { read_mode: snapshot_and_cdc }
                    serve:
                      from: native_joined_orders
                      sync: [ { source: %s } ]
                    """.formatted(pipeline, target));
        }
        return Map.copyOf(files);
    }

    private static Map<String, Object> hashes(Path root) throws Exception {
        Map<String, Object> result = new LinkedHashMap<>();
        for (Class<?> type : List.of(NativeJoinedTakeoverLogIT.class, NativeTelemetryIdentityJdiSession.class,
                NativeTelemetryMirror.class, NativeTelemetryPositiveCalibrationIT.class, RealProcessServer.class, TwoMemberCluster.class)) {
            result.put(type.getSimpleName() + "Source", PipelineBenchmarkLiveRunIT.artifact(root.resolve("e2e/src/test/java/io/tapstate/e2e/" + type.getSimpleName() + ".java")));
            try (InputStream stream = type.getResourceAsStream(type.getSimpleName() + ".class")) {
                assertThat(stream).isNotNull(); byte[] bytes = stream.readNBytes(MAX_BYTES + 1);
                assertThat(bytes.length).isLessThanOrEqualTo(MAX_BYTES); result.put(type.getSimpleName() + "ExecutingClassSha256", digest(bytes));
            }
        }
        String parked = "NativeTelemetryIdentityJdiSession$PublisherSweepPark";
        try (InputStream stream = NativeJoinedTakeoverLogIT.class.getResourceAsStream(parked + ".class")) {
            assertThat(stream).isNotNull();
            byte[] bytes = stream.readNBytes(MAX_BYTES + 1);
            assertThat(bytes.length).isLessThanOrEqualTo(MAX_BYTES);
            result.put(parked + "ExecutingClassSha256", digest(bytes));
        }
        return Map.copyOf(result);
    }

    private static Duration remaining(long deadline) {
        long nanos = deadline - System.nanoTime();
        if (nanos <= 0) { throw new AssertionError("the original bounded native deadline expired"); }
        return Duration.ofNanos(nanos);
    }
    private static String required(String name) {
        String value = System.getProperty(PREFIX + name);
        if (value == null || value.isBlank()) { throw new AssertionError("missing immutable input: " + PREFIX + name); }
        return value;
    }
    private static String digest(byte[] bytes) throws Exception { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)); }
    private static Throwable add(Throwable first, Throwable next) { if (first == null) { return next; } if (first != next) { first.addSuppressed(next); } return first; }
}
