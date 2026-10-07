package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoSocketException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.MongoWorkloadClaimStore;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimReading;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/** Qualifies a real transport fault against an issued capture renewal, without a lifecycle claim. */
@RequiresDocker
@EnabledIfSystemProperty(named = "tapstate.e2e.capture-renew-wire-drop", matches = "true")
class CaptureRenewWireProxyIT {
    private static final Duration WAIT = Duration.ofMinutes(2);
    private static final Duration TTL = Duration.ofMinutes(2);
    private static final Duration IO_BOUND = Duration.ofSeconds(5);

    @Test
    void anIssuedCaptureRenewalAloneSelectsARealSocketDropAndRecoversWhenDisarmed(
            @TempDir Path temporary) throws Exception {
        long deadline = System.nanoTime() + WAIT.toNanos();
        String run = UUID.randomUUID().toString();
        String database = "capture_wire_" + run.replace("-", "");
        String rawUri = SharedMongo.replicaSetUrl(database);
        ConnectionString raw = new ConnectionString(rawUri);
        assertThat(raw.getHosts()).hasSize(1);
        assertThat(raw.getCredential()).as("this owned fixture has no authentication relay").isNull();
        assertThat(Boolean.TRUE.equals(raw.getSslEnabled())).as("TLS is not inspectable").isFalse();
        assertThat(raw.getCompressorList() == null || raw.getCompressorList().isEmpty())
                .as("compressed traffic cannot qualify this selector").isTrue();
        ServerAddress address = new ServerAddress(raw.getHosts().get(0));
        String reportPath = System.getProperty("tapstate.e2e.capture-renew-wire-drop.report");
        BenchmarkLiveReport report = new BenchmarkLiveReport(reportPath == null
                ? temporary.resolve("capture-renew-wire-drop.json").toAbsolutePath()
                : Path.of(reportPath).toAbsolutePath());
        report.begin(Map.of("database", database, "deadline", WAIT.toString(),
                        "claimInputs", "CONTROLLED_STORE_FIXTURE_REQUESTS", "fixtureLeaseTtl", TTL.toString()),
                Map.of("transport", "OWNED_SINGLE_NIO_RELAY", "directConnection", true),
                List.of(Map.of("subject", "ACTUAL_MONGO_WORKLOAD_CLAIM_RENEWAL")));
        CaptureRenewWireProxy proxy = new CaptureRenewWireProxy(
                new InetSocketAddress(address.getHost(), address.getPort()), deadline);
        try (proxy;
             MongoClient admin = MongoClients.create(settings(raw, deadline));
             MongoClient routed = MongoClients.create(settings(new ConnectionString(
                     "mongodb://127.0.0.1:" + proxy.port() + "/" + database
                             + "?directConnection=true&appName=capture-wire-" + run), deadline))) {
            proxy.check();
            Supplier<MongoWorkloadClaimStore> real = () -> store(routed, database, deadline);
            Supplier<MongoWorkloadClaimStore> direct = () -> store(admin, database, deadline);
            // These are acquisition requests. Every fence below is the store's actual returned value.
            WorkloadOwner requestedOwner = new WorkloadOwner("wire-owner-" + run, "wire-boot-" + run);
            String cluster = "wire-cluster-" + run;
            WorkloadClaim capture = acquire(real,
                    new WorkloadClaimKey(cluster, WorkloadClaimType.CAPTURE, "capture-" + run), requestedOwner);
            WorkloadClaim pipeline = acquire(real,
                    new WorkloadClaimKey(cluster, WorkloadClaimType.PIPELINE_ACTUATION, "pipeline-" + run), requestedOwner);
            WorkloadClaim node = acquire(real,
                    new WorkloadClaimKey(cluster, WorkloadClaimType.NODE_SESSION, requestedOwner.nodeId()), requestedOwner);
            proxy.expectRenewal(database, capture, TTL);
            capture = real.get().renew(capture, TTL).orElseThrow();
            pipeline = real.get().renew(pipeline, TTL).orElseThrow();
            node = real.get().renew(node, TTL).orElseThrow();
            for (WorkloadClaim claim : List.of(capture, pipeline, node)) {
                assertExactLease(direct, claim);
                assertExactLease(real, claim);
            }
            Await.until("actual forwarded capture renewal and byte-identical drained relay",
                    remaining(deadline), () -> proxy.matchedForwarded() > 0 && proxy.drainedAndBytePreserving(),
                    () -> proxy.snapshot().toString());
            Map<String, Object> normal = new LinkedHashMap<>(proxy.snapshot());
            normal.remove("records");
            normal.put("recordsReference", "finalWireSnapshot.records");
            report.addFork(Map.of("stage", "NO_FAULT", "bytePreserving", true, "wire", normal,
                    "actualLeases", List.of(claimEvidence(capture), claimEvidence(pipeline), claimEvidence(node))));

            WorkloadClaim beforeFault = capture;
            proxy.arm();
            Throwable actualFailure;
            try {
                actualFailure = catchThrowable(() -> real.get().renew(beforeFault, TTL));
                proxy.check();
                assertThat(actualFailure).as("the real synchronous client must see a transport failure")
                        .isInstanceOf(TapstateException.class);
                assertThat(((TapstateException) actualFailure).code()).isEqualTo(IoError.STORE_UNAVAILABLE);
                assertThat(causes(actualFailure).stream().anyMatch(MongoSocketException.class::isInstance))
                        .as("an actual Mongo socket error, not a manufactured Mongo command reply").isTrue();
                assertThat(proxy.dropped()).isPositive();
                assertThat(proxy.snapshot().get("selectorUnavailable")).isEqualTo(List.of());
                // A raw admin route proves the dropped update did not change the issued lease.
                assertExactLease(direct, beforeFault);
            } finally {
                proxy.disarm();
            }
            remaining(deadline);
            capture = real.get().renew(beforeFault, TTL).orElseThrow();
            assertExactLease(direct, capture);
            assertExactLease(real, capture);
            assertThat(capture.leaseUntil()).isAfter(beforeFault.leaseUntil());
            assertThat(capture.key()).isEqualTo(beforeFault.key());
            assertThat(capture.owner()).isEqualTo(beforeFault.owner());
            assertThat(capture.claimGeneration()).isEqualTo(beforeFault.claimGeneration());
            assertThat(capture.executionGeneration()).isEqualTo(beforeFault.executionGeneration());
            assertThat(capture.topologyRevision()).isEqualTo(beforeFault.topologyRevision());
            // Successful reads are factual here; they do not qualify unaffected shared-client SDAM.
            assertExactLease(direct, pipeline);
            assertExactLease(direct, node);
            report.addFork(Map.of("stage", "FAULT_OFF_ACTUAL_RENEW", "actualLease", claimEvidence(capture),
                    "transportFailure", failureEvidence(actualFailure)));
            assertThat(real.get().release(capture)).isTrue();
            assertThat(real.get().release(pipeline)).isTrue();
            assertThat(real.get().release(node)).isTrue();
            for (WorkloadClaim claim : List.of(capture, pipeline, node)) {
                assertThat(direct.get().read(claim.key()).orElseThrow().leased()).isFalse();
            }
        } catch (Throwable first) {
            try {
                report.addFork(Map.of("stage", "UNVERIFIED", "finalWireSnapshot", boundedSnapshot(proxy)));
                report.fail(first);
            } catch (Throwable evidenceFailure) { first.addSuppressed(evidenceFailure); }
            throw first;
        }
        try {
            remaining(deadline);
            Map<String, Object> finalWire = boundedSnapshot(proxy);
            assertThat(finalWire.get("ownerFinished")).isEqualTo(true);
            assertThat(finalWire.get("ownerFailure")).isEqualTo("ABSENT");
            assertThat(finalWire.get("selectorUnavailable")).isEqualTo(List.of());
            report.completeDiagnostic(Map.of("protocolQualification", "EXACT_CAPTURE_RENEW_TRANSPORT_DROP",
                    "finalWireSnapshot", finalWire, "selectedAttemptCount", proxy.snapshot().get("selectedDropped"),
                    "localJvmAuthority", "UNVERIFIED", "unpublishedCapture", "UNVERIFIED",
                    "sharedClientPoolCollateral", "UNVERIFIED", "taskAcceptance", false, "performance", false));
        } catch (Throwable first) {
            try { report.fail(first); } catch (Throwable evidenceFailure) { first.addSuppressed(evidenceFailure); }
            throw first;
        }
    }

    private static WorkloadClaim acquire(Supplier<MongoWorkloadClaimStore> store, WorkloadClaimKey key,
            WorkloadOwner owner) {
        var returned = store.get().acquire(key, owner, 1, TTL);
        assertThat(returned.acquired()).as("actual acquisition for %s", key).isTrue();
        assertThat(returned.claim().key()).isEqualTo(key);
        assertThat(returned.claim().owner()).isEqualTo(owner);
        return returned.claim();
    }

    private static void assertExactLease(Supplier<MongoWorkloadClaimStore> store, WorkloadClaim expected) {
        WorkloadClaimReading actual = store.get().read(expected.key()).orElseThrow();
        assertThat(actual.leased()).isTrue();
        assertThat(actual.claim()).isEqualTo(expected);
    }

    private static MongoWorkloadClaimStore store(MongoClient client, String database, long deadline) {
        remaining(deadline);
        // A collection timeout changes the driver's retry policy, so retain its native defaults.
        return new MongoWorkloadClaimStore(client.getDatabase(database).getCollection("workload_claims"));
    }

    private static MongoClientSettings settings(ConnectionString uri, long deadline) {
        int millis = Math.toIntExact(Math.max(1, Math.min(IO_BOUND.toMillis(), remaining(deadline).toMillis())));
        return MongoClientSettings.builder().applyConnectionString(uri)
                .applyToClusterSettings(builder -> builder.serverSelectionTimeout(millis, TimeUnit.MILLISECONDS))
                .applyToSocketSettings(builder -> builder.connectTimeout(millis, TimeUnit.MILLISECONDS)
                        .readTimeout(millis, TimeUnit.MILLISECONDS))
                .build();
    }

    private static Duration remaining(long deadline) {
        long left = deadline - System.nanoTime();
        if (left <= 0) { throw new AssertionError("original two-minute qualification deadline exhausted"); }
        return Duration.ofNanos(left);
    }

    private static List<Throwable> causes(Throwable failure) {
        List<Throwable> causes = new ArrayList<>();
        while (failure != null && !causes.contains(failure)) {
            if (causes.size() >= 16) { throw new AssertionError("failure cause chain exceeds evidence cap"); }
            causes.add(failure);
            failure = failure.getCause();
        }
        return causes;
    }

    private static List<Map<String, Object>> failureEvidence(Throwable failure) {
        List<Throwable> chain = causes(failure);
        for (Throwable cause : chain) {
            if (cause.getStackTrace().length > 256) { throw new AssertionError("failure stack exceeds evidence cap"); }
        }
        var evidence = chain.stream().map(value -> Map.<String, Object>of(
                "type", value.getClass().getName(), "message", String.valueOf(value.getMessage()),
                "stack", java.util.Arrays.stream(value.getStackTrace()).map(StackTraceElement::toString).toList())).toList();
        if (JsonWriter.write(evidence).getBytes(StandardCharsets.UTF_8).length > CaptureRenewWireProxy.MAX_RECORD_BYTES) {
            throw new AssertionError("full transport failure exceeds record byte cap");
        }
        return evidence;
    }

    private static Map<String, Object> boundedSnapshot(CaptureRenewWireProxy proxy) {
        Map<String, Object> snapshot = proxy.snapshot();
        assertThat(JsonWriter.write(snapshot).getBytes(StandardCharsets.UTF_8).length)
                .as("complete wire evidence retains every record within its original phase cap")
                .isLessThanOrEqualTo(CaptureRenewWireProxy.MAX_PHASE_BYTES);
        return snapshot;
    }

    private static Map<String, Object> claimEvidence(WorkloadClaim claim) {
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("key", Map.of("clusterId", claim.key().clusterId(), "resourceType", claim.key().type().name(),
                "resourceId", claim.key().resourceId()));
        value.put("owner", Map.of("nodeId", claim.owner().nodeId(), "bootId", claim.owner().bootId()));
        value.put("claimGeneration", claim.claimGeneration());
        value.put("executionGeneration", claim.executionGeneration());
        value.put("topologyRevision", claim.topologyRevision());
        value.put("leaseUntil", claim.leaseUntil().toString());
        value.put("contextExecutionGeneration", claim.contextExecutionGeneration());
        value.put("executionClaimGeneration", claim.executionClaimGeneration());
        value.put("executionNodeIds", claim.executionNodeIds().stream().sorted().toList());
        value.put("failureClaimGeneration", claim.failureClaimGeneration());
        value.put("failureAfterMemberLoss", claim.failureAfterMemberLoss());
        return value;
    }
}
