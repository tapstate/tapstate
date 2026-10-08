package io.tapstate.e2e;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.MongoObservationStore;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.testsupport.RequiresDocker;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.bson.Document;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import static org.assertj.core.api.Assertions.assertThat;

/** Actual refusal, cold process recovery and the subsequent successful admission use the same durable store. */
@RequiresDocker
class PreExecutionFailureLifecycleIT {
    private static final String PIPE = "refused_orders", SOURCE = "src_orders";
    private static final String CODE = "actuation.source-schema-not-discovered";
    private static final Duration WAIT = Duration.ofMinutes(1);

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void refusalRefreshAndColdRecoveryNeverAdmitUntilDiscoveryAndANewStart(Tiers tier, @TempDir Path directory) throws Exception {
        Path source = Files.createDirectory(directory.resolve("source"));
        Path target = Files.createDirectory(directory.resolve("target"));
        Files.writeString(source.resolve("orders.csv"), "id,amount\n1,10\n2,20\n3,30\n");
        Path connector = E2eConnectorJar.buildInto(directory);
        String database = "e2e_refusal_lifecycle_" + UUID.randomUUID().toString().replace("-", "");
        String uri = SharedMongo.replicaSetUrl(database);
        ObservationStore.Stored refused;
        try (var client = MongoClients.create(uri)) {
            MongoDatabase db = client.getDatabase(database);
            MongoObservationStore latest = new MongoObservationStore(client, SystemCollections.PIPELINE_OBSERVATION.on(db),
                    SystemCollections.PIPELINE_OBSERVATION_CHUNKS.on(db));
            try (ServerHandle first = tier.launch(uri)) {
                ControlPlane control = new ControlPlane(first.baseUrl());
                control.bootstrapAndLogin("e2e", "e2e-password");
                control.registerConnector(E2eConnectorJar.CONNECTOR_ID, Files.readAllBytes(connector));
                control.apply(Map.of("source.tap.yml", endpoint(SOURCE, source, true),
                        "target.tap.yml", endpoint("tgt_orders", target, false), "pipeline.tap.yml", pipeline()));
                control.lifecycle(PIPE, LifecycleVerb.START);
                Await.until("the original undiscovered start to publish its coded failure", WAIT,
                        () -> control.failureCode(PIPE).filter(CODE::equals).isPresent(),
                        () -> "state=" + control.state(PIPE) + ", code=" + control.failureCode(PIPE));
                refused = latest.readStored(PIPE).orElseThrow();
                assertRefusal(refused);
                assertReadSurfaces(first.baseUrl(), control.credential());
                awaitFreshTicks(latest, null);
                assertNoAdmissionHistoryOrEvents(db);
                assertThat(target.resolve("orders.csv")).doesNotExist();
                refused = latest.readStored(PIPE).orElseThrow();
            }

            var originalOwner = refused.refusal().orElseThrow();
            Instant beforeRestart = refused.observation().observedAt();
            try (ServerHandle cold = tier.launch(uri)) {
                ControlPlane control = new ControlPlane(cold.baseUrl());
                control.login("e2e", "e2e-password");
                Await.until("a cold runtime to refresh the original durable diagnostic", WAIT,
                        () -> latest.readStored(PIPE).filter(value -> value.refusal().filter(originalOwner::equals).isPresent()
                                && value.observation().observedAt().isAfter(beforeRestart)).isPresent(),
                        () -> latest.readStored(PIPE).toString());
                assertRefusal(latest.readStored(PIPE).orElseThrow());
                assertReadSurfaces(cold.baseUrl(), control.credential());
                assertNoAdmissionHistoryOrEvents(db);

                control.discoverSchema(SOURCE, E2eConnectorJar.CONNECTOR_ID, Map.of("uri", source.toString()));
                assertNoAdmissionHistoryOrEvents(db);
                // The existing public lifecycle recovers FAILED with desired RUNNING by stop then start.
                control.stop(PIPE, false);
                Await.until("the original failed intent to complete its qualified stop", WAIT,
                        () -> {
                            Document actual = SystemCollections.PIPELINE_STATE.on(db).find(new Document("_id", PIPE)).first();
                            return actual != null && StateJson.parse(actual.getString("stateJson")) == PipelineState.STOPPED;
                        }, () -> String.valueOf(SystemCollections.PIPELINE_STATE.on(db).find(new Document("_id", PIPE)).first()));
                assertThat(control.state(PIPE)).as("the invalidated failure cannot answer for the new STOPPED intent").isEmpty();
                // STOP's existing absent-authority write fence may install an unadmitted coordination row.
                Document unadmitted = SystemCollections.WORKLOAD_CLAIMS.on(db).find(claimFilter()).first();
                assertThat(unadmitted).isNotNull();
                assertThat(unadmitted).doesNotContainKeys("executionGeneration", "ownerNodeId", "ownerBootId", "leaseUntil");
                assertNoHistoryOrEvents(db);
                control.lifecycle(PIPE, LifecycleVerb.START);
                Await.until("the discovered source to start one actual execution and deliver its rows", WAIT,
                        () -> latest.readStored(PIPE).filter(value -> value.scope().filter(scope -> scope.executionGeneration() == 1).isPresent()
                                && value.observation().state() == PipelineState.RUNNING).isPresent()
                                && Files.isRegularFile(target.resolve("orders.csv"))
                                && rows(target.resolve("orders.csv")) == 3,
                        () -> "current=" + latest.readStored(PIPE) + ", code=" + control.failureCode(PIPE));
                ObservationStore.Stored running = latest.readStored(PIPE).orElseThrow();
                assertThat(running.refusal()).isEmpty();
                assertThat(running.scope().orElseThrow().pipelineIncarnationId()).isEqualTo(originalOwner.pipelineIncarnationId());
                assertThat(running.observation().failure()).isNull();
                assertThat(control.state(PIPE)).contains(PipelineState.RUNNING);
                assertThat(control.failureCode(PIPE)).isEmpty();
                awaitFreshTicks(latest, running.scope().orElseThrow());
                Document claim = SystemCollections.WORKLOAD_CLAIMS.on(db).find(claimFilter()).first();
                assertThat(claim).isNotNull();
                assertThat(((Number) claim.get("executionGeneration")).longValue()).isEqualTo(1L);
                assertThat(SystemCollections.WORKLOAD_CLAIMS.on(db).countDocuments(claimFilter())).isEqualTo(1);
                control.stop(PIPE, false);
            }
        }
    }

    private static void assertRefusal(ObservationStore.Stored value) {
        assertThat(value.refusal()).isPresent();
        assertThat(value.refusal().orElseThrow().generationFrontier()).isEmpty();
        assertThat(value.scope()).isEmpty();
        assertThat(value.observation().state()).isEqualTo(PipelineState.FAILED);
        assertThat(value.observation().failure().code()).isEqualTo(CODE);
        assertThat(value.observation().failure().params()).isEqualTo(Map.of("source", SOURCE));
        assertThat(value.observation().metrics()).isEmpty();
        assertThat(value.observation().snapshot()).isEmpty();
        assertThat(value.observation().positions()).isEmpty();
        assertThat(value.observation().facts()).isEmpty();
    }

    private static void assertNoAdmissionHistoryOrEvents(MongoDatabase db) {
        assertThat(SystemCollections.WORKLOAD_CLAIMS.on(db).countDocuments(claimFilter())).isZero();
        assertNoHistoryOrEvents(db);
    }

    private static void assertNoHistoryOrEvents(MongoDatabase db) {
        for (var collection : List.of(SystemCollections.PIPELINE_RATE_HISTORY, SystemCollections.PIPELINE_EVENTS)) {
            assertThat(collection.on(db).countDocuments(new Document("pipelineId", PIPE))).isZero();
        }
    }

    private static Document claimFilter() { return new Document("resourceType", "PIPELINE_ACTUATION").append("resourceId", PIPE); }

    private static void awaitFreshTicks(MongoObservationStore latest, ObservationStore.Scope expected) {
        Set<Instant> timestamps = new HashSet<>();
        Await.until("three actual telemetry ticks to preserve the same private owner", WAIT, () -> {
            var value = latest.readStored(PIPE).orElseThrow();
            if (expected == null) { assertRefusal(value); }
            else { assertThat(value.scope()).contains(expected); assertThat(value.refusal()).isEmpty(); }
            timestamps.add(value.observation().observedAt());
            return timestamps.size() >= 3;
        }, () -> "timestamps=" + timestamps);
    }

    private static void assertReadSurfaces(URI base, String token) throws Exception {
        var status = get(base, token, "status");
        var explanation = get(base, token, "explain");
        assertThat(status.get("state")).isEqualTo("FAILED");
        assertThat(status.get("failure")).isInstanceOf(Map.class);
        Map<?, ?> failure = (Map<?, ?>) status.get("failure");
        assertThat(failure.get("code")).isEqualTo(CODE);
        assertThat(failure.get("params")).isEqualTo(Map.of("source", SOURCE));
        assertThat(explanation.get("state")).isEqualTo("FAILED");
        assertThat(explanation.get("kind")).isEqualTo("CODED_FAILURE");
        assertThat(explanation.get("evidence")).isInstanceOf(List.class);
        List<?> evidence = (List<?>) explanation.get("evidence");
        assertThat(evidence).singleElement().satisfies(item -> {
            Map<?, ?> entry = (Map<?, ?>) item;
            assertThat(entry.get("field")).isEqualTo("failure");
            assertThat(((Map<?, ?>) entry.get("value")).get("code")).isEqualTo(CODE);
        });
    }

    private static Map<?, ?> get(URI base, String token, String suffix) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(base.resolve("/api/pipelines/" + PIPE + "/" + suffix))
                .timeout(Duration.ofSeconds(20)).header("Authorization", "Bearer " + token).GET().build();
        var response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).as("GET %s: %s", suffix, response.body()).isEqualTo(200);
        assertThat(response.headers().firstValue("Cache-Control").orElseThrow()).contains("no-store");
        return (Map<?, ?>) JsonReader.parse(response.body());
    }

    private static int rows(Path file) {
        try { return Files.readAllLines(file).size() - 1; }
        catch (java.io.IOException failure) { throw new java.io.UncheckedIOException(failure); }
    }

    private static String endpoint(String id, Path directory, boolean source) {
        return "version: tapstate/v1\nkind: source\nid: " + id + "\nconnector: e2e_file\nconfig: { uri: \"" + directory + "\" }\n"
                + (source ? "mode: cdc\ntables: [ orders ]\n" : "");
    }

    private static String pipeline() {
        return """
                version: tapstate/v1
                kind: pipeline
                id: refused_orders
                source: src_orders
                settings: { read_mode: snapshot_and_cdc }
                serve:
                  from: orders
                  sync:
                    - source: tgt_orders
                """;
    }
}
