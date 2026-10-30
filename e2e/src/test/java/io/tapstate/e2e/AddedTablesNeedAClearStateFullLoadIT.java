package io.tapstate.e2e;

import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.adapters.mongostore.MongoConnectionSettings;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineIntentSnapshot;
import io.tapstate.spi.store.SrsConsumerId;
import io.tapstate.testsupport.RequiresDocker;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.bson.Document;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;

/** Real lifecycle refusal and explicit full-load recovery; a retained run never silently gains a table. */
@RequiresDocker
class AddedTablesNeedAClearStateFullLoadIT {
    private static final String SOURCE = "selection_source", TARGET = "selection_target", PIPELINE = "selection_pipeline";
    private static final String ORDERS = "orders", CUSTOMERS = "customers";
    private static final Duration BOUND = Duration.ofSeconds(90);

    @ParameterizedTest
    @CsvSource({"true, START", "true, RESUME", "false, START", "false, RESUME"})
    void retainedStartsRejectAddedTablesAndOnlyAnExplicitFullLoadAppliesThem(
            boolean shared, LifecycleVerb verb, @TempDir Path temporary) throws Exception {
        Path source = Files.createDirectory(temporary.resolve("source"));
        Path target = Files.createDirectory(temporary.resolve("target"));
        FileEndpoints.replaceTable(source.resolve(ORDERS + ".csv"), "id,priority\n1,initial\n");
        FileEndpoints.replaceTable(source.resolve(CUSTOMERS + ".csv"), "id,priority\n1,new-customer\n");
        byte[] connector = CdcRecoveryFixture.connectorJar(E2eConnectorJar.buildInto(
                Files.createDirectory(temporary.resolve("connectors"))));
        try (PreparedNativeStore storage = PreparedNativeStore.create();
                NativeSrsDocuments documents = new NativeSrsDocuments(storage);
                ServerHandle server = Tiers.IN_PROCESS.launch(storage)) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);
            control.registerConnector(E2eConnectorJar.CONNECTOR_ID, connector);
            control.apply(resources(source, target, shared, ORDERS));
            control.discoverSchema(SOURCE, E2eConnectorJar.CONNECTOR_ID, Map.of("uri", source.toString()));
            control.lifecycle(PIPELINE, LifecycleVerb.START);
            awaitState(control, PipelineState.RUNNING);
            awaitPriority(control, target, ORDERS, "1", "initial");
            FileEndpoints.replaceTable(source.resolve(ORDERS + ".csv"), "id,priority\n1,confirmed\n");
            CdcRecoveryFixture.change(source, 1, ORDERS, "1", "initial", "confirmed");
            awaitPriority(control, target, ORDERS, "1", "confirmed");
            Await.until("the original change to have an actual source-token acknowledgement", BOUND,
                    () -> hasCdcAck(state(documents, shared, 1)), () -> String.valueOf(state(documents, shared, 1)));
            PipelineState retained = verb == LifecycleVerb.RESUME ? PipelineState.PAUSED : PipelineState.STOPPED;
            if (verb == LifecycleVerb.RESUME) control.lifecycle(PIPELINE, LifecycleVerb.PAUSE);
            else control.stop(PIPELINE, false);
            awaitState(control, retained);
            Saved before = state(documents, shared, 1);
            assertThat(before).isNotNull();
            var batches = shared ? List.<Document>of() : documents.directBatches(before.key());

            FileEndpoints.replaceTable(source.resolve(ORDERS + ".csv"), "id,priority\n1,while-stopped\n");
            CdcRecoveryFixture.change(source, 2, ORDERS, "1", "confirmed", "while-stopped");
            control.apply(resources(source, target, shared, ORDERS + ", " + CUSTOMERS));
            control.discoverSchema(SOURCE, E2eConnectorJar.CONNECTOR_ID, Map.of("uri", source.toString()));
            PipelineIntentSnapshot originalIntent = intent(storage);
            var response = HttpClient.newHttpClient().send(HttpRequest.newBuilder(server.baseUrl().resolve(
                            "/api/pipelines/" + PIPELINE + ":" + verb.id()))
                    .timeout(Duration.ofSeconds(10)).header("Authorization", "Bearer " + control.credential())
                    .POST(HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode()).as("the retained selection refusal: %s", response.body()).isEqualTo(shared ? 409 : 500);
            var refusal = (Map<?, ?>) JsonReader.parse(response.body());
            assertThat(refusal.get("code")).isEqualTo(shared ? IoError.PIPELINE_TABLE_SELECTION_REBUILD_REQUIRED.code()
                    : IoError.PIPELINE_CAPTURE_IDENTITY_UNPROVEN.code());
            assertThat(String.valueOf(refusal.get("message"))).contains("Clear all state", "full load");
            assertThat(intent(storage)).as("a refused start or resume never commits a new desired intent or revision")
                    .isEqualTo(originalIntent);
            assertThat(control.state(PIPELINE)).contains(retained);
            Saved refused = state(documents, shared, 1);
            assertThat(refused.key()).isEqualTo(before.key());
            assertThat(refused.tables()).containsExactly(ORDERS);
            for (String field : List.of("snapshotEpoch", "cdcStartPosition", "snapshotCompletedTables", "sinkAcked"))
                assertThat(refused.cursor().get(field)).as("retained %s", field).isEqualTo(before.cursor().get(field));
            if (verb == LifecycleVerb.START) {
                assertThat(refused.raw()).isEqualTo(before.raw());
                if (!shared) assertThat(documents.directBatches(before.key())).isEqualTo(batches);
            }
            assertThat(priority(target, ORDERS, "1")).isEqualTo("confirmed");
            assertThat(priority(target, CUSTOMERS, "1")).isNull();

            control.stop(PIPELINE, true);
            awaitState(control, PipelineState.STOPPED);
            control.lifecycle(PIPELINE, LifecycleVerb.START);
            awaitState(control, PipelineState.RUNNING);
            awaitPriority(control, target, ORDERS, "1", "while-stopped");
            awaitPriority(control, target, CUSTOMERS, "1", "new-customer");
            Await.until("the full load to confirm both selected tables in exactly one successor generation", BOUND,
                    () -> {
                        Saved current = state(documents, shared, 2);
                        return current != null && current.cursor().getList("snapshotCompletedTables", String.class, List.of())
                                .containsAll(List.of(ORDERS, CUSTOMERS));
                    }, () -> String.valueOf(state(documents, shared, 2)));
            Saved rebuilt = state(documents, shared, 2);
            assertThat(rebuilt.tables()).containsExactlyInAnyOrder(ORDERS, CUSTOMERS);
            if (shared) assertThat(rebuilt.key()).isEqualTo(before.key());
            else {
                assertThat(rebuilt.key().getString("channelId")).isEqualTo(before.key().getString("channelId"));
                assertThat(rebuilt.key().getString("captureId")).isNotEqualTo(before.key().getString("captureId"));
            }
            try (var configuration = MongoConfigurationDomain.openExisting(new MongoConnectionSettings(
                    storage.configurationUri(), null, Duration.ofSeconds(5)))) {
                assertThat(configuration.pipelineStates().read(PIPELINE).orElseThrow().stateGeneration()).isEqualTo(2);
            }
            Await.until("the new run to publish the two full table reads", BOUND,
                    () -> Long.valueOf(1).equals(control.snapshotRowsRead(PIPELINE).get(CUSTOMERS)),
                    () -> String.valueOf(control.snapshotRowsRead(PIPELINE)));
            assertThat(control.snapshotRowsRead(PIPELINE)).containsEntry(ORDERS, 1L).containsEntry(CUSTOMERS, 1L);
            CdcRecoveryFixture.change(source, 3, ORDERS, "2", "", "after-full");
            awaitPriority(control, target, ORDERS, "2", "after-full");
            assertThat(control.state(PIPELINE)).contains(PipelineState.RUNNING);
        }
    }

    private record Saved(Document key, List<String> tables, Document cursor, Document raw) {}

    private static PipelineIntentSnapshot intent(PreparedNativeStore storage) {
        try (var configuration = MongoConfigurationDomain.openExisting(new MongoConnectionSettings(
                storage.configurationUri(), null, Duration.ofSeconds(5)))) {
            return configuration.intents().load(PIPELINE).orElseThrow();
        }
    }

    private static Saved state(NativeSrsDocuments documents, boolean shared, long generation) {
        if (!shared) {
            Document channel = documents.directChannel(PIPELINE, SOURCE, generation);
            return channel == null ? null : new Saved(channel.get("_id", Document.class),
                    channel.getList("tables", String.class), channel.get("cursor", Document.class), channel);
        }
        Document member = documents.pipelineMembership(PIPELINE, SOURCE);
        if (member == null || member.getLong("stateGeneration") != generation) return null;
        Document key = member.get("_id", Document.class);
        Document cursor = documents.consumerOffset(key.getString("chain"), SrsConsumerId.of(PIPELINE, SOURCE).value());
        if (cursor == null || !member.get("stateGeneration").equals(cursor.get("stateGeneration"))
                || !member.get("revision").equals(cursor.get("membershipRevision"))) return null;
        return new Saved(key, member.getList("tables", String.class), cursor.get("progress", Document.class),
                new Document("member", member).append("cursor", cursor));
    }

    private static boolean hasCdcAck(Saved state) {
        if (state == null) return false;
        Document ack = state.cursor().get("sinkAcked", Document.class);
        return ack != null && ack.getString("token") != null
                && !ack.getString("token").equals(state.cursor().getString("cdcStartPosition"));
    }

    private static Map<String, String> resources(Path source, Path target, boolean shared, String tables) {
        var resources = new LinkedHashMap<String, String>();
        resources.put(SOURCE + ".tap.yml", """
                version: tapstate/v1
                kind: source
                id: %s
                connector: e2e_file
                config: {uri: "%s"}
                mode: cdc
                tables: [%s]
                """.formatted(SOURCE, source, tables));
        resources.put(TARGET + ".tap.yml", Workspaces.targetYaml(TARGET, target));
        resources.put(PIPELINE + ".tap.yml", """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: [{id: %s, srs: %s}]
                settings: {read_mode: snapshot_and_cdc}
                transforms:
                  - {id: all_rows, from: [/^(orders|customers)$/], type: filter, expr: "true"}
                serve:
                  from: all_rows
                  sync: [{source: %s}]
                """.formatted(PIPELINE, SOURCE, shared, TARGET));
        return resources;
    }

    private static String priority(Path target, String table, String id) {
        List<String> lines = CdcRecoveryFixture.lines(target.resolve(table + ".csv"));
        if (lines.isEmpty()) return null;
        List<String> columns = List.of(lines.getFirst().split(",", -1));
        int idColumn = columns.indexOf("id"), priorityColumn = columns.indexOf("priority");
        for (String line : lines.subList(1, lines.size())) {
            String[] row = line.split(",", -1);
            if (idColumn >= 0 && priorityColumn >= 0 && row.length > Math.max(idColumn, priorityColumn)
                    && id.equals(row[idColumn])) return row[priorityColumn];
        }
        return null;
    }

    private static void awaitPriority(ControlPlane control, Path target, String table, String id, String value) {
        Await.until(table + " row " + id + " to hold " + value, BOUND,
                () -> value.equals(priority(target, table, id)),
                () -> "rows=" + CdcRecoveryFixture.lines(target.resolve(table + ".csv"))
                        + ", state=" + control.state(PIPELINE) + ", failure=" + control.failureCode(PIPELINE));
    }

    private static void awaitState(ControlPlane control, PipelineState expected) {
        Await.until(PIPELINE + " to reach " + expected, BOUND,
                () -> control.state(PIPELINE).filter(expected::equals).isPresent(),
                () -> "state=" + control.state(PIPELINE) + ", failure=" + control.failureCode(PIPELINE));
    }
}
