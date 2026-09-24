package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.testsupport.DockerGate;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Reads the shipped snapshot observation after a pipeline starts from a multi-table source. */
class APipelineOnlyCapturesReferencedTablesIT {

    @TempDir
    Path temporary;

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void theSnapshotListsOnlyTheTableNamedByThePipelineGraph() throws Exception {
        Path sourceDirectory = Files.createDirectory(temporary.resolve("source"));
        Path targetDirectory = Files.createDirectory(temporary.resolve("target"));
        FileEndpoints.replaceTable(sourceDirectory.resolve("account.csv"), "id,name\n1,account\n");
        FileEndpoints.replaceTable(sourceDirectory.resolve("contact.csv"), "id,name\n1,contact\n");
        FileEndpoints.replaceTable(sourceDirectory.resolve("pricebook2.csv"), "id,name\n1,first\n2,second\n");
        Path connectorJar = E2eConnectorJar.buildInto(Files.createDirectory(temporary.resolve("connectors")));

        try (ServerHandle server = Tiers.IN_PROCESS.launch(SharedMongo.replicaSetUrl("selected_tables_state"))) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            control.registerConnector(E2eConnectorJar.CONNECTOR_ID, Files.readAllBytes(connectorJar));

            Map<String, String> resources = new LinkedHashMap<>();
            resources.put("crm.tap.yml", """
                    version: tapstate/v1
                    kind: source
                    id: crm
                    connector: e2e_file
                    config: { uri: "%s" }
                    mode: cdc
                    tables: [ account, contact, pricebook2 ]
                    """.formatted(sourceDirectory));
            resources.put("target.tap.yml", """
                    version: tapstate/v1
                    kind: source
                    id: target
                    connector: e2e_file
                    config: { uri: "%s" }
                    """.formatted(targetDirectory));
            resources.put("pricebook_state.tap.yml", """
                    version: tapstate/v1
                    kind: pipeline
                    id: pricebook_state
                    source: crm
                    settings: { read_mode: snapshot_and_cdc }
                    transforms:
                      - { id: t_pricebook2, from: [ pricebook2 ], type: filter, expr: "true" }
                    serve:
                      from: t_pricebook2
                      sync:
                        - source: target
                    """);
            control.apply(resources);
            control.discoverSchema("crm", E2eConnectorJar.CONNECTOR_ID,
                    Map.of("uri", sourceDirectory.toString()));
            control.lifecycle("pricebook_state", LifecycleVerb.START);

            Await.until("pricebook2 to finish its snapshot", () ->
                            control.snapshotRowsRead("pricebook_state").getOrDefault("pricebook2", 0L) == 2L,
                    () -> control.snapshotRowsRead("pricebook_state").toString());
            assertThat(control.snapshotRowsRead("pricebook_state"))
                    .containsExactly(entry("pricebook2", 2L));
        }
    }
}
