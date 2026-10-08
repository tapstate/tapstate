package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.SrsConsumerId;
import io.tapstate.testsupport.DockerGate;
import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** A quiet table's last, filtered change is confirmed while another table keeps flowing through a union. */
class AFilteredChangeStillAdvancesItsTableIT {

    private static final String SOURCE = "filtered_source";
    private static final String PIPELINE = "filtered_pipeline";
    private static final Duration TIMEOUT = Duration.ofSeconds(90);

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void aQuietTablesDroppedTailDoesNotHoldItsAcknowledgement(Tiers tier, @TempDir Path directory)
            throws Exception {
        Path source = Files.createDirectory(directory.resolve("source"));
        Path target = Files.createDirectory(directory.resolve("target"));
        FileEndpoints.replaceTable(source.resolve("orders.csv"), "id,priority\n1,keep\n");
        FileEndpoints.replaceTable(source.resolve("activity.csv"), "id,priority\n1,keep\n");
        Path delegate = E2eConnectorJar.buildInto(Files.createDirectory(directory.resolve("connector")));
        String store = SharedMongo.replicaSetUrl("filtered_tail_" + tier.name().toLowerCase(Locale.ROOT));
        try (ServerHandle server = tier.launch(store); StoreDocuments documents = StoreDocuments.at(store)) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            control.registerConnector(E2eConnectorJar.CONNECTOR_ID, CdcRecoveryFixture.connectorJar(delegate));
            Map<String, String> resources = workspace(source, target);
            control.apply(Map.of(SOURCE + ".tap.yml", resources.get(SOURCE + ".tap.yml")));
            control.discoverSchema(SOURCE, E2eConnectorJar.CONNECTOR_ID, Map.of("uri", source.toString()));
            control.apply(resources);
            control.lifecycle(PIPELINE, LifecycleVerb.START);
            Await.until("both tables' initial rows to land", TIMEOUT,
                    () -> rowCount(target, "orders") == 1 && rowCount(target, "activity") == 1
                            && documents.miningChainIds().size() == 1,
                    () -> control.logs(PIPELINE).toString());
            String chain = documents.miningChainIds().iterator().next();
            String consumer = SrsConsumerId.of(PIPELINE, SOURCE).value();

            CdcRecoveryFixture.change(source, 1, "orders", "2", "", "keep");
            Await.until("a passing order to establish the confirmation baseline", TIMEOUT,
                    () -> rowCount(target, "orders") == 2
                            && tableSequence(documents.consumerOffset(chain, consumer), "orders") >= 1,
                    () -> String.valueOf(documents.consumerOffset(chain, consumer)));
            long readBeforeDrop = documents.consumerReadSeq(chain, consumer, "orders");
            CdcRecoveryFixture.change(source, 2, "orders", "3", "", "drop");
            Await.until("the dropped order to be read before the other table changes", TIMEOUT,
                    () -> documents.consumerReadSeq(chain, consumer, "orders") > readBeforeDrop,
                    () -> String.valueOf(documents.consumerOffset(chain, consumer)));
            CdcRecoveryFixture.change(source, 3, "activity", "2", "", "keep");
            Await.until("another table's later change to land", TIMEOUT,
                    () -> rowCount(target, "activity") == 2,
                    () -> control.logs(PIPELINE).toString());

            Await.until("the quiet table's dropped tail and the other table's later change to be confirmed",
                    Duration.ofSeconds(30),
                    () -> {
                        Document offset = documents.consumerOffset(chain, consumer);
                        return tableSequence(offset, "orders") >= 2
                                && tableSequence(offset, "activity") >= 3;
                    }, () -> String.valueOf(documents.consumerOffset(chain, consumer)));
            assertThat(rowCount(target, "orders")).as("the dropped order never becomes a target row").isEqualTo(2);
            assertThat(sequence(control.resumePoint(PIPELINE).get("token")))
                    .as("the resume point passes the dropped change without another passing order")
                    .isGreaterThanOrEqualTo(2);
            assertThat(control.state(PIPELINE)).contains(PipelineState.RUNNING);
        }
    }

    private static long rowCount(Path directory, String table) {
        return Math.max(0, CdcRecoveryFixture.lines(directory.resolve(table + ".csv")).size() - 1L);
    }

    private static long sequence(Document position) {
        return position == null ? -1L : sequence(position.getString("sinkAckedSrcpos"));
    }

    private static long tableSequence(Document offset, String table) {
        Document byTable = offset == null ? null : offset.get("sinkAckedByTable", Document.class);
        return byTable == null ? -1L : sequence(byTable.get(table, Document.class));
    }

    private static long sequence(String token) {
        if (token == null) {
            return -1L;
        }
        try (ObjectInputStream input = new ObjectInputStream(
                new ByteArrayInputStream(Base64.getDecoder().decode(token)))) {
            return ((Number) ((Map<?, ?>) input.readObject()).get("sequence")).longValue();
        } catch (Exception failure) {
            throw new AssertionError("the fixture's recorded source position must be readable", failure);
        }
    }

    private static Map<String, String> workspace(Path source, Path target) {
        Map<String, String> resources = new LinkedHashMap<>();
        resources.put(SOURCE + ".tap.yml", """
                version: tapstate/v1
                kind: source
                id: %s
                connector: e2e_file
                config: { uri: "%s" }
                mode: cdc
                tables: [ orders, activity ]
                """.formatted(SOURCE, source));
        resources.put("filtered_target.tap.yml", Workspaces.targetYaml("filtered_target", target));
        resources.put(PIPELINE + ".tap.yml", """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: %s
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - { id: accepted, from: [ orders ], type: filter, expr: "after.priority != 'drop'" }
                  - { id: combined, from: [ accepted, activity ], type: union }
                serve:
                  from: combined
                  sync:
                    - source: filtered_target
                """.formatted(PIPELINE, SOURCE));
        return resources;
    }
}
