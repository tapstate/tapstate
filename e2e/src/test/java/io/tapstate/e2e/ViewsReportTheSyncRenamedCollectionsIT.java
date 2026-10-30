package io.tapstate.e2e;

import io.tapstate.core.common.JsonReader;
import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** The public Views read points to each sync's renamed collection while retaining its stable row id. */
@RequiresDocker
class ViewsReportTheSyncRenamedCollectionsIT {

    private static MongoDBContainer mongo;

    @BeforeAll
    static void startStore() {
        mongo = new MongoDBContainer(DockerImageName.parse(
                System.getProperty("tapstate.e2e.mongo-image", "mongo:7.0")));
        String owner = System.getProperty("tapstate.e2e.resource-owner");
        if (owner != null && !owner.isBlank()) {
            mongo.withLabel("io.tapstate.issue-loop.owner", owner);
        }
        mongo.withTmpFs(Map.of("/data/db", "rw,size=512m", "/data/configdb", "rw,size=16m"));
        mongo.withCreateContainerCmdModifier(command -> command.getHostConfig()
                .withMemory(1L << 30).withNanoCPUs(1_000_000_000L));
        mongo.start();
    }

    @AfterAll
    static void stopStore() {
        if (mongo != null) mongo.stop();
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void replicasWithDifferentPrefixesReportDifferentTargetCollections(Tiers tier) {
        String suffix = tier.name().toLowerCase(Locale.ROOT);
        String database = "views_target_" + suffix;
        try (PreparedNativeStore storage = PreparedNativeStore.createOn(mongo);
                ServerHandle server = tier.launch(storage)) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);
            Map<String, String> workspace = new LinkedHashMap<>();
            workspace.put("source.tap.yml", """
                    version: tapstate/v1
                    kind: source
                    id: mysql_source
                    connector: mysql
                    config: { host: localhost, port: 3306, database: acceptance, username: fixture, password: fixture }
                    mode: cdc
                    tables: [ ACC_COMPOSITE ]
                    """);
            workspace.put("target.tap.yml", """
                    version: tapstate/v1
                    kind: source
                    id: atlas-store
                    connector: mongodb
                    config: { uri: "%s" }
                    """.formatted(mongo.getReplicaSetUrl(database)));
            for (String prefix : List.of("mysql", "ora", "pg")) {
                workspace.put(prefix + ".tap.yml", """
                        version: tapstate/v1
                        kind: pipeline
                        id: copy_%s
                        source: mysql_source
                        serve:
                          from: ACC_COMPOSITE
                          sync:
                            - id: sink
                              source: atlas-store
                              rename: { prefix: "%s_" }
                        """.formatted(prefix, prefix));
            }
            control.apply(workspace);

            var response = control.sourceRequest("GET", "/api/views", null, null, 200);
            Map<?, ?> body = (Map<?, ?>) JsonReader.parse(response.body());
            List<?> items = (List<?>) body.get("items");
            assertThat(items).hasSize(3);
            for (int index = 0; index < items.size(); index++) {
                String prefix = List.of("mysql", "ora", "pg").get(index);
                Map<?, ?> view = (Map<?, ?>) items.get(index);
                assertThat(view.get("id")).isEqualTo("copy_" + prefix + ":sink:ACC_COMPOSITE");
                assertThat(view.get("location")).isEqualTo(Map.of(
                        "storeId", "atlas-store", "database", database, "collection", prefix + "_ACC_COMPOSITE"));
            }
        }
    }
}
