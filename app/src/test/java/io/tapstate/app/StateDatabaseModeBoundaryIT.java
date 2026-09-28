package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.control.core.ApplyService;
import io.tapstate.control.core.ArtifactDraft;
import io.tapstate.control.core.ControlError;
import io.tapstate.control.core.StateDatabasePolicy;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.model.TransformResource;
import io.tapstate.runtime.engine.nest.NestSettings;
import io.tapstate.runtime.engine.nest.NestStatePlacement;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.OperatorStateStores;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Mode-selected admission over real Mongo storage and the runtime's write-through Nest map bridge. */
@RequiresDocker
class StateDatabaseModeBoundaryIT {

    private static final String NAMESPACE = "nest.mode_boundary.definition.$root";

    @Container
    private static final MongoDBContainer MONGO =
            new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

    @TempDir
    Path work;
    private ConfigurableApplicationContext context;

    @AfterEach
    void stop() {
        if (context != null) {
            context.close();
        }
    }

    @ParameterizedTest
    @EnumSource(CloudRuntimeSettings.Mode.class)
    void cloudRejectsAuthoredPlacementWhileOnPremPlacementSurvivesARealStoreRestart(
            CloudRuntimeSettings.Mode mode) {
        String suffix = Long.toUnsignedString(System.nanoTime(), 16);
        String metadata = "mode_metadata_" + suffix;
        String deploymentDatabase = "mode_default_" + suffix;
        String customDatabase = "mode_custom_" + suffix;
        start(mode, metadata, deploymentDatabase);

        ApplyService apply = context.getBean(ApplyService.class);
        String explicit = definition(customDatabase);
        String implicit = definition(null);
        String accepted;
        if (mode == CloudRuntimeSettings.Mode.CLOUD) {
            assertThat(context.getBean(StateDatabasePolicy.class)).isEqualTo(StateDatabasePolicy.CLOUD);
            assertThat(apply.validate(List.of(new ArtifactDraft(null, explicit))).diagnostics())
                    .singleElement().extracting(diagnostic -> diagnostic.code())
                    .isEqualTo(ControlError.STATE_DATABASE_UNAVAILABLE.code());
            assertThatThrownBy(() -> apply.apply("fixture-verified-user", List.of(new ArtifactDraft(null, explicit))))
                    .isInstanceOfSatisfying(TapstateException.class, error ->
                            assertThat(error.code()).isEqualTo(ControlError.STATE_DATABASE_UNAVAILABLE));
            assertThat(context.getBean(ArtifactStore.class).get("nest_definition")).isEmpty();
            try (var raw = MongoClients.create(MONGO.getReplicaSetUrl())) {
                assertThat(raw.getDatabase(customDatabase).listCollectionNames().into(new ArrayList<>())).isEmpty();
            }
            accepted = implicit;
        } else {
            assertThat(context.getBean(StateDatabasePolicy.class)).isEqualTo(StateDatabasePolicy.ON_PREM);
            assertThat(apply.validate(List.of(new ArtifactDraft(null, explicit))).valid()).isTrue();
            accepted = explicit;
        }
        // The principal is a controlled test input. This witnesses admission, not Cloud SDK authentication.
        apply.apply("fixture-verified-user", List.of(new ArtifactDraft(null, accepted)));
        String resolved = routeStoredDefinition();
        assertThat(resolved).isEqualTo(mode == CloudRuntimeSettings.Mode.CLOUD
                ? deploymentDatabase : customDatabase);
        HazelcastInstance member = context.getBean(HazelcastInstance.class);
        member.getMap(NAMESPACE).put("same-key", "durable-value");
        try (var raw = MongoClients.create(MONGO.getReplicaSetUrl())) {
            Document written = raw.getDatabase(resolved)
                    .getCollection(SystemCollections.OPERATOR_STATE.collectionName())
                    .find(new Document("_id.ns", NAMESPACE)).first();
            assertThat(written).as("the runtime map acknowledged a real, selected-database write").isNotNull();
            String unused = mode == CloudRuntimeSettings.Mode.CLOUD ? customDatabase : deploymentDatabase;
            assertThat(raw.getDatabase(unused).getCollection(SystemCollections.OPERATOR_STATE.collectionName())
                    .countDocuments(new Document("_id.ns", NAMESPACE))).isZero();
            assertThat(raw.getDatabase(metadata).getCollection(SystemCollections.OPERATOR_STATE.collectionName())
                    .countDocuments()).isZero();
        }
        context.close();
        context = null;
        start(mode, metadata, deploymentDatabase);
        assertThat(routeStoredDefinition()).isEqualTo(resolved);
        assertThat(context.getBean(HazelcastInstance.class).getMap(NAMESPACE).get("same-key"))
                .as("a new member loads the cold state through the same database-specific map bridge")
                .isEqualTo("durable-value");
    }

    private String routeStoredDefinition() {
        TransformResource resource = (TransformResource) context.getBean(ArtifactStore.class)
                .get("nest_definition").orElseThrow();
        TransformBody.Nest nest = (TransformBody.Nest) resource.body();
        OperatorStateStores stores = context.getBean(OperatorStateStores.class);
        String database = nest.state() == null ? stores.defaultDatabase() : nest.state().database();
        NestStatePlacement.applyTo(context.getBean(HazelcastInstance.class), Map.of(NAMESPACE, database),
                NestSettings.defaults());
        return database;
    }

    private void start(CloudRuntimeSettings.Mode mode, String database, String deploymentDatabase) {
        List<String> args = new ArrayList<>(List.of(
                "--server.address=127.0.0.1", "--server.port=0", "--tapstate.hz.member-port=0",
                "--tapstate.hz.jet.cooperative-thread-count=2",
                "--tapstate.connectors.plugins-dir=" + work.resolve("plugins"),
                "--tapstate.connectors.seed-dir=" + work.resolve("no-optional-seeds"),
                "--tapstate.store.mongo.uri=" + MONGO.getReplicaSetUrl(database),
                "--tapstate.store.mongo.operator-state-database=" + deploymentDatabase));
        if (mode == CloudRuntimeSettings.Mode.CLOUD) {
            args.addAll(List.of("--tapstate.cloud.base-url=https://cloud.example.invalid",
                    "--tapstate.cloud.token=mode-fixture-token",
                    "--tapstate.cloud.atlas-uri=" + MONGO.getReplicaSetUrl(database)));
        }
        context = new SpringApplicationBuilder(Bootstrap.class).run(args.toArray(String[]::new));
    }

    private static String definition(String database) {
        return """
                version: tapstate/v1
                kind: transform
                id: nest_definition
                type: nest
                """
                + (database == null ? "" : "state: { database: " + database + " }\n")
                + "root: { from: c, key: [id] }\n";
    }
}
