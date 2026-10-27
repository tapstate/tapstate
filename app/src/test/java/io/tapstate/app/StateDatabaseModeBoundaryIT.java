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
import io.tapstate.runtime.engine.OperatorMapBinding;
import io.tapstate.runtime.engine.OperatorMapCapacity;
import io.tapstate.runtime.engine.OperatorMapCoordinates;
import io.tapstate.runtime.engine.OperatorRunManifest;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.WorkloadClaimFence;
import java.time.Duration;
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

    private static final String OWNER = "mode_owner";

    @Container
    private static final MongoDBContainer MONGO =
            new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

    @TempDir
    Path work;
    private ConfigurableApplicationContext context;
    private NativeAppStorage storage;

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
        storage = NativeAppStorage.prepare(MONGO.getReplicaSetUrl(metadata),
                MONGO.getReplicaSetUrl(metadata + "_cache"), MONGO.getReplicaSetUrl(metadata + "_master"),
                mode == CloudRuntimeSettings.Mode.CLOUD ? StorageBinding.Mode.CLOUD : StorageBinding.Mode.OP,
                "state-mode-" + suffix);
        start(mode, deploymentDatabase);

        var child = NativeAppStorage.business(context);
        ApplyService apply = child.getBean(ApplyService.class);
        String explicit = definition(customDatabase);
        String implicit = definition(null);
        String accepted;
        if (mode == CloudRuntimeSettings.Mode.CLOUD) {
            assertThat(child.getBean(StateDatabasePolicy.class)).isEqualTo(StateDatabasePolicy.CLOUD);
            assertThat(apply.validate(List.of(new ArtifactDraft(null, explicit))).diagnostics())
                    .singleElement().extracting(diagnostic -> diagnostic.code())
                    .isEqualTo(ControlError.STATE_DATABASE_UNAVAILABLE.code());
            assertThatThrownBy(() -> apply.apply("fixture-verified-user", List.of(new ArtifactDraft(null, explicit))))
                    .isInstanceOfSatisfying(TapstateException.class, error ->
                            assertThat(error.code()).isEqualTo(ControlError.STATE_DATABASE_UNAVAILABLE));
            assertThat(child.getBean(ArtifactStore.class).get("nest_definition")).isEmpty();
            try (var raw = MongoClients.create(MONGO.getReplicaSetUrl())) {
                assertThat(raw.getDatabase(customDatabase).listCollectionNames().into(new ArrayList<>())).isEmpty();
            }
            accepted = implicit;
        } else {
            assertThat(child.getBean(StateDatabasePolicy.class)).isEqualTo(StateDatabasePolicy.ON_PREM);
            assertThat(apply.validate(List.of(new ArtifactDraft(null, explicit))).valid()).isTrue();
            accepted = explicit;
        }
        // The principal is a controlled test input. This witnesses admission, not Cloud SDK authentication.
        apply.apply("fixture-verified-user", List.of(new ArtifactDraft(null, accepted)));
        var first = routeStoredDefinition();
        String resolved = first.placement().database();
        assertThat(resolved).isEqualTo(mode == CloudRuntimeSettings.Mode.CLOUD ? storage.cacheDatabase() : customDatabase);
        HazelcastInstance member = child.getBean(HazelcastInstance.class);
        member.getMap(first.mapName()).put(List.of("same-key"), "durable-value");
        try (var raw = MongoClients.create(MONGO.getReplicaSetUrl())) {
            var rows = raw.getDatabase(resolved).getCollection(SystemCollections.CACHE_NEST_STATE.collectionName())
                    .find().into(new ArrayList<>());
            assertThat(rows).as("the original scoped map acknowledged a real selected-database write").hasSize(1);
            Document placement = rows.getFirst().get("_id", Document.class).get("placement", Document.class);
            assertThat(placement.getString("database")).isEqualTo(resolved);
            assertThat(placement.getString("resource")).isEqualTo("NEST_STATE");
            var unused = new ArrayList<>(List.of(metadata, storage.masterDatabase(), deploymentDatabase));
            unused.add(mode == CloudRuntimeSettings.Mode.CLOUD ? customDatabase : storage.cacheDatabase());
            for (String database : unused) {
                assertThat(raw.getDatabase(database).getCollection(SystemCollections.CACHE_NEST_STATE.collectionName())
                        .countDocuments()).as("no operator state in %s", database).isZero();
            }
        }
        context.close();
        context = null;
        start(mode, deploymentDatabase);
        var restarted = routeStoredDefinition();
        assertThat(restarted.placement()).as("restart retains the complete cold identity and state generation")
                .isEqualTo(first.placement());
        assertThat(restarted.mapName()).as("a new execution receives a separate hot map").isNotEqualTo(first.mapName());
        assertThat(((ExecutionFence) restarted.executionIdentity()).claimGeneration())
                .as("the new owner advances the retained claim history")
                .isGreaterThan(((ExecutionFence) first.executionIdentity()).claimGeneration());
        assertThat(NativeAppStorage.business(context).getBean(HazelcastInstance.class)
                .getMap(restarted.mapName()).get(List.of("same-key")))
                .as("the new authorized member reloads the same cold state").isEqualTo("durable-value");
    }

    private OperatorMapBinding routeStoredDefinition() {
        var child = NativeAppStorage.business(context);
        TransformResource resource = (TransformResource) child.getBean(ArtifactStore.class)
                .get("nest_definition").orElseThrow();
        TransformBody.Nest nest = (TransformBody.Nest) resource.body();
        OperatorStateStores stores = child.getBean(io.tapstate.spi.store.StorePort.class).operatorStateStores();
        String database = nest.state() == null ? stores.defaultDatabase() : nest.state().database();
        var root = context.getBean(StorageRoot.class);
        var configuration = root.runtime().requireConfiguration();
        var bound = root.runtime().current().orElseThrow();
        // This fixture isolates map placement from source capture, using a real native claim owner.
        var pipeline = new PipelineResource(OWNER, null, List.of(), null, null, null, null, null);
        if (configuration.artifacts().get(OWNER).isEmpty()) configuration.artifacts().create(pipeline);
        var mapping = configuration.pipelineStates().read(OWNER).orElseThrow();
        var business = bound.preparation().businessScope();
        var scope = new PipelineIntentScope(business.clusterId(), business.bindingVersion(), business.cacheIncarnation(),
                OWNER, mapping.stateGeneration());
        child.getBean(PipelineFirstStatePreparer.class).prepare(scope);
        if (configuration.intents().load(OWNER).isEmpty()) {
            assertThat(configuration.intents().compareAndSet(scope, 0,
                    new DesiredState(OWNER, PipelineState.STOPPED, CanonicalHash.of(pipeline)))).isTrue();
        }
        var intent = configuration.intents().loadForActuation(OWNER).orElseThrow();
        var ownership = child.getBean(PipelineActuationOwnership.class);
        // Shutdown preserves the old claim history; a replacement must wait for the original lease.
        var takeoverBudget = child.getBean(ClusterProperties.class).getWorkloadClaimTtl().plusSeconds(15);
        org.awaitility.Awaitility.await().atMost(takeoverBudget).until(() -> ownership.permit(scope).granted());
        var permit = ownership.permit(scope);
        var execution = ownership.beginExecution(scope, WorkloadClaimFence.from(permit.claim()));
        assertThat(execution.allowed()).isTrue();
        var claim = execution.claimFence();
        Runnable active = () -> {
            bound.requireBusiness();
            assertThat(configuration.intents().loadForActuation(OWNER)).contains(intent);
            assertThat(ownership.proveClaim(scope, claim)).isTrue();
        };
        var pass = new PipelineConvergencePass(intent, claim, bound.cache(), active);
        pass.prepareOperatorDatabases(claim, List.of(database), active);
        pass.recordNestShape(claim, database, OWNER, "definition", java.util.Set.of(OperatorMapCoordinates.path(List.of())));
        var placement = OperatorMapCoordinates.nest(scope, database, "definition", List.of(),
                OperatorMapCoordinates.NestMap.STATE, null);
        var binding = new OperatorMapBinding(placement, execution.fence(), execution.fence().operatorExecutionKey());
        var manifest = new OperatorRunManifest(OWNER, execution.fence(), List.of(binding));
        var member = child.getBean(HazelcastInstance.class);
        new HazelcastOperatorReadiness(member, Duration.ofSeconds(5)).prepare(manifest,
                List.of(new OperatorMapCapacity(placement, NestSettings.defaults().entriesHeldInMemory())),
                List.of(ClusterMembershipGate.stableIdOf(member.getCluster().getLocalMember())), active);
        return binding;
    }

    private void start(CloudRuntimeSettings.Mode mode, String deploymentDatabase) {
        List<String> args = new ArrayList<>(List.of(
                "--server.address=127.0.0.1", "--server.port=0", "--tapstate.hz.member-port=0",
                "--tapstate.hz.jet.cooperative-thread-count=2",
                "--SDK_STATUS_SENDER_ENABLED=false",
                "--tapstate.connectors.plugins-dir=" + work.resolve("plugins"),
                "--tapstate.connectors.seed-dir=" + (mode == CloudRuntimeSettings.Mode.CLOUD
                        ? CloudConnectorTestInputs.seedDirectory() : work.resolve("no-optional-seeds")),
                "--tapstate.store.mongo.uri=" + storage.configurationUri(),
                "--tapstate.store.mongo.operator-state-database=" + deploymentDatabase));
        if (mode == CloudRuntimeSettings.Mode.CLOUD) {
            args.addAll(List.of("--tapstate.cloud.base-url=https://cloud.example.invalid",
                    "--tapstate.cloud.token=mode-fixture-token",
                    "--tapstate.cloud.atlas-uri=" + storage.configurationUri(),
                    "--tapstate.cloud.cluster-id=" + storage.clusterId()));
        }
        context = new SpringApplicationBuilder(Bootstrap.class).environment(CloudFixtureEnvironment.isolated())
                .run(args.toArray(String[]::new));
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
