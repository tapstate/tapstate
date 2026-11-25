package io.tapstate.e2e;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.adapters.mongostore.MongoConnectionSettings;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;
import io.tapstate.runtime.engine.nest.ResolverState;
import io.tapstate.runtime.engine.nest.RootAssembly;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.runtime.engine.nest.NestElement;
import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.io.IOException;
import org.bson.types.Binary;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import static org.assertj.core.api.Assertions.assertThat;

/** Real private Nest payload is reclaimed only for the deleted Pipeline; its neighbour keeps assembling. */
class ArtifactDeleteLeavesNestStateStandingIT {
    private static final String DEPARTING = "departing_nest";
    private static final String SURVIVING = "surviving_nest";
    private static final String DEPARTING_VIEW = "departed_orders";
    private static final String SURVIVING_VIEW = "surviving_orders";
    private static final Duration WAIT = Duration.ofSeconds(90);

    @BeforeAll
    static void requireDockerAndRealConnectors() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void deletionReclaimsOnlyItsOriginalNestPayloadAndTheSurvivorCompletesItsWaitingChild(Tiers tier) throws Exception {
        var source = SharedMySql.settings("delete_nest_" + UUID.randomUUID().toString().replace("-", ""));
        seed(source);
        try (PreparedNativeStore storage = PreparedNativeStore.create();
                ServerHandle server = tier.launch(storage);
                MongoConfigurationDomain configuration = MongoConfigurationDomain.openExisting(
                        new MongoConnectionSettings(storage.configurationUri(), null, Duration.ofSeconds(5)));
                var raw = MongoClients.create(storage.cacheUri())) {
            var control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);
            control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
            control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
            control.apply(workspace(source));
            control.discoverSchema("parents", "mysql", source);
            control.discoverSchema("children", "mysql", source);
            control.lifecycle(DEPARTING, LifecycleVerb.START);
            control.lifecycle(SURVIVING, LifecycleVerb.START);
            var cache = raw.getDatabase(storage.cacheDatabase());
            var master = raw.getDatabase(storage.masterDatabase());
            Await.until("both real nests to assemble the initial parent and child", WAIT,
                    () -> assembled(master, DEPARTING_VIEW, 1, "original", "existing-sku")
                            && assembled(master, SURVIVING_VIEW, 1, "original", "existing-sku"),
                    () -> "departed=" + documents(master, DEPARTING_VIEW) + " survivor=" + documents(master, SURVIVING_VIEW));
            Await.until("both nests to durably hold the specific child whose parent is absent", WAIT,
                    () -> !pendingChildren(cache, new Document("_id.placement.scope.pipelineId", DEPARTING)).isEmpty()
                            && !pendingChildren(cache, new Document("_id.placement.scope.pipelineId", SURVIVING)).isEmpty(),
                    () -> "departed state=" + stateRows(cache, DEPARTING) + " survivor state=" + stateRows(cache, SURVIVING));
            assertThat(master.getCollection(DEPARTING_VIEW).countDocuments()).isEqualTo(1);
            assertThat(master.getCollection(SURVIVING_VIEW).countDocuments()).isEqualTo(1);
            var delivered = documents(master, DEPARTING_VIEW);
            var departing = freeze(configuration, cache, DEPARTING, storage.cacheDatabase());
            var surviving = freeze(configuration, cache, SURVIVING, storage.cacheDatabase());
            var originalPending = pendingChildren(cache, new Document("_id.placement.scope", surviving.scope()));
            assertThat(originalPending).isNotEmpty();
            long survivorExecution = control.executionGenerationOf(SURVIVING).orElseThrow();

            // Preserve this generation's state: the deletion itself must retire and reclaim it.
            control.stop(DEPARTING, false);
            Await.until("the departing nest to stop without clearing its original state", WAIT,
                    () -> control.state(DEPARTING).filter(PipelineState.STOPPED::equals).isPresent(),
                    () -> control.pipelineStatus(DEPARTING).toString());
            assertThat(stateRows(cache, departing.scope())).isPositive();
            try {
                control.deleteArtifact(DEPARTING, control.contentHash(DEPARTING));
            } catch (RuntimeException | Error failure) {
                FailureScene.writeThreads(storage.clusterId(), server);
                throw failure;
            }
            assertThat(control.artifact(DEPARTING)).isEmpty();
            var inventory = SystemCollections.CACHE_OPERATOR_CLEANUP.on(cache);
            var retired = inventory.find(new Document("_id", departing.scope())).first();
            assertThat(retired).isNotNull();
            assertThat(retired.get("retirement", Document.class)).containsEntry("phase", "RETIRED").containsEntry("purpose", "DELETE");
            assertThat(retired.get("placements")).isEqualTo(departing.inventory().get("placements"));
            assertThat(retired.get("shapes")).isEqualTo(departing.inventory().get("shapes"));
            Await.until("only the retired generation's bounded payload to be reclaimed", WAIT,
                    () -> stateRows(cache, departing.scope()) == 0 && shapeRows(cache, departing.scope()).isEmpty(),
                    () -> "remaining departed state=" + stateRows(cache, departing.scope()));
            assertThat(inventory.find(new Document("_id", departing.scope())).first()).isNotNull();
            assertThat(stateIds(cache, surviving.scope())).containsAll(surviving.stateIds());
            assertThat(shapeRows(cache, surviving.scope())).containsExactlyInAnyOrderElementsOf(surviving.shapes());
            assertThat(pendingChildren(cache, new Document("_id.placement.scope", surviving.scope())))
                    .as("the same durable child, original order and source positions survive the neighbour's deletion")
                    .containsAll(originalPending);
            var survivorMapping = configuration.pipelineStates().read(SURVIVING).orElseThrow();
            assertThat(survivorMapping.phase()).isEqualTo(io.tapstate.spi.store.PipelineStateMapping.Phase.ACTIVE);
            assertThat(survivorMapping.stateGeneration()).isEqualTo(surviving.scope().getLong("stateGeneration"));

            try (var connection = SharedMySql.connect(source); var statement = connection.createStatement()) {
                statement.executeUpdate("INSERT INTO orders VALUES (2, 'arrived-later')");
                statement.executeUpdate("UPDATE orders SET customer='survivor-change' WHERE id=1");
            }
            Await.until("the surviving original nest to join its retained child and carry later CDC", WAIT,
                    () -> assembled(master, SURVIVING_VIEW, 2, "arrived-later", "waiting-sku")
                            && assembled(master, SURVIVING_VIEW, 1, "survivor-change", "existing-sku"),
                    () -> documents(master, SURVIVING_VIEW).toString());
            assertThat(master.getCollection(SURVIVING_VIEW).countDocuments()).isEqualTo(2);
            assertThat(control.executionGenerationOf(SURVIVING)).contains(survivorExecution);
            assertThat(documents(master, DEPARTING_VIEW)).containsExactlyInAnyOrderElementsOf(delivered);
            assertThat(stateRows(cache, departing.scope())).isZero();
        }
    }

    private record Frozen(Document scope, Document inventory, List<Document> stateIds, List<Document> shapes) {}

    private static Frozen freeze(MongoConfigurationDomain configuration, MongoDatabase cache, String pipeline, String expectedDatabase) {
        var preparation = configuration.preparations().read(pipeline).orElseThrow();
        assertThat(preparation.phase()).isEqualTo(io.tapstate.spi.store.PipelinePreparation.Phase.COMPLETE);
        var row = SystemCollections.CACHE_OPERATOR_CLEANUP.on(cache).find(new Document("_id.pipelineId", pipeline)
                .append("_id.stateGeneration", preparation.stateGeneration())).first();
        assertThat(row).isNotNull();
        assertThat(row.getList("placements", Document.class)).isNotEmpty().allSatisfy(placement ->
                assertThat(placement.getString("database")).isEqualTo(expectedDatabase));
        assertThat(row.containsKey("retirement")).isFalse();
        var scope = row.get("_id", Document.class);
        assertThat(scope.getString("cacheIncarnation")).isEqualTo(preparation.scope().cacheIncarnation());
        var ids = stateIds(cache, scope); var shapes = shapeRows(cache, scope);
        assertThat(ids).isNotEmpty(); assertThat(shapes).isNotEmpty();
        return new Frozen(scope, row, ids, shapes);
    }

    private record Pending(Document id, NestElement child, Map<String, ChainPosition> heldPositions) {}

    /** Reads the production serialized state, without guessing a cold key or inventing stored bytes. */
    private static List<Pending> pendingChildren(MongoDatabase cache, Document filter) {
        var found = new ArrayList<Pending>();
        for (var row : SystemCollections.CACHE_NEST_STATE.on(cache).find(filter)) {
            try (var input = new ObjectInputStream(new ByteArrayInputStream(row.get("state", Binary.class).getData()))) {
                Object decoded = input.readObject();
                List<NestElement> children;
                Map<String, ChainPosition> held;
                if (decoded instanceof RootAssembly root && !root.rootPresent()) {
                    held = Map.copyOf(root.lowestHeldByChain());
                    // This is an isolated decoded copy; the persisted/live assembly is never mutated.
                    children = root.detachEverything();
                } else if (decoded instanceof ResolverState state && state.parentKey() == null) {
                    held = Map.copyOf(state.lowestHeldByChain());
                    children = state.waiting();
                } else continue;
                for (var child : children) {
                    var fields = child.fields();
                    if (fields != null && "waiting-sku".equals(fields.get("sku"))
                            && fields.get("id") instanceof Number id && id.intValue() == 2
                            && fields.get("order_id") instanceof Number parent && parent.intValue() == 2) {
                        assertThat(held).isNotEmpty();
                        found.add(new Pending(row.get("_id", Document.class), child, held));
                    }
                }
            } catch (IOException | ClassNotFoundException unreadable) {
                throw new AssertionError("cannot decode the real native Nest fixture state", unreadable);
            }
        }
        return List.copyOf(found);
    }

    private static long stateRows(MongoDatabase cache, String pipeline) {
        return SystemCollections.CACHE_NEST_STATE.on(cache).countDocuments(new Document("_id.placement.scope.pipelineId", pipeline));
    }
    private static long stateRows(MongoDatabase cache, Document scope) {
        return SystemCollections.CACHE_NEST_STATE.on(cache).countDocuments(new Document("_id.placement.scope", scope));
    }
    private static List<Document> stateIds(MongoDatabase cache, Document scope) {
        return SystemCollections.CACHE_NEST_STATE.on(cache).find(new Document("_id.placement.scope", scope))
                .map(row -> row.get("_id", Document.class)).into(new ArrayList<>());
    }
    private static List<Document> shapeRows(MongoDatabase cache, Document scope) {
        return SystemCollections.CACHE_NEST_SHAPES.on(cache).find(new Document("_id.scope", scope)).into(new ArrayList<>());
    }
    private static List<Document> documents(MongoDatabase master, String view) {
        return master.getCollection(view).find().into(new ArrayList<>());
    }
    private static boolean assembled(MongoDatabase master, String view, int id, String customer, String sku) {
        var row = master.getCollection(view).find(new Document("id", id)).first();
        if (row == null || !customer.equals(row.get("customer")) || !(row.get("items") instanceof List<?> children)) return false;
        return children.size() == 1 && children.getFirst() instanceof Document child && sku.equals(child.get("sku"));
    }

    private static void seed(Map<String, Object> source) throws Exception {
        try (var connection = SharedMySql.connect(source); var statement = connection.createStatement()) {
            statement.execute("CREATE TABLE orders (id INT PRIMARY KEY, customer VARCHAR(64))");
            statement.execute("CREATE TABLE order_items (id INT PRIMARY KEY, order_id INT, sku VARCHAR(64))");
            statement.executeUpdate("INSERT INTO orders VALUES (1, 'original')");
            statement.executeUpdate("INSERT INTO order_items VALUES (1, 1, 'existing-sku'), (2, 2, 'waiting-sku')");
        }
    }
    private static Map<String, String> workspace(Map<String, Object> source) {
        var resources = new LinkedHashMap<String, String>();
        resources.put("parents.tap.yml", source("parents", "orders", source));
        resources.put("children.tap.yml", source("children", "order_items", source));
        resources.put("departing.tap.yml", pipeline(DEPARTING, DEPARTING_VIEW));
        resources.put("surviving.tap.yml", pipeline(SURVIVING, SURVIVING_VIEW));
        return resources;
    }
    private static String source(String id, String table, Map<String, Object> settings) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mysql
                config: %s
                mode: cdc
                tables: [ %s ]
                """.formatted(id, JsonWriter.write(settings), table);
    }
    private static String pipeline(String id, String view) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: [ parents, children ]
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - id: order_doc
                    type: nest
                    from: { o: orders, i: order_items }
                    root:
                      from: o
                      key: [ id ]
                      embed:
                        - { from: i, on: { order_id: id }, as: array, path: items, arrayKey: [ id ] }
                view: { id: %s, from: order_doc, primary_key: id }
                """.formatted(id, view);
    }
}
