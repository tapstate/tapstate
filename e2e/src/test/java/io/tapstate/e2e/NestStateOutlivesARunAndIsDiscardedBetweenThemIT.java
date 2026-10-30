package io.tapstate.e2e;

import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.MongoCacheDomain;
import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.adapters.mongostore.MongoConnectionSettings;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.runtime.engine.OperatorMapCoordinates;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.NestDeadLetterRecord;
import io.tapstate.spi.store.OperatorPlacement;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.RequiredDeadLetterContext;
import io.tapstate.spi.store.RequiredDeadLetterFact;
import io.tapstate.testsupport.DockerGate;
import org.bson.Document;
import org.bson.types.Binary;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Native installation identity bounds retained Nest state and its required configuration diagnostics. */
class NestStateOutlivesARunAndIsDiscardedBetweenThemIT {
    private static final String PARENTS = "orders";
    private static final String CHILDREN = "order_items";
    private static final String VIEW = "order_state";
    private static final String PIPELINE = "assembled_orders";
    private static final String STEP = "order_doc";
    private static final String SEEDED = "seeded-customer";
    private static final String ONLY_THE_FIRST_RUN = "written-by-the-first-run";

    @BeforeAll
    static void requireDockerAndRealConnectors() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
    }

    @Test
    void equalPipelineNamesInIndependentInstallationsDoNotInheritEachOthersAssembly() throws Exception {
        var firstSource = SharedMySql.settings("nest_inherit_src_one");
        var secondSource = SharedMySql.settings("nest_inherit_src_two");
        seed(firstSource);
        seed(secondSource);
        try (PreparedNativeStore first = PreparedNativeStore.create();
                PreparedNativeStore second = PreparedNativeStore.create();
                MongoEndpoints mongo = new MongoEndpoints()) {
            assertThat(first.clusterId()).isNotEqualTo(second.clusterId());
            assertThat(first.cacheDatabase()).isNotEqualTo(second.cacheDatabase());
            try (ServerHandle server = Tiers.IN_PROCESS.launch(first)) {
                start(server, first, firstSource);
                awaitCustomer(mongo, first, SEEDED, "the first installation's own assembly");
                update(firstSource, ONLY_THE_FIRST_RUN);
                awaitCustomer(mongo, first, ONLY_THE_FIRST_RUN, "the first installation's own change");
            }
            var original = stateRows(first);
            assertThat(original).as("the first installation retains actual native state").isNotEmpty();
            assertThat(shapeRows(first)).isNotEmpty();
            assertThat(stateRows(second)).isEmpty();
            assertThat(mongo.documents(EndpointAddress.uri(second.masterUri()), VIEW)).isEmpty();
            try (ServerHandle server = Tiers.IN_PROCESS.launch(second)) {
                start(server, second, secondSource);
                awaitCustomer(mongo, second, SEEDED, "the second installation to serve its own source");
                Document served = only(mongo.documents(EndpointAddress.uri(second.masterUri()), VIEW));
                assertThat(served.getString("customer")).isEqualTo(SEEDED).isNotEqualTo(ONLY_THE_FIRST_RUN);
                assertThat(served.getList("items", Document.class)).extracting(item -> item.getString("sku"))
                        .containsExactlyInAnyOrder("sku-1", "sku-2");
                assertThat(stateRows(second)).isNotEmpty();
                assertThat(shapeRows(second)).isNotEmpty();
            }
            assertThat(stateRows(first)).as("the second installation never mutates the retained first state")
                    .containsExactlyElementsOf(original);
        }
    }

    @Test
    void theSameInstallationRetainsAWaitingChildAcrossAStoppingServer() throws Exception {
        var source = SharedMySql.settings("nest_continuation_source");
        seedChildWaitingForParent(source);
        try (PreparedNativeStore storage = PreparedNativeStore.create(); MongoEndpoints mongo = new MongoEndpoints()) {
            PipelineIntentScope original;
            try (ServerHandle server = Tiers.IN_PROCESS.launch(storage)) {
                ControlPlane control = start(server, storage, source);
                awaitPendingState(storage);
                original = scope(storage);
                assertThat(shapeRows(storage)).isNotEmpty();
                control.stop(PIPELINE, false);
                awaitState(control, PipelineState.STOPPED);
                assertThat(stateRows(storage)).isNotEmpty();
            }
            var held = stateRows(storage);
            var shapes = shapeRows(storage);
            try (ServerHandle server = Tiers.IN_PROCESS.launch(storage)) {
                ControlPlane control = new ControlPlane(server.baseUrl());
                storage.loginAndAwaitBusiness(control);
                assertThat(scope(storage)).isEqualTo(original);
                assertThat(stateRows(storage)).containsExactlyElementsOf(held);
                assertThat(shapeRows(storage)).containsExactlyElementsOf(shapes);
                // No reapply, installation preparation, or connector registration on this server.
                control.lifecycle(PIPELINE, LifecycleVerb.START);
                awaitState(control, PipelineState.RUNNING);
                insertParent(source, "arrived-after-restart");
                Await.until("the retained waiting child to join its parent after restart",
                        () -> hasWaitingChild(mongo, storage),
                        () -> mongo.documents(EndpointAddress.uri(storage.masterUri()), VIEW).toString());
                Document served = only(mongo.documents(EndpointAddress.uri(storage.masterUri()), VIEW));
                assertThat(served.getString("customer")).isEqualTo("arrived-after-restart");
                assertThat(served.getList("items", Document.class)).extracting(item -> item.getString("sku"))
                        .containsExactly("waiting-sku");
                control.stop(PIPELINE, true);
                Await.until("the original installation's Nest payload to be cleared",
                        () -> control.state(PIPELINE).orElse(null) == PipelineState.STOPPED
                                && stateRows(storage).isEmpty() && shapeRows(storage).isEmpty(),
                        () -> "state=" + stateRows(storage) + ", shapes=" + shapeRows(storage));
            }
        }
    }

    @Test
    void copiedStateCannotBeAdoptedUnderItsForeignOriginalIdentity() throws Exception {
        var firstSource = SharedMySql.settings("nest_identity_src_one");
        var secondSource = SharedMySql.settings("nest_identity_src_two");
        seed(firstSource);
        seed(secondSource);
        try (PreparedNativeStore first = PreparedNativeStore.create();
                PreparedNativeStore second = PreparedNativeStore.create();
                MongoEndpoints mongo = new MongoEndpoints()) {
            try (ServerHandle server = Tiers.IN_PROCESS.launch(first)) {
                ControlPlane control = start(server, first, firstSource);
                awaitCustomer(mongo, first, SEEDED, "the original installation's real assembly");
                awaitPendingState(first);
                control.stop(PIPELINE, false);
                awaitState(control, PipelineState.STOPPED);
            }
            try (ServerHandle server = Tiers.IN_PROCESS.launch(second)) {
                ControlPlane control = start(server, second, secondSource);
                awaitCustomer(mongo, second, SEEDED, "the receiving installation's own real assembly");
                awaitPendingState(second);
                control.stop(PIPELINE, false);
                awaitState(control, PipelineState.STOPPED);
            }
            var firstRows = stateRows(first);
            var secondRows = stateRows(second);
            var original = scope(first);
            var receiving = scope(second);
            assertThat(original.clusterId()).isNotEqualTo(receiving.clusterId());
            assertThat(original.cacheIncarnation()).isNotEqualTo(receiving.cacheIncarnation());
            var retained = placement(original, first.cacheDatabase());
            var retargeted = new OperatorPlacement(original, second.cacheDatabase(), retained.resource(),
                    retained.stepId(), retained.pathId(), retained.namespace());
            byte[] copied = firstRows.getFirst().get("state", Binary.class).getData();
            try (MongoCacheDomain origin = cache(first); MongoCacheDomain target = cache(second)) {
                assertThat(origin.operatorState(retained).count(retained.mapName()))
                        .as("the source coordinates really hold a retained assembly").isPositive();
                // Submit copied bytes through the existing guarded port with their unchanged original
                // identity. No database rows are cloned or rewritten as a relocation feature.
                assertThatThrownBy(() -> target.operatorState(retargeted)
                        .save(retargeted.mapName(), "copied-root", copied))
                        .isInstanceOfSatisfying(TapstateException.class,
                                failure -> assertThat(failure.code()).isEqualTo(IoError.CACHE_BUSINESS_FENCED));
            }
            assertThat(stateRows(first)).containsExactlyElementsOf(firstRows);
            assertThat(stateRows(second)).containsExactlyElementsOf(secondRows);
        }
    }

    @Test
    void requiredDeadLettersRemainInConfigurationUntilTheirOriginalPipelineIsRetired() throws Exception {
        var source = SharedMySql.settings("nest_required_diagnostic_source");
        seedChildWaitingForParent(source);
        try (PreparedNativeStore storage = PreparedNativeStore.create()) {
            RequiredDeadLetterContext context;
            RequiredDeadLetterFact fact;
            try (ServerHandle server = Tiers.IN_PROCESS.launch(storage);
                    MongoConfigurationDomain configuration = configuration(storage)) {
                ControlPlane control = start(server, storage, source);
                awaitPendingState(storage);
                var at = placement(scope(storage), storage.cacheDatabase());
                var original = at.scope();
                context = new RequiredDeadLetterContext(original.clusterId(), original.bindingVersion(), PIPELINE,
                        STEP, at.pathId(), original.stateGeneration(), at.mapName());
                fact = new RequiredDeadLetterFact(original.cacheIncarnation(),
                        Map.of("src_children", new ChainPosition(new SourceOrder(1, 1), "1:1")),
                        new NestDeadLetterRecord(context.namespace(), "waiting-child", "src_children", "1:1", 0, 9_000,
                                Map.of("sku", "waiting-sku")));
                assertThat(configuration.requiredDeadLetters().record(context, 0, fact)).isTrue();
                assertThat(configuration.requiredDeadLetters().find(context, "waiting-child").orElseThrow().fact())
                        .isEqualTo(fact);
                assertThat(deadLetterRows(storage)).hasSize(1);
                assertNoCachedDeadLetters(storage);
                control.stop(PIPELINE, false);
                awaitState(control, PipelineState.STOPPED);
                assertThat(configuration.requiredDeadLetters().find(context, "waiting-child")).isPresent();
            }
            try (ServerHandle server = Tiers.IN_PROCESS.launch(storage);
                    MongoConfigurationDomain configuration = configuration(storage)) {
                ControlPlane control = new ControlPlane(server.baseUrl());
                storage.loginAndAwaitBusiness(control);
                var retained = configuration.requiredDeadLetters().find(context, "waiting-child").orElseThrow();
                assertThat(retained.revision()).isEqualTo(1);
                assertThat(retained.fact()).isEqualTo(fact);
                assertNoCachedDeadLetters(storage);
                // Retirement uses the control plane; the test never fabricates a retirement permit.
                control.deleteArtifact(PIPELINE, control.contentHash(PIPELINE));
                Await.until("the retired pipeline's required diagnostics and native payload to be reclaimed",
                        () -> deadLetterRows(storage).isEmpty() && stateRows(storage).isEmpty() && shapeRows(storage).isEmpty(),
                        () -> "diagnostics=" + deadLetterRows(storage) + ", state=" + stateRows(storage));
                assertThat(configuration.requiredDeadLetters().find(context, "waiting-child")).isEmpty();
                assertThatThrownBy(() -> configuration.requiredDeadLetters().record(context, 0, fact))
                        .isInstanceOfSatisfying(TapstateException.class,
                                failure -> assertThat(failure.code()).isEqualTo(IoError.REQUIRED_DEAD_LETTER_FENCED));
            }
        }
    }

    private static ControlPlane start(ServerHandle server, PreparedNativeStore storage, Map<String, Object> source) {
        ControlPlane control = new ControlPlane(server.baseUrl());
        storage.loginAndAwaitBusiness(control);
        control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
        control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
        var resources = new LinkedHashMap<String, String>();
        resources.put("parents.tap.yml", sourceYaml("src_parents", source, PARENTS));
        resources.put("children.tap.yml", sourceYaml("src_children", source, CHILDREN));
        resources.put("pipeline.tap.yml", pipelineYaml());
        control.apply(resources);
        control.discoverSchema("src_parents", "mysql", source);
        control.discoverSchema("src_children", "mysql", source);
        control.lifecycle(PIPELINE, LifecycleVerb.START);
        return control;
    }

    private static MongoConfigurationDomain configuration(PreparedNativeStore storage) {
        return MongoConfigurationDomain.openExisting(new MongoConnectionSettings(
                storage.configurationUri(), null, Duration.ofSeconds(5)));
    }

    private static MongoCacheDomain cache(PreparedNativeStore storage) {
        try (var configuration = configuration(storage)) {
            return MongoCacheDomain.connect(configuration.bindings().load().orElseThrow().cache(),
                    Duration.ofSeconds(5), "native-nest-witness");
        }
    }

    private static PipelineIntentScope scope(PreparedNativeStore storage) {
        try (var configuration = configuration(storage)) {
            var business = configuration.bindings().cachePreparation().orElseThrow().businessScope();
            var mapping = configuration.pipelineStates().read(PIPELINE).orElseThrow();
            return new PipelineIntentScope(business.clusterId(), business.bindingVersion(), business.cacheIncarnation(),
                    PIPELINE, mapping.stateGeneration());
        }
    }

    private static OperatorPlacement placement(PipelineIntentScope scope, String database) {
        return OperatorMapCoordinates.nest(scope, database, STEP, List.of(), OperatorMapCoordinates.NestMap.STATE, null);
    }

    private static List<Document> stateRows(PreparedNativeStore storage) {
        return rows(storage.cacheUri(), storage.cacheDatabase(), SystemCollections.CACHE_NEST_STATE,
                new Document("_id.placement.scope.pipelineId", PIPELINE));
    }

    private static List<Document> shapeRows(PreparedNativeStore storage) {
        return rows(storage.cacheUri(), storage.cacheDatabase(), SystemCollections.CACHE_NEST_SHAPES,
                new Document("_id.scope.pipelineId", PIPELINE));
    }

    private static List<Document> deadLetterRows(PreparedNativeStore storage) {
        return rows(storage.configurationUri(), storage.configurationDatabase(), SystemCollections.CONFIG_NEST_DEAD_LETTERS,
                new Document("_id.pipelineId", PIPELINE));
    }

    private static List<Document> rows(String uri, String database, SystemCollections table, Document filter) {
        try (var client = MongoClients.create(uri)) {
            return table.on(client.getDatabase(database)).find(filter).sort(new Document("_id", 1)).into(new ArrayList<>());
        }
    }

    private static void assertNoCachedDeadLetters(PreparedNativeStore storage) {
        try (var client = MongoClients.create(storage.cacheUri())) {
            assertThat(client.getDatabase(storage.cacheDatabase()).listCollectionNames().into(new ArrayList<>()))
                    .doesNotContain(SystemCollections.CONFIG_NEST_DEAD_LETTERS.collectionName());
        }
    }

    private static void awaitPendingState(PreparedNativeStore storage) {
        Await.until("the native Nest to retain its real state", () -> !stateRows(storage).isEmpty(),
                () -> stateRows(storage).toString());
    }

    private static void awaitState(ControlPlane control, PipelineState expected) {
        Await.until("the pipeline to reach " + expected, () -> control.state(PIPELINE).orElse(null) == expected,
                () -> control.state(PIPELINE).toString());
    }

    private static void awaitCustomer(MongoEndpoints mongo, PreparedNativeStore storage, String expected, String what) {
        Await.until(what, () -> {
            var rows = mongo.documents(EndpointAddress.uri(storage.masterUri()), VIEW);
            return rows.size() == 1 && expected.equals(rows.getFirst().getString("customer"));
        }, () -> mongo.documents(EndpointAddress.uri(storage.masterUri()), VIEW).toString());
    }

    private static boolean hasWaitingChild(MongoEndpoints mongo, PreparedNativeStore storage) {
        var rows = mongo.documents(EndpointAddress.uri(storage.masterUri()), VIEW);
        if (rows.size() != 1) return false;
        var items = rows.getFirst().getList("items", Document.class);
        return items != null && items.size() == 1 && "waiting-sku".equals(items.getFirst().getString("sku"));
    }

    private static Document only(List<Document> rows) {
        assertThat(rows).hasSize(1);
        return rows.getFirst();
    }

    private static void seed(Map<String, Object> settings) throws Exception {
        try (Connection connection = SharedMySql.connect(settings); Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS " + CHILDREN);
            statement.execute("DROP TABLE IF EXISTS " + PARENTS);
            statement.execute("CREATE TABLE " + PARENTS + " (id INT PRIMARY KEY, customer VARCHAR(64))");
            statement.execute("CREATE TABLE " + CHILDREN + " (id INT PRIMARY KEY, order_id INT, sku VARCHAR(64))");
            statement.execute("INSERT INTO " + PARENTS + " (id, customer) VALUES (1, '" + SEEDED + "')");
            statement.execute("INSERT INTO " + CHILDREN + " (id, order_id, sku) VALUES (1, 1, 'sku-1'), (2, 1, 'sku-2')");
        }
    }

    private static void seedChildWaitingForParent(Map<String, Object> settings) throws Exception {
        try (Connection connection = SharedMySql.connect(settings); Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS " + CHILDREN);
            statement.execute("DROP TABLE IF EXISTS " + PARENTS);
            statement.execute("CREATE TABLE " + PARENTS + " (id INT PRIMARY KEY, customer VARCHAR(64))");
            statement.execute("CREATE TABLE " + CHILDREN + " (id INT PRIMARY KEY, order_id INT, sku VARCHAR(64))");
            statement.execute("INSERT INTO " + CHILDREN + " (id, order_id, sku) VALUES (1, 1, 'waiting-sku')");
        }
    }

    private static void insertParent(Map<String, Object> settings, String customer) throws Exception {
        try (Connection connection = SharedMySql.connect(settings); Statement statement = connection.createStatement()) {
            statement.execute("INSERT INTO " + PARENTS + " (id, customer) VALUES (1, '" + customer + "')");
        }
    }

    private static void update(Map<String, Object> settings, String customer) throws Exception {
        try (Connection connection = SharedMySql.connect(settings); Statement statement = connection.createStatement()) {
            statement.execute("UPDATE " + PARENTS + " SET customer = '" + customer + "' WHERE id = 1");
        }
    }

    private static String sourceYaml(String id, Map<String, Object> config, String table) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mysql
                config: { host: %s, port: %s, database: %s, username: %s, password: %s }
                mode: cdc
                tables: [ %s ]
                """.formatted(id, config.get("host"), config.get("port"), config.get("database"),
                        config.get("username"), config.get("password"), table);
    }

    private static String pipelineYaml() {
        return """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: [ src_parents, src_children ]
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
                view:
                  id: %s
                  from: order_doc
                  primary_key: id
                """.formatted(PIPELINE, VIEW);
    }
}
