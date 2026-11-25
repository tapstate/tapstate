package io.tapstate.adapters.mongostore;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ServerId;
import com.mongodb.event.ClusterClosedEvent;
import com.mongodb.event.ClusterListener;
import com.mongodb.event.ClusterOpeningEvent;
import com.mongodb.event.ConnectionPoolClosedEvent;
import com.mongodb.event.ConnectionPoolCreatedEvent;
import com.mongodb.event.ConnectionPoolListener;
import io.tapstate.adapters.mongostore.migration.MigrationRunner;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** A real shared server still gives each case its own stores, indexes, migrations and cleanup. */
@RequiresDocker
class SharedMongoCaseIT {

    @RegisterExtension
    static final SharedMongoCase MONGO = SharedMongoCase.forSuite(SharedMongoCaseIT.class);

    private ExtensionContext invocationContext;

    @RegisterExtension
    private final BeforeEachCallback captureContext = context -> invocationContext = context;

    private static final DslParser PARSER = new DslParser();
    private static final String PIPELINE = "same-pipeline";
    private static final String CHAIN = "orders@mysql-1";
    private static final String NAMESPACE = "same-namespace";
    private static final String KEY = "same-key";
    private static final Instant CREATED = Instant.parse("2026-07-06T00:00:00Z");
    private static final String ORDERS = """
            version: tapstate/v1
            kind: source
            id: orders
            connector: mysql
            config:
              host: localhost
            """;

    @Test
    void fencingAndDefinitionCasesWriteToTheScopeThatTeardownActuallyDrops() throws Exception {
        var invocations = List.of(
                new ScopedInvocation(MongoStateStoreIT.class,
                        "anOldExecutionCannotCompleteTheUnchangedCheckpointOfItsReplacement", "fenced_completion"),
                new ScopedInvocation(MongoStateStoreIT.class,
                        "provingTheClaimAndWritingCompletionCannotBeSeparatedByAReplacement", "atomic_completion"),
                new ScopedInvocation(MongoPipelineDraftStoreIT.class,
                        "changesTheDefinitionAndTheDraftBasedOnItInOneTransaction", "tapstate_definition_change_it"),
                new ScopedInvocation(MongoPipelineDraftStoreIT.class,
                        "leavesADraftBasedOnAnotherDefinitionAsItIs", "tapstate_definition_change_behind_it"),
                new ScopedInvocation(MongoPipelineDraftStoreIT.class,
                        "refusesADefinitionThatMovedOrADependencyThatMovedAndWritesNothing", "tapstate_definition_change_conflict_it"));
        try (MongoClient observer = MongoClients.create(MONGO.getReplicaSetUrl())) {
            List<org.junit.jupiter.api.function.Executable> controls = new ArrayList<>();
            for (ScopedInvocation invocation : invocations) {
                controls.add(() -> assertInvocationDatabaseIsReleased(invocation, observer));
            }
            org.junit.jupiter.api.Assertions.assertAll("shared database ownership", controls);
        }
    }

    private void assertInvocationDatabaseIsReleased(ScopedInvocation invocation, MongoClient observer) throws Exception {
        var field = invocation.suite().getDeclaredField("REPLICA_SET");
        field.setAccessible(true);
        SharedMongoCase fixture = (SharedMongoCase) field.get(null);
        String owner = invocationContext.getUniqueId() + "/cleanup-control/" + invocation.method();
        ExtensionContext context = (ExtensionContext) java.lang.reflect.Proxy.newProxyInstance(
                ExtensionContext.class.getClassLoader(), new Class<?>[] { ExtensionContext.class },
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "getUniqueId" -> owner;
                    case "getRoot" -> invocationContext.getRoot();
                    default -> throw new AssertionError("Unexpected scope context access: " + method.getName());
                });
        fixture.beforeEach(context);
        String physical = fixture.database(invocation.database());
        List<String> collections;
        try (AutoCloseable teardown = () -> fixture.afterEach(context)) {
            invocation.suite().getDeclaredMethod(invocation.method())
                    .invoke(invocation.suite().getDeclaredConstructor().newInstance());
            collections = observer.getDatabase(physical).listCollectionNames().into(new ArrayList<>());
        }
        List<String> remaining = observer.listDatabaseNames().into(new ArrayList<>());
        org.junit.jupiter.api.Assertions.assertAll(
                () -> assertThat(collections)
                        .as("the actual %s body wrote to its registered case database", invocation.method()).isNotEmpty(),
                () -> assertThat(remaining)
                        .as("teardown removed the actual database used by %s", invocation.method())
                        .isNotEmpty().doesNotContain(physical, invocation.database()));
    }

    private record ScopedInvocation(Class<?> suite, String method, String database) { }

    @Test
    void anotherInvocationsTeardownCannotReleaseTheLiveCasesDatabase() {
        String ownedUri = MONGO.getReplicaSetUrl("foreign_teardown");
        String ownedDatabase = MONGO.database("foreign_teardown");
        String foreignId = "unrelated-teardown:" + java.util.UUID.randomUUID();
        org.junit.jupiter.api.extension.ExtensionContext foreign =
                (org.junit.jupiter.api.extension.ExtensionContext) java.lang.reflect.Proxy.newProxyInstance(
                        org.junit.jupiter.api.extension.ExtensionContext.class.getClassLoader(),
                        new Class<?>[] { org.junit.jupiter.api.extension.ExtensionContext.class },
                        (proxy, method, arguments) -> {
                            if (method.getName().equals("getUniqueId") && method.getParameterCount() == 0) {
                                return foreignId;
                            }
                            throw new AssertionError("Unexpected teardown context access: " + method.getName());
                        });

        try (MongoClient raw = MongoClients.create(ownedUri)) {
            var markers = raw.getDatabase(ownedDatabase).getCollection("ownership_marker");
            markers.insertOne(new Document("_id", "live-case").append("value", "owned"));

            MONGO.afterEach(foreign);
            assertThatCode(() -> MONGO.getReplicaSetUrl("foreign_teardown")).doesNotThrowAnyException();

            assertThat(MONGO.getReplicaSetUrl("foreign_teardown")).isEqualTo(ownedUri);
            assertThat(MONGO.database("foreign_teardown")).isEqualTo(ownedDatabase);
            assertThat(markers.find(new Document("_id", "live-case")).first())
                    .containsEntry("value", "owned");
            assertThat(markers.countDocuments()).isEqualTo(1);
        }
    }

    @Test
    void aliasesKeepTheDefaultDatabaseAndTheRealServerWhileSeparatingLogicalDatabases() {
        String uri = MONGO.getReplicaSetUrl();
        String control = MONGO.database("test");
        String operator = MONGO.database("tapstate_nest");
        String routed = MONGO.database("operator_route");

        assertThat(new ConnectionString(uri).getDatabase()).isEqualTo(control);
        assertThat(uri).isEqualTo(MONGO.getReplicaSetUrl("test"));
        assertThat(MONGO.getReplicaSetUrl(control)).isEqualTo(uri);
        assertThat(MONGO.database(control)).isEqualTo(control);
        assertThat(List.of(control, operator, routed)).doesNotHaveDuplicates();
        assertThat(new ConnectionString(MONGO.getReplicaSetUrl("tapstate_nest")).getDatabase())
                .isEqualTo(operator);
        assertThat(new ConnectionString(MONGO.getReplicaSetUrl("operator_route")).getDatabase())
                .isEqualTo(routed);
        assertSameServer(uri, MONGO.getReplicaSetUrl("tapstate_nest"));
        assertSameServer(uri, MONGO.getReplicaSetUrl("operator_route"));

        for (String reserved : List.of("admin", "config", "local", "ADMIN", "CONFIG", "LOCAL")) {
            assertThatThrownBy(() -> MONGO.database(reserved))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> MONGO.database("invalid/name"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void freshCasesMigrateAndIndexTheirOwnDatabasesWithoutChangingALiveCase() {
        try (SharedMongoCase.Scope first = MONGO.additionalScope()) {
            String firstUri = first.uri("tapstate");
            String firstControl = first.database("tapstate");
            String firstOperator = first.database("tapstate_nest");
            String firstRouted = first.database("operator_route");
            try (MongoConnection firstConnection = connection(firstUri);
                 MongoClient firstRaw = MongoClients.create(firstUri)) {
                firstConnection.verify();
                MongoStorePort firstPort = new MongoStorePort(firstConnection, firstOperator);
                writeSameBusinessIdentity(firstPort, firstRouted, "first");
                MongoDatabase firstDatabase = firstRaw.getDatabase(firstControl);
                firstDatabase.getCollection("items").insertOne(new Document("_id", "same-id").append("case", "first"));
                firstDatabase.getCollection("items").createIndex(new Document("case", 1),
                        new IndexOptions().name("case_marker"));
                assertCurrentMigrationAndIndexes(firstDatabase);

                List<String> secondDatabases;
                try (SharedMongoCase.Scope second = MONGO.additionalScope()) {
                    String secondUri = second.uri("tapstate");
                    String secondControl = second.database("tapstate");
                    String secondOperator = second.database("tapstate_nest");
                    String secondRouted = second.database("operator_route");
                    secondDatabases = List.of(secondControl, secondOperator, secondRouted);
                    assertThat(secondDatabases).doesNotContain(firstControl, firstOperator, firstRouted);
                    assertSameServer(firstUri, secondUri);
                    try (MongoConnection secondConnection = connection(secondUri);
                         MongoClient secondRaw = MongoClients.create(secondUri)) {
                        MongoDatabase secondDatabase = secondRaw.getDatabase(secondControl);
                        assertThat(MigrationRunner.inspect(secondDatabase).installed()).isZero();
                        assertThat(secondDatabase.listCollectionNames().into(new ArrayList<>())).isEmpty();

                        secondConnection.verify();
                        MongoStorePort secondPort = new MongoStorePort(secondConnection, secondOperator);
                        assertEmptyBusinessIdentity(secondPort, secondRouted);
                        assertThat(secondDatabase.getCollection("items").countDocuments()).isZero();
                        assertCurrentMigrationAndIndexes(secondDatabase);

                        writeSameBusinessIdentity(secondPort, secondRouted, "second");
                        secondDatabase.getCollection("items").insertOne(
                                new Document("_id", "same-id").append("case", "second"));
                        secondDatabase.getCollection("items").createIndex(new Document("case", 1),
                                new IndexOptions().name("case_marker"));
                        assertBusinessIdentity(secondPort, secondRouted, "second");
                        assertBusinessIdentity(firstPort, firstRouted, "first");
                        assertThat(firstDatabase.getCollection("items").find(new Document("_id", "same-id"))
                                .first()).containsEntry("case", "first");
                    }
                }

                assertThat(firstRaw.listDatabaseNames().into(new ArrayList<>()))
                        .contains(firstControl, firstOperator, firstRouted)
                        .doesNotContainAnyElementsOf(secondDatabases);
                assertBusinessIdentity(firstPort, firstRouted, "first");
                assertCurrentMigrationAndIndexes(firstDatabase);
                assertThat(firstDatabase.getCollection("items").listIndexes().into(new ArrayList<>()))
                        .extracting(index -> index.getString("name")).contains("case_marker");
            }
        }
    }

    @Test
    void aFailedCaseDropsOnlyItsOwnedDatabasesAndCannotReuseItsReleasedScope() {
        try (SharedMongoCase.Scope survivor = MONGO.additionalScope()) {
            String survivorUri = survivor.uri("tapstate");
            String survivorControl = survivor.database("tapstate");
            String survivorOperator = survivor.database("tapstate_nest");
            String survivorRouted = survivor.database("operator_route");
            try (MongoConnection survivorConnection = connection(survivorUri);
                 MongoClient survivorRaw = MongoClients.create(survivorUri)) {
                survivorConnection.verify();
                MongoStorePort survivorPort = new MongoStorePort(survivorConnection, survivorOperator);
                writeSameBusinessIdentity(survivorPort, survivorRouted, "survivor");

                SharedMongoCase.Scope failed = MONGO.additionalScope();
                String failedUri = failed.uri("tapstate");
                String failedControl = failed.database("tapstate");
                String failedOperator = failed.database("tapstate_nest");
                String failedRouted = failed.database("operator_route");
                RuntimeException intendedFailure = new IllegalStateException("the case failed after its writes");
                assertThatThrownBy(() -> {
                    try (SharedMongoCase.Scope ignored = failed;
                         MongoConnection failedConnection = connection(failedUri);
                         MongoClient failedRaw = MongoClients.create(failedUri)) {
                        failedConnection.verify();
                        MongoStorePort failedPort = new MongoStorePort(failedConnection, failedOperator);
                        writeSameBusinessIdentity(failedPort, failedRouted, "failed");
                        assertBusinessIdentity(failedPort, failedRouted, "failed");
                        assertCurrentMigrationAndIndexes(failedRaw.getDatabase(failedControl));
                        throw intendedFailure;
                    }
                }).isSameAs(intendedFailure).satisfies(thrown ->
                        assertThat(thrown.getSuppressed()).as("cleanup failures were not hidden").isEmpty());

                assertThat(survivorRaw.listDatabaseNames().into(new ArrayList<>()))
                        .contains(survivorControl, survivorOperator, survivorRouted)
                        .doesNotContain(failedControl, failedOperator, failedRouted);
                assertBusinessIdentity(survivorPort, survivorRouted, "survivor");
                assertCurrentMigrationAndIndexes(survivorRaw.getDatabase(survivorControl));
                assertThatThrownBy(() -> failed.database("tapstate"))
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessage("The database scope has been released");
                assertThatThrownBy(() -> failed.uri("tapstate"))
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessage("The database scope has been released");
                failed.close();
                assertBusinessIdentity(survivorPort, survivorRouted, "survivor");
            }
        }
    }

    @Test
    void aRealRawClientClosesItsOwnClusterAndPoolsAndRejectsFurtherOperations() throws Exception {
        String uri = MONGO.getReplicaSetUrl("client_lifecycle");
        String database = MONGO.database("client_lifecycle");
        ClientLifecycle events = new ClientLifecycle();
        MongoClient client = MongoClients.create(MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(uri))
                .applyToClusterSettings(cluster -> cluster.addClusterListener(events))
                .applyToConnectionPoolSettings(pool -> pool.addConnectionPoolListener(events))
                .build());
        try (client) {
            client.getDatabase(database).getCollection("lifecycle")
                    .insertOne(new Document("_id", "same-id"));
            assertThat(client.getDatabase(database).getCollection("lifecycle").countDocuments()).isEqualTo(1);
            assertThat(events.openedClusters).hasSize(1);
            assertThat(events.pools).isNotEmpty();
        }

        assertThat(events.clusterClosed.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(events.closedClusters).containsExactlyInAnyOrderElementsOf(events.openedClusters);
        for (Map.Entry<ServerId, CountDownLatch> pool : events.pools.entrySet()) {
            assertThat(events.openedClusters).contains(pool.getKey().getClusterId());
            assertThat(pool.getValue().await(5, TimeUnit.SECONDS))
                    .as("the owned pool closed: %s", pool.getKey()).isTrue();
        }
        assertThatThrownBy(() -> client.getDatabase(database).runCommand(new Document("ping", 1)))
                .isInstanceOf(IllegalStateException.class);
    }

    private static MongoConnection connection(String uri) {
        return new MongoConnection(new MongoConnectionSettings(uri, null, Duration.ofSeconds(5)));
    }

    private static void assertSameServer(String firstUri, String secondUri) {
        ConnectionString first = new ConnectionString(firstUri);
        ConnectionString second = new ConnectionString(secondUri);
        assertThat(second.getHosts()).isEqualTo(first.getHosts());
        assertThat(second.getRequiredReplicaSetName()).isEqualTo(first.getRequiredReplicaSetName());
        assertThat(second.isDirectConnection()).isEqualTo(first.isDirectConnection());
    }

    private static void writeSameBusinessIdentity(MongoStorePort port, String routedDatabase, String value) {
        port.artifacts().save(PARSER.parse(ORDERS));
        port.state().create(PIPELINE, "{\"case\":\"" + value + "\"}", CREATED);
        port.meta().create(CHAIN, "7d");
        port.keyedState().save(NAMESPACE, KEY, value.getBytes(StandardCharsets.UTF_8));
        port.operatorStateStores().inDatabase(routedDatabase).state()
                .save(NAMESPACE, KEY, (value + "-routed").getBytes(StandardCharsets.UTF_8));
    }

    private static void assertEmptyBusinessIdentity(MongoStorePort port, String routedDatabase) {
        assertThat(port.artifacts().get("orders")).isEmpty();
        assertThat(port.state().read(PIPELINE)).isEmpty();
        assertThat(port.meta().read(CHAIN)).isEmpty();
        assertThat(port.keyedState().load(NAMESPACE, KEY)).isEmpty();
        assertThat(port.operatorStateStores().inDatabase(routedDatabase).state().load(NAMESPACE, KEY)).isEmpty();
    }

    private static void assertBusinessIdentity(MongoStorePort port, String routedDatabase, String value) {
        assertThat(port.artifacts().get("orders")).isPresent();
        assertThat(port.state().read(PIPELINE)).hasValueSatisfying(checkpoint ->
                assertThat(checkpoint.stateJson()).isEqualTo("{\"case\":\"" + value + "\"}"));
        assertThat(port.meta().read(CHAIN)).isPresent();
        assertThat(port.keyedState().load(NAMESPACE, KEY).orElseThrow())
                .isEqualTo(value.getBytes(StandardCharsets.UTF_8));
        assertThat(port.operatorStateStores().inDatabase(routedDatabase).state().load(NAMESPACE, KEY).orElseThrow())
                .isEqualTo((value + "-routed").getBytes(StandardCharsets.UTF_8));
    }

    private static void assertCurrentMigrationAndIndexes(MongoDatabase database) {
        MigrationRunner.Status status = MigrationRunner.inspect(database);
        assertThat(status.installed()).isEqualTo(MigrationRunner.SUPPORTED_VERSION);
        assertThat(status.pending()).isEmpty();
        for (SystemCollections collection : SystemCollections.values()) {
            if (collection.database() != SystemCollections.Database.STORE) continue;
            List<Document> indexes = collection.indexTargetOn(database).listIndexes().into(new ArrayList<>());
            for (SystemCollections.IndexSpec expected : collection.indexes()) {
                Document actual = indexes.stream()
                        .filter(index -> expected.indexName().equals(index.getString("name")))
                        .findFirst().orElseThrow(() -> new AssertionError("Missing index " + expected.indexName()));
                Document keys = new Document();
                expected.keys().forEach(key -> keys.append(key, 1));
                assertThat(actual.get("key", Document.class)).isEqualTo(keys);
                assertThat(Boolean.TRUE.equals(actual.getBoolean("unique"))).isEqualTo(expected.unique());
                if (expected.expireAfterSeconds() != null) {
                    assertThat(((Number) actual.get("expireAfterSeconds")).longValue())
                            .isEqualTo(expected.expireAfterSeconds().longValue());
                }
            }
        }
    }

    /** Observes only the newly created control client, never another case using the same server. */
    private static final class ClientLifecycle implements ClusterListener, ConnectionPoolListener {
        private final Set<ClusterId> openedClusters = ConcurrentHashMap.newKeySet();
        private final Set<ClusterId> closedClusters = ConcurrentHashMap.newKeySet();
        private final Map<ServerId, CountDownLatch> pools = new ConcurrentHashMap<>();
        private final CountDownLatch clusterClosed = new CountDownLatch(1);

        @Override
        public void clusterOpening(ClusterOpeningEvent event) {
            openedClusters.add(event.getClusterId());
        }

        @Override
        public void clusterClosed(ClusterClosedEvent event) {
            closedClusters.add(event.getClusterId());
            clusterClosed.countDown();
        }

        @Override
        public void connectionPoolCreated(ConnectionPoolCreatedEvent event) {
            pools.computeIfAbsent(event.getServerId(), ignored -> new CountDownLatch(1));
        }

        @Override
        public void connectionPoolClosed(ConnectionPoolClosedEvent event) {
            pools.computeIfAbsent(event.getServerId(), ignored -> new CountDownLatch(1)).countDown();
        }
    }
}
