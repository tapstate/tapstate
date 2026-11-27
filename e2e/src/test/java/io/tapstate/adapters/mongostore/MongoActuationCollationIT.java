package io.tapstate.adapters.mongostore;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandSucceededEvent;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.PipelineIntentStore;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.spi.store.StorageSetupScope;
import io.tapstate.testsupport.RequiresDocker;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

@RequiresDocker
class MongoActuationCollationIT {
    private static final String INTENTS = SystemCollections.PIPELINE_INTENTS.collectionName();

    @Container
    private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
            .withCommand("--replSet", "docker-rs", "--bind_ip_all", "--setParameter", "enableTestCommands=1")
            .withTmpFs(Map.of("/data/db", "rw", "/data/configdb", "rw"));

    @Test void aDelayedAuditDoesNotReopenBatchReadsAfterANonSimpleCollationIsObserved() throws Exception {
        var metadataReceived = new CountDownLatch(1);
        var resumeFirstAudit = new CountDownLatch(1);
        var scans = new AtomicInteger();
        var pauseFirstResponse = new CommandListener() {
            @Override public void commandSucceeded(CommandSucceededEvent event) {
                if (!event.getCommandName().equals("listCollections") || scans.incrementAndGet() != 1) return;
                metadataReceived.countDown();
                try {
                    assertThat(resumeFirstAudit.await(5, TimeUnit.SECONDS)).as("release first metadata response").isTrue();
                } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(failure);
                }
            }
        };
        try (var client = observedClient(pauseFirstResponse); var worker = Executors.newSingleThreadExecutor()) {
            var database = client.getDatabase("actuation_collation_race");
            database.createCollection(INTENTS);
            var store = intentStore(client, database);
            var delayed = worker.submit(() -> intentCommands(store));
            try {
                assertThat(metadataReceived.await(5, TimeUnit.SECONDS)).as("first metadata response arrived").isTrue();
                SystemCollections.PIPELINE_INTENTS.on(database).drop();
                database.createCollection(INTENTS, new CreateCollectionOptions().collation(
                        Collation.builder().locale("en").build()));
                assertThat(intentCommands(store)).as("new read sees the non-simple collation").containsExactly("find");
            } finally {
                resumeFirstAudit.countDown();
            }
            var delayedCommands = delayed.get(5, TimeUnit.SECONDS);
            assertThat(intentCommands(store)).as("read after the delayed audit").containsExactly("find");
            assertThat(delayedCommands).as("read whose audit was delayed").containsExactly("find");
            assertThat(scans).hasValue(2);
        }
    }

    @Test void simpleCollationStillUsesTheBatchedRead() {
        try (var client = observedClient()) {
            var database = client.getDatabase("actuation_simple_collation");
            database.createCollection(INTENTS);
            assertThat(intentCommands(intentStore(client, database))).containsExactly("aggregate");
        }
    }

    @Test void aDeniedCollationAuditStillReadsTheIntentCollection() {
        var auditFailures = new ArrayList<Throwable>();
        var observeDenial = new CommandListener() {
            @Override public void commandFailed(CommandFailedEvent event) {
                if (event.getCommandName().equals("listCollections")) auditFailures.add(event.getThrowable());
            }
        };
        try (var client = observedClient(observeDenial)) {
            var database = client.getDatabase("actuation_denied_collation_audit");
            database.createCollection(INTENTS);
            var store = intentStore(client, database);
            client.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand")
                    .append("mode", new Document("times", 1))
                    .append("data", new Document("failCommands", List.of("listCollections")).append("errorCode", 13)));

            assertThat(intentCommands(store)).containsExactly("find");
            assertThat(auditFailures).singleElement().isInstanceOfSatisfying(MongoCommandException.class,
                    failure -> assertThat(failure.getErrorCode()).isEqualTo(13));
        }
    }

    @Test void executionBindingsStillUseTheBatchedReadWithANonSimpleCollation() {
        String databaseName = "execution_bindings_collation";
        var settings = new MongoConnectionSettings(MONGO.getReplicaSetUrl(databaseName), null, Duration.ofSeconds(5));
        try (var client = observedClient();
             var domain = MongoConfigurationBootstrap.prepare(settings, "cluster", "installation", "setup", 1)) {
            var database = client.getDatabase(databaseName);
            SystemCollections.PIPELINE_INTENTS.on(database).drop();
            database.createCollection(INTENTS, new CreateCollectionOptions().collation(
                    Collation.builder().locale("en").build()));
            var cache = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB,
                    "mongodb://cache.invalid/cache", "cache", null);
            var target = new SourceResource(StorageBinding.defaultSourceId(StorageBinding.Mode.OP), null, "mongodb",
                    Map.of("uri", "mongodb://master.invalid/master"), null, null, null, null);
            domain.bindings().commit(new StorageSetupScope("cluster", "installation", "setup", 1),
                    StorageBinding.Mode.OP, cache, target);
            var pipeline = new DslParser().parse("version: tapstate/v1\nkind: pipeline\nid: flow\nsource: []\n");
            domain.artifacts().create(pipeline);
            var business = domain.bindings().cachePreparation().orElseThrow().businessScope();
            var scope = new PipelineIntentScope(business.clusterId(), business.bindingVersion(),
                    business.cacheIncarnation(), "flow", 1);
            var intents = domain.intents();
            var desired = new DesiredState("flow", PipelineState.STOPPED, CanonicalHash.of(pipeline),
                    false, null, false, null);
            assertThat(intents.compareAndSet(scope, 0, desired)).isTrue();
            var saved = intents.load("flow").orElseThrow();
            var bindings = intents.connectorBindings().seed(saved, List.of());

            var commands = new ArrayList<String>();
            MongoCommandObservation.COLLECTIONS.set((command, collection) -> commands.add(command + ":" + collection));
            try {
                assertThat(intents.executionBindings(scope))
                        .contains(new PipelineIntentStore.ExecutionBindings(bindings, saved.intentRevision()));
                assertThat(commands).containsExactly("aggregate:" + SystemCollections.CONFIG_CLUSTER_IDENTITY.collectionName());
            } finally {
                MongoCommandObservation.COLLECTIONS.remove();
            }
        }
    }

    private static MongoClient observedClient(CommandListener... listeners) {
        var settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl()))
                .addCommandListener(MongoCommandObservation.LISTENER);
        for (var listener : listeners) settings.addCommandListener(listener);
        return MongoClients.create(settings.build());
    }

    private static PipelineIntentStore intentStore(MongoClient client, MongoDatabase database) {
        var ciphers = SourceConfigCipherProvider.fixed(new SourceConfigCipher(new byte[32]));
        return new MongoPipelineIntentStore(client, database, "cluster",
                new MongoStorageBindingStore(client, database, ciphers));
    }

    private static List<String> intentCommands(PipelineIntentStore store) {
        var commands = new ArrayList<String>();
        MongoCommandObservation.COLLECTIONS.set((command, collection) -> {
            if (INTENTS.equals(collection)) commands.add(command);
        });
        try {
            assertThat(store.readForActuation("flow")).isEmpty();
            return commands;
        } finally {
            MongoCommandObservation.COLLECTIONS.remove();
        }
    }
}
