package io.tapstate.adapters.mongostore;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClients;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import io.tapstate.testsupport.RequiresDocker;
import io.tapstate.spi.store.StorageBinding;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

@RequiresDocker
class MongoConfigurationBootstrapOptionsIT {
    @Container
    private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:8.0"));

    @ParameterizedTest
    @ValueSource(strings = {"", "w=majority", "retryWrites=true&w=majority", "w=1"})
    void explicitConnectionWriteConcernDoesNotPreventAtomicFirstConfiguration(String options) {
        String database = "bootstrap_options_" + UUID.randomUUID().toString().replace("-", "");
        String uri = MONGO.getReplicaSetUrl(database);
        if (!options.isEmpty()) uri += (uri.contains("?") ? "&" : "?") + options;
        var settings = new MongoConnectionSettings(uri, null, Duration.ofSeconds(5));
        try (var domain = MongoConfigurationBootstrap.prepare(settings, "cluster", "installation", "operation", 1)) {
            assertThat(domain.clusterId()).isEqualTo("cluster");
            assertThat(domain.setupStates().load().phase()).isEqualTo(io.tapstate.spi.store.StorageSetupStateStore.Phase.CONFIG_PREPARED);
        }
    }

    @Test
    void transactionalSchemaCommandsOmitPerCommandConcernWhileCommitRemainsDurable() {
        String database = "bootstrap_wire_" + UUID.randomUUID().toString().replace("-", "");
        String uri = MONGO.getReplicaSetUrl(database);
        uri += (uri.contains("?") ? "&" : "?") + "w=majority";
        List<BsonDocument> commands = new CopyOnWriteArrayList<>();
        var options = MongoClientSettings.builder().applyConnectionString(new ConnectionString(uri))
                .addCommandListener(new CommandListener() {
                    @Override public void commandStarted(CommandStartedEvent event) {
                        if (List.of("create", "createIndexes", "commitTransaction").contains(event.getCommandName()))
                            commands.add(event.getCommand().clone());
                    }
                }).build();
        try (var client = MongoClients.create(options); var session = client.startSession()) {
            var target = client.getDatabase(database);
            Throwable failure = catchThrowable(() -> session.withTransaction(() -> {
                SystemCollections.CAPTURE_PREPARATIONS.prepareCollectionOn(target, session);
                for (var index : SystemCollections.CAPTURE_PREPARATIONS.indexes())
                    IndexEnsure.createInTransaction(session, SystemCollections.CAPTURE_PREPARATIONS.on(target), index);
                return null;
            }, TransactionOptions.builder().readConcern(ReadConcern.LOCAL)
                    .writeConcern(WriteConcern.MAJORITY.withJournal(true)).build()));
            assertThat(failure).as("schema commands must be accepted inside the original transaction").isNull();
            assertThat(commands).filteredOn(command -> command.containsKey("create") || command.containsKey("createIndexes"))
                    .hasSize(2).allSatisfy(command -> {
                        assertThat(command.containsKey("writeConcern")).isFalse();
                        assertThat(command.getBoolean("autocommit").getValue()).isFalse();
                    });
            assertThat(commands).filteredOn(command -> command.containsKey("commitTransaction")).singleElement()
                    .satisfies(command -> {
                        var concern = command.getDocument("writeConcern");
                        assertThat(concern.getString("w").getValue()).isEqualTo("majority");
                        assertThat(concern.getBoolean("j").getValue()).isTrue();
                    });
            assertThat(target.getWriteConcern()).isEqualTo(WriteConcern.MAJORITY);
        }
    }

    @Test
    void gridFsSchemaCreationUsesTheSameTransactionConcernBoundary() {
        String database = "gridfs_options_" + UUID.randomUUID().toString().replace("-", "");
        String uri = MONGO.getReplicaSetUrl(database);
        uri += (uri.contains("?") ? "&" : "?") + "w=majority";
        try (var client = MongoClients.create(uri); var session = client.startSession()) {
            var target = client.getDatabase(database);
            session.withTransaction(() -> {
                SystemCollections.CACHE_CONNECTOR_ARTIFACTS.prepareGridFsCollectionsOn(target, session);
                return null;
            }, TransactionOptions.builder().readConcern(ReadConcern.LOCAL)
                    .writeConcern(WriteConcern.MAJORITY.withJournal(true)).build());
            assertThat(target.listCollectionNames().into(new ArrayList<>()))
                    .containsExactlyInAnyOrderElementsOf(SystemCollections.CACHE_CONNECTOR_ARTIFACTS.physicalNames());
            assertThat(target.getWriteConcern()).isEqualTo(WriteConcern.MAJORITY);
        }
    }

    @Test
    void connectionTestingAcceptsExplicitConcernAndLeavesNoProbeNamespace() {
        String database = "probe_options_" + UUID.randomUUID().toString().replace("-", "");
        String uri = MONGO.getReplicaSetUrl(database);
        uri += (uri.contains("?") ? "&" : "?") + "w=majority";
        var probe = new MongoStorageConnectionProbe(Duration.ofSeconds(5));
        var connection = probe.normalize(new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, uri, database, null));
        assertThat(connection.uri()).contains("w=majority");
        probe.test(connection);
        try (var client = MongoClients.create(uri)) {
            assertThat(client.getDatabase(database).listCollectionNames().into(new ArrayList<>())).isEmpty();
        }
    }
}
