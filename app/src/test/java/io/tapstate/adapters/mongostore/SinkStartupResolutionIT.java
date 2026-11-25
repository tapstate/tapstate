package io.tapstate.adapters.mongostore;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClients;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import io.tapstate.adapters.pdk.ConnectorIntrospector;
import io.tapstate.adapters.pdk.RegistryConnectorProvisioner;
import io.tapstate.adapters.pdk.SyntheticSinkJars;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.CacheAuthScope;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.RegistrationSource;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.testsupport.RequiresDocker;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.UUID;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.*;

@RequiresDocker
class SinkStartupResolutionIT {
    @Container private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    @TempDir Path work;

    @Test void aWarmSinkArtifactUsesOneFreshNativeAdmissionInsteadOfTwoTransactions() throws Exception {
        String name = "sink_resolution_" + UUID.randomUUID().toString().replace("-", "");
        var binding = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, MONGO.getReplicaSetUrl(name), name, null);
        var scope = new CacheBusinessScope("cluster-a", 1, "cache-a");
        var auth = new CacheAuthScope("cluster-a", 1, StorageBinding.Mode.OP, "cache-a", 1);
        try (var admin = MongoClients.create(MONGO.getReplicaSetUrl());
                var domain = MongoCacheDomain.connect(binding, Duration.ofSeconds(5), "sink-resolution")) {
            var database = admin.getDatabase(name);
            try {
                domain.prepareAuth(auth, "auth-a", () -> {});
                try (var session = admin.startSession()) {
                    session.withTransaction(() -> {
                        new MongoCacheMetadataStructure(database, MongoRateHistoryStore.DEFAULT_RETENTION).prepareInTransaction(session, () -> {});
                        SystemCollections.CACHE_WORKLOAD_CLAIMS.prepareCollectionOn(database, session);
                        return null;
                    }, TransactionOptions.builder().readConcern(ReadConcern.LOCAL).writeConcern(WriteConcern.MAJORITY.withJournal(true)).build());
                }
                SystemCollections.CACHE_IDENTITY.on(database).updateOne(new Document("_id", "cluster"),
                        new Document("$set", new Document("businessPhase", "READY").append("businessPreparationId", "business-a").append("claimAdmissionFence", 0L)));
                SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(database).insertOne(new Document("_id", "business").append("installedVersion", 1)
                        .append("phase", "READY").append("epoch", 1L).append("operationId", "business-a"));
                var base = domain.connectors(scope, () -> {});
                var trace = Files.createFile(work.resolve("trace"));
                byte[] bytes = Files.readAllBytes(SyntheticSinkJars.registeredStorageProbe(work.resolve("jar"), trace, "example", "fixture://"));
                var registered = base.register("example", "2.0.8", RegistrationSource.SEED, bytes).registration();
                for (int sibling = 0; sibling < 100; sibling++)
                    base.register("sibling-" + sibling, "1", RegistrationSource.SEED, ("sibling-" + sibling).getBytes(java.nio.charset.StandardCharsets.UTF_8));
                var commands = new ArrayList<String>();
                var settings = MongoClientSettings.builder().applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl()))
                        .addCommandListener(new CommandListener() {
                            @Override public void commandStarted(CommandStartedEvent event) { commands.add(event.getCommandName()); }
                        }).build();
                try (var client = MongoClients.create(settings)) {
                    client.getDatabase("admin").runCommand(new Document("ping", 1));
                    var registry = new MongoCacheConnectorRegistry(client, client.getDatabase(name), scope, () -> {}, () -> {});
                    var seeds = Files.createDirectories(work.resolve("seeds"));
                    Files.write(seeds.resolve("example-connector.jar"), bytes);
                    var provisioner = new RegistryConnectorProvisioner(registry, new ConnectorIntrospector(), work.resolve("plugins"), seeds, true);
                    commands.clear();
                    var original = provisioner.resolve("example");
                    System.out.println("SINK_RESOLUTION_ORIGINAL " + commands);
                    assertThat(commands).hasSize(6);
                    commands.clear();
                    assertThat(provisioner.resolveStartup("example")).isEqualTo(original);
                    System.out.println("SINK_RESOLUTION_STARTUP " + commands);
                    assertThat(commands).containsExactly("aggregate");
                    commands.clear();
                    assertThat(provisioner.resolveStartup("example", registered.contentHash())).isEqualTo(original);
                    System.out.println("SINK_RESOLUTION_EXACT " + commands);
                    assertThat(commands).containsExactly("aggregate");
                    SystemCollections.CACHE_IDENTITY.on(database).updateOne(new Document("_id", "cluster"),
                            new Document("$set", new Document("businessPhase", "RETIRED")));
                    assertThatThrownBy(() -> provisioner.resolveStartup("example", registered.contentHash()))
                            .isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.code()).isEqualTo(IoError.CACHE_BUSINESS_FENCED));
                    assertThat(Files.readAllLines(trace)).isEmpty();
                }
            } finally { database.drop(); }
        }
    }
}
