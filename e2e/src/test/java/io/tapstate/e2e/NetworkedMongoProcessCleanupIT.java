package io.tapstate.e2e;

import io.tapstate.testsupport.RequiresDocker;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static io.tapstate.e2e.ClusterProcessControlFixtures.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.assertThatCode;

/** Real Mongo read-back witnesses destruction order; explicit stubs only supply unknown OS answers. */
@RequiresDocker
class NetworkedMongoProcessCleanupIT {
    private static final String DATABASE = "store_process_cleanup_probe";

    @Test
    void liveOwnedChildKeepsItsRealStoreAndARetiredChildReleasesItDespiteALiveForeignChild(
            @TempDir Path scratch) throws Exception {
        NetworkedMongo store = NetworkedMongo.start();
        String uri = store.uriForThisHost(DATABASE);
        try (MongoClient client = client(uri);
             RealProcessServer owned = ownedChild(uri, scratch, "owned");
             RealProcessServer foreign = child(scratch, "foreign", "quiet", null, null, null)) {
            seed(client);
            Throwable refusal = capture(store::close);
            // Read first: a missing guard must fail on the actual store, not only on an exception type.
            assertThatCode(() -> assertThat(sentinel(client)).isEqualTo(1))
                    .as("the live owned child still has its real writable store").doesNotThrowAnyException();
            assertThat(owned.isAlive()).isTrue();
            assertThat(foreign.isAlive()).isTrue();
            assertThat(foreign.stagingDirectory()).isDirectory();
            assertThat(refusal).isInstanceOf(AssertionError.class).hasMessageContaining("confirmed exit");
            owned.close();
            assertThat(owned.isAlive()).isFalse();
            store.close();
            assertThatThrownBy(() -> sentinel(client)).isInstanceOf(MongoException.class);
            assertThat(foreign.isAlive()).as("a process which never borrowed this store cannot retain it").isTrue();
        } finally {
            store.close();
        }
    }

    @Test
    void syntheticUnknownExitRetainsTheRealStoreDuringActualTryWithResourcesUnwinding(
            @TempDir Path scratch) throws Exception {
        NetworkedMongo store = NetworkedMongo.start();
        String uri = store.uriForThisHost(DATABASE);
        StoreProcessScopeTest.UnknownProcess unknown = new StoreProcessScopeTest.UnknownProcess();
        RealProcessServer boundary = synthetic(unknown, scratch);
        String runId = "retained-store-cleanup-" + UUID.randomUUID();
        try (MongoClient client = client(uri);
             RealProcessServer peer = ownedChild(uri, scratch, "real-peer");
             RealProcessServer foreign = child(scratch, "foreign", "quiet", null, null, null)) {
            seed(client);
            processes(store).launched(unknown);
            Map<String, RealProcessServer> owned = new LinkedHashMap<>();
            owned.put("synthetic-unknown", boundary);
            owned.put("real-peer", peer);
            Map<String, Path> logs = new LinkedHashMap<>();
            owned.forEach((name, server) -> logs.put(name, server.output()));
            Throwable primary = capture(() -> {
                try (store) {
                    closePartitionBatch(owned, runId, logs);
                }
            });
            assertThat(primary).isInstanceOf(IllegalStateException.class)
                    .hasMessage("synthetic wait answer is unknown");
            assertThat(primary.getSuppressed()).anySatisfy(failure -> assertThat(failure)
                    .isInstanceOf(AssertionError.class).hasMessageContaining("owned store is retained"));
            assertThatCode(() -> assertThat(sentinel(client)).isEqualTo(1))
                    .as("TWR did not destroy a store whose owned exit is unknown").doesNotThrowAnyException();
            assertThat(peer.isAlive()).isFalse();
            assertThat(peer.stagingDirectory()).doesNotExist();
            assertThat(boundary.stagingDirectory()).isDirectory();
            assertThat(Files.readString(savedLog(runId, "synthetic-unknown")))
                    .contains("explicit synthetic unknown boundary; no OS process");
            assertThat(Files.readString(savedLog(runId, "real-peer"))).contains("control child pid=" + peer.pid());
            assertThat(foreign.isAlive()).isTrue();
            assertThat(foreign.stagingDirectory()).isDirectory();
            unknown.confirm();
            store.close();
            assertThatThrownBy(() -> sentinel(client)).isInstanceOf(MongoException.class);
            assertThat(foreign.isAlive()).isTrue();
        } finally {
            unknown.confirm();
            store.close();
            boundary.close();
        }
    }

    private static MongoClient client(String uri) {
        return MongoClients.create(MongoClientSettings.builder().applyConnectionString(new ConnectionString(uri))
                .applyToClusterSettings(settings -> settings.serverSelectionTimeout(1, TimeUnit.SECONDS)).build());
    }

    private static void seed(MongoClient client) {
        client.getDatabase(DATABASE).getCollection("sentinel").insertOne(new Document("_id", 1));
    }

    private static long sentinel(MongoClient client) {
        client.getDatabase(DATABASE).runCommand(new Document("ping", 1));
        return client.getDatabase(DATABASE).getCollection("sentinel").countDocuments();
    }

    private static StoreProcessScope processes(NetworkedMongo store) throws Exception {
        Field processes = NetworkedMongo.class.getDeclaredField("processes");
        processes.setAccessible(true);
        return (StoreProcessScope) processes.get(store);
    }

    private static RealProcessServer ownedChild(String uri, Path scratch, String name)
            throws Exception {
        Path ready = scratch.resolve(name + ".ready");
        Method prepare = RealProcessServer.class.getDeclaredMethod("prepare", String.class, String.class,
                Path.class, String.class, List.class, Path.class, java.util.function.IntFunction.class);
        prepare.setAccessible(true);
        var launchArguments = (java.util.function.IntFunction<List<String>>)
                ignored -> arguments(ready, "quiet", null, null, null);
        try (RealProcessServer.PreparedLaunch prepared = (RealProcessServer.PreparedLaunch) prepare.invoke(
                null, uri, SharedMongo.OPERATOR_STATE_DATABASE, jar(scratch), "127.0.0.1", List.of(), null, launchArguments)) {
            RealProcessServer server = prepared.launch();
            try {
                Await.until("the owned store control child to acknowledge its PID", Duration.ofSeconds(5),
                        () -> Files.isRegularFile(ready), server::tail);
                assertThat(Files.readString(ready)).isEqualTo(Long.toString(server.pid()));
                return server;
            } catch (RuntimeException | Error | java.io.IOException failure) {
                try { server.close(); }
                catch (RuntimeException | Error cleanup) { failure.addSuppressed(cleanup); }
                throw failure;
            }
        }
    }

    private static RealProcessServer synthetic(Process process, Path scratch) throws Exception {
        Path staging = Files.createDirectory(scratch.resolve("synthetic-unknown-staging"));
        Path output = Files.writeString(scratch.resolve("synthetic-unknown.out"),
                "explicit synthetic unknown boundary; no OS process\n");
        Constructor<RealProcessServer> constructor = RealProcessServer.class.getDeclaredConstructor(
                Process.class, URI.class, Path.class, Path.class, boolean.class);
        constructor.setAccessible(true);
        return constructor.newInstance(process, URI.create("http://127.0.0.1:1"), output, staging, true);
    }

    private static void closePartitionBatch(Map<String, RealProcessServer> owned, String runId,
            Map<String, Path> logs) throws Exception {
        Method close = PartitionableCluster.class.getDeclaredMethod("closeOwned",
                Map.class, Map.class, Map.class, String.class, Map.class);
        close.setAccessible(true);
        try { close.invoke(null, owned, Map.of(), Map.of(), runId, logs); }
        catch (InvocationTargetException wrapper) {
            Throwable cause = wrapper.getCause();
            if (cause instanceof Error failure) throw failure;
            if (cause instanceof RuntimeException failure) throw failure;
            if (cause instanceof Exception failure) throw failure;
            throw new AssertionError(cause);
        }
    }

    @FunctionalInterface
    private interface Action { void run() throws Exception; }

    private static Throwable capture(Action action) {
        try { action.run(); }
        catch (Exception | Error failure) { return failure; }
        return null;
    }
}
