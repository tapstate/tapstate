package io.tapstate.adapters.mongostore;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Atomic first-writer state against MongoDB, including the server response a losing upsert receives.
 * Concurrent writes need not produce a duplicate-key response on every server run, so a failpoint
 * deterministically exercises that response through the real driver as a separate witness.
 */
@RequiresDocker
class MongoKeyedStateStoreSaveIfAbsentIT {

    private static final String NAMESPACE = "pdk.state.orders.source";
    private static final String KEY = "firstConnectorId";

    @Container
    private static final MongoDBContainer REPLICA_SET =
            new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
                    .withCommand("--replSet", "docker-rs", "--bind_ip_all",
                            "--setParameter", "enableTestCommands=1");

    @Test
    void theFirstWriterStoresItsValueAndReportsNoPreviousValue() {
        withStore((store, trace) -> {
            assertThat(store.saveIfAbsent(NAMESPACE, KEY, bytes("winner"))).isEmpty();
            assertThat(store.load(NAMESPACE, KEY)).contains(bytes("winner"));
            assertThat(store.count(NAMESPACE)).isEqualTo(1);
        });
    }

    @Test
    void aLaterWriterReadsTheExistingValueWithoutReplacingIt() {
        withStore((store, trace) -> {
            store.save(NAMESPACE, KEY, bytes("winner"));

            assertThat(store.saveIfAbsent(NAMESPACE, KEY, bytes("candidate")))
                    .contains(bytes("winner"));
            assertThat(store.load(NAMESPACE, KEY)).contains(bytes("winner"));
        });
    }

    @Test
    void aDuplicateKeyCommandResponseReturnsThePersistedWinner() {
        withStore((store, trace) -> {
            store.save(NAMESPACE, KEY, bytes("winner"));
            trace.clear();

            failNextFindAndModify(11000, () ->
                    assertThat(store.saveIfAbsent(NAMESPACE, KEY, bytes("candidate")))
                            .contains(bytes("winner")));

            assertThat(trace.failures).singleElement().isInstanceOfSatisfying(
                    MongoCommandException.class, failure -> assertThat(failure.getErrorCode()).isEqualTo(11000));
            assertThat(trace.commands).containsExactly("findAndModify", "find");
            assertThat(store.load(NAMESPACE, KEY)).contains(bytes("winner"));
            assertThat(store.count(NAMESPACE)).isEqualTo(1);
        });
    }

    @Test
    void anotherCommandFailureIsNotMistakenForALostRace() {
        withStore((store, trace) -> {
            store.save(NAMESPACE, KEY, bytes("winner"));
            trace.clear();

            failNextFindAndModify(2, () -> {
                Throwable failure = catchThrowable(() ->
                        store.saveIfAbsent(NAMESPACE, KEY, bytes("candidate")));

                assertThat(failure).isInstanceOfSatisfying(TapstateException.class, coded -> {
                    assertThat(coded.code()).isEqualTo(IoError.STORE_UNAVAILABLE);
                    assertThat(coded.getCause()).isInstanceOfSatisfying(MongoCommandException.class,
                            command -> assertThat(command.getErrorCode()).isEqualTo(2));
                    assertThat(coded.getCause()).isSameAs(trace.failures.getFirst());
                });
            });

            assertThat(trace.commands).containsExactly("findAndModify");
            assertThat(store.load(NAMESPACE, KEY)).contains(bytes("winner"));
        });
    }

    @Test
    void concurrentWritersAgreeOnExactlyOneStoredIdentity() throws Exception {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            MongoKeyedStateStore store = emptyStore(client);
            int writers = 8;
            CountDownLatch ready = new CountDownLatch(writers);
            CountDownLatch start = new CountDownLatch(1);
            var pool = Executors.newFixedThreadPool(writers);
            try {
                List<Future<Optional<byte[]>>> attempts = new ArrayList<>();
                for (int i = 0; i < writers; i++) {
                    byte[] candidate = bytes("candidate-" + i);
                    attempts.add(pool.submit(() -> {
                        ready.countDown();
                        if (!start.await(10, TimeUnit.SECONDS)) {
                            throw new AssertionError("writers were not released");
                        }
                        return store.saveIfAbsent(NAMESPACE, KEY, candidate);
                    }));
                }
                assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
                start.countDown();
                List<Optional<byte[]>> results = new ArrayList<>();
                for (Future<Optional<byte[]>> attempt : attempts) {
                    results.add(attempt.get(30, TimeUnit.SECONDS));
                }

                assertThat(results.stream().filter(Optional::isEmpty)).hasSize(1);
                byte[] winner = store.load(NAMESPACE, KEY).orElseThrow();
                int winningWriter = results.indexOf(Optional.empty());
                assertThat(winner).isEqualTo(bytes("candidate-" + winningWriter));
                results.stream().filter(Optional::isPresent)
                        .forEach(result -> assertThat(result).contains(winner));
                assertThat(store.count(NAMESPACE)).isEqualTo(1);
            } finally {
                start.countDown();
                pool.shutdownNow();
                assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
            }
        }
    }

    private static void withStore(BiConsumer<MongoKeyedStateStore, CommandTrace> test) {
        CommandTrace trace = new CommandTrace();
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(REPLICA_SET.getReplicaSetUrl()))
                .addCommandListener(trace)
                .build();
        try (MongoClient client = MongoClients.create(settings)) {
            test.accept(emptyStore(client), trace);
        }
    }

    private static MongoKeyedStateStore emptyStore(MongoClient client) {
        MongoCollection<Document> collection = client.getDatabase("tapstate")
                .getCollection(MongoStorePort.OPERATOR_STATE);
        collection.drop();
        return new MongoKeyedStateStore(collection);
    }

    private static void failNextFindAndModify(int errorCode, Runnable test) {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            var admin = client.getDatabase("admin");
            Document failure = new Document("failCommands", List.of("findAndModify"))
                    .append("errorCode", errorCode);
            if (errorCode == 11000) {
                failure.append("errorExtraInfo", new Document("keyPattern", new Document("_id", 1))
                        .append("keyValue", new Document("_id",
                                new Document("ns", NAMESPACE).append("k", KEY))));
            }
            admin.runCommand(new Document("configureFailPoint", "failCommand")
                    .append("mode", new Document("times", 1))
                    .append("data", failure));
            try {
                test.run();
            } finally {
                admin.runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off"));
            }
        }
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static final class CommandTrace implements CommandListener {
        private final List<String> commands = new CopyOnWriteArrayList<>();
        private final List<Throwable> failures = new CopyOnWriteArrayList<>();

        @Override
        public void commandStarted(CommandStartedEvent event) {
            commands.add(event.getCommandName());
        }

        @Override
        public void commandFailed(CommandFailedEvent event) {
            failures.add(event.getThrowable());
        }

        void clear() {
            commands.clear();
            failures.clear();
        }
    }
}
