package io.tapstate.adapters.mongostore;

import com.mongodb.MongoException;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.Resource;
import io.tapstate.spi.store.IoError;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/** Retry boundaries independent of how long a real competing transaction holds the keyring fence. */
class MongoArtifactStoreRetryTest {
    private static final Resource SOURCE = new DslParser().parse("""
            version: tapstate/v1
            kind: source
            id: views
            connector: mongodb
            config: {}
            """);

    @Test
    void aDriverFailureWithoutTheTransientLabelIsNotReplayed() {
        AtomicInteger attempts = new AtomicInteger();
        MongoArtifactStore store = failingStore(new MongoException(112, "driver-secret-sentinel"), false, attempts);

        assertSafeFailure(catchThrowable(() -> store.create(SOURCE)));
        assertThat(attempts.get()).isEqualTo(1);
    }

    @Test
    void anUnknownCommitResultIsNotReplayedEvenWithTheTransientLabel() {
        MongoException error = transientConflict();
        error.addLabel("UnknownTransactionCommitResult");
        AtomicInteger attempts = new AtomicInteger();
        MongoArtifactStore store = failingStore(error, true, attempts);

        assertSafeFailure(catchThrowable(() -> store.create(SOURCE)));
        assertThat(attempts.get()).isEqualTo(1);
    }

    @Test
    void anInterruptedRetryPreservesTheInterruptAndTheSafeStoreDiagnostic() {
        AtomicInteger attempts = new AtomicInteger();
        MongoArtifactStore store = failingStore(transientConflict(), false, attempts);
        Thread.currentThread().interrupt();
        try {
            assertSafeFailure(catchThrowable(() -> store.create(SOURCE)));
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
            assertThat(attempts.get()).isEqualTo(1);
        } finally {
            Thread.interrupted();
        }
    }

    private static MongoException transientConflict() {
        MongoException error = new MongoException(112, "driver-secret-sentinel");
        error.addLabel("TransientTransactionError");
        return error;
    }

    private static void assertSafeFailure(Throwable failure) {
        assertThat(failure).isInstanceOfSatisfying(TapstateException.class, coded -> {
            assertThat(coded.code()).isEqualTo(IoError.STORE_UNAVAILABLE);
            assertThat(coded.args()).containsExactlyEntriesOf(Map.of("detail", "MongoException code=112"));
            assertThat(coded.getCause()).isNull();
        });
    }

    @SuppressWarnings("unchecked")
    private static MongoArtifactStore failingStore(MongoException error, boolean atCommit, AtomicInteger attempts) {
        MongoClient client = (MongoClient) Proxy.newProxyInstance(MongoClient.class.getClassLoader(),
                new Class<?>[]{MongoClient.class}, (proxy, method, args) -> {
                    if (!method.getName().equals("startSession")) throw new AssertionError(method.getName());
                    attempts.incrementAndGet();
                    AtomicBoolean active = new AtomicBoolean();
                    return Proxy.newProxyInstance(ClientSession.class.getClassLoader(), new Class<?>[]{ClientSession.class},
                            (session, operation, parameters) -> switch (operation.getName()) {
                                case "startTransaction" -> { active.set(true); yield null; }
                                case "abortTransaction", "close" -> { active.set(false); yield null; }
                                case "hasActiveTransaction" -> active.get();
                                case "commitTransaction" -> throw error;
                                default -> throw new AssertionError(operation.getName());
                            });
                });
        MongoCollection<Document> collection = (MongoCollection<Document>) Proxy.newProxyInstance(
                MongoCollection.class.getClassLoader(), new Class<?>[]{MongoCollection.class}, (proxy, method, args) -> {
                    if (method.getName().equals("withReadPreference") || method.getName().equals("withReadConcern")
                            || method.getName().equals("withWriteConcern")) return proxy;
                    if (!method.getName().equals("insertOne")) throw new AssertionError(method.getName());
                    if (!atCommit) throw error;
                    return null;
                });
        SourceConfigCipherProvider ciphers = new SourceConfigCipherProvider() {
            private final SourceConfigCipher cipher = new SourceConfigCipher(new byte[32]);
            @Override public SourceConfigCipher current() { return cipher; }
            @Override public SourceConfigCipher refresh() { return cipher; }
            @Override public boolean requiresWriteFence() { return true; }
        };
        return new MongoArtifactStore(client, collection, ciphers);
    }
}
