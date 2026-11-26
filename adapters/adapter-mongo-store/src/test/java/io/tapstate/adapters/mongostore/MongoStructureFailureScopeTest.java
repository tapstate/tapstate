package io.tapstate.adapters.mongostore;

import com.mongodb.MongoException;
import com.mongodb.client.ClientSession;
import com.mongodb.client.ListCollectionsIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.TransactionBody;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import io.tapstate.core.common.OperationErrorDiagnostics;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class MongoStructureFailureScopeTest {
    private final MongoClient client = mock(MongoClient.class);
    private final MongoDatabase database = mock(MongoDatabase.class);
    private final CommandListener listener = MongoStructureFailureScope.listener(() -> client, () -> database,
            SystemCollections.Database.CONFIG);
    private final String collection = SystemCollections.CONFIG_TOKENS.collectionName();

    MongoStructureFailureScopeTest() {
        when(database.getName()).thenReturn("test_config");
        when(database.withTimeout(anyLong(), eq(TimeUnit.MILLISECONDS))).thenReturn(database);
    }

    @SuppressWarnings("unchecked")
    private ListCollectionsIterable<Document> missing() {
        var metadata = (ListCollectionsIterable<Document>) mock(ListCollectionsIterable.class);
        when(database.listCollections()).thenReturn(metadata);
        when(metadata.filter(any())).thenReturn(metadata);
        when(metadata.timeoutMode(any())).thenReturn(metadata);
        when(metadata.into(any())).thenAnswer(call -> call.getArgument(0));
        return metadata;
    }

    private MongoException fail(int code) { return fail(listener, collection, code); }

    private MongoException fail(CommandListener targetListener, String target, int code) {
        var started = mock(CommandStartedEvent.class);
        when(started.getCommandName()).thenReturn("find");
        when(started.getCommand()).thenReturn(new BsonDocument("find", new BsonString(target)));
        when(started.getDatabaseName()).thenReturn("test_config");
        when(started.getRequestId()).thenReturn(4);
        targetListener.commandStarted(started);
        var failure = new MongoException(code, "private-row-and-driver-message");
        var failed = mock(CommandFailedEvent.class);
        when(failed.getRequestId()).thenReturn(4);
        when(failed.getThrowable()).thenReturn(failure);
        targetListener.commandFailed(failed);
        return failure;
    }

    @Test void exactNamespaceProbePreservesOriginalCodeArgumentsAndDoesNotLeakDriverMessage() {
        var metadata = missing();
        try (var request = OperationErrorDiagnostics.open()) {
            var thrown = catchThrowable(() -> MongoStructureFailureScope.call(client, new MongoReadBudget(Duration.ofSeconds(2)),
                    () -> StoreIo.call(() -> { throw fail(26); })));
            assertThat(thrown).isInstanceOf(TapstateException.class);
            var coded = (TapstateException) thrown;
            assertThat(coded.code()).isEqualTo(IoError.STORE_UNAVAILABLE);
            assertThat(coded.args()).containsOnlyKeys("detail");
            assertThat(OperationErrorDiagnostics.appendTo("original message"))
                    .startsWith("original message Storage structure: CONFIG test_config." + collection)
                    .contains("COLLECTION_MISSING").doesNotContain("private-row", "repair", "createIndex");
            verify(metadata).filter(new Document("name", collection));
            verify(database, never()).getCollection(anyString());
        }
        assertThat(OperationErrorDiagnostics.appendTo("next request")).isEqualTo("next request");
    }

    @Test void unknownRequestBudgetAndExpiredBudgetDoNotProbe() {
        assertThatThrownBy(() -> MongoStructureFailureScope.call(client, new MongoReadBudget(Duration.ofSeconds(1)),
                () -> StoreIo.call(() -> { throw fail(26); }))).isInstanceOf(TapstateException.class);
        try (var request = OperationErrorDiagnostics.open()) {
            assertThatThrownBy(() -> MongoStructureFailureScope.call(client, new MongoReadBudget(Duration.ofNanos(1)),
                    () -> StoreIo.call(() -> { throw fail(26); }))).isInstanceOf(TapstateException.class);
            assertThat(OperationErrorDiagnostics.appendTo("error")).isEqualTo("error");
        }
        verify(database, never()).listCollections();
    }

    @Test void unrelatedTerminalFailureDoesNotReuseEarlierStructuredError() {
        var original = new TapstateException(IoError.STORE_UNAVAILABLE, Map.of("detail", "later-failure"), null);
        try (var request = OperationErrorDiagnostics.open()) {
            var thrown = catchThrowable(() -> MongoStructureFailureScope.call(client, new MongoReadBudget(Duration.ofSeconds(1)), () -> {
                try { StoreIo.call(() -> { throw fail(26); }); } catch (TapstateException ignored) { }
                throw original;
            }));
            assertThat(thrown).isSameAs(original);
            assertThat(OperationErrorDiagnostics.appendTo("error")).isEqualTo("error");
        }
        verify(database, never()).listCollections();
    }

    @Test void duplicatePhysicalNameUsesFactoryKnownCacheRole() {
        var metadata = missing();
        var cacheListener = MongoStructureFailureScope.listener(() -> client, () -> database, SystemCollections.Database.CACHE);
        String target = SystemCollections.CACHE_PIPELINE_STATE.collectionName();
        try (var request = OperationErrorDiagnostics.open()) {
            assertThatThrownBy(() -> MongoStructureFailureScope.call(client, new MongoReadBudget(Duration.ofSeconds(2)),
                    () -> StoreIo.call(() -> { throw fail(cacheListener, target, 26); }))).isInstanceOf(TapstateException.class);
            assertThat(OperationErrorDiagnostics.appendTo("error"))
                    .contains("CACHE test_config." + target).doesNotContain("STORE test_config.");
            verify(metadata).filter(new Document("name", target));
        }
    }

    @Test void newCallbackDropsPreviouslyHandledDriverFailure() {
        try (var request = OperationErrorDiagnostics.open()) {
            assertThatThrownBy(() -> MongoStructureFailureScope.call(client, new MongoReadBudget(Duration.ofSeconds(1)), () -> {
                var prior = fail(26);
                try { StoreIo.call(() -> { throw prior; }); } catch (TapstateException ignored) { }
                MongoStructureFailureScope.beginCallback();
                return StoreIo.call(() -> { throw prior; });
            })).isInstanceOf(TapstateException.class);
            assertThat(OperationErrorDiagnostics.appendTo("error")).isEqualTo("error");
        }
        verify(database, never()).listCollections();
    }

    @Test void unrelatedStructuredErrorDoesNotProbe() {
        try (var request = OperationErrorDiagnostics.open()) {
            assertThatThrownBy(() -> MongoStructureFailureScope.call(client, new MongoReadBudget(Duration.ofSeconds(1)),
                    () -> StoreIo.call(() -> { throw fail(13); }))).isInstanceOf(TapstateException.class);
        }
        verify(database, never()).listCollections();
    }

    @Test @SuppressWarnings("unchecked") void transactionSessionClosesBeforeSupplementaryProbe() {
        var metadata = missing();
        var session = mock(ClientSession.class);
        var closed = new AtomicBoolean();
        when(client.startSession()).thenReturn(session);
        doAnswer(call -> { closed.set(true); return null; }).when(session).close();
        when(session.withTransaction(any(), any())).thenAnswer(call -> ((TransactionBody<Object>) call.getArgument(0)).execute());
        when(metadata.into(any())).thenAnswer(call -> {
            assertThat(closed).isTrue();
            return call.getArgument(0);
        });
        try (var request = OperationErrorDiagnostics.open()) {
            assertThatThrownBy(() -> new MongoReadBudget(Duration.ofSeconds(2)).transaction(client, ignored -> { throw fail(26); }))
                    .isInstanceOf(TapstateException.class);
            assertThat(OperationErrorDiagnostics.appendTo("error")).contains("COLLECTION_MISSING");
        }
        verify(session).close();
    }
    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"returned", "threw", "other-client", "commit", "abort"})
    void commandsInsideNestedScopesCannotBecomeAZeroCommandRecoveryProof(String scenario) {
        var observer = new MongoStructureFailureScope.CommandObserver();
        observer.bind(client);
        MongoStructureFailureScope.inspectOnFailure(client, () -> {
            assertThat(MongoStructureFailureScope.provedNoCommandStarted(client)).isTrue();
            var nested = scenario.equals("other-client") ? mock(MongoClient.class) : client;
            try {
                MongoStructureFailureScope.inspectOnFailure(nested, () -> {
                    var command = mock(CommandStartedEvent.class);
                    when(command.getCommandName()).thenReturn(scenario.equals("commit") ? "commitTransaction"
                            : scenario.equals("abort") ? "abortTransaction" : "findAndModify");
                    observer.commandStarted(command);
                    if (scenario.equals("threw")) throw new com.mongodb.MongoOperationTimeoutException("test nested timeout");
                    return null;
                });
            } catch (com.mongodb.MongoOperationTimeoutException expected) { }
            assertThat(MongoStructureFailureScope.provedNoCommandStarted(client)).isFalse();
            return null;
        });
    }

}
