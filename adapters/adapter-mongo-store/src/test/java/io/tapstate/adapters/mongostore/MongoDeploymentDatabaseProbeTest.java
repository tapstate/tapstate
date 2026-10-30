package io.tapstate.adapters.mongostore;

import com.mongodb.MongoException;
import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import io.tapstate.core.common.TapstateException;
import java.time.Duration;
import java.util.List;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class MongoDeploymentDatabaseProbeTest {
    @Test
    void successfulCrudAlwaysAbortsWithTheOriginalDurableTransactionPolicy() {
        var f = new Fixture();
        f.run();
        var options = ArgumentCaptor.forClass(TransactionOptions.class);
        verify(f.session).startTransaction(options.capture());
        assertThat(options.getValue().getReadConcern()).isEqualTo(ReadConcern.SNAPSHOT);
        assertThat(options.getValue().getWriteConcern()).isEqualTo(WriteConcern.MAJORITY.withJournal(true));
        verify(f.session).abortTransaction();
        verify(f.session).close();
        verify(f.session, never()).commitTransaction();
        verify(f.collection).deleteOne(any(ClientSession.class), any(Bson.class));
    }

    @ParameterizedTest
    @ValueSource(strings = {"null-read", "wrong-read", "no-update", "no-delete"})
    void incompleteCrudAbortsAndReportsOnlyTheValidatedDatabase(String failure) {
        var f = new Fixture();
        switch (failure) {
            case "null-read" -> when(f.read.first()).thenReturn(null);
            case "wrong-read" -> when(f.read.first()).thenReturn(new Document("value", "other"));
            case "no-update" -> when(f.collection.updateOne(any(ClientSession.class), any(Bson.class), any(Bson.class)))
                    .thenReturn(UpdateResult.acknowledged(1, 0L, null));
            case "no-delete" -> when(f.collection.deleteOne(any(ClientSession.class), any(Bson.class)))
                    .thenReturn(DeleteResult.acknowledged(0));
            default -> throw new AssertionError(failure);
        }
        assertDatabaseFailure(catchThrowable(f::run));
        verify(f.session).abortTransaction();
        verify(f.session).close();
        verify(f.session, never()).commitTransaction();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void driverRefusalIsCodedWithoutLeakingItsMessage(boolean abortFails) {
        var f = new Fixture();
        doThrow(new MongoException("credential-sentinel")).when(f.collection)
                .insertOne(any(ClientSession.class), any(Document.class));
        if (abortFails) doThrow(new MongoException("abort-sentinel")).when(f.session).abortTransaction();
        var failure = catchThrowable(f::run);
        assertDatabaseFailure(failure);
        assertThat(failure.getCause()).isNull();
        assertThat(failure.getMessage()).doesNotContain("credential-sentinel", "abort-sentinel");
        verify(f.session).abortTransaction();
        verify(f.session).close();
    }

    @ParameterizedTest
    @ValueSource(strings = {"active", "inactive", "abort-refused", "error"})
    void aProgrammerFailureRetainsItsIdentityAcrossCleanup(String state) {
        var f = new Fixture();
        Throwable original = state.equals("error") ? new AssertionError("invariant") : new IllegalStateException("invariant");
        doThrow(original).when(f.collection).insertOne(any(ClientSession.class), any(Document.class));
        when(f.session.hasActiveTransaction()).thenReturn(!state.equals("inactive"));
        if (state.equals("abort-refused")) doThrow(new MongoException("abort refused")).when(f.session).abortTransaction();
        assertThat(catchThrowable(f::run)).isSameAs(original);
        verify(f.session, times(state.equals("inactive") ? 0 : 1)).abortTransaction();
        verify(f.session).close();
    }

    @Test
    void aFinalAbortRefusalIsCodedAndNeverCommits() {
        var f = new Fixture();
        doThrow(new MongoException("abort refused")).when(f.session).abortTransaction();
        assertDatabaseFailure(catchThrowable(f::run));
        verify(f.session, never()).commitTransaction();
        verify(f.session).close();
    }

    @Test
    void anInvalidDatabaseIsRejectedBeforeAcquiringAClient() {
        var f = new Fixture();
        assertThat(catchThrowable(() -> f.connection.verifyDeploymentDatabases(List.of("invalid/name"))))
                .isInstanceOf(IllegalArgumentException.class);
        verifyNoInteractions(f.client);
    }

    private static void assertDatabaseFailure(Throwable failure) {
        assertThat(failure).isInstanceOfSatisfying(TapstateException.class, coded -> {
            assertThat(coded.code()).isEqualTo(StoreError.DATABASE_ACCESS_FAILED);
            assertThat(coded.args()).containsOnlyKeys("database").containsEntry("database", "deployment");
        });
    }

    private static final class Fixture {
        final MongoClient client = mock(MongoClient.class);
        final ClientSession session = mock(ClientSession.class);
        final MongoDatabase database = mock(MongoDatabase.class);
        @SuppressWarnings("unchecked")
        final MongoCollection<Document> collection = mock(MongoCollection.class);
        @SuppressWarnings("unchecked")
        final FindIterable<Document> read = mock(FindIterable.class);
        final MongoConnection connection = spy(new MongoConnection(new MongoConnectionSettings(
                "mongodb://localhost:27017/deployment", null, Duration.ofSeconds(2))));

        Fixture() {
            doReturn(client).when(connection).client();
            when(client.getDatabase("deployment")).thenReturn(database);
            when(database.getCollection(anyString())).thenReturn(collection);
            when(client.startSession()).thenReturn(session);
            when(session.hasActiveTransaction()).thenReturn(true);
            when(collection.find(any(ClientSession.class), any(Bson.class))).thenReturn(read);
            when(read.first()).thenReturn(new Document("value", "initial"));
            when(collection.updateOne(any(ClientSession.class), any(Bson.class), any(Bson.class)))
                    .thenReturn(UpdateResult.acknowledged(1, 1L, null));
            when(collection.deleteOne(any(ClientSession.class), any(Bson.class)))
                    .thenReturn(DeleteResult.acknowledged(1));
        }

        void run() { connection.verifyDeploymentDatabases(List.of("deployment")); }
    }
}
