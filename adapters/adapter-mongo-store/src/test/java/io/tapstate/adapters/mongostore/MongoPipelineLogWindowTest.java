package io.tapstate.adapters.mongostore;

import com.mongodb.TransactionOptions;
import com.mongodb.client.ClientSession;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.TransactionBody;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.PipelineRetirementRequest;
import io.tapstate.spi.store.PipelineStateMappings.LogWindow;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MongoPipelineLogWindowTest {
    private static final String PIPELINE = "flow";
    private static final String HASH = "a".repeat(64);
    private static final String OPERATION = "retire-flow";

    @Test
    void onlyAGenerationAfterCompletedRetirementCanStartANewLifetime() {
        var fixture = new Fixture();
        fixture.reads(true, mapping(4, "ACTIVE"));
        assertThat(fixture.store.logWindow(PIPELINE)).contains(new LogWindow(1, 4));

        fixture.reads(true, mapping(7, "ACTIVE").append("retirement", retirement(4, "COMPLETE")));
        assertThat(fixture.store.logWindow(PIPELINE)).contains(new LogWindow(5, 7));

        fixture.reads(true, mapping(4, "ACTIVE").append("retirement", retirement(4, "COMPLETE")));
        assertUnreadable(fixture);
    }

    @Test
    void missingAndRetiredPipelinesHaveNoLogWindow() {
        var fixture = new Fixture();
        fixture.reads(false, null);
        assertThat(fixture.store.logWindow(PIPELINE)).isEmpty();

        fixture.reads(true, mapping(4, "RETIRING"));
        assertThat(fixture.store.logWindow(PIPELINE)).isEmpty();

        fixture.reads(false, mapping(4, "RETIRED"));
        assertThat(fixture.store.logWindow(PIPELINE)).isEmpty();
    }

    @Test
    void inconsistentLifetimeFactsCannotExposeAnUnscopedTail() {
        var fixture = new Fixture();
        fixture.reads(true, mapping(7, "ACTIVE").append("retirement", retirement(4, "RETIRING")));
        assertUnreadable(fixture);

        fixture.reads(false, mapping(7, "ACTIVE"));
        assertUnreadable(fixture);
    }

    private static void assertUnreadable(Fixture fixture) {
        assertThatThrownBy(() -> fixture.store.logWindow(PIPELINE))
                .isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
    }

    private static Document mapping(long generation, String phase) {
        return new Document("_id", PIPELINE).append("stateGeneration", generation)
                .append("phase", phase).append("operationId", OPERATION).append("noteWriteFence", 0L);
    }

    private static Document retirement(long generation, String phase) {
        var scope = new PipelineIntentScope("cluster", 1, "cache", PIPELINE, generation);
        var request = new PipelineRetirementRequest(OPERATION, scope, HASH, Optional.empty());
        return MongoPipelineRetirementFacts.encode(new MongoPipelineRetirementFacts.Fact(
                request, HASH, 0, phase, Optional.empty(), Optional.empty()));
    }

    private static final class Fixture {
        final MongoClient client = mock(MongoClient.class);
        final MongoDatabase database = mock(MongoDatabase.class);
        final ClientSession session = mock(ClientSession.class);
        final FindIterable<Document> pipelineRows = rows(SystemCollections.PIPELINES);
        final FindIterable<Document> mappingRows = rows(SystemCollections.OPERATOR_GENERATIONS);
        final MongoPipelineStateMappings store = new MongoPipelineStateMappings(client, database);

        Fixture() {
            when(client.startSession()).thenReturn(session);
            when(session.withTransaction(any(), any(TransactionOptions.class)))
                    .thenAnswer(call -> ((TransactionBody<?>) call.getArgument(0)).execute());
        }

        @SuppressWarnings("unchecked")
        private FindIterable<Document> rows(SystemCollections collection) {
            var mongoCollection = (MongoCollection<Document>) mock(MongoCollection.class, RETURNS_SELF);
            var result = (FindIterable<Document>) mock(FindIterable.class);
            when(database.getCollection(collection.collectionName())).thenReturn(mongoCollection);
            when(mongoCollection.find(eq(session), eq(new Document("_id", PIPELINE)))).thenReturn(result);
            return result;
        }

        void reads(boolean exists, Document mapping) {
            when(pipelineRows.first()).thenReturn(exists ? new Document("_id", PIPELINE) : null);
            when(mappingRows.first()).thenReturn(mapping);
        }
    }
}
