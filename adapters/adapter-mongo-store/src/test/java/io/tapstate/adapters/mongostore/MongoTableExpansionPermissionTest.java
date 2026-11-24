package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineTableExpansion;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class MongoTableExpansionPermissionTest {
    @Test void aCallerCannotForgeConfigAuthorizationOrReachCacheThroughItsCallbacks() {
        var client = mock(MongoClient.class);
        var database = mock(MongoDatabase.class);
        @SuppressWarnings("unchecked") MongoCollection<Document> collection = mock(MongoCollection.class);
        when(database.getCollection(anyString())).thenReturn(collection);
        var preparation = new MongoCachePipelinePreparations(client, database,
                new CacheBusinessScope("cluster", 1, "incarnation"), () -> {});
        var forged = mock(PipelineTableExpansion.class);
        clearInvocations(database);
        assertThatThrownBy(() -> preparation.expandSources(forged, null))
                .isInstanceOfSatisfying(TapstateException.class, failure ->
                        assertThat(failure.code()).isEqualTo(IoError.PIPELINE_INTENT_FENCED));
        verifyNoInteractions(forged, client, database, collection);
    }
}
