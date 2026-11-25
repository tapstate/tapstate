package io.tapstate.adapters.mongostore;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import java.util.Map;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MongoRetrySettingsStoreTest {
    @Test
    @SuppressWarnings("unchecked")
    void corruptStoredRevisionKeepsTheDecodingCauseAndSafeArguments() {
        var database = mock(MongoDatabase.class);
        var collection = (MongoCollection<Document>) mock(MongoCollection.class, RETURNS_SELF);
        var result = (FindIterable<Document>) mock(FindIterable.class);
        when(database.getCollection(SystemCollections.CONFIG_RETRY_SETTINGS.collectionName())).thenReturn(collection);
        when(collection.find(new Document("_id", "cluster"))).thenReturn(result);
        when(result.first()).thenReturn(new Document("_id", "cluster")
                .append("clusterId", "local")
                .append("formatVersion", 1)
                .append("revision", "invalid-revision")
                .append("defaults", new Document())
                .append("operations", new Document()));

        var store = new MongoRetrySettingsStore(database, "local", () -> {});
        assertThatThrownBy(store::read).isInstanceOfSatisfying(TapstateException.class, error -> {
            assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);
            assertThat(error.getCause()).isInstanceOf(ClassCastException.class);
            assertThat(error.args()).containsExactlyInAnyOrderEntriesOf(
                    Map.of("id", "retry-settings", "field", "policy"));
        });
    }
}
