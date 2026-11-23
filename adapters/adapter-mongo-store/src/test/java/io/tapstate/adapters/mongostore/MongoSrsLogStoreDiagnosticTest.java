package io.tapstate.adapters.mongostore;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MongoSrsLogStoreDiagnosticTest {
    @SuppressWarnings("unchecked")
    @Test void malformedDriverSequenceRetainsTheCompleteDiagnosticContract() {
        MongoCollection<Document> collection = mock(MongoCollection.class, org.mockito.Mockito.RETURNS_SELF);
        FindIterable<Document> missingBounds = mock(FindIterable.class);
        FindIterable<Document> retained = mock(FindIterable.class);
        when(collection.find(any(Bson.class))).thenReturn(missingBounds, retained);
        when(missingBounds.first()).thenReturn(null);
        when(retained.sort(any(Bson.class))).thenReturn(retained);
        when(retained.limit(anyInt())).thenReturn(retained);
        when(retained.first()).thenReturn(new Document("_id", new Document("ring", "orders").append("seq", "invalid")));

        assertThatThrownBy(() -> new MongoSrsLogStore(collection).bounds("orders"))
                .isInstanceOfSatisfying(TapstateException.class, error -> {
                    assertThat(error.code()).isSameAs(IoError.DOCUMENT_UNREADABLE);
                    assertThat(error.args()).containsEntry("field", "_id.seq");
                    assertThat(error.args().keySet()).isEqualTo(error.code().placeholders());
                });
    }
}
