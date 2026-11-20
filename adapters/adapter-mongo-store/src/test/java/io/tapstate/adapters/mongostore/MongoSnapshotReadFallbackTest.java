package io.tapstate.adapters.mongostore;

import com.mongodb.MongoNamespace;
import com.mongodb.client.*;
import com.mongodb.connection.*;
import java.util.List;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

class MongoSnapshotReadFallbackTest {
    @ParameterizedTest @ValueSource(strings={"unknown","router","old-wire","empty"})
    void actuationUsesTheExistingTransactionWhenSnapshotReadsAreUnavailable(String topology){
        var client=mock(MongoClient.class);var db=mock(MongoDatabase.class);var session=mock(ClientSession.class);
        @SuppressWarnings("unchecked") var collection=(MongoCollection<Document>)mock(MongoCollection.class);
        @SuppressWarnings("unchecked") var query=(FindIterable<Document>)mock(FindIterable.class);
        when(db.getName()).thenReturn("test");when(db.getCollection(anyString())).thenReturn(collection);
        when(collection.getNamespace()).thenReturn(new MongoNamespace("test","pipeline_intents"));
        when(collection.find(eq(session),any(Bson.class))).thenReturn(query);when(query.first()).thenReturn(null);
        when(client.getDatabase("test")).thenReturn(db);
        var bindings=mock(MongoStorageBindingStore.class);when(bindings.belongsTo(client,"test")).thenReturn(true);
        var description=mock(ClusterDescription.class);var server=mock(ServerDescription.class);
        when(description.getType()).thenReturn(topology.equals("unknown")?ClusterType.UNKNOWN:topology.equals("router")?ClusterType.SHARDED:ClusterType.REPLICA_SET);
        when(description.getServerDescriptions()).thenReturn(topology.equals("empty")?List.of():List.of(server));
        when(server.getMaxWireVersion()).thenReturn(topology.equals("old-wire")?12:21);
        when(client.getClusterDescription()).thenReturn(description);when(client.startSession()).thenReturn(session);
        when(session.withTransaction(any(),any())).thenAnswer(call->((TransactionBody<?>)call.getArgument(0)).execute());
        assertThat(new MongoPipelineIntentStore(client,db,"cluster",bindings).loadForActuation("orders")).isEmpty();
        verify(client).startSession();verify(client,never()).startSession(any());verify(session).withTransaction(any(),any());verify(session).close();
    }
}
