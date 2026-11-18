package io.tapstate.adapters.mongostore;

import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ServerDescription;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.bson.Document;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

class MongoIntentSnapshotCloseTest {
    @ParameterizedTest @ValueSource(strings={"already-closed","topology","session"})
    void aClosedConfigurationIsDiagnosedBeforeADriverFailure(String stage){
        var client=mock(MongoClient.class);var db=mock(MongoDatabase.class);
        @SuppressWarnings("unchecked") var collection=(MongoCollection<Document>)mock(MongoCollection.class);
        when(db.getName()).thenReturn("test");when(db.getCollection(anyString())).thenReturn(collection);
        when(collection.getNamespace()).thenReturn(new MongoNamespace("test","pipeline_intents"));
        when(client.getDatabase("test")).thenReturn(db);
        var bindings=mock(MongoStorageBindingStore.class);when(bindings.belongsTo(client,"test")).thenReturn(true);
        var active=new AtomicBoolean(!stage.equals("already-closed"));
        var description=mock(ClusterDescription.class);var server=mock(ServerDescription.class);
        when(description.getType()).thenReturn(ClusterType.REPLICA_SET);when(description.getServerDescriptions()).thenReturn(List.of(server));
        when(server.getMaxWireVersion()).thenReturn(21);
        when(client.getClusterDescription()).thenAnswer(c->{
            if(!stage.equals("session")){active.set(false);throw new IllegalStateException("client is closed");}
            return description;
        });
        when(client.startSession(any())).thenAnswer(c->{active.set(false);throw new IllegalStateException("client is closed");});
        var store=new MongoPipelineIntentStore(client,db,"cluster",bindings).guardedBy(()->{
            if(!active.get())throw new TapstateException(IoError.PIPELINE_INTENT_FENCED,Map.of(),null);
        });
        assertThatThrownBy(()->store.loadForActuation("orders")).isInstanceOfSatisfying(TapstateException.class,
                failure->assertThat(failure.code()).isEqualTo(IoError.PIPELINE_INTENT_FENCED));
    }
}
