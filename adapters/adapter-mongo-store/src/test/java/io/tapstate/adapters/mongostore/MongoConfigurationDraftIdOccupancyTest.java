package io.tapstate.adapters.mongostore;

import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.TransactionBody;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ServerDescription;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Date;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@SuppressWarnings({"unchecked", "rawtypes"})
class MongoConfigurationDraftIdOccupancyTest {
    private static final String ID = "orders";
    private final MongoClient client = mock(MongoClient.class);
    private final MongoDatabase database = mock(MongoDatabase.class, RETURNS_SELF);
    private final MongoCollection<Document> identity = mock(MongoCollection.class, RETURNS_SELF);
    private final AggregateIterable<Document> result = mock(AggregateIterable.class);
    private final ClusterDescription cluster = mock(ClusterDescription.class);
    private final MongoConfigurationDraftStore store;

    MongoConfigurationDraftIdOccupancyTest() {
        when(client.getClusterDescription()).thenReturn(cluster);
        when(cluster.getServerDescriptions()).thenReturn(List.of());
        when(database.getCollection(anyString())).thenAnswer(call -> {
            var collection = mock(MongoCollection.class, RETURNS_SELF);
            when(collection.getNamespace()).thenReturn(new MongoNamespace("config", call.getArgument(0, String.class)));
            return collection;
        });
        when(database.getCollection(SystemCollections.CONFIG_CLUSTER_IDENTITY.collectionName())).thenReturn(identity);
        when(identity.aggregate(anyList())).thenReturn(result);
        var cipher = mock(SourceConfigCipherProvider.class);
        var artifacts = new MongoConfigurationArtifactStore(client, database, cipher);
        store = new MongoConfigurationDraftStore(client, database, cipher, artifacts);
    }

    @Test
    void absentIdUsesOnePrimarySnapshotAndOnlyExactIdProjections() {
        when(result.first()).thenReturn(empty());
        assertThat(store.idOccupied(ID)).contains(false);

        verify(database).withReadPreference(ReadPreference.primary());
        verify(database).withReadConcern(ReadConcern.SNAPSHOT);
        ArgumentCaptor<List<? extends Bson>> stages = ArgumentCaptor.forClass(List.class);
        verify(identity).aggregate(stages.capture());
        verify(client, never()).startSession();
        var pipeline = stages.getValue().stream().map(Document.class::cast).toList();
        assertThat(pipeline.getFirst()).isEqualTo(new Document("$match", new Document("_id", "cluster")));
        var lookups = pipeline.subList(2, pipeline.size()).stream().map(stage -> stage.get("$lookup", Document.class)).toList();
        assertThat(lookups).hasSize(6).extracting(lookup -> lookup.getString("from"))
                .containsExactlyInAnyOrder("sources", "pipelines", "transform_definitions", "view_definitions",
                        "serve_definitions", "pipeline_drafts");
        assertThat(lookups).allSatisfy(lookup -> assertThat(lookup.getList("pipeline", Document.class).getFirst())
                .isEqualTo(new Document("$match", new Document("_id", ID))));
        assertThat(lookups.subList(0, 5)).allSatisfy(lookup -> assertThat(lookup.getList("pipeline", Document.class).getLast())
                .isEqualTo(new Document("$project", new Document("_id", 1))));
        assertThat(lookups.getLast().getList("pipeline", Document.class).getLast().toJson())
                .contains("tombstone", "$bsonSize").doesNotContain("ciphertext");
    }

    @Test
    void publishedAndActiveDraftIdsOccupyTheNamespaceButValidTombstonesDoNot() {
        when(result.first()).thenReturn(
                empty().append("artifact4", List.of(new Document("_id", ID))),
                withDraft(new Document("_id", ID).append("deleted", false)),
                withDraft(new Document("_id", ID).append("deleted", true).append("tombstone",
                        new Document("_id", ID).append("formatVersion", 1).append("revision", 2L)
                                .append("deleted", true).append("updatedAt", new Date(0)))));
        assertThat(store.idOccupied(ID)).contains(true);
        assertThat(store.idOccupied(ID)).contains(true);
        assertThat(store.idOccupied(ID)).contains(false);
    }

    @Test
    void missingContractAnchorMalformedTombstonesAndPrimaryErrorsFailClosed() {
        when(result.first()).thenReturn(null, withDraft(new Document("deleted", true).append("tombstone", false)),
                withDraft(new Document("deleted", true).append("tombstone", new Document("_id", ID))))
                .thenThrow(new MongoException("primary unavailable"));
        for (int i = 0; i < 3; i++) {
            assertThatThrownBy(() -> store.idOccupied(ID))
                    .isInstanceOfSatisfying(TapstateException.class,
                            failure -> assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
        }
        assertThatThrownBy(() -> store.idOccupied(ID)).isInstanceOfSatisfying(TapstateException.class,
                failure -> assertThat(failure.code()).isEqualTo(IoError.STORE_UNAVAILABLE));
    }

    @Test
    void olderServersKeepTheSamePredicateInOneSnapshotTransaction() {
        var legacyServer = mock(ServerDescription.class);
        when(legacyServer.getMaxWireVersion()).thenReturn(12);
        when(cluster.getServerDescriptions()).thenReturn(List.of(legacyServer));
        var session = mock(ClientSession.class);
        when(client.startSession()).thenReturn(session);
        when(session.withTransaction(any(TransactionBody.class), any()))
                .thenAnswer(call -> call.getArgument(0, TransactionBody.class).execute());
        when(identity.aggregate(eq(session), anyList())).thenReturn(result);
        when(result.first()).thenReturn(empty());
        assertThat(store.idOccupied(ID)).contains(false);
        verify(identity).aggregate(eq(session), anyList());
        verify(identity, never()).aggregate(anyList());
        verify(session).close();
    }

    private static Document empty() {
        var properties = new Document("draft", List.of());
        for (int i = 0; i < 5; i++) properties.append("artifact" + i, List.of());
        return properties;
    }

    private static Document withDraft(Document draft) {
        return empty().append("draft", List.of(draft));
    }
}
