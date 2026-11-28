package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.FindIterable;
import com.mongodb.client.ListCollectionNamesIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.CacheAuthScope;
import io.tapstate.spi.store.StorageBinding;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Covers MongoInitialCloudRecovery.requirePristineSource connector-scheme
 * condition change: both "mongodb" and "mongodb-atlas" now accepted in CLOUD mode.
 */
class MongoInitialCloudRecoveryTest {

    private static final java.util.Set<String> ALL = java.util.Set.of(
            SystemCollections.CAPTURE_PREPARATIONS.collectionName(),
            SystemCollections.PDK_NOTE_SCOPES.collectionName(),
            SystemCollections.PDK_NOTE_ORIGINS.collectionName(),
            SystemCollections.PDK_NOTE_CARRIES.collectionName(),
            SystemCollections.CONFIG_PIPELINE_DRAFTS.collectionName(),
            SystemCollections.CONFIGURATION_IMPORTS.collectionName(),
            SystemCollections.CONFIG_NEST_DEAD_LETTERS.collectionName(),
            SystemCollections.PIPELINES.collectionName(),
            SystemCollections.PIPELINE_INTENTS.collectionName(),
            SystemCollections.OPERATOR_GENERATIONS.collectionName(),
            SystemCollections.CONFIG_PIPELINE_LAYOUTS.collectionName(),
            SystemCollections.TRANSFORM_DEFINITIONS.collectionName(),
            SystemCollections.VIEW_DEFINITIONS.collectionName(),
            SystemCollections.SERVE_DEFINITIONS.collectionName(),
            SystemCollections.PDK_NOTES.collectionName(),
            SystemCollections.PDK_NOTE_MIGRATIONS.collectionName(),
            SystemCollections.CONFIG_CONNECTIONS.collectionName(),
            SystemCollections.CONFIG_USERS.collectionName()
    );

    private static final Document PRISTINE = new Document("_id", new Document("bindingVersion", 1L).append("namespace", "pdk.global-state"))
            .append("ownerKind", "GLOBAL").append("ownerId", "cluster-id").append("generation", 1L).append("writeFence", 0L)
            .append("outgoingCarries", 0L).append("incomingCarries", 0L).append("originGeneration", 0L);

    @Test void requirePristineSourceAcceptsPlainMongodbConnector() {
        var source = new SourceResource("default-source", null, "mongodb", Map.of("uri", "mongodb://host/db"), null, null, null, null);
        assertThat(requirePristineSourceWith(source)).isSameAs(source);
    }

    @Test void requirePristineSourceAcceptsAtlasConnector() {
        var source = new SourceResource("default-source", null, "mongodb-atlas", Map.of("uri", "mongodb+srv://host/db"), null, null, null, null);
        assertThat(requirePristineSourceWith(source)).isSameAs(source);
    }

    @Test void requirePristineSourceRejectsUnknownConnector() {
        var source = new SourceResource("default-source", null, "unknown-connector", Map.of("uri", "mongodb://host/db"), null, null, null, null);
        assertThatThrownBy(() -> requirePristineSourceWith(source)).isInstanceOf(RuntimeException.class);
    }

    private SourceResource requirePristineSourceWith(SourceResource source) {
        var artifacts = mock(MongoConfigurationArtifactStore.class);
        var database = mock(MongoDatabase.class);
        var session = mock(ClientSession.class);

        // listCollectionNames().into(new ArrayList<>())
        var listNames = mock(ListCollectionNamesIterable.class);
        when(listNames.into(org.mockito.ArgumentMatchers.any())).thenAnswer(inv -> {
            @SuppressWarnings("unchecked") var dest = (java.util.Collection<String>) inv.getArgument(0);
            dest.addAll(ALL);
            return inv.getArgument(0);
        });
        when(database.listCollectionNames()).thenReturn(listNames);

        // default collection returns null for every find().limit().first()
        var emptyFind = mock(FindIterable.class);
        var emptyLimited = mock(FindIterable.class);
        when(emptyFind.limit(anyInt())).thenReturn(emptyLimited);
        when(emptyLimited.first()).thenReturn(null);
        var defaultColl = mock(MongoCollection.class);
        when(defaultColl.find(session)).thenReturn(emptyFind);
        when(database.getCollection(anyString())).thenReturn(defaultColl);

        // PDK_NOTE_SCOPES — into(2) should contain pristine
        var pdkScopes = mock(MongoCollection.class);
        var pdkScopesFind = mock(FindIterable.class);
        var pdkScopesLimit = mock(FindIterable.class);
        when(pdkScopes.find(session)).thenReturn(pdkScopesFind);
        when(pdkScopesFind.limit(2)).thenReturn(pdkScopesLimit);
        when(pdkScopesLimit.into(org.mockito.ArgumentMatchers.any())).thenAnswer(inv -> {
            @SuppressWarnings("unchecked") var dest = (java.util.Collection<Document>) inv.getArgument(0);
            dest.add(PRISTINE);
            return inv.getArgument(0);
        });
        when(database.getCollection(SystemCollections.PDK_NOTE_SCOPES.collectionName())).thenReturn(pdkScopes);

        // SOURCES — countDocuments == 1
        var sources = mock(MongoCollection.class);
        when(sources.countDocuments(session)).thenReturn(1L);
        when(database.getCollection(SystemCollections.SOURCES.collectionName())).thenReturn(sources);

        // bindings and scope
        var binding = mock(StorageBinding.class);
        when(binding.bindingVersion()).thenReturn(1L);
        when(binding.defaultSourceId()).thenReturn("default-source");
        var scope = mock(CacheAuthScope.class);
        when(scope.clusterId()).thenReturn("cluster-id");

        when(artifacts.getInTransaction(session, "default-source")).thenReturn(Optional.of(source));

        return MongoInitialCloudRecovery.requirePristineSource(session, database, binding, artifacts, scope, false);
    }
}
