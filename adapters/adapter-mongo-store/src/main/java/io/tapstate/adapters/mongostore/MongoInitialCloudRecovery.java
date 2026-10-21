package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.CacheAuthScope;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.spi.store.StorageRecoveryCommands;
import java.util.Map;
import java.util.Set;
import org.bson.Document;

/** The narrow initial binding closure has no pipeline or prior execution history to reconstruct. */
final class MongoInitialCloudRecovery {
    private MongoInitialCloudRecovery() {}

    static boolean shape(StorageRecoveryCommands.Manifest manifest) {
        return manifest.selection().extent() == StorageRecoveryCommands.Extent.CLUSTER
                && manifest.affectedPipelineIds().isEmpty()
                && manifest.definitionHashes().keySet().equals(Set.of(StorageBinding.defaultSourceId(StorageBinding.Mode.CLOUD)));
    }

    static SourceResource require(ClientSession session, MongoDatabase database, MongoStorageBindingStore bindings,
            MongoConfigurationArtifactStore artifacts, CacheAuthScope scope) {
        if (scope.mode() != StorageBinding.Mode.CLOUD || scope.authGeneration() < 2) throw MongoCachePipelineAccess.fenced();
        var binding = bindings.loadForTransaction(session).orElseThrow(MongoCachePipelineAccess::fenced);
        var authentication = MongoStorageRecoveryStore.authentication(session, database, binding);
        if (authentication.pending() || !authentication.preparation().authScope().equals(scope)) throw MongoCachePipelineAccess.fenced();
        Document setup = SystemCollections.STORAGE_SETUP.on(database).find(session, new Document("_id", "cluster")).first();
        if (setup == null) throw MongoCachePipelineAccess.fenced();
        if (!"STORAGE_BOUND".equals(setup.get("phase"))) {
            var row = SystemCollections.STORAGE_RECOVERY.on(database).find(session, new Document("_id", "cluster")).first();
            var previous = row == null ? null : MongoStorageRecoveryCommands.pending(row).orElse(null);
            if (!"COMPLETE".equals(setup.get("phase")) || previous == null || !shape(previous.manifest()))
                throw MongoCachePipelineAccess.fenced();
        }
        return requirePristineSource(session, database, binding, artifacts, scope, false);
    }

    static SourceResource requirePristineSource(ClientSession session, MongoDatabase database, StorageBinding binding,
            MongoConfigurationArtifactStore artifacts, CacheAuthScope scope, boolean pendingAdministrator) {
        var existing = new java.util.HashSet<>(database.listCollectionNames().into(new java.util.ArrayList<>()));
        for (var installed : new SystemCollections[]{SystemCollections.CAPTURE_PREPARATIONS, SystemCollections.PDK_NOTE_SCOPES,
                SystemCollections.PDK_NOTE_ORIGINS, SystemCollections.PDK_NOTE_CARRIES, SystemCollections.CONFIG_PIPELINE_DRAFTS,
                SystemCollections.CONFIGURATION_IMPORTS, SystemCollections.CONFIG_NEST_DEAD_LETTERS}) {
            if (!existing.contains(installed.collectionName())) throw MongoCachePipelineAccess.fenced();
        }
        for (var table : new SystemCollections[]{SystemCollections.PIPELINES, SystemCollections.PIPELINE_INTENTS,
                SystemCollections.CAPTURE_PREPARATIONS, SystemCollections.OPERATOR_GENERATIONS, SystemCollections.CONFIG_PIPELINE_LAYOUTS,
                SystemCollections.CONFIG_PIPELINE_DRAFTS, SystemCollections.TRANSFORM_DEFINITIONS, SystemCollections.VIEW_DEFINITIONS,
                SystemCollections.SERVE_DEFINITIONS, SystemCollections.PDK_NOTES,
                SystemCollections.PDK_NOTE_ORIGINS, SystemCollections.PDK_NOTE_CARRIES, SystemCollections.PDK_NOTE_MIGRATIONS,
                SystemCollections.CONFIG_NEST_DEAD_LETTERS, SystemCollections.CONFIG_CONNECTIONS, SystemCollections.CONFIGURATION_IMPORTS, SystemCollections.CONFIG_USERS}) {
            if (table.on(database).find(session).limit(1).first() != null) throw MongoCachePipelineAccess.fenced();
        }
        if (!pendingAdministrator && SystemCollections.OP_BOOTSTRAP.on(database).find(session).limit(1).first() != null) throw MongoCachePipelineAccess.fenced();
        var scopes = SystemCollections.PDK_NOTE_SCOPES.on(database).find(session).limit(2).into(new java.util.ArrayList<>());
        var pristine = new Document("_id", new Document("bindingVersion", binding.bindingVersion()).append("namespace", "pdk.global-state"))
                .append("ownerKind", "GLOBAL").append("ownerId", scope.clusterId()).append("generation", 1L).append("writeFence", 0L)
                .append("outgoingCarries", 0L).append("incomingCarries", 0L).append("originGeneration", 0L);
        if (scopes.size() != 1 || !scopes.getFirst().equals(pristine)) throw MongoCachePipelineAccess.fenced();
        if (SystemCollections.SOURCES.on(database).countDocuments(session) != 1) throw MongoCachePipelineAccess.fenced();
        var resource = artifacts.getInTransaction(session, binding.defaultSourceId()).orElseThrow(MongoCachePipelineAccess::fenced);
        if (!(resource instanceof SourceResource source) || (scope.mode() == StorageBinding.Mode.CLOUD
                ? !"mongodb-atlas".equals(source.connector()) : !Set.of("mongodb", "mongodb-atlas").contains(source.connector()))) throw MongoCachePipelineAccess.fenced();
        return source;
    }

    static void requireManifest(ClientSession session, MongoDatabase database, MongoStorageBindingStore bindings,
            MongoConfigurationArtifactStore artifacts, CacheAuthScope scope, StorageRecoveryCommands.Manifest manifest) {
        if (!shape(manifest)) throw MongoCachePipelineAccess.fenced();
        var source = require(session, database, bindings, artifacts, scope);
        if (!manifest.definitionHashes().equals(Map.of(source.id(), CanonicalHash.of(source)))) throw MongoCachePipelineAccess.fenced();
    }
}
