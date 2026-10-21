package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.CacheAuthScope;
import io.tapstate.spi.store.FirstAdminStore;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.spi.store.StorageRecoveryCommands;
import java.util.Map;
import java.util.Set;
import org.bson.Document;

/** A never-completed OP binding has no account, issued credential, pipeline or execution to recover. */
final class MongoInitialOpRecovery {
    private MongoInitialOpRecovery() {}
    static boolean shape(StorageRecoveryCommands.Manifest manifest) {
        return manifest.selection().extent() == StorageRecoveryCommands.Extent.CLUSTER
                && manifest.affectedPipelineIds().isEmpty()
                && manifest.definitionHashes().keySet().equals(Set.of(StorageBinding.defaultSourceId(StorageBinding.Mode.OP)));
    }
    static SourceResource require(MongoClient client, ClientSession session, MongoDatabase database,
            MongoStorageBindingStore bindings, MongoConfigurationArtifactStore artifacts, CacheAuthScope scope, boolean allowCompleted) {
        if (scope.mode() != StorageBinding.Mode.OP || scope.authGeneration() != 1) throw MongoCachePipelineAccess.fenced();
        var binding = bindings.loadForTransaction(session).orElseThrow(MongoCachePipelineAccess::fenced);
        if (binding.mode() != StorageBinding.Mode.OP || !bindings.cachePreparationForTransaction(session).orElseThrow(MongoCachePipelineAccess::fenced).authScope().equals(scope))
            throw MongoCachePipelineAccess.fenced();
        var setup = SystemCollections.STORAGE_SETUP.on(database).find(session, new Document("_id", "cluster")).first();
        if (setup == null || SystemCollections.CONFIG_TOKENS.on(database).find(session).limit(1).first() != null) throw MongoCachePipelineAccess.fenced();
        boolean completed = "COMPLETE".equals(setup.get("phase"));
        if (!"STORAGE_BOUND".equals(setup.get("phase"))) {
            var root = SystemCollections.STORAGE_RECOVERY.on(database).find(session, new Document("_id", "cluster")).first();
            var prior = root == null ? null : MongoStorageRecoveryCommands.pending(root).orElse(null);
            if (!allowCompleted || !completed || prior == null || prior.phase() != StorageRecoveryCommands.Phase.COMPLETE
                    || !prior.originalScope().equals(scope) || !shape(prior.manifest())) throw MongoCachePipelineAccess.fenced();
            var administrator = new MongoFirstAdminStore(client, database, bindings).load(session).orElseThrow(MongoCachePipelineAccess::fenced);
            if (administrator.phase() != FirstAdminStore.Phase.PENDING) throw MongoCachePipelineAccess.fenced();
        }
        return MongoInitialCloudRecovery.requirePristineSource(session, database, binding, artifacts, scope, completed);
    }
    static void requireManifest(MongoClient client, ClientSession session, MongoDatabase database,
            MongoStorageBindingStore bindings, MongoConfigurationArtifactStore artifacts, CacheAuthScope scope,
            StorageRecoveryCommands.Manifest manifest, boolean allowCompleted) {
        if (!shape(manifest)) throw MongoCachePipelineAccess.fenced();
        var source = require(client, session, database, bindings, artifacts, scope, allowCompleted);
        if (!manifest.definitionHashes().equals(Map.of(source.id(), CanonicalHash.of(source)))) throw MongoCachePipelineAccess.fenced();
    }
}
