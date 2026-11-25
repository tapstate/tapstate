package io.tapstate.e2e;

import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.adapters.mongostore.MongoConnectionSettings;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.PipelinePreparation;
import io.tapstate.spi.store.PipelineStateMapping;
import java.time.Duration;
import org.bson.Document;

/** Read-only native scope observations; a display state is not a replacement-completion receipt. */
final class NativePipelineScopes {
    private NativePipelineScopes() {}

    static PipelineIntentScope current(PreparedNativeStore storage, String pipeline) {
        try (var configuration = open(storage)) {
            return configuration.intents().load(pipeline).orElseThrow().scope();
        }
    }

    static PipelineIntentScope awaitCleared(ControlPlane control, PreparedNativeStore storage,
            PipelineIntentScope original, Duration bound) {
        var successor = new PipelineIntentScope(original.clusterId(), original.bindingVersion(), original.cacheIncarnation(),
                original.pipelineId(), Math.addExact(original.stateGeneration(), 1));
        try (var configuration = open(storage); var cache = MongoClients.create(storage.cacheUri())) {
            Await.until("the clearing stop to finish in its original successor scope", bound, () -> {
                var mapping = configuration.pipelineStates().read(original.pipelineId()).orElse(null);
                var preparation = configuration.preparations().read(original.pipelineId()).orElse(null);
                var intent = configuration.intents().load(original.pipelineId()).orElse(null);
                if (mapping == null || mapping.phase() != PipelineStateMapping.Phase.ACTIVE
                        || mapping.stateGeneration() != successor.stateGeneration() || mapping.operationId() == null
                        || preparation == null || preparation.phase() != PipelinePreparation.Phase.COMPLETE
                        || !successor.equals(preparation.scope()) || intent == null || intent.deleted()
                        || !successor.equals(intent.scope()) || intent.desired().targetState() != PipelineState.STOPPED
                        || intent.desired().purgeState() || intent.desired().reassemble()) return false;
                var state = SystemCollections.CACHE_PIPELINE_STATE.on(cache.getDatabase(storage.cacheDatabase()))
                        .find(new Document("_id", original.pipelineId())).first();
                return state != null && document(successor).equals(state.get("scope"))
                        && StateJson.parse(state.getString("stateJson")) == PipelineState.STOPPED
                        && control.state(original.pipelineId()).filter(PipelineState.STOPPED::equals).isPresent();
            }, () -> "mapping=" + configuration.pipelineStates().read(original.pipelineId())
                    + ", preparation=" + configuration.preparations().read(original.pipelineId())
                    + ", intent=" + configuration.intents().load(original.pipelineId()));
        }
        return successor;
    }

    static Document document(PipelineIntentScope scope) {
        return new Document("clusterId", scope.clusterId()).append("bindingVersion", scope.bindingVersion())
                .append("cacheIncarnation", scope.cacheIncarnation()).append("pipelineId", scope.pipelineId())
                .append("stateGeneration", scope.stateGeneration());
    }

    private static MongoConfigurationDomain open(PreparedNativeStore storage) {
        return MongoConfigurationDomain.openExisting(new MongoConnectionSettings(
                storage.configurationUri(), null, Duration.ofSeconds(5)));
    }
}
