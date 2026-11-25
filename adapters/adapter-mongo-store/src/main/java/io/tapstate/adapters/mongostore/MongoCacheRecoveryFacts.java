package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.*;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.bson.Document;

/** Strict decoding of the current mapping's immutable predecessor and replacement scopes. */
final class MongoCacheRecoveryFacts {
    private MongoCacheRecoveryFacts() {}
    static Document scope(PipelineIntentScope value) {
        return new Document("clusterId", value.clusterId()).append("bindingVersion", value.bindingVersion())
                .append("cacheIncarnation", value.cacheIncarnation()).append("pipelineId", value.pipelineId()).append("stateGeneration", value.stateGeneration());
    }
    static PipelineIntentScope scope(Document value) {
        if (value == null || !value.keySet().equals(Set.of("clusterId", "bindingVersion", "cacheIncarnation", "pipelineId", "stateGeneration"))
                || !(value.get("bindingVersion") instanceof Long) || !(value.get("stateGeneration") instanceof Long)) throw unreadable();
        return new PipelineIntentScope(value.getString("clusterId"), value.getLong("bindingVersion"), value.getString("cacheIncarnation"),
                value.getString("pipelineId"), value.getLong("stateGeneration"));
    }
    static Document encode(PipelineCacheRecovery value) {
        return new Document("recoveryId", value.recoveryId()).append("previousScope", value.previousScope() == null ? null : scope(value.previousScope()))
                .append("targetScope", scope(value.targetScope()));
    }
    static Optional<PipelineCacheRecovery> read(Document mapping) {
        if (mapping == null || !mapping.containsKey("cacheRecovery")) return Optional.empty();
        try {
            if (!(mapping.get("cacheRecovery") instanceof Document value)
                    || !value.keySet().equals(Set.of("recoveryId", "previousScope", "targetScope"))) throw unreadable();
            var result = new PipelineCacheRecovery(value.getString("recoveryId"), value.get("previousScope") == null ? null : scope(value.get("previousScope", Document.class)),
                    scope(value.get("targetScope", Document.class)));
            if (!result.targetScope().pipelineId().equals(mapping.get("_id"))
                    || !Long.valueOf(result.targetScope().stateGeneration()).equals(mapping.get("stateGeneration"))) throw unreadable();
            return Optional.of(result);
        } catch (IllegalArgumentException | NullPointerException | ClassCastException invalid) { throw unreadable(); }
    }
    static TapstateException unreadable() { return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", "pipeline-cache-recovery", "field", "scopeSummary"), null); }
    static TapstateException refused() { return new TapstateException(IoError.CACHE_BUSINESS_FENCED, Map.of(), null); }
}
