package io.tapstate.adapters.pdk;

import io.tapstate.core.common.TapstateException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

/** Checks every potentially matched document before any row in the batch reaches the native writer. */
final class PdkMongoIdentityGuard {
    private static final int CHUNK_SIZE = 128;

    private PdkMongoIdentityGuard() { }

    static void verify(PdkConnector connector, TargetIdentityPlan plan, List<Map<String, Object>> keys) {
        List<Map<String, Object>> distinct = new ArrayList<>(new LinkedHashSet<>(keys));
        for (int start = 0; start < distinct.size(); start += CHUNK_SIZE) {
            List<Map<String, Object>> chunk = distinct.subList(start, Math.min(start + CHUNK_SIZE, distinct.size()));
            verifyChunk(connector, plan, chunk);
        }
    }

    private static void verifyChunk(PdkConnector connector, TargetIdentityPlan plan, List<Map<String, Object>> keys) {
        try {
            // Extended JSON survives the connector's JSON round trip with the write predicate's BSON types.
            var rows = PdkIdentityIndexes.execute(connector, "aggregate", Map.of("collection", plan.target().name(),
                    "pipeline", List.of(Map.of("$match", Map.of("$or", keys)), Map.of("$limit", keys.size() + 1),
                            Map.of("$facet", Map.of("rows", List.of(Map.of("$project",
                                    Map.of("_id", 0, "safe", MongoIdentityKeys.storedTypes(plan))))))),
                    "batchSize", CHUNK_SIZE));
            // The facet returns one envelope even when no document matches. Zero callbacks remain an error.
            if (rows.size() != 1 || !(rows.getFirst().get("rows") instanceof List<?> matches)) {
                throw new IllegalStateException("target key inspection returned an incomplete result");
            }
            if (matches.size() > keys.size() || matches.stream().anyMatch(row ->
                    !(row instanceof Map<?, ?> values) || !Boolean.TRUE.equals(values.get("safe")))) {
                throw TargetIdentityProjection.failure(plan, "key-representation-unsupported");
            }
        } catch (TapstateException coded) {
            throw coded;
        } catch (Throwable failure) {
            throw new TapstateException(ConnectorError.TARGET_IDENTITY_CHECK_FAILED,
                    Map.of("connector", connector.connectorId(), "table", plan.target().name(),
                            "detail", failure.getMessage() == null ? failure.getClass().getSimpleName() : failure.getMessage()), failure);
        }
    }
}
