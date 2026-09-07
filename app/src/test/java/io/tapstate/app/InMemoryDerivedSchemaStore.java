package io.tapstate.app;

import io.tapstate.spi.store.DerivedSchema;
import io.tapstate.spi.store.DerivedSchemaStore;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * In-memory {@link DerivedSchemaStore} for the assembly-layer tests: the versioned side record of what a
 * step works its own columns out to be, held in a map. It carries the same append-on-change /
 * refresh-in-place rule the port documents, because a test double that recorded every start as a new
 * version would make the drift gate look like it fires when it does not.
 */
final class InMemoryDerivedSchemaStore implements DerivedSchemaStore {

    private final Map<String, List<DerivedSchema>> byStep = new LinkedHashMap<>();

    @Override
    public Optional<DerivedSchema> latest(String pipelineId, String stepId) {
        List<DerivedSchema> versions = byStep.get(key(pipelineId, stepId));
        return versions == null || versions.isEmpty()
                ? Optional.empty()
                : Optional.of(versions.get(versions.size() - 1));
    }

    @Override
    public void record(String pipelineId, String stepId, Map<String, String> schema, String statement,
            String derivedFrom, String derivedBy) {
        List<DerivedSchema> versions =
                byStep.computeIfAbsent(key(pipelineId, stepId), ignored -> new ArrayList<>());
        DerivedSchema last = versions.isEmpty() ? null : versions.get(versions.size() - 1);
        if (last != null && last.schema().equals(schema)) {
            versions.set(versions.size() - 1,
                    new DerivedSchema(last.version(), last.schema(), statement, derivedFrom, derivedBy));
            return;
        }
        versions.add(new DerivedSchema(
                last == null ? 0L : last.version() + 1, schema, statement, derivedFrom, derivedBy));
    }

    @Override
    public void delete(String pipelineId) {
        byStep.keySet().removeIf(key -> key.startsWith(pipelineId + "/"));
    }

    private static String key(String pipelineId, String stepId) {
        return pipelineId + "/" + stepId;
    }
}
