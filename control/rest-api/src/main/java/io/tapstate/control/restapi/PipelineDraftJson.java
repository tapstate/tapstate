package io.tapstate.control.restapi;

import io.tapstate.spi.store.PipelineDraft;
import tools.jackson.databind.ObjectMapper;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Maps the lower-case wire contract to the driver-free PipelineDraft model. */
final class PipelineDraftJson {

    private PipelineDraftJson() {
    }

    static PipelineDraft read(ObjectMapper json, Map<String, Object> body, String pathId, long defaultRevision,
            String updatedBy) {
        String id = text(body.get("pipelineId"), "pipelineId");
        if (pathId != null && !pathId.equals(id)) {
            throw new IllegalArgumentException("pipelineId does not match the path");
        }
        int schemaVersion = number(body.getOrDefault("schemaVersion", PipelineDraft.CURRENT_SCHEMA_VERSION),
                "schemaVersion").intValue();
        long revision = defaultRevision;
        String mode = text(body.get("mode"), "mode").toUpperCase(java.util.Locale.ROOT);
        Map<String, Object> graph = object(body.get("graph"));
        Map<String, Object> wizard = object(body.get("wizard"));
        PipelineDraft.Graph graphModel = graph == null ? null
                : json.convertValue(graph, PipelineDraft.Graph.class);
        PipelineDraft.Wizard wizardModel = wizard == null ? null
                : json.convertValue(normalizeWizardForModel(wizard), PipelineDraft.Wizard.class);
        return new PipelineDraft(
                id,
                schemaVersion,
                revision,
                PipelineDraft.Mode.valueOf(mode),
                textOrNull(body.get("name")),
                textOrNull(body.get("description")),
                graphModel,
                wizardModel,
                null,
                null,
                null,
                java.time.Instant.EPOCH,
                java.time.Instant.EPOCH,
                requireActor(updatedBy));
    }

    static Map<String, Object> write(ObjectMapper json, PipelineDraft draft) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("pipelineId", draft.pipelineId());
        result.put("schemaVersion", draft.schemaVersion());
        result.put("revision", draft.revision());
        result.put("mode", draft.mode().name().toLowerCase(java.util.Locale.ROOT));
        result.put("name", draft.name());
        result.put("description", draft.description());
        result.put("graph", draft.graph() == null ? null : json.convertValue(draft.graph(), Map.class));
        result.put("wizard", draft.wizard() == null ? null
                : normalizeWizardForWire(json.convertValue(draft.wizard(), Map.class)));
        result.put("baseArtifactHash", draft.baseArtifactHash());
        result.put("publishedDraftRevision", draft.publishedDraftRevision());
        result.put("publishedArtifactHash", draft.publishedArtifactHash());
        result.put("createdAt", draft.createdAt());
        result.put("updatedAt", draft.updatedAt());
        result.put("updatedBy", draft.updatedBy());
        return result;
    }

    /** The Wizard API uses lower-case shape names while the storage model uses enum constants. */
    private static Map<String, Object> normalizeWizardForModel(Map<String, Object> wizard) {
        Map<String, Object> normalized = new LinkedHashMap<>(wizard);
        Object relatedValue = normalized.get("related");
        if (!(relatedValue instanceof List<?> related)) {
            return normalized;
        }

        normalized.put("related", related.stream().map(item -> {
            if (!(item instanceof Map<?, ?> relationEntry)) {
                return item;
            }
            Map<String, Object> nextEntry = new LinkedHashMap<>();
            relationEntry.forEach((key, value) -> nextEntry.put(String.valueOf(key), value));
            Object relationValue = nextEntry.get("relation");
            if (relationValue instanceof Map<?, ?> relation) {
                Map<String, Object> nextRelation = new LinkedHashMap<>();
                relation.forEach((key, value) -> nextRelation.put(String.valueOf(key), value));
                Object shape = nextRelation.get("shape");
                if (shape instanceof String value) {
                    nextRelation.put("shape", value.toUpperCase(java.util.Locale.ROOT));
                }
                nextEntry.put("relation", nextRelation);
            }
            return nextEntry;
        }).toList());
        return normalized;
    }

    /** The Wizard wire contract lowercases only the relation shape enum, never arbitrary user config. */
    private static Map<String, Object> normalizeWizardForWire(Map<String, Object> wizard) {
        Map<String, Object> normalized = new LinkedHashMap<>(wizard);
        Object relatedValue = normalized.get("related");
        if (!(relatedValue instanceof List<?> related)) {
            return normalized;
        }

        normalized.put("related", related.stream().map(item -> {
            if (!(item instanceof Map<?, ?> relationEntry)) {
                return item;
            }
            Map<String, Object> nextEntry = new LinkedHashMap<>();
            relationEntry.forEach((key, value) -> nextEntry.put(String.valueOf(key), value));
            Object relationValue = nextEntry.get("relation");
            if (relationValue instanceof Map<?, ?> relation) {
                Map<String, Object> nextRelation = new LinkedHashMap<>();
                relation.forEach((key, value) -> nextRelation.put(String.valueOf(key), value));
                Object shape = nextRelation.get("shape");
                if (shape instanceof String value) {
                    nextRelation.put("shape", value.toLowerCase(java.util.Locale.ROOT));
                }
                nextEntry.put("relation", nextRelation);
            }
            return nextEntry;
        }).toList());
        return normalized;
    }

    private static String text(Object value, String field) {
        if (!(value instanceof String string) || string.isBlank()) {
            throw new IllegalArgumentException(field + " must be non-blank");
        }
        return string;
    }

    private static String requireActor(String updatedBy) {
        if (updatedBy == null || updatedBy.isBlank()) {
            throw new IllegalArgumentException("updatedBy must be supplied by the authenticated caller");
        }
        return updatedBy;
    }

    private static String textOrNull(Object value) {
        if (value == null) {
            return null;
        }
        if (!(value instanceof String string)) {
            throw new IllegalArgumentException("text field must be a string");
        }
        return string;
    }

    private static Number number(Object value, String field) {
        if (!(value instanceof Number number)
                || !Double.isFinite(number.doubleValue())
                || number.doubleValue() != Math.rint(number.doubleValue())) {
            throw new IllegalArgumentException(field + " must be numeric");
        }
        return number;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> object(Object value) {
        return value instanceof Map<?, ?> map ? (Map<String, Object>) map : null;
    }
}
