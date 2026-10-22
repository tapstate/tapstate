package io.tapstate.spi.store;

import io.tapstate.core.model.Resource;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Durable, server-owned authoring document for one Pipeline. */
public record PipelineDraft(
        String pipelineId,
        int schemaVersion,
        long revision,
        Mode mode,
        String name,
        String description,
        Graph graph,
        Wizard wizard,
        String baseArtifactHash,
        Long publishedDraftRevision,
        String publishedArtifactHash,
        Instant createdAt,
        Instant updatedAt,
        String updatedBy) {

    public static final int CURRENT_SCHEMA_VERSION = 1;

    public PipelineDraft {
        Objects.requireNonNull(pipelineId, "pipelineId");
        if (pipelineId.isBlank()) {
            throw new IllegalArgumentException("pipelineId must not be blank");
        }
        if (schemaVersion != CURRENT_SCHEMA_VERSION) {
            throw new IllegalArgumentException("unsupported draft schema version: " + schemaVersion);
        }
        if (revision < 1) {
            throw new IllegalArgumentException("revision must be positive");
        }
        Objects.requireNonNull(mode, "mode");
        if (mode == Mode.DAG && (graph == null || wizard != null)) {
            throw new IllegalArgumentException("dag drafts require graph and no wizard payload");
        }
        if (mode == Mode.WIZARD && (wizard == null || graph != null)) {
            throw new IllegalArgumentException("wizard drafts require wizard and no graph payload");
        }
        Objects.requireNonNull(createdAt, "createdAt");
        Objects.requireNonNull(updatedAt, "updatedAt");
        Objects.requireNonNull(updatedBy, "updatedBy");
        if (updatedBy.isBlank()) {
            throw new IllegalArgumentException("updatedBy must not be blank");
        }
        if (publishedDraftRevision != null && publishedDraftRevision > revision) {
            throw new IllegalArgumentException("published revision cannot exceed draft revision");
        }
    }

    public enum Mode {
        DAG,
        WIZARD
    }

    public record Graph(List<Node> nodes, List<Edge> edges, Viewport viewport) {
        public Graph {
            nodes = List.copyOf(nodes);
            edges = List.copyOf(edges);
            Objects.requireNonNull(viewport, "viewport");
        }
    }

    public record Node(String id, String type, String sourceId, String table,
            Map<String, Object> config, Map<String, Object> metadata) {
        public Node {
            requireText(id, "node id");
            requireText(type, "node type");
            config = copy(config);
            metadata = copy(metadata);
        }
    }

    public record Edge(String id, String source, String target) {
        public Edge {
            requireText(id, "edge id");
            requireText(source, "edge source");
            requireText(target, "edge target");
        }
    }

    public record Viewport(double x, double y, double zoom) {
        public Viewport {
            if (!Double.isFinite(x) || !Double.isFinite(y) || !Double.isFinite(zoom) || zoom <= 0) {
                throw new IllegalArgumentException("viewport must be finite with a positive zoom");
            }
        }
    }

    public record Wizard(Root root, List<Related> related, List<Transform> transforms, Output output) {
        public Wizard {
            related = List.copyOf(related);
            transforms = List.copyOf(transforms);
        }
    }

    public record Root(String id, String sourceId, String table, List<String> key,
            List<Transform> preTransforms) {
        public Root {
            requireText(id, "root id");
            key = key == null ? List.of() : List.copyOf(key);
            preTransforms = preTransforms == null ? List.of() : List.copyOf(preTransforms);
        }
    }

    public record Related(String id, String parentId, String sourceId, String table,
            Relation relation, List<Transform> preTransforms) {
        public Related {
            requireText(id, "related id");
            requireText(parentId, "related parent id");
            Objects.requireNonNull(relation, "relation");
            preTransforms = preTransforms == null ? List.of() : List.copyOf(preTransforms);
        }
    }

    public record Relation(List<FieldPair> on, Shape shape, String path, List<String> key,
            List<String> arrayKey) {
        public Relation(List<FieldPair> on, Shape shape, String path, List<String> arrayKey) {
            this(on, shape, path, List.of(), arrayKey);
        }

        public Relation {
            on = List.copyOf(on);
            Objects.requireNonNull(shape, "shape");
            if (shape != Shape.ARRAY && arrayKey != null && !arrayKey.isEmpty()) {
                throw new IllegalArgumentException("arrayKey is only valid for array relations");
            }
            if (shape == Shape.FLAT && path != null && !path.isBlank()) {
                throw new IllegalArgumentException("flat relations do not have a path");
            }
            key = key == null ? List.of() : List.copyOf(key);
            arrayKey = arrayKey == null ? List.of() : List.copyOf(arrayKey);
        }
    }

    public record FieldPair(String childField, String parentField) {
        public FieldPair {
            Objects.requireNonNull(childField, "child field");
            Objects.requireNonNull(parentField, "parent field");
        }
    }

    public enum Shape {
        FLAT,
        OBJECT,
        ARRAY
    }

    public record Transform(String id, String type, Map<String, Object> fields) {
        public Transform {
            requireText(id, "transform id");
            requireText(type, "transform type");
            fields = copy(fields);
        }
    }

    public record Output(String kind, Map<String, Object> config) {
        public Output {
            requireText(kind, "output kind");
            config = copy(config);
        }
    }

    /**
     * A pipeline's definition rewritten from outside its draft -- an answer a person gave when starting it --
     * and the same change for the draft, written together or not at all.
     *
     * <p>The draft is changed only when it is based on the definition being replaced: a draft based on that
     * definition would otherwise publish the old setting straight back over the new one, while a draft based
     * on another one already conflicts with the stored definition and is left for its author to resolve.
     *
     * @param expectedArtifactHash the definition being replaced; the write is refused if it is no longer stored
     * @param artifact             the definition written in its place
     * @param artifactHash         its content hash
     * @param workspacePreconditions the content hash each resource it depends on must still have
     * @param draftChange          the same change for a draft based on {@code expectedArtifactHash}; it returns
     *                             the draft to store -- its base moved to {@code artifactHash} -- or the draft it
     *                             was given, unchanged, when the change cannot be expressed in it
     */
    public record DefinitionChange(String pipelineId, String expectedArtifactHash, Resource artifact,
            String artifactHash, Map<String, String> workspacePreconditions,
            java.util.function.UnaryOperator<PipelineDraft> draftChange) {

        public DefinitionChange {
            requireText(pipelineId, "pipeline id");
            requireText(expectedArtifactHash, "expected artifact hash");
            Objects.requireNonNull(artifact, "artifact");
            requireText(artifactHash, "artifact hash");
            workspacePreconditions = Map.copyOf(workspacePreconditions);
            Objects.requireNonNull(draftChange, "draftChange");
        }
    }

    /** Candidate Artifact and conditions used by an atomic publish operation. */
    public record Publication(String pipelineId, long expectedDraftRevision,
            String expectedArtifactHash, Resource artifact, String publishedArtifactHash,
            Instant publishedAt, String updatedBy, Map<String, String> workspacePreconditions) {
        public Publication(String pipelineId, long expectedDraftRevision, String expectedArtifactHash,
                Resource artifact, String publishedArtifactHash, Instant publishedAt, String updatedBy) {
            this(pipelineId, expectedDraftRevision, expectedArtifactHash, artifact, publishedArtifactHash,
                    publishedAt, updatedBy, Map.of());
        }

        public Publication {
            requireText(pipelineId, "pipeline id");
            if (expectedDraftRevision < 1) {
                throw new IllegalArgumentException("expected draft revision must be positive");
            }
            Objects.requireNonNull(artifact, "artifact");
            if (!pipelineId.equals(artifact.id())) {
                throw new IllegalArgumentException("publication artifact id must equal pipeline id");
            }
            requireText(publishedArtifactHash, "published artifact hash");
            Objects.requireNonNull(publishedAt, "publishedAt");
            requireText(updatedBy, "updatedBy");
            workspacePreconditions = workspacePreconditions == null ? Map.of() : Map.copyOf(workspacePreconditions);
        }
    }

    private static Map<String, Object> copy(Map<String, Object> values) {
        return values == null ? Map.of()
                : java.util.Collections.unmodifiableMap(new LinkedHashMap<>(values));
    }

    private static void requireText(String value, String label) {
        Objects.requireNonNull(value, label);
        if (value.isBlank()) {
            throw new IllegalArgumentException(label + " must not be blank");
        }
    }
}
