package io.tapstate.adapters.mongostore;

import com.mongodb.ErrorCategory;
import com.mongodb.MongoException;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.ReplaceOptions;
import io.tapstate.core.model.Resource;
import io.tapstate.spi.store.PipelineDraft;
import io.tapstate.spi.store.PipelineDraftMutation;
import io.tapstate.spi.store.PipelineDraftStore;
import org.bson.Document;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Mongo persistence for Pipeline drafts and the cross-collection publish transaction. */
public final class MongoPipelineDraftStore implements PipelineDraftStore {

    private static final String REVISION = "revision";
    private final MongoClient client;
    private final MongoCollection<Document> drafts;
    private final MongoCollection<Document> artifacts;

    public MongoPipelineDraftStore(MongoClient client, MongoCollection<Document> drafts,
            MongoCollection<Document> artifacts) {
        this.client = Objects.requireNonNull(client, "client");
        this.drafts = Objects.requireNonNull(drafts, "drafts");
        this.artifacts = Objects.requireNonNull(artifacts, "artifacts");
        drafts.createIndex(Indexes.ascending(REVISION), new IndexOptions().name("pipeline_drafts_revision"));
        drafts.createIndex(Indexes.ascending("updatedAt"), new IndexOptions().name("pipeline_drafts_updated_at"));
    }

    @Override
    public Optional<PipelineDraft> get(String pipelineId) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Document document = StoreIo.call(() -> drafts.find(new Document("_id", pipelineId)).first());
        return document == null ? Optional.empty() : Optional.of(fromDocument(PipelineDraftMigrations.migrate(document)));
    }

    @Override
    public List<PipelineDraft> list() {
        return StoreIo.call(() -> {
            List<PipelineDraft> result = new ArrayList<>();
            try (MongoCursor<Document> cursor = drafts.find().sort(Indexes.ascending("_id")).iterator()) {
                while (cursor.hasNext()) {
                    result.add(fromDocument(PipelineDraftMigrations.migrate(cursor.next())));
                }
            }
            return List.copyOf(result);
        });
    }

    @Override
    public PipelineDraftMutation create(PipelineDraft draft) {
        Objects.requireNonNull(draft, "draft");
        return StoreIo.call(() -> {
            try {
                drafts.insertOne(toDocument(draft));
                return PipelineDraftMutation.CREATED;
            } catch (MongoException error) {
                if (ErrorCategory.fromErrorCode(error.getCode()) == ErrorCategory.DUPLICATE_KEY) {
                    return PipelineDraftMutation.ALREADY_EXISTS;
                }
                throw error;
            }
        });
    }

    @Override
    public PipelineDraftMutation replace(String pipelineId, long expectedRevision, PipelineDraft replacement) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(replacement, "replacement");
        if (!pipelineId.equals(replacement.pipelineId())) {
            throw new IllegalArgumentException("replacement id must equal pipeline id");
        }
        if (replacement.revision() != expectedRevision + 1) {
            throw new IllegalArgumentException("replacement revision must increment by one");
        }
        return StoreIo.call(() -> {
            Document current = drafts.find(new Document("_id", pipelineId)).first();
            if (current != null && !replacement.mode().name().toLowerCase().equals(current.getString("mode"))) {
                return PipelineDraftMutation.MODE_CONFLICT;
            }
            if (drafts.replaceOne(new Document("_id", pipelineId).append(REVISION, expectedRevision),
                    toDocument(replacement)).getMatchedCount() == 1) {
                return PipelineDraftMutation.REPLACED;
            }
            return drafts.find(new Document("_id", pipelineId)).first() == null
                    ? PipelineDraftMutation.NOT_FOUND : PipelineDraftMutation.REVISION_CONFLICT;
        });
    }

    @Override
    public PipelineDraftMutation publish(PipelineDraft.Publication publication) {
        Objects.requireNonNull(publication, "publication");
        return StoreIo.call(() -> publishInTransaction(publication));
    }

    private PipelineDraftMutation publishInTransaction(PipelineDraft.Publication publication) {
        try (ClientSession session = client.startSession()) {
            session.startTransaction();
            try {
                Document currentDraft = drafts.find(session, new Document("_id", publication.pipelineId())
                        .append(REVISION, publication.expectedDraftRevision())).first();
                if (currentDraft == null) {
                    session.abortTransaction();
                    return drafts.find(new Document("_id", publication.pipelineId())).first() == null
                            ? PipelineDraftMutation.NOT_FOUND : PipelineDraftMutation.REVISION_CONFLICT;
                }

                Document currentArtifact = artifacts.find(session,
                        new Document("_id", publication.pipelineId())).projection(new Document("contentHash", 1)).first();
                String currentHash = currentArtifact == null ? null : currentArtifact.getString("contentHash");
                if (!Objects.equals(currentHash, publication.expectedArtifactHash())) {
                    session.abortTransaction();
                    return PipelineDraftMutation.ARTIFACT_CONFLICT;
                }

                artifacts.replaceOne(session, new Document("_id", publication.pipelineId()),
                        MongoArtifactStore.toDocument(publication.artifact()), new ReplaceOptions().upsert(true));
                Document updatedDraft = toDocument(fromDocument(PipelineDraftMigrations.migrate(currentDraft)))
                        .append("baseArtifactHash", publication.publishedArtifactHash())
                        .append("publishedDraftRevision", publication.expectedDraftRevision())
                        .append("publishedArtifactHash", publication.publishedArtifactHash())
                        .append("updatedAt", Date.from(publication.publishedAt()))
                        .append("updatedBy", publication.updatedBy());
                drafts.replaceOne(session, new Document("_id", publication.pipelineId())
                        .append(REVISION, publication.expectedDraftRevision()), updatedDraft);
                session.commitTransaction();
                return PipelineDraftMutation.PUBLISHED;
            } catch (RuntimeException error) {
                try {
                    session.abortTransaction();
                } catch (RuntimeException abortFailure) {
                    error.addSuppressed(abortFailure);
                }
                throw error;
            }
        }
    }

    static Document toDocument(PipelineDraft draft) {
        Document document = new Document("_id", draft.pipelineId())
                .append("schemaVersion", draft.schemaVersion())
                .append(REVISION, draft.revision())
                .append("mode", draft.mode().name().toLowerCase())
                .append("name", draft.name())
                .append("description", draft.description())
                .append("baseArtifactHash", draft.baseArtifactHash())
                .append("publishedDraftRevision", draft.publishedDraftRevision())
                .append("publishedArtifactHash", draft.publishedArtifactHash())
                .append("createdAt", Date.from(draft.createdAt()))
                .append("updatedAt", Date.from(draft.updatedAt()))
                .append("updatedBy", draft.updatedBy());
        if (draft.mode() == PipelineDraft.Mode.DAG) {
            document.append("graph", graphDocument(draft.graph()));
        } else {
            document.append("wizard", wizardDocument(draft.wizard()));
        }
        return document;
    }

    static PipelineDraft fromDocument(Document document) {
        PipelineDraft.Mode mode = "wizard".equals(document.getString("mode"))
                ? PipelineDraft.Mode.WIZARD : PipelineDraft.Mode.DAG;
        Date createdAt = document.getDate("createdAt");
        Date updatedAt = document.getDate("updatedAt");
        return new PipelineDraft(
                document.getString("_id"),
                document.getInteger("schemaVersion", PipelineDraft.CURRENT_SCHEMA_VERSION),
                document.getLong(REVISION), mode, document.getString("name"), document.getString("description"),
                mode == PipelineDraft.Mode.DAG ? graph(document.get("graph", Document.class)) : null,
                mode == PipelineDraft.Mode.WIZARD ? wizard(document.get("wizard", Document.class)) : null,
                document.getString("baseArtifactHash"), document.getLong("publishedDraftRevision"),
                document.getString("publishedArtifactHash"),
                (createdAt == null ? Instant.EPOCH : createdAt.toInstant()),
                (updatedAt == null ? Instant.EPOCH : updatedAt.toInstant()), document.getString("updatedBy"));
    }

    private static Document graphDocument(PipelineDraft.Graph graph) {
        return new Document("nodes", graph.nodes().stream().map(MongoPipelineDraftStore::nodeDocument).toList())
                .append("edges", graph.edges().stream().map(MongoPipelineDraftStore::edgeDocument).toList())
                .append("viewport", new Document("x", graph.viewport().x()).append("y", graph.viewport().y())
                        .append("zoom", graph.viewport().zoom()));
    }

    private static Document nodeDocument(PipelineDraft.Node node) {
        return new Document("id", node.id()).append("type", node.type()).append("sourceId", node.sourceId())
                .append("table", node.table()).append("config", new Document(node.config()))
                .append("metadata", new Document(node.metadata()));
    }

    private static Document edgeDocument(PipelineDraft.Edge edge) {
        return new Document("id", edge.id()).append("source", edge.source()).append("target", edge.target());
    }

    private static Document wizardDocument(PipelineDraft.Wizard wizard) {
        return new Document("root", rootDocument(wizard.root()))
                .append("related", wizard.related().stream().map(MongoPipelineDraftStore::relatedDocument).toList())
                .append("transforms", wizard.transforms().stream().map(MongoPipelineDraftStore::transformDocument).toList())
                .append("output", wizard.output() == null ? null
                        : new Document("kind", wizard.output().kind()).append("config", new Document(wizard.output().config())));
    }

    private static Document rootDocument(PipelineDraft.Root root) {
        return new Document("id", root.id()).append("sourceId", root.sourceId()).append("table", root.table())
                .append("key", root.key()).append("preTransforms", root.preTransforms().stream()
                        .map(MongoPipelineDraftStore::transformDocument).toList());
    }

    private static Document relatedDocument(PipelineDraft.Related related) {
        PipelineDraft.Relation relation = related.relation();
        List<Document> pairs = relation.on().stream().map(pair -> new Document("childField", pair.childField())
                .append("parentField", pair.parentField())).toList();
        return new Document("id", related.id()).append("parentId", related.parentId())
                .append("sourceId", related.sourceId()).append("table", related.table())
                .append("relation", new Document("on", pairs).append("shape", relation.shape().name().toLowerCase())
                        .append("path", relation.path()).append("arrayKey", relation.arrayKey()))
                .append("preTransforms", related.preTransforms().stream().map(MongoPipelineDraftStore::transformDocument).toList());
    }

    private static Document transformDocument(PipelineDraft.Transform transform) {
        return new Document("id", transform.id()).append("type", transform.type())
                .append("fields", new Document(transform.fields()));
    }

    private static PipelineDraft.Graph graph(Document document) {
        List<PipelineDraft.Node> nodes = documents(document, "nodes").stream().map(d -> new PipelineDraft.Node(
                d.getString("id"), d.getString("type"), d.getString("sourceId"), d.getString("table"),
                map(d.get("config")), map(d.get("metadata")))).toList();
        List<PipelineDraft.Edge> edges = documents(document, "edges").stream().map(d -> new PipelineDraft.Edge(
                d.getString("id"), d.getString("source"), d.getString("target"))).toList();
        Document viewport = document.get("viewport", Document.class);
        return new PipelineDraft.Graph(nodes, edges, new PipelineDraft.Viewport(viewport.getDouble("x"),
                viewport.getDouble("y"), viewport.getDouble("zoom")));
    }

    private static PipelineDraft.Wizard wizard(Document document) {
        Document root = document.get("root", Document.class);
        return new PipelineDraft.Wizard(new PipelineDraft.Root(root.getString("id"), root.getString("sourceId"),
                root.getString("table"), strings(root.get("key")), transforms(root.get("preTransforms"))),
                documents(document, "related").stream().map(MongoPipelineDraftStore::related).toList(),
                transforms(document.get("transforms")), output(document.get("output", Document.class)));
    }

    private static PipelineDraft.Related related(Document document) {
        Document relation = document.get("relation", Document.class);
        List<PipelineDraft.FieldPair> on = documents(relation, "on").stream().map(d ->
                new PipelineDraft.FieldPair(d.getString("childField"), d.getString("parentField"))).toList();
        return new PipelineDraft.Related(document.getString("id"), document.getString("parentId"),
                document.getString("sourceId"), document.getString("table"), new PipelineDraft.Relation(on,
                        PipelineDraft.Shape.valueOf(relation.getString("shape").toUpperCase()),
                        relation.getString("path"), strings(relation.get("arrayKey"))),
                transforms(document.get("preTransforms")));
    }

    private static List<PipelineDraft.Transform> transforms(Object value) {
        if (!(value instanceof List<?> values)) {
            return List.of();
        }
        return values.stream().map(item -> {
            Document document = asDocument(item);
            return new PipelineDraft.Transform(document.getString("id"), document.getString("type"), map(document.get("fields")));
        }).toList();
    }

    private static PipelineDraft.Output output(Document document) {
        return document == null ? null : new PipelineDraft.Output(document.getString("kind"), map(document.get("config")));
    }

    private static List<String> strings(Object value) {
        if (!(value instanceof List<?> values)) {
            return List.of();
        }
        return values.stream().map(String.class::cast).toList();
    }

    private static List<Document> documents(Document parent, String field) {
        Object value = parent == null ? null : parent.get(field);
        if (!(value instanceof List<?> values)) {
            return List.of();
        }
        return values.stream().map(MongoPipelineDraftStore::asDocument).toList();
    }

    private static Document asDocument(Object value) {
        return value instanceof Document document ? document : new Document((Map<String, Object>) value);
    }

    private static Map<String, Object> map(Object value) {
        if (!(value instanceof Map<?, ?> values)) {
            return Map.of();
        }
        Map<String, Object> result = new LinkedHashMap<>();
        values.forEach((key, item) -> result.put(String.valueOf(key), item));
        return Map.copyOf(result);
    }
}
