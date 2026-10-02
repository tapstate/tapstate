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
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineDraft;
import io.tapstate.spi.store.PipelineDraftMutation;
import io.tapstate.spi.store.PipelineDraftStore;
import io.tapstate.spi.store.PipelineDraftSummary;
import org.bson.Document;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

/** Mongo persistence for Pipeline drafts and the cross-collection publish transaction. */
public final class MongoPipelineDraftStore implements PipelineDraftStore {

    private static final String REVISION = "revision";
    private static final Logger LOG = Logger.getLogger(MongoPipelineDraftStore.class.getName());
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
        Document document = StoreIo.call(() -> drafts.find(activeDraft(pipelineId)).first());
        return document == null ? Optional.empty() : Optional.of(readDraft(document));
    }

    @Override
    public List<PipelineDraft> list() {
        return StoreIo.call(() -> {
            List<PipelineDraft> result = new ArrayList<>();
            try (MongoCursor<Document> cursor = drafts.find(new Document("deleted", new Document("$ne", true)))
                    .sort(Indexes.ascending("_id")).iterator()) {
                while (cursor.hasNext()) {
                    Document document = cursor.next();
                    try {
                        result.add(readDraft(document));
                    } catch (TapstateException unreadable) {
                        if (unreadable.code() != IoError.DOCUMENT_UNREADABLE) {
                            throw unreadable;
                        }
                        LOG.log(Level.WARNING, "Skipping unreadable Pipeline draft {0}", document.getString("_id"));
                    }
                }
            }
            return List.copyOf(result);
        });
    }

    @Override
    public List<PipelineDraft> list(int offset, int limit) {
        if (offset < 0 || limit < 1) {
            throw new IllegalArgumentException("draft page offset must be non-negative and limit must be positive");
        }
        return StoreIo.call(() -> {
            List<PipelineDraft> result = new ArrayList<>();
            try (MongoCursor<Document> cursor = drafts.find(new Document("deleted", new Document("$ne", true)))
                    .sort(Indexes.ascending("_id")).skip(offset).limit(limit).iterator()) {
                while (cursor.hasNext()) {
                    Document document = cursor.next();
                    try {
                        result.add(readDraft(document));
                    } catch (TapstateException unreadable) {
                        if (unreadable.code() != IoError.DOCUMENT_UNREADABLE) throw unreadable;
                        LOG.log(Level.WARNING, "Skipping unreadable Pipeline draft {0}", document.getString("_id"));
                    }
                }
            }
            return List.copyOf(result);
        });
    }

    @Override
    public List<PipelineDraftSummary> listSummaries() {
        return listSummaries(0, Integer.MAX_VALUE);
    }

    @Override
    public List<PipelineDraftSummary> listSummaries(int offset, int limit) {
        if (offset < 0 || limit < 1) {
            throw new IllegalArgumentException("draft page offset must be non-negative and limit must be positive");
        }
        Document projection = new Document("_id", 1).append("schemaVersion", 1).append("mode", 1)
                .append("name", 1).append("description", 1).append(REVISION, 1)
                .append("baseArtifactHash", 1).append("publishedDraftRevision", 1)
                .append("publishedArtifactHash", 1).append("createdAt", 1).append("updatedAt", 1)
                .append("updatedBy", 1);
        return StoreIo.call(() -> {
            List<PipelineDraftSummary> result = new ArrayList<>();
            try (MongoCursor<Document> cursor = drafts.find(new Document("deleted", new Document("$ne", true)))
                    .projection(projection).sort(Indexes.ascending("_id")).skip(offset).limit(limit).iterator()) {
                while (cursor.hasNext()) {
                    Document document = cursor.next();
                    try {
                        result.add(readSummary(document));
                    } catch (TapstateException unreadable) {
                        if (unreadable.code() != IoError.DOCUMENT_UNREADABLE) throw unreadable;
                        LOG.log(Level.WARNING, "Skipping unreadable Pipeline draft {0}", document.getString("_id"));
                    }
                }
            }
            return List.copyOf(result);
        });
    }

    @Override
    public PipelineDraftMutation create(PipelineDraft draft) {
        Objects.requireNonNull(draft, "draft");
        return StoreIo.call(() -> {
            while (true) {
                Document current = drafts.find(new Document("_id", draft.pipelineId())).first();
                if (current == null) {
                    try {
                        drafts.insertOne(toDocument(draft));
                        return PipelineDraftMutation.CREATED;
                    } catch (MongoException error) {
                        if (ErrorCategory.fromErrorCode(error.getCode()) != ErrorCategory.DUPLICATE_KEY) {
                            throw error;
                        }
                        continue;
                    }
                }
                if (!Boolean.TRUE.equals(current.getBoolean("deleted"))) {
                    return PipelineDraftMutation.ALREADY_EXISTS;
                }
                long previousRevision = ((Number) current.get(REVISION)).longValue();
                PipelineDraft revived = withRevision(draft, previousRevision + 1);
                Document filter = new Document("_id", draft.pipelineId())
                        .append(REVISION, previousRevision).append("deleted", true);
                if (drafts.replaceOne(filter, toDocument(revived)).getMatchedCount() == 1) {
                    return PipelineDraftMutation.CREATED;
                }
            }
        });
    }

    @Override
    public PipelineDraftMutation replace(String pipelineId, long expectedRevision, PipelineDraft replacement) {
        return replaceGuarded(pipelineId, expectedRevision, replacement.baseArtifactHash(), replacement);
    }

    @Override
    public PipelineDraftMutation rebase(String pipelineId, long expectedRevision,
            String expectedBaseArtifactHash, PipelineDraft replacement) {
        return replaceGuarded(pipelineId, expectedRevision, expectedBaseArtifactHash, replacement);
    }

    private PipelineDraftMutation replaceGuarded(String pipelineId, long expectedRevision,
            String expectedBaseArtifactHash, PipelineDraft replacement) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(replacement, "replacement");
        if (!pipelineId.equals(replacement.pipelineId())) {
            throw new IllegalArgumentException("replacement id must equal pipeline id");
        }
        if (replacement.revision() != expectedRevision + 1) {
            throw new IllegalArgumentException("replacement revision must increment by one");
        }
        return StoreIo.call(() -> {
            Document current = drafts.find(activeDraft(pipelineId)).first();
            Document migrated = current == null ? null : PipelineDraftMigrations.migrate(current);
            if (migrated != null && !replacement.mode().name().toLowerCase(java.util.Locale.ROOT)
                    .equals(migrated.getString("mode"))) {
                return PipelineDraftMutation.MODE_CONFLICT;
            }
            Document filter = activeDraft(pipelineId).append(REVISION, expectedRevision);
            if (migrated != null) {
                filter.append("baseArtifactHash", expectedBaseArtifactHash)
                        .append("publishedDraftRevision", replacement.publishedDraftRevision())
                        .append("publishedArtifactHash", replacement.publishedArtifactHash());
            }
            if (drafts.replaceOne(filter,
                    toDocument(replacement)).getMatchedCount() == 1) {
                return PipelineDraftMutation.REPLACED;
            }
            return drafts.find(activeDraft(pipelineId)).first() == null
                    ? PipelineDraftMutation.NOT_FOUND : PipelineDraftMutation.REVISION_CONFLICT;
        });
    }

    @Override
    public PipelineDraftMutation delete(String pipelineId, long expectedRevision) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        if (expectedRevision < 1) {
            throw new IllegalArgumentException("expected revision must be positive");
        }
        return StoreIo.call(() -> {
            Document filter = activeDraft(pipelineId).append(REVISION, expectedRevision);
            Document update = new Document("$set", new Document("deleted", true).append("deletedAt", new Date()))
                    .append("$unset", new Document("schemaVersion", "").append("mode", "")
                            .append("name", "").append("description", "").append("graph", "")
                            .append("wizard", "").append("baseArtifactHash", "")
                            .append("publishedDraftRevision", "").append("publishedArtifactHash", "")
                            .append("createdAt", "").append("updatedAt", "").append("updatedBy", ""));
            if (drafts.updateOne(filter, update).getMatchedCount() == 1) {
                return PipelineDraftMutation.DELETED;
            }
            return drafts.find(activeDraft(pipelineId)).first() == null
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
                Document currentDraft = drafts.find(session, activeDraft(publication.pipelineId())
                        .append(REVISION, publication.expectedDraftRevision())).first();
                if (currentDraft == null) {
                    session.abortTransaction();
                    return drafts.find(activeDraft(publication.pipelineId())).first() == null
                            ? PipelineDraftMutation.NOT_FOUND : PipelineDraftMutation.REVISION_CONFLICT;
                }

                Document currentArtifact = artifacts.find(session,
                        new Document("_id", publication.pipelineId()))
                        .projection(new Document("contentHash", 1).append("kind", 1)).first();
                if (currentArtifact != null && !"pipeline".equals(currentArtifact.getString("kind"))) {
                    session.abortTransaction();
                    return PipelineDraftMutation.ARTIFACT_CONFLICT;
                }
                String currentHash = currentArtifact == null ? null : currentArtifact.getString("contentHash");
                if (!Objects.equals(currentHash, publication.expectedArtifactHash())) {
                    session.abortTransaction();
                    return PipelineDraftMutation.ARTIFACT_CONFLICT;
                }

                if (hasStaleWorkspacePrecondition(session, publication.workspacePreconditions())) {
                    session.abortTransaction();
                    return PipelineDraftMutation.ARTIFACT_CONFLICT;
                }

                Document pipelineArtifact = new Document("_id", publication.pipelineId())
                        .append("kind", "pipeline");
                artifacts.replaceOne(session, pipelineArtifact,
                        MongoArtifactStore.toDocument(publication.artifact()), new ReplaceOptions().upsert(true));
                Document updatedDraft = toDocument(fromDocument(PipelineDraftMigrations.migrate(currentDraft)))
                        .append("baseArtifactHash", publication.publishedArtifactHash())
                        .append("publishedDraftRevision", publication.expectedDraftRevision())
                        .append("publishedArtifactHash", publication.publishedArtifactHash())
                        .append("updatedAt", Date.from(publication.publishedAt()))
                        .append("updatedBy", publication.updatedBy());
                if (drafts.replaceOne(session, activeDraft(publication.pipelineId())
                        .append(REVISION, publication.expectedDraftRevision()), updatedDraft).getMatchedCount() != 1) {
                    session.abortTransaction();
                    return drafts.find(activeDraft(publication.pipelineId())).first() == null
                            ? PipelineDraftMutation.NOT_FOUND : PipelineDraftMutation.REVISION_CONFLICT;
                }
                while (true) {
                    try {
                        session.commitTransaction();
                        break;
                    } catch (MongoException commitError) {
                        if (!commitError.hasErrorLabel(MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL)) {
                            throw commitError;
                        }
                    }
                }
                return PipelineDraftMutation.PUBLISHED;
            } catch (MongoException error) {
                try {
                    session.abortTransaction();
                } catch (RuntimeException abortFailure) {
                    error.addSuppressed(abortFailure);
                }
                if (ErrorCategory.fromErrorCode(error.getCode()) == ErrorCategory.DUPLICATE_KEY) {
                    return PipelineDraftMutation.ARTIFACT_CONFLICT;
                }
                if (error.hasErrorLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL)) {
                    Document latestDraft = drafts.find(activeDraft(publication.pipelineId())).first();
                    if (latestDraft == null) return PipelineDraftMutation.NOT_FOUND;
                    if (number(latestDraft.get(REVISION)) != publication.expectedDraftRevision()) {
                        return PipelineDraftMutation.REVISION_CONFLICT;
                    }
                }
                throw error;
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

    @Override
    public PipelineDraftMutation changeDefinition(PipelineDraft.DefinitionChange change) {
        Objects.requireNonNull(change, "change");
        return StoreIo.call(() -> changeDefinitionInTransaction(change));
    }

    /**
     * The definition and the draft that would publish it back, in one transaction: the definition is
     * replaced only while it is still the one the change was worked out against, and the draft only while
     * it is based on that same definition and still at the revision read here.
     */
    private PipelineDraftMutation changeDefinitionInTransaction(PipelineDraft.DefinitionChange change) {
        try (ClientSession session = client.startSession()) {
            session.startTransaction();
            try {
                Document currentArtifact = artifacts.find(session, new Document("_id", change.pipelineId()))
                        .projection(new Document("contentHash", 1).append("kind", 1)).first();
                if (currentArtifact == null || !"pipeline".equals(currentArtifact.getString("kind"))
                        || !change.expectedArtifactHash().equals(currentArtifact.getString("contentHash"))
                        || hasStaleWorkspacePrecondition(session, change.workspacePreconditions())) {
                    session.abortTransaction();
                    return PipelineDraftMutation.ARTIFACT_CONFLICT;
                }
                if (artifacts.replaceOne(session, new Document("_id", change.pipelineId())
                                .append("contentHash", change.expectedArtifactHash()),
                        MongoArtifactStore.toDocument(change.artifact())).getMatchedCount() != 1) {
                    session.abortTransaction();
                    return PipelineDraftMutation.ARTIFACT_CONFLICT;
                }
                Document currentDraft = drafts.find(session, activeDraft(change.pipelineId())).first();
                if (currentDraft != null
                        && change.expectedArtifactHash().equals(currentDraft.getString("baseArtifactHash"))) {
                    PipelineDraft draft = fromDocument(PipelineDraftMigrations.migrate(currentDraft));
                    PipelineDraft changed = change.draftChange().apply(draft);
                    if (!changed.equals(draft) && drafts.replaceOne(session, activeDraft(change.pipelineId())
                            .append(REVISION, draft.revision()), toDocument(changed)).getMatchedCount() != 1) {
                        session.abortTransaction();
                        return PipelineDraftMutation.ARTIFACT_CONFLICT;
                    }
                }
                while (true) {
                    try {
                        session.commitTransaction();
                        break;
                    } catch (MongoException commitError) {
                        if (!commitError.hasErrorLabel(MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL)) {
                            throw commitError;
                        }
                    }
                }
                return PipelineDraftMutation.REPLACED;
            } catch (RuntimeException error) {
                try {
                    session.abortTransaction();
                } catch (RuntimeException abortFailure) {
                    error.addSuppressed(abortFailure);
                }
                if (error instanceof MongoException mongo
                        && mongo.hasErrorLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL)) {
                    return PipelineDraftMutation.ARTIFACT_CONFLICT;
                }
                throw error;
            }
        }
    }

    private boolean hasStaleWorkspacePrecondition(ClientSession session, Map<String, String> preconditions) {
        for (Map.Entry<String, String> expected : preconditions.entrySet()) {
            Document dependency = artifacts.find(session, new Document("_id", expected.getKey()))
                    .projection(new Document("contentHash", 1)).first();
            if (dependency == null || !Objects.equals(dependency.getString("contentHash"), expected.getValue())) {
                return true;
            }
        }
        return false;
    }

    static Document toDocument(PipelineDraft draft) {
        Document document = new Document("_id", draft.pipelineId())
                .append("schemaVersion", draft.schemaVersion())
                .append(REVISION, draft.revision())
                .append("mode", draft.mode().name().toLowerCase(java.util.Locale.ROOT))
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

    private static Document activeDraft(String pipelineId) {
        return new Document("_id", pipelineId).append("deleted", new Document("$ne", true));
    }

    private static PipelineDraft withRevision(PipelineDraft draft, long revision) {
        return new PipelineDraft(draft.pipelineId(), draft.schemaVersion(), revision, draft.mode(), draft.name(),
                draft.description(), draft.graph(), draft.wizard(), draft.baseArtifactHash(),
                draft.publishedDraftRevision(), draft.publishedArtifactHash(), draft.createdAt(),
                draft.updatedAt(), draft.updatedBy());
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
        return new Document("root", wizard.root() == null ? null : rootDocument(wizard.root()))
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
                        .append("path", relation.path()).append("key", relation.key())
                        .append("arrayKey", relation.arrayKey()))
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
        if (document == null) {
            return new PipelineDraft.Wizard(null, List.of(), List.of(), null);
        }
        Document root = document.get("root", Document.class);
        return new PipelineDraft.Wizard(root == null ? null
                : new PipelineDraft.Root(root.getString("id"), root.getString("sourceId"),
                        root.getString("table"), strings(root.get("key")), transforms(root.get("preTransforms"))),
                documents(document, "related").stream().map(MongoPipelineDraftStore::related).toList(),
                transforms(document.get("transforms")), output(document.get("output", Document.class)));
    }

    private static PipelineDraft readDraft(Document document) {
        String id = String.valueOf(document.get("_id"));
        try {
            return fromDocument(PipelineDraftMigrations.migrate(document));
        } catch (IllegalArgumentException | ClassCastException | NullPointerException error) {
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                    Map.of("id", id, "field", "schema"), error);
        }
    }

    private static PipelineDraftSummary readSummary(Document raw) {
        String id = String.valueOf(raw.get("_id"));
        try {
            Document document = PipelineDraftMigrations.migrate(raw);
            Date createdAt = document.getDate("createdAt");
            Date updatedAt = document.getDate("updatedAt");
            return new PipelineDraftSummary(id,
                    PipelineDraft.Mode.valueOf(document.getString("mode").toUpperCase(java.util.Locale.ROOT)),
                    document.getString("name"), document.getString("description"),
                    ((Number) document.get(REVISION)).longValue(), document.getString("baseArtifactHash"),
                    document.getLong("publishedDraftRevision"), document.getString("publishedArtifactHash"),
                    createdAt == null ? Instant.EPOCH : createdAt.toInstant(),
                    updatedAt == null ? Instant.EPOCH : updatedAt.toInstant(), document.getString("updatedBy"));
        } catch (IllegalArgumentException | ClassCastException | NullPointerException error) {
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                    Map.of("id", id, "field", "schema"), error);
        }
    }

    private static long number(Object value) {
        return value instanceof Number number ? number.longValue() : -1;
    }

    private static PipelineDraft.Related related(Document document) {
        Document relation = document.get("relation", Document.class);
        List<PipelineDraft.FieldPair> on = documents(relation, "on").stream().map(d ->
                new PipelineDraft.FieldPair(d.getString("childField"), d.getString("parentField"))).toList();
        return new PipelineDraft.Related(document.getString("id"), document.getString("parentId"),
                document.getString("sourceId"), document.getString("table"), new PipelineDraft.Relation(on,
                        PipelineDraft.Shape.valueOf(relation.getString("shape").toUpperCase()),
                        relation.getString("path"), strings(relation.get("key")),
                        strings(relation.get("arrayKey"))),
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
        return Collections.unmodifiableMap(result);
    }
}
