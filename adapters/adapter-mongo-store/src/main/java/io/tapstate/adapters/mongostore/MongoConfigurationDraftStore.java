package io.tapstate.adapters.mongostore;

import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ReplaceOptions;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.canonical.CanonicalWriter;
import io.tapstate.spi.store.ArtifactWrite;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineDraft;
import io.tapstate.spi.store.PipelineDraftMutation;
import io.tapstate.spi.store.PipelineDraftStore;
import io.tapstate.spi.store.PipelineDraftSummary;
import org.bson.Document;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

/** Private draft saves keep their own CAS; publication joins the existing configuration artifact transaction. */
final class MongoConfigurationDraftStore implements PipelineDraftStore {
    private static final TransactionOptions TRANSACTION = TransactionOptions.builder()
            .readConcern(ReadConcern.SNAPSHOT).writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();
    private static final Logger LOG = Logger.getLogger(MongoConfigurationDraftStore.class.getName());
    private final MongoClient client;
    private final MongoCollection<Document> drafts;
    private final MongoCollection<Document> versions;
    private final MongoConfigurationArtifactStore artifacts;
    private final EncryptedDraftCodec codec;

    MongoConfigurationDraftStore(MongoClient client, MongoDatabase database, SourceConfigCipherProvider ciphers,
            MongoConfigurationArtifactStore artifacts) {
        this.client = Objects.requireNonNull(client, "client");
        this.drafts = SystemCollections.CONFIG_PIPELINE_DRAFTS.on(database).withReadConcern(ReadConcern.MAJORITY)
                .withWriteConcern(WriteConcern.MAJORITY.withJournal(true));
        this.versions = SystemCollections.CONFIGURATION_VERSION.on(database);
        this.artifacts = Objects.requireNonNull(artifacts, "artifacts");
        this.codec = new EncryptedDraftCodec(ciphers);
    }

    static void prepareIndexes(MongoDatabase database) {
        StoreIo.run(() -> {
            for (var index : SystemCollections.CONFIG_PIPELINE_DRAFTS.indexes()) {
                IndexEnsure.createIfAbsent(SystemCollections.CONFIG_PIPELINE_DRAFTS.on(database), index);
            }
        });
    }

    void verify() {
        StoreIo.run(() -> {
            var existing = drafts.listIndexes().into(new ArrayList<>());
            // No index may expire necessary authoring data or its anti-resurrection revision.
            if (existing.stream().anyMatch(index -> index.containsKey("expireAfterSeconds"))) {
                throw new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", "pipeline-drafts", "field", "index"), null);
            }
            for (var declared : SystemCollections.CONFIG_PIPELINE_DRAFTS.indexes()) {
                var index = existing.stream().filter(candidate -> declared.indexName().equals(candidate.get("name"))).findFirst().orElse(null);
                if (index == null || !new Document(declared.keys().getFirst(), 1).equals(index.get("key"))
                        || Boolean.TRUE.equals(index.get("unique")) || index.containsKey("partialFilterExpression")
                        || Boolean.TRUE.equals(index.get("sparse")) || Boolean.TRUE.equals(index.get("hidden"))) {
                    throw new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", "pipeline-drafts", "field", "index"), null);
                }
            }
            try (var cursor = drafts.find().iterator()) {
                while (cursor.hasNext()) codec.verify(cursor.next());
            }
        });
    }

    @Override public Optional<PipelineDraft> get(String id) {
        Objects.requireNonNull(id, "id");
        return StoreIo.call(() -> {
            Document row = drafts.find(new Document("_id", id)).first();
            return row == null || deleted(row) ? Optional.empty() : Optional.of(codec.decode(row));
        });
    }

    @Override public List<PipelineDraft> list() { return list(0, Integer.MAX_VALUE); }
    @Override public List<PipelineDraft> list(int offset, int limit) {
        return browse(offset, limit, false, codec::decode);
    }
    @Override public List<PipelineDraftSummary> listSummaries() { return listSummaries(0, Integer.MAX_VALUE); }
    @Override public List<PipelineDraftSummary> listSummaries(int offset, int limit) {
        return browse(offset, limit, true, codec::summaryProjection);
    }

    private <T> List<T> browse(int offset, int limit, boolean summary, Function<Document, T> read) {
        if (offset < 0 || limit < 1) throw new IllegalArgumentException("draft page offset must be non-negative and limit must be positive");
        return StoreIo.call(() -> {
            var query = drafts.find(new Document("deleted", new Document("$ne", true)))
                    .sort(new Document("_id", 1)).skip(offset).limit(limit);
            if (summary) query = query.projection(new Document("_id", 1).append("formatVersion", 1).append("revision", 1)
                    .append("deleted", 1).append("updatedAt", 1).append("summary", 1));
            List<T> result = new ArrayList<>();
            try (var cursor = query.iterator()) {
                while (cursor.hasNext()) {
                    Document row = cursor.next();
                    try { result.add(read.apply(row)); }
                    catch (TapstateException invalid) {
                        if (invalid.code() != IoError.DOCUMENT_UNREADABLE) throw invalid;
                        LOG.log(Level.WARNING, "Skipping unreadable Pipeline draft {0}", row.get("_id") instanceof String id ? id : "<invalid>");
                    }
                }
            }
            return List.copyOf(result);
        });
    }

    @Override public PipelineDraftMutation create(PipelineDraft draft) {
        PipelineDraft captured = codec.capture(Objects.requireNonNull(draft, "draft"));
        return transaction(session -> {
            Document current = drafts.find(session, new Document("_id", captured.pipelineId())).first();
            PipelineDraft candidate = captured;
            if (current != null) {
                if (!deleted(current)) { codec.summary(current); return PipelineDraftMutation.ALREADY_EXISTS; }
                candidate = withRevision(captured, codec.tombstoneRevision(current) + 1);
            }
            Document encoded = codec.encode(candidate, session);
            drafts.replaceOne(session, new Document("_id", candidate.pipelineId()), encoded, new ReplaceOptions().upsert(true));
            return PipelineDraftMutation.CREATED;
        });
    }

    @Override public PipelineDraftMutation replace(String id, long expectedRevision, PipelineDraft replacement) {
        return replaceGuarded(id, expectedRevision, replacement.baseArtifactHash(), replacement);
    }
    @Override public PipelineDraftMutation rebase(String id, long expectedRevision, String expectedBaseArtifactHash, PipelineDraft replacement) {
        return replaceGuarded(id, expectedRevision, expectedBaseArtifactHash, replacement);
    }
    private PipelineDraftMutation replaceGuarded(String id, long expectedRevision, String expectedBase, PipelineDraft replacement) {
        Objects.requireNonNull(id, "id");
        PipelineDraft captured = codec.capture(Objects.requireNonNull(replacement, "replacement"));
        if (!id.equals(captured.pipelineId()) || captured.revision() != expectedRevision + 1) {
            throw new IllegalArgumentException("replacement must match the id and increment the draft revision by one");
        }
        return transaction(session -> {
            Document row = drafts.find(session, new Document("_id", id)).first();
            if (row == null || deleted(row)) return PipelineDraftMutation.NOT_FOUND;
            var current = codec.summary(row);
            if (current.mode() != captured.mode()) return PipelineDraftMutation.MODE_CONFLICT;
            if (current.revision() != expectedRevision || !Objects.equals(current.baseArtifactHash(), expectedBase)
                    || !Objects.equals(current.publishedDraftRevision(), captured.publishedDraftRevision())
                    || !Objects.equals(current.publishedArtifactHash(), captured.publishedArtifactHash())) return PipelineDraftMutation.REVISION_CONFLICT;
            drafts.replaceOne(session, new Document("_id", id).append("revision", expectedRevision).append("deleted", false),
                    codec.encode(captured, session));
            return PipelineDraftMutation.REPLACED;
        });
    }

    @Override public PipelineDraftMutation delete(String id, long expectedRevision) {
        Objects.requireNonNull(id, "id");
        if (expectedRevision < 1) throw new IllegalArgumentException("expected revision must be positive");
        Date deletedAt = new Date();
        return transaction(session -> {
            Document row = drafts.find(session, new Document("_id", id)).first();
            if (row == null || deleted(row)) return PipelineDraftMutation.NOT_FOUND;
            var current = codec.summary(row);
            if (current.revision() != expectedRevision) return PipelineDraftMutation.REVISION_CONFLICT;
            codec.cipherForWrite(session);
            drafts.replaceOne(session, new Document("_id", id).append("revision", expectedRevision).append("deleted", false),
                    new Document("_id", id).append("formatVersion", 1).append("revision", expectedRevision)
                            .append("deleted", true).append("updatedAt", deletedAt));
            return PipelineDraftMutation.DELETED;
        });
    }

    @Override public PipelineDraftMutation publish(PipelineDraft.Publication publication) {
        Objects.requireNonNull(publication, "publication");
        if (!(publication.artifact() instanceof PipelineResource)) throw new IllegalArgumentException("draft publication requires a Pipeline");
        var captured = (PipelineResource) new DslParser().parse(new CanonicalWriter().write(publication.artifact()));
        if (!publication.publishedArtifactHash().equals(CanonicalHash.of(captured))) {
            throw new IllegalArgumentException("publication hash must match the captured Pipeline");
        }
        try {
            return transaction(session -> {
                Document row = drafts.find(session, new Document("_id", publication.pipelineId())).first();
                if (row == null || deleted(row)) return PipelineDraftMutation.NOT_FOUND;
                PipelineDraft current = codec.decode(row);
                if (current.revision() != publication.expectedDraftRevision()) return PipelineDraftMutation.REVISION_CONFLICT;
                var observed = artifacts.getInTransaction(session, captured.id());
                if (observed.isPresent() && !(observed.orElseThrow() instanceof PipelineResource)) return PipelineDraftMutation.ARTIFACT_CONFLICT;
                String hash = observed.map(CanonicalHash::of).orElse(null);
                if (!Objects.equals(hash, publication.expectedArtifactHash())) return PipelineDraftMutation.ARTIFACT_CONFLICT;
                ArtifactWrite write = (hash == null ? ArtifactWrite.createOnly(captured) : ArtifactWrite.replaceOnly(captured, hash))
                        .guardedBy(publication.workspacePreconditions());
                ConfigurationVersion.advance(session, versions);
                artifacts.writeAllInTransaction(session, List.of(write));
                var published = new PipelineDraft(current.pipelineId(), current.schemaVersion(), current.revision(), current.mode(),
                        current.name(), current.description(), current.graph(), current.wizard(), publication.publishedArtifactHash(),
                        publication.expectedDraftRevision(), publication.publishedArtifactHash(), current.createdAt(), publication.publishedAt(), publication.updatedBy());
                drafts.replaceOne(session, new Document("_id", current.pipelineId()).append("revision", current.revision()).append("deleted", false),
                        codec.encode(published, session));
                return PipelineDraftMutation.PUBLISHED;
            });
        } catch (MongoConfigurationArtifactStore.Refused conflict) {
            return PipelineDraftMutation.ARTIFACT_CONFLICT;
        }
    }

    @Override public PipelineDraftMutation changeDefinition(PipelineDraft.DefinitionChange change) {
        Objects.requireNonNull(change, "change");
        if (!(change.artifact() instanceof PipelineResource) || !change.pipelineId().equals(change.artifact().id())) {
            throw new IllegalArgumentException("definition change requires the same Pipeline id");
        }
        var captured = (PipelineResource) new DslParser().parse(new CanonicalWriter().write(change.artifact()));
        if (!change.artifactHash().equals(CanonicalHash.of(captured))) {
            throw new IllegalArgumentException("definition change hash must match the captured Pipeline");
        }
        try {
            return transaction(session -> {
                var observed = artifacts.getInTransaction(session, change.pipelineId());
                if (observed.isEmpty() || !(observed.orElseThrow() instanceof PipelineResource)
                        || !change.expectedArtifactHash().equals(CanonicalHash.of(observed.orElseThrow()))) {
                    return PipelineDraftMutation.ARTIFACT_CONFLICT;
                }
                Document row = drafts.find(session, new Document("_id", change.pipelineId())).first();
                PipelineDraft current = row == null || deleted(row) ? null : codec.decode(row);
                PipelineDraft candidate = null;
                if (current != null && change.expectedArtifactHash().equals(current.baseArtifactHash())) {
                    candidate = codec.capture(Objects.requireNonNull(change.draftChange().apply(current), "changed draft"));
                    if (!candidate.equals(current) && (!current.pipelineId().equals(candidate.pipelineId())
                            || candidate.revision() != current.revision() + 1
                            || !change.artifactHash().equals(candidate.baseArtifactHash()))) {
                        throw new IllegalArgumentException("changed draft must retain its id and advance its revision and base");
                    }
                }
                ArtifactWrite write = ArtifactWrite.replaceOnly(captured, change.expectedArtifactHash())
                        .guardedBy(change.workspacePreconditions());
                ConfigurationVersion.advance(session, versions);
                artifacts.writeAllInTransaction(session, List.of(write));
                if (candidate != null && !candidate.equals(current)
                        && drafts.replaceOne(session, new Document("_id", current.pipelineId())
                                        .append("revision", current.revision()).append("deleted", false),
                                codec.encode(candidate, session)).getMatchedCount() != 1) {
                    throw new DefinitionConflict();
                }
                return PipelineDraftMutation.REPLACED;
            });
        } catch (MongoConfigurationArtifactStore.Refused | DefinitionConflict conflict) {
            return PipelineDraftMutation.ARTIFACT_CONFLICT;
        }
    }

    private static final class DefinitionConflict extends RuntimeException {
        private DefinitionConflict() { super(null, null, false, false); }
    }

    private boolean deleted(Document row) {
        if (!Boolean.TRUE.equals(row.get("deleted"))) return false;
        codec.tombstoneRevision(row); return true;
    }
    private <T> T transaction(Function<ClientSession, T> action) {
        return StoreIo.call(() -> {
            try (var session = client.startSession()) {
                return session.withTransaction(() -> action.apply(session), TRANSACTION);
            }
        });
    }
    private static PipelineDraft withRevision(PipelineDraft draft, long revision) {
        return new PipelineDraft(draft.pipelineId(), draft.schemaVersion(), revision, draft.mode(), draft.name(), draft.description(),
                draft.graph(), draft.wizard(), draft.baseArtifactHash(), draft.publishedDraftRevision(), draft.publishedArtifactHash(),
                draft.createdAt(), draft.updatedAt(), draft.updatedBy());
    }
}
