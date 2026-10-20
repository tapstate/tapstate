package io.tapstate.adapters.mongostore;

import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.ReferenceGraph;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.CacheAuthScope;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.StorageRecoveryCommands;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.Function;
import org.bson.Document;

/** A current recovery summary on the existing Cluster recovery authority, using CONFIG write competition. */
final class MongoStorageRecoveryCommands implements StorageRecoveryCommands {
    private static final TransactionOptions TX = TransactionOptions.builder().readConcern(ReadConcern.SNAPSHOT)
            .writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();
    private final MongoClient client;
    private final MongoDatabase database;
    private final MongoStorageBindingStore bindings;
    private final MongoConfigurationArtifactStore artifacts;
    private final Runnable active;
    MongoStorageRecoveryCommands(MongoClient client, MongoDatabase database, MongoStorageBindingStore bindings,
            MongoConfigurationArtifactStore artifacts, Runnable active) {
        this.client = client; this.database = database; this.bindings = bindings; this.artifacts = artifacts; this.active = active;
    }
    @Override public StorageRecoveryCommands guardedBy(Runnable guard) {
        Objects.requireNonNull(guard, "guard");
        return new MongoStorageRecoveryCommands(client, database, bindings, artifacts, () -> { active.run(); guard.run(); });
    }
    @Override public Plan plan(CacheAuthScope current, Manifest verified, String operationId, String previousOperationId) {
        Objects.requireNonNull(current, "current"); Objects.requireNonNull(verified, "verified");
        String id = UUID.randomUUID().toString();
        return transaction(session -> {
            Document root = root(session, current);
            Plan previous = pending(root).orElse(null);
            long revision = revision(session);
            if (operationId != null) {
                if (previousOperationId != null || previous == null || !operationId.equals(previous.operationId())) throw refused();
                if (previous.phase() == Phase.RUNNING || previous.phase() == Phase.COMPLETE) {
                    if (!previous.manifest().selection().equals(verified.selection())) throw refused();
                    return previous;
                }
                if (sameClosure(previous.manifest(), verified) && previous.controlRevision()+previous.phase().ordinal() == revision) return previous;
            } else if (previous != null) {
                // A retried initial request returns its original operation, even after completion.
                // A new operation explicitly names the completed predecessor; it cannot be inferred from a missing response.
                if (Objects.equals(previous.previousOperationId(), previousOperationId)
                        && previous.manifest().selection().equals(verified.selection())) return previous;
                if (previous.phase() != Phase.COMPLETE || !previous.operationId().equals(previousOperationId)) throw refused();
            } else if (previousOperationId != null) throw refused();
            requireManifest(session, verified);
            if (verified.configurationRevision() != revision) throw refused();
            ConfigurationVersion.advance(session, SystemCollections.CONFIGURATION_VERSION.on(database));
            long next = revision + 1;
            String originalId = operationId != null ? previous.operationId() : id;
            String predecessor = operationId != null ? previous.previousOperationId() : previousOperationId;
            Plan prepared = new Plan(originalId, predecessor, current, verified, hash(verified), next, Phase.PLANNED);
            save(session, root, prepared);
            return prepared;
        });
    }
    @Override public Optional<Plan> current(CacheAuthScope current) {
        return transaction(session -> pending(root(session, current)));
    }
    @Override public Optional<Plan> find(CacheAuthScope current, String originalId) {
        Objects.requireNonNull(originalId, "originalId");
        return transaction(session -> pending(root(session, current)).filter(plan -> plan.operationId().equals(originalId)));
    }
    @Override public Plan confirm(Plan original) { return transition(original, Phase.PLANNED, Phase.CONFIRMED, true); }
    @Override public Plan start(Plan confirmed) { return transition(confirmed, Phase.CONFIRMED, Phase.RUNNING, true); }
    @Override public Plan complete(Plan running) { return transition(running, Phase.RUNNING, Phase.COMPLETE, false); }

    private Plan transition(Plan original, Phase from, Phase to, boolean requireRevision) {
        Objects.requireNonNull(original, "original");
        return transaction(session -> {
            Document root = root(session, original.originalScope());
            Plan current = pending(root).orElseThrow(MongoStorageRecoveryCommands::refused);
            if (!sameOriginal(current, original)) throw refused();
            if (current.phase().ordinal() >= to.ordinal()) return current;
            if (current.phase() != from || original.phase() != from) throw refused();
            if (requireRevision && revision(session) != current.controlRevision()+current.phase().ordinal()) throw refused();
            requireManifest(session, original.manifest());
            ConfigurationVersion.advance(session, SystemCollections.CONFIGURATION_VERSION.on(database));
            Plan result = new Plan(current.operationId(), current.previousOperationId(), current.originalScope(), current.manifest(), current.planHash(), current.controlRevision(), to);
            save(session, root, result);
            return result;
        });
    }
    private Document root(ClientSession session, CacheAuthScope expected) {
        var preparation = bindings.cachePreparationForTransaction(session).orElseThrow(MongoStorageRecoveryCommands::refused);
        if (!preparation.authScope().equals(expected)) throw refused();
        Document setup = SystemCollections.STORAGE_SETUP.on(database).find(session, new Document("_id", "cluster")).first();
        if (setup == null || !"COMPLETE".equals(setup.get("phase"))) throw refused();
        Document root = SystemCollections.STORAGE_RECOVERY.on(database).find(session, new Document("_id", "cluster")).first();
        if (root == null) throw unreadable();
        return root;
    }
    private void requireManifest(ClientSession session, Manifest manifest) {
        var found = new TreeMap<String,Resource>();
        for (var entry : manifest.definitionHashes().entrySet()) {
            Resource resource = artifacts.getInTransaction(session, entry.getKey()).orElseThrow(MongoStorageRecoveryCommands::refused);
            if (!entry.getValue().equals(CanonicalHash.of(resource))) throw refused();
            found.put(entry.getKey(), resource);
        }
        for (String pipeline : manifest.affectedPipelineIds()) if (!(found.get(pipeline) instanceof PipelineResource)) throw refused();
        for (Resource resource : found.values()) for (var edge : ReferenceGraph.declaredReferences(resource)) {
            var dependency = found.get(edge.id());
            if (dependency == null || !dependency.kind().equals(edge.kind())) throw refused();
        }
        if (manifest.selection().extent() == Extent.CLUSTER) {
            var actual = new TreeSet<String>();
            try (var cursor = SystemCollections.PIPELINES.on(database).find(session).iterator()) {
                while (cursor.hasNext()) {
                    Object id = cursor.next().get("_id");
                    if (!(id instanceof String value) || value.isBlank()) throw unreadable();
                    actual.add(value);
                }
            }
            if (!actual.equals(manifest.affectedPipelineIds())) throw refused();
        }
    }
    private void save(ClientSession session, Document root, Plan value) {
        Document filter = new Document("_id", "cluster").append("clusterId", root.get("clusterId"))
                .append("bindingVersion", root.get("bindingVersion")).append("revision", root.get("revision"))
                .append("pendingRecovery", root.containsKey("pendingRecovery") ? root.get("pendingRecovery") : new Document("$exists", false));
        if (SystemCollections.STORAGE_RECOVERY.on(database).updateOne(session, filter,
                new Document("$set", new Document("pendingRecovery", encode(value)))).getMatchedCount() != 1) throw refused();
    }
    private long revision(ClientSession session) {
        Document row = SystemCollections.CONFIGURATION_VERSION.on(database).find(session, new Document("_id", ConfigurationVersion.ID)).first();
        if (row == null || !(row.get("revision") instanceof Long value) || value < 0 || value >= Long.MAX_VALUE-4) throw unreadable();
        return value;
    }
    static Optional<Plan> pending(Document root) {
        if (!root.containsKey("pendingRecovery")) return Optional.empty();
        try {
            Object value = root.get("pendingRecovery");
            if (!(value instanceof Document row) || !row.keySet().equals(Set.of("formatVersion", "operationId", "previousOperationId", "scope", "selection",
                    "affectedPipelineIds", "definitionHashes", "dependencyHash", "configurationRevision", "planHash", "controlRevision", "phase"))
                    || !Integer.valueOf(1).equals(row.get("formatVersion"))) throw unreadable();
            Document scope = row.get("scope", Document.class);
            if (scope == null || !scope.keySet().equals(Set.of("clusterId", "bindingVersion", "mode", "cacheIncarnation", "authGeneration"))) throw unreadable();
            var auth = new CacheAuthScope(scope.getString("clusterId"), scope.getLong("bindingVersion"),
                    io.tapstate.spi.store.StorageBinding.Mode.valueOf(scope.getString("mode")), scope.getString("cacheIncarnation"), scope.getLong("authGeneration"));
            if (!auth.clusterId().equals(root.get("clusterId")) || !Long.valueOf(auth.bindingVersion()).equals(root.get("bindingVersion"))) throw unreadable();
            Document selected = row.get("selection", Document.class);
            if (selected == null || !selected.keySet().equals(Set.of("extent", "pipelineIds"))) throw unreadable();
            var selection = new Selection(Extent.valueOf(selected.getString("extent")), strings(selected.get("pipelineIds")));
            Document hashes = row.get("definitionHashes", Document.class);
            if (hashes == null || hashes.values().stream().anyMatch(hash -> !(hash instanceof String))) throw unreadable();
            var definitions = new TreeMap<String,String>(); hashes.forEach((key, hash) -> definitions.put(key, (String) hash));
            var manifest = new Manifest(selection, strings(row.get("affectedPipelineIds")), definitions, row.getString("dependencyHash"), row.getLong("configurationRevision"));
            var result = new Plan(row.getString("operationId"), row.getString("previousOperationId"), auth, manifest, row.getString("planHash"), row.getLong("controlRevision"), Phase.valueOf(row.getString("phase")));
            if (!result.planHash().equals(hash(manifest))) throw unreadable();
            return Optional.of(result);
        } catch (IllegalArgumentException | ClassCastException | NullPointerException invalid) { throw unreadable(); }
    }
    private static Set<String> strings(Object values) {
        if (!(values instanceof List<?> list) || list.stream().anyMatch(value -> !(value instanceof String))) throw unreadable();
        var result = new TreeSet<String>(); list.forEach(value -> result.add((String) value));
        if (result.size() != list.size()) throw unreadable(); return result;
    }
    private static Document encode(Plan plan) {
        var scope = plan.originalScope(); var manifest = plan.manifest();
        return new Document("formatVersion", 1).append("operationId", plan.operationId()).append("previousOperationId", plan.previousOperationId())
                .append("scope", new Document("clusterId", scope.clusterId()).append("bindingVersion", scope.bindingVersion())
                        .append("mode", scope.mode().name()).append("cacheIncarnation", scope.cacheIncarnation()).append("authGeneration", scope.authGeneration()))
                .append("selection", new Document("extent", manifest.selection().extent().name()).append("pipelineIds", new ArrayList<>(manifest.selection().pipelineIds())))
                .append("affectedPipelineIds", new ArrayList<>(manifest.affectedPipelineIds())).append("definitionHashes", new Document(manifest.definitionHashes()))
                .append("dependencyHash", manifest.dependencyHash()).append("configurationRevision", manifest.configurationRevision())
                .append("planHash", plan.planHash()).append("controlRevision", plan.controlRevision()).append("phase", plan.phase().name());
    }
    private static String hash(Manifest manifest) {
        return CanonicalHash.ofText(JsonWriter.write(List.of(manifest.selection().extent().name(), new ArrayList<>(manifest.selection().pipelineIds()),
                new ArrayList<>(manifest.affectedPipelineIds()), manifest.definitionHashes(), manifest.dependencyHash(), manifest.configurationRevision())));
    }
    private static boolean sameClosure(Manifest a, Manifest b) {
        return a.selection().equals(b.selection()) && a.affectedPipelineIds().equals(b.affectedPipelineIds())
                && a.definitionHashes().equals(b.definitionHashes()) && a.dependencyHash().equals(b.dependencyHash());
    }
    private static boolean sameOriginal(Plan a, Plan b) {
        return a.operationId().equals(b.operationId()) && Objects.equals(a.previousOperationId(), b.previousOperationId()) && a.originalScope().equals(b.originalScope())
                && a.manifest().equals(b.manifest()) && a.planHash().equals(b.planHash());
    }
    private <T> T transaction(Function<ClientSession,T> body) {
        return StoreIo.call(() -> {
            try (var session = client.startSession()) {
                return session.withTransaction(() -> { active.run(); T result = body.apply(session); active.run(); return result; }, TX);
            }
        });
    }
    private static TapstateException refused() { return new TapstateException(IoError.CACHE_BUSINESS_FENCED, Map.of(), null); }
    private static TapstateException unreadable() { return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", "storage-recovery", "field", "pending-operation"), null); }
}
