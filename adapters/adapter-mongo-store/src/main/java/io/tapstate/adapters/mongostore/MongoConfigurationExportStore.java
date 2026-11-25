package io.tapstate.adapters.mongostore;

import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.dsl.ReferenceGraph;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.spi.store.ConfigurationExportStore;
import io.tapstate.spi.store.ConfigurationImportStore;
import io.tapstate.spi.store.FirstAdminStore;
import io.tapstate.spi.store.PipelineLayout;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.spi.store.User;
import org.bson.Document;
import java.util.*;
import static io.tapstate.adapters.mongostore.ConfigurationImportPayload.refused;
import static io.tapstate.adapters.mongostore.ConfigurationImportPayload.limit;

/** Reads the selected typed reference closure and optional users/layouts in one snapshot transaction. */
final class MongoConfigurationExportStore implements ConfigurationExportStore {
    private static final TransactionOptions READ = TransactionOptions.builder().readConcern(ReadConcern.SNAPSHOT)
            .writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();
    private final MongoClient client;
    private final MongoDatabase database;
    private final String clusterId;
    private final MongoStorageBindingStore bindings;
    private final MongoConfigurationArtifactStore artifacts;
    private final Runnable active;
    MongoConfigurationExportStore(MongoClient client, MongoDatabase database, String clusterId,
            MongoStorageBindingStore bindings, MongoConfigurationArtifactStore artifacts, Runnable active) {
        this.client = client; this.database = database; this.clusterId = clusterId;
        this.bindings = bindings; this.artifacts = artifacts; this.active = active;
    }
    @Override public ConfigurationExportStore guardedBy(Runnable guard) {
        Objects.requireNonNull(guard);
        return new MongoConfigurationExportStore(client, database, clusterId, bindings, artifacts, () -> { active.run(); guard.run(); });
    }
    @Override public Snapshot snapshot(ConfigurationImportStore.Scope scope, Selection selection) {
        return StoreIo.call(() -> {
            try (var session = client.startSession()) {
                return session.withTransaction(() -> {
                    active.run();
                    ConfigurationVersion.read(session, SystemCollections.CONFIGURATION_VERSION.on(database));
                    var binding = bindings.loadForTransaction(session).orElseThrow(() -> refused("not-ready"));
                    var setup = SystemCollections.STORAGE_SETUP.on(database).find(session, new Document("_id", "cluster")).first();
                    if (setup == null || !"COMPLETE".equals(setup.get("phase"))) throw refused("not-ready");
                    if (!scope.clusterId().equals(clusterId) || !clusterId.equals(binding.setup().clusterId())
                            || scope.bindingVersion() != binding.bindingVersion() || scope.mode() != binding.mode()) throw refused("scope-mismatch");
                    var administrator = new MongoFirstAdminStore(client, database, bindings).load(session);
                    if (scope.mode() == StorageBinding.Mode.OP && (administrator.isEmpty() || administrator.get().phase() != FirstAdminStore.Phase.COMPLETE)) throw refused("not-ready");
                    if (scope.mode() == StorageBinding.Mode.CLOUD && !selection.usernames().isEmpty()) throw refused("cloud-users");
                    var resources = new TreeMap<String, Resource>();
                    var pending = new ArrayDeque<ReferenceGraph.Edge>();
                    selection.resourceIds().stream().sorted().forEach(id -> pending.add(new ReferenceGraph.Edge(id, null)));
                    boolean needsDefault = false;
                    while (!pending.isEmpty()) {
                        var edge = pending.remove();
                        Resource resource = resources.get(edge.id());
                        if (resource == null) resource = artifacts.getInTransaction(session, edge.id()).orElseThrow(() -> refused("reference-missing"));
                        if (edge.kind() != null && !edge.kind().equals(resource.kind())) throw refused("reference-kind");
                        if (edge.id().equals(binding.defaultSourceId())) { needsDefault = true; continue; }
                        if (resources.putIfAbsent(resource.id(), resource) != null) continue;
                        if (resources.size() + selection.usernames().size() > ConfigurationImportStore.MAX_OBJECTS) throw limit("objects");
                        if (resource instanceof PipelineResource pipeline && pipeline.view() != null) needsDefault = true;
                        pending.addAll(ReferenceGraph.declaredReferences(resource));
                    }
                    var users = new ArrayList<User>();
                    for (String username : new TreeSet<>(selection.usernames())) {
                        var row = SystemCollections.CONFIG_USERS.on(database).find(session, new Document("_id", username)).first();
                        if (row == null) throw refused("user-missing");
                        users.add(MongoUserStore.toUser(row));
                    }
                    var layouts = new ArrayList<PipelineLayout>();
                    for (var resource : resources.values()) if (resource instanceof PipelineResource) {
                        var row = SystemCollections.CONFIG_PIPELINE_LAYOUTS.on(database).find(session, new Document("_id", resource.id())).first();
                        if (row != null) layouts.add(MongoPipelineLayoutStore.toLayout(row));
                    }
                    var frozen = new ConfigurationImportPayload(new ConfigurationImportStore.Payload(
                            List.copyOf(resources.values()), users, layouts, Map.of(), Map.of()));
                    active.run();
                    return new Snapshot(binding.mode(), needsDefault ? binding.defaultSourceId() : null, frozen.payload);
                }, READ);
            }
        });
    }
}
