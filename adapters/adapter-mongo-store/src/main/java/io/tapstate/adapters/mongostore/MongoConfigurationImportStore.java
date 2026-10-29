package io.tapstate.adapters.mongostore;

import com.mongodb.ErrorCategory;
import com.mongodb.MongoException;
import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.dsl.ReferenceGraph;
import io.tapstate.core.dsl.Workspace;
import io.tapstate.core.dsl.DslException;
import io.tapstate.spi.store.ConfigurationImportStore;
import io.tapstate.spi.store.FirstAdminStore;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.StorageBinding;
import org.bson.Document;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.Function;
import static io.tapstate.adapters.mongostore.ConfigurationImportPayload.refused;
import static io.tapstate.adapters.mongostore.ConfigurationImportPayload.limit;

/** One original CONFIG client owns registration, attempt fencing and the all-or-nothing import receipt. */
final class MongoConfigurationImportStore implements ConfigurationImportStore {
    private static final TransactionOptions TRANSACTION = TransactionOptions.builder().readConcern(ReadConcern.SNAPSHOT)
            .writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();
    private static final Set<String> BASE = Set.of("_id","formatVersion","scope","inputHash","mappingHash","destinationRevision",
            "createdAt","expiresAt","phase","attempts");
    private final MongoClient client;
    private final MongoDatabase database;
    private final String clusterId;
    private final MongoStorageBindingStore bindings;
    private final MongoConfigurationArtifactStore artifacts;
    private final MongoFirstAdminStore firstAdmin;
    private final MongoCollection<Document> operations,versions,users,layouts,setup;
    private final Runnable requireActive;

    MongoConfigurationImportStore(MongoClient client,MongoDatabase database,String clusterId,MongoStorageBindingStore bindings,
            MongoConfigurationArtifactStore artifacts,Runnable requireActive) {
        this.client = Objects.requireNonNull(client,"client"); this.database = Objects.requireNonNull(database,"database");
        this.clusterId = Objects.requireNonNull(clusterId,"clusterId"); this.bindings = Objects.requireNonNull(bindings,"bindings");
        this.artifacts = Objects.requireNonNull(artifacts,"artifacts"); this.requireActive = Objects.requireNonNull(requireActive,"requireActive");
        if (!bindings.belongsTo(client,database.getName()) || !artifacts.belongsTo(client,database.getName()))
            throw new IllegalArgumentException("import stores must share one original configuration client and database");
        firstAdmin = new MongoFirstAdminStore(client,database,bindings);
        operations = SystemCollections.CONFIGURATION_IMPORTS.on(database);
        versions = SystemCollections.CONFIGURATION_VERSION.on(database); users = SystemCollections.CONFIG_USERS.on(database);
        layouts = SystemCollections.CONFIG_PIPELINE_LAYOUTS.on(database); setup = SystemCollections.STORAGE_SETUP.on(database);
    }
    @Override public ConfigurationImportStore guardedBy(Runnable additional) {
        Objects.requireNonNull(additional,"requireActive");
        return new MongoConfigurationImportStore(client,database,clusterId,bindings,artifacts,() -> { requireActive.run(); additional.run(); });
    }
    static void prepareIndexes(MongoDatabase database) {
        StoreIo.run(() -> { for (var index : SystemCollections.CONFIGURATION_IMPORTS.indexes())
            IndexEnsure.createIfAbsent(SystemCollections.CONFIGURATION_IMPORTS.on(database),index); });
    }
    @Override public Status preflight(Scope scope,Payload payload) {
        Objects.requireNonNull(scope,"scope"); var frozen = new ConfigurationImportPayload(Objects.requireNonNull(payload,"payload"));
        String id = UUID.randomUUID().toString();
        return StoreIo.call(() -> withSlotRetry(session -> {
            var binding = requireScope(session,scope,frozen);
            Instant now = serverNow(session); var rows = all(session);
            for (var row : rows) {
                if (!sameDestination(scope,row.status().ticket().scope())) throw unreadable();
                if (scope.equals(row.status().ticket().scope()) && !now.isBefore(row.status().ticket().expiresAt())
                        && (row.status().phase() == Phase.PENDING || row.status().phase() == Phase.ABORTED))
                    operations.deleteOne(session,filter(row));
            }
            rows = all(session);
            if (rows.stream().filter(row -> row.historySlot() != null).count() >= MAX_RESULTS) throw limit("retained-results");
            int slot = available(rows.stream().map(Stored::activeSlot).filter(Objects::nonNull).toList(),MAX_PENDING,"pending-operations");
            long revision = revision(session);
            validateCandidate(session,frozen,binding);
            var ticket = new Ticket(id,scope,frozen.inputHash,frozen.mappingHash,revision,now,now.plus(PREFLIGHT_LIFETIME));
            frozen.receipt(ticket);
            var result = new Stored(new Status(ticket,Phase.PENDING,0,null),slot,null);
            operations.insertOne(session,encode(result)); return result.status();
        }));
    }
    @Override public Optional<Status> find(Scope scope,String id) {
        Objects.requireNonNull(scope,"scope"); requireId(id);
        return StoreIo.call(() -> transaction(session -> {
            requireScope(session,scope,null);
            Document row = operations.find(session,new Document("_id",id)).first();
            if (row == null) return Optional.empty();
            var stored = decode(row); if (!scope.equals(stored.status().ticket().scope())) throw refused("scope-mismatch");
            var status = stored.status();
            if (status.phase() == Phase.PENDING && !serverNow(session).isBefore(status.ticket().expiresAt()))
                status = new Status(status.ticket(),Phase.EXPIRED,0,null);
            return Optional.of(status);
        }));
    }
    @Override public Receipt commit(Ticket original,Payload payload) {
        Objects.requireNonNull(original,"original"); var frozen = new ConfigurationImportPayload(Objects.requireNonNull(payload,"payload"));
        if (!original.inputHash().equals(frozen.inputHash) || !original.mappingHash().equals(frozen.mappingHash)) throw refused("input-mismatch");
        Receipt candidate = frozen.receipt(original);
        return StoreIo.call(() -> {
            Stored attempt = withSlotRetry(session -> {
                requireScope(session,original.scope(),frozen);
                Stored current = original(session,original);
                if (current.status().phase() == Phase.COMMITTED) return current;
                if (!serverNow(session).isBefore(original.expiresAt())) throw refused("expired");
                if (current.status().phase() == Phase.ABORTED) throw refused("aborted");
                if (current.status().attempts() >= MAX_ATTEMPTS) throw limit("commit-attempts");
                int history = current.historySlot() == null
                        ? available(all(session).stream().map(Stored::historySlot).filter(Objects::nonNull).toList(),MAX_RESULTS,"retained-results")
                        : current.historySlot();
                var next = new Stored(new Status(original,Phase.COMMITTING,current.status().attempts()+1,null),current.activeSlot(),history);
                replace(session,current,next); return next;
            });
            if (attempt.status().phase() == Phase.COMMITTED) return attempt.status().receipt();
            try {
                return transaction(session -> {
                    var binding = requireScope(session,original.scope(),frozen);
                    Stored current = original(session,original);
                    if (current.status().phase() == Phase.COMMITTED) return current.status().receipt();
                    if (!serverNow(session).isBefore(original.expiresAt())) throw refused("expired");
                    if (current.status().phase() != Phase.COMMITTING || current.status().attempts() != attempt.status().attempts()) throw refused("attempt-fenced");
                    if (revision(session) != original.destinationRevision()) throw refused("destination-changed");
                    ConfigurationVersion.advance(session, versions, client);
                    validateCandidate(session,frozen,binding);
                    try { artifacts.writeAllInTransaction(session,frozen.writes); }
                    catch (MongoConfigurationArtifactStore.Refused conflict) { throw refused("resource-conflict"); }
                    for (var user : frozen.payload.users()) users.insertOne(session,MongoUserStore.toDocument(user));
                    for (var layout : frozen.payload.layouts()) layouts.insertOne(session,MongoPipelineLayoutStore.toDocument(layout));
                    if (!serverNow(session).isBefore(original.expiresAt())) throw refused("expired");
                    replace(session,current,new Stored(new Status(original,Phase.COMMITTED,attempt.status().attempts(),candidate),null,current.historySlot()));
                    return candidate;
                });
            } catch (TapstateException failure) {
                if (failure.code() == IoError.CONFIGURATION_IMPORT_REFUSED) {
                    try { abortKnown(original,attempt.status().attempts()); }
                    catch (RuntimeException cleanup) { if (cleanup != failure) failure.addSuppressed(cleanup); }
                }
                throw failure;
            }
        });
    }
    private void abortKnown(Ticket original,int attempt) {
        transaction(session -> {
            requireScope(session,original.scope(),null); Stored current = original(session,original);
            if (current.status().phase() == Phase.COMMITTING && current.status().attempts() == attempt)
                replace(session,current,new Stored(new Status(original,Phase.ABORTED,attempt,null),null,null));
            return null;
        });
    }
    private StorageBinding requireScope(ClientSession session,Scope scope,ConfigurationImportPayload frozen) {
        var binding = bindings.loadForTransaction(session).orElseThrow(() -> refused("not-ready"));
        var state = setup.find(session,new Document("_id","cluster")).first();
        if (state == null || !"COMPLETE".equals(state.get("phase"))) throw refused("not-ready");
        if (!clusterId.equals(scope.clusterId()) || !clusterId.equals(binding.setup().clusterId())
                || scope.bindingVersion() != binding.bindingVersion() || scope.mode() != binding.mode()) throw refused("scope-mismatch");
        var administrator = firstAdmin.load(session);
        if (scope.mode() == StorageBinding.Mode.OP && (administrator.isEmpty() || administrator.get().phase() != FirstAdminStore.Phase.COMPLETE))
            throw refused("not-ready");
        if (scope.mode() == StorageBinding.Mode.CLOUD && frozen != null && !frozen.payload.users().isEmpty()) throw refused("cloud-users");
        return binding;
    }
    private void validateCandidate(ClientSession session,ConfigurationImportPayload frozen,StorageBinding binding) {
        var external = new java.util.HashMap<String,Resource>();
        for (var expected : frozen.payload.referenceHashes().entrySet()) {
            var observed = artifacts.getInTransaction(session,expected.getKey());
            if (observed.isEmpty() || !expected.getValue().equals(CanonicalHash.of(observed.get()))) throw refused("reference-changed");
            external.put(expected.getKey(),observed.get());
        }
        for (var resource : frozen.payload.resources())
            if (artifacts.getInTransaction(session,resource.id()).isPresent()) throw refused("resource-exists");
        for (var user : frozen.payload.users()) if (users.find(session,new Document("_id",user.username())).first() != null) throw refused("user-exists");
        for (var layout : frozen.payload.layouts()) if (layouts.find(session,new Document("_id",layout.pipelineId())).first() != null) throw refused("layout-exists");
        var closure = new java.util.LinkedHashMap<String,Resource>(); frozen.payload.resources().forEach(resource -> closure.put(resource.id(),resource));
        for (var mapping : frozen.payload.defaultSourceMapping().entrySet()) {
            if (!binding.defaultSourceId().equals(mapping.getValue())) throw refused("default-target-mismatch");
            var target = external.get(mapping.getValue());
            if (!(target instanceof SourceResource)) throw refused("default-target-unproven");
            closure.putIfAbsent(target.id(),target);
        }
        var queue = new java.util.ArrayDeque<Resource>(closure.values()); var visited = new HashSet<String>();
        while (!queue.isEmpty()) {
            var resource = queue.remove(); if (!visited.add(resource.id())) continue;
            for (var declared : ReferenceGraph.declaredReferences(resource)) {
                var target = closure.get(declared.id()); if (target == null) target = external.get(declared.id());
                if (target == null) throw refused("reference-unproven");
                if (!declared.kind().equals(target.kind())) throw refused("reference-kind");
                if (closure.putIfAbsent(target.id(),target) == null) queue.add(target);
            }
        }
        try { Workspace.of(List.copyOf(closure.values())); }
        catch (DslException invalid) { throw refused("reference-closure"); }
    }
    private static boolean sameDestination(Scope first,Scope second) {
        return first.clusterId().equals(second.clusterId()) && first.bindingVersion() == second.bindingVersion() && first.mode() == second.mode();
    }
    private long revision(ClientSession session) {
        Document row = versions.find(session,new Document("_id",ConfigurationVersion.ID)).first();
        if (row == null || !(row.get("revision") instanceof Long revision) || revision < 0 || revision >= Long.MAX_VALUE - 1) throw unreadable();
        return revision;
    }
    private Instant serverNow(ClientSession session) {
        Document row = versions.aggregate(session,List.of(new Document("$match",new Document("_id",ConfigurationVersion.ID)),
                new Document("$project",new Document("_id",0).append("now","$$NOW")))).first();
        if (row == null || !(row.get("now") instanceof Date now)) throw unreadable(); return now.toInstant();
    }
    private Stored original(ClientSession session,Ticket ticket) {
        Document row = operations.find(session,new Document("_id",ticket.id())).first();
        if (row == null) throw refused("unknown-operation"); var stored = decode(row);
        if (!ticket.equals(stored.status().ticket())) throw refused("input-mismatch"); return stored;
    }
    private List<Stored> all(ClientSession session) {
        var rows = new ArrayList<Stored>();
        try (var cursor = operations.find(session).iterator()) {
            while (cursor.hasNext()) { requireActive.run(); rows.add(decode(cursor.next())); }
        }
        return rows;
    }
    private static int available(List<Integer> occupied,int bound,String budget) {
        var used = new HashSet<>(occupied); if (used.size() != occupied.size()) throw unreadable();
        for (int slot = 0; slot < bound; slot++) if (!used.contains(slot)) return slot;
        throw limit(budget);
    }
    private Document filter(Stored original) {
        return new Document("_id",original.status().ticket().id()).append("phase",original.status().phase().name()).append("attempts",original.status().attempts())
                .append("inputHash",original.status().ticket().inputHash()).append("scope",scope(original.status().ticket().scope()));
    }
    private void replace(ClientSession session,Stored before,Stored next) {
        if (operations.replaceOne(session,filter(before),encode(next)).getMatchedCount() != 1)
            throw new IllegalStateException("import operation changed inside its original transaction snapshot");
    }
    private <T> T withSlotRetry(Function<ClientSession,T> body) {
        for (int attempt = 0; ; attempt++) {
            try { return transaction(body); }
            catch (MongoException concurrent) {
                if (attempt >= MAX_PENDING || ErrorCategory.fromErrorCode(concurrent.getCode()) != ErrorCategory.DUPLICATE_KEY) throw concurrent;
            }
        }
    }
    private <T> T transaction(Function<ClientSession,T> body) {
        requireActive.run();
        try (var session = client.startSession()) {
            return session.withTransaction(() -> {
                requireActive.run(); verifyStructure(); requireActive.run(); T result = body.apply(session); requireActive.run(); return result;
            },TRANSACTION);
        }
    }
    private void verifyStructure() {
        if (!database.listCollectionNames().into(new ArrayList<>()).contains(SystemCollections.CONFIGURATION_IMPORTS.collectionName())) throw unreadable();
        var indexes = operations.listIndexes().into(new ArrayList<>());
        if (indexes.stream().anyMatch(index -> index.containsKey("expireAfterSeconds"))) throw unreadable();
        for (var expected : SystemCollections.CONFIGURATION_IMPORTS.indexes()) {
            var actual = indexes.stream().filter(index -> expected.indexName().equals(index.get("name"))).findFirst().orElse(null);
            if (actual == null || !new Document(expected.keys().getFirst(),1).equals(actual.get("key"))
                    || !Boolean.TRUE.equals(actual.get("unique")) || Boolean.TRUE.equals(actual.get("sparse")) || Boolean.TRUE.equals(actual.get("hidden"))
                    || !Document.parse(expected.partialFilterJson()).equals(actual.get("partialFilterExpression"))) throw unreadable();
        }
    }
    private record Stored(Status status,Integer activeSlot,Integer historySlot) {}
    private static Document scope(Scope scope) {
        return new Document("clusterId",scope.clusterId()).append("bindingVersion",scope.bindingVersion()).append("mode",scope.mode().name()).append("organizationId",scope.organizationId());
    }
    private static Document encode(Stored row) {
        var ticket = row.status().ticket();
        var document = new Document("_id",ticket.id()).append("formatVersion",1).append("scope",scope(ticket.scope()))
                .append("inputHash",ticket.inputHash()).append("mappingHash",ticket.mappingHash()).append("destinationRevision",ticket.destinationRevision())
                .append("createdAt",Date.from(ticket.createdAt())).append("expiresAt",Date.from(ticket.expiresAt()))
                .append("phase",row.status().phase().name()).append("attempts",row.status().attempts());
        if (row.activeSlot() != null) document.append("activeSlot",row.activeSlot());
        if (row.historySlot() != null) document.append("historySlot",row.historySlot());
        if (row.status().receipt() != null) {
            var receipt = ConfigurationImportPayload.receiptTree(row.status().receipt());
            document.append("receipt",receipt).append("receiptHash",CanonicalHash.ofText(JsonWriter.write(receipt)));
        }
        return document;
    }
    private static Stored decode(Document document) {
        try {
            if (!Integer.valueOf(1).equals(document.get("formatVersion")) || !(document.get("scope") instanceof Document coordinates)
                    || !coordinates.keySet().equals(Set.of("clusterId","bindingVersion","mode","organizationId"))
                    || !(coordinates.get("bindingVersion") instanceof Long binding)
                    || !(document.get("destinationRevision") instanceof Long revision) || !(document.get("attempts") instanceof Integer attempts)
                    || !(document.get("createdAt") instanceof Date created) || !(document.get("expiresAt") instanceof Date expires)) throw unreadable();
            var ticket = new Ticket(document.getString("_id"),new Scope(coordinates.getString("clusterId"),binding,
                    StorageBinding.Mode.valueOf(coordinates.getString("mode")),coordinates.getString("organizationId")),document.getString("inputHash"),
                    document.getString("mappingHash"),revision,created.toInstant(),expires.toInstant());
            Phase phase = Phase.valueOf(document.getString("phase")); if (phase == Phase.EXPIRED) throw unreadable();
            var expected = new HashSet<>(BASE); Integer active = null,history = null; Receipt receipt = null;
            if (phase == Phase.PENDING || phase == Phase.COMMITTING) {
                expected.add("activeSlot"); if (!(document.get("activeSlot") instanceof Integer slot) || slot < 0 || slot >= MAX_PENDING) throw unreadable(); active = slot;
            }
            if (phase == Phase.COMMITTING || phase == Phase.COMMITTED) {
                expected.add("historySlot"); if (!(document.get("historySlot") instanceof Integer slot) || slot < 0 || slot >= MAX_RESULTS) throw unreadable(); history = slot;
            }
            if (phase == Phase.COMMITTED) {
                expected.add("receipt"); expected.add("receiptHash");
                if (!(document.get("receipt") instanceof Document body)) throw unreadable(); receipt = receipt(body);
                if (!CanonicalHash.ofText(JsonWriter.write(ConfigurationImportPayload.receiptTree(receipt))).equals(document.get("receiptHash"))) throw unreadable();
            }
            if (!expected.equals(document.keySet())) throw unreadable(); return new Stored(new Status(ticket,phase,attempts,receipt),active,history);
        } catch (NullPointerException | IllegalArgumentException | ClassCastException invalid) { throw unreadable(); }
    }
    private static Receipt receipt(Document body) {
        if (!body.keySet().equals(Set.of("id","inputHash","configurationRevision","resourceHashes","usernames","layoutIds"))
                || !(body.get("configurationRevision") instanceof Long revision) || !(body.get("resourceHashes") instanceof Document hashes)) throw unreadable();
        var resources = new TreeMap<String,String>();
        hashes.forEach((id,hash) -> { if (!(hash instanceof String text)) throw unreadable(); resources.put(id,(String)hash); });
        var users = strings(body.get("usernames")); var layouts = strings(body.get("layoutIds"));
        if (resources.size()+users.size()+layouts.size() < 1 || resources.size()+users.size()+layouts.size() > MAX_OBJECTS) throw unreadable();
        var receipt = new Receipt(body.getString("id"),body.getString("inputHash"),revision,resources,users,layouts);
        if (JsonWriter.write(ConfigurationImportPayload.receiptTree(receipt)).getBytes(java.nio.charset.StandardCharsets.UTF_8).length > MAX_RECEIPT_BYTES) throw unreadable();
        return receipt;
    }
    private static Set<String> strings(Object value) {
        if (!(value instanceof List<?> list)) throw unreadable(); var result = new TreeSet<String>();
        for (var item : list) if (!(item instanceof String text) || text.isBlank() || !result.add(text)) throw unreadable();
        if (!List.copyOf(result).equals(list)) throw unreadable(); return result;
    }
    private static void requireId(String id) { if (id == null || !UUID.fromString(id).toString().equals(id)) throw new IllegalArgumentException("invalid import id"); }
    private static TapstateException unreadable() { return new TapstateException(IoError.DOCUMENT_UNREADABLE,Map.of("id","configuration-import","field","operation"),null); }
}
