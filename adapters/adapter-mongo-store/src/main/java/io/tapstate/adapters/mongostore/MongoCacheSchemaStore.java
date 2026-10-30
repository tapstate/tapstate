package io.tapstate.adapters.mongostore;

import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.ReturnDocument;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.SchemaStore;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/** The existing discovery writer/generation protocol, bound to one original CACHE domain. */
final class MongoCacheSchemaStore implements SchemaStore {
    private static final String WRITERS = "_writers";
    private static final String REVISION = "_revision";
    private static final String GENERATION = MongoSchemaStore.GENERATION;
    private static final long WRITE_LEASE_MILLIS = 300_000L;
    private static final int ATTEMPTS = 8;
    private static final int INSERT_BATCH = 64;
    private static final Set<String> PENDING = Set.of("_id", "cacheScope", WRITERS, REVISION);
    private static final Set<String> PUBLISHED = Set.of("_id", "cacheScope", WRITERS, REVISION, "modelVersion", "connectorId", "discoveredAt", GENERATION, "tableCount");
    private final MongoCacheMetadataAccess access;
    private final MongoCollection<Document> rows;

    MongoCacheSchemaStore(MongoCacheMetadataAccess access) {
        this.access = Objects.requireNonNull(access, "access");
        this.rows = access.collection.withWriteConcern(WriteConcern.MAJORITY.withJournal(true));
    }

    @Override public void save(DiscoveredSourceModel discovered) {
        Objects.requireNonNull(discovered, "discovered");
        String id = discovered.connectionId(); ownedKeys(id);
        String generation = UUID.randomUUID().toString();
        var tables = new ArrayList<Document>();
        for (int order = 0; order < discovered.model().tables().size(); order++) {
            tables.add(access.stamp(MongoSchemaStore.tableDocument(discovered.model().tables().get(order))
                    .append("_id", tableKey(id, generation, order)).append(GENERATION, generation).append("order", order)));
        }
        reserve(id, generation);
        try {
            // A schema may exceed one document or transaction budget. These rows remain unreachable.
            for (int at = 0; at < tables.size(); at += INSERT_BATCH) {
                List<Document> batch = List.copyOf(tables.subList(at, Math.min(at + INSERT_BATCH, tables.size())));
                access.verify(); StoreIo.run(id, () -> rows.insertMany(batch)); access.verify();
            }
            publish(discovered, generation);
        } catch (RuntimeException failure) {
            try { release(id, generation); }
            catch (RuntimeException releaseFailure) { failure.addSuppressed(releaseFailure); }
            throw failure;
        }
        reclaim(id);
    }

    private void reserve(String id, String generation) {
        access.write(session -> {
            Document current = rows.find(session, new Document("_id", id)).first();
            if (current == null) {
                rows.insertOne(session, access.stamp(new Document("_id", id).append(WRITERS, new Document()).append(REVISION, 1L)));
            } else if (!access.inScope(current, id)) {
                // Explicit discovery rebuilds this derived envelope; old generation rows retain their scope.
                if (rows.replaceOne(session, new Document("_id", id).append("cacheScope", current.get("cacheScope")),
                        access.stamp(new Document("_id", id).append(WRITERS, new Document()).append(REVISION, 1L))).getMatchedCount() != 1) throw contention(id);
            } else { validate(current, id); }
            Document reserved = rows.find(session, new Document("_id", id)).first(); validate(reserved, id); incrementable(reserved, id);
            if (rows.updateOne(session, fence(reserved), new Document("$currentDate", new Document(WRITERS + "." + generation, true))
                    .append("$inc", new Document(REVISION, 1L))).getMatchedCount() != 1) throw contention(id);
            return null;
        });
    }

    private void publish(DiscoveredSourceModel discovered, String generation) {
        String id = discovered.connectionId();
        access.write(session -> {
            Document current = current(session, id); if (current == null) throw contention(id);
            Document writers = new Document(current.get(WRITERS, Document.class));
            if (!writers.containsKey(generation)) throw contention(id);
            writers.remove(generation); incrementable(current, id);
            Document replacement = access.stamp(MongoSchemaStore.envelope(discovered, generation)
                    .append("tableCount", discovered.model().tables().size()).append(WRITERS, writers).append(REVISION, revision(current) + 1));
            Document displaced = rows.findOneAndReplace(session, fence(current), replacement,
                    new FindOneAndReplaceOptions().returnDocument(ReturnDocument.BEFORE));
            if (displaced == null) throw contention(id);
            return null;
        });
    }

    private void release(String id, String generation) {
        access.write(session -> {
            Document current = current(session, id);
            if (current != null && current.get(WRITERS, Document.class).containsKey(generation)) {
                incrementable(current, id);
                if (rows.updateOne(session, fence(current), new Document("$unset", new Document(WRITERS + "." + generation, ""))
                        .append("$inc", new Document(REVISION, 1L))).getMatchedCount() != 1) throw contention(id);
            }
            return null;
        });
    }

    @Override public Optional<DiscoveredSourceModel> get(String id) {
        Objects.requireNonNull(id, "connectionId"); ownedKeys(id);
        // The envelope and its complete generation are read in one original CACHE snapshot.
        return access.read(session -> {
            Document envelope = current(session, id);
            if (envelope == null || !envelope.containsKey(GENERATION)) return Optional.empty();
            String generation = envelope.getString(GENERATION);
            var tables = new ArrayList<Document>();
            try (var cursor = rows.find(session, scoped(new Document("_id", ownedKeys(id)).append(GENERATION, generation)))
                    .sort(new Document("order", 1)).iterator()) {
                while (cursor.hasNext()) {
                    Document row = cursor.next(); int order = tables.size();
                    if (!access.inScope(row, id) || !Integer.valueOf(order).equals(row.get("order"))
                            || !tableKey(id, generation, order).equals(row.get("_id"))) throw unreadable(id);
                    Document payload = access.payload(row); payload.remove("_id"); payload.remove(GENERATION); payload.remove("order");
                    try {
                        if (!payload.equals(MongoSchemaStore.tableDocument(MongoSchemaStore.toTable(payload, id)))) throw unreadable(id);
                    } catch (TapstateException | IllegalArgumentException | NullPointerException | ClassCastException invalid) { throw unreadable(id); }
                    tables.add(row);
                }
            }
            if (tables.size() != envelope.getInteger("tableCount")) throw unreadable(id);
            try { return Optional.of(MongoSchemaStore.toDiscovered(envelope, tables)); }
            catch (TapstateException | IllegalArgumentException | NullPointerException | ClassCastException invalid) { throw unreadable(id); }
        });
    }

    private void reclaim(String id) {
        for (int attempt = 0; attempt < ATTEMPTS; attempt++) {
            Document observed = access.read(session -> {
                Document found = rows.aggregate(session, List.of(new Document("$match", new Document("_id", id)),
                        new Document("$set", new Document("_reclaimBefore", new Document("$subtract", List.of("$$NOW", WRITE_LEASE_MILLIS)))))).first();
                if (found == null || !access.inScope(found, id)) return null;
                Document clean = new Document(found); clean.remove("_reclaimBefore"); validate(clean, id); return found;
            });
            if (observed == null) return;
            Document original = new Document(observed); original.remove("_reclaimBefore");
            Date cutoff = observed.getDate("_reclaimBefore");
            var active = new Document(observed.get(WRITERS, Document.class));
            active.entrySet().removeIf(e -> ((Date) e.getValue()).compareTo(cutoff) <= 0);
            Set<String> protectedGenerations = new HashSet<>(active.keySet());
            if (observed.containsKey(GENERATION)) protectedGenerations.add(observed.getString(GENERATION));
            Set<String> garbage = access.read(session -> {
                var found = new HashSet<String>();
                try (var cursor = rows.find(session, scoped(new Document("_id", ownedKeys(id))))
                        .projection(new Document(GENERATION, 1).append("cacheScope", 1)).iterator()) {
                    while (cursor.hasNext()) {
                        Document row = cursor.next();
                        if (!access.inScope(row, id)) throw unreadable(id);
                        Object value = row.get(GENERATION);
                        if (!(value instanceof String candidate) || !canonicalUuid(candidate)) throw unreadable(id);
                        if (!protectedGenerations.contains(candidate)) found.add(candidate);
                    }
                }
                return found;
            });
            boolean revoked = access.write(session -> {
                Document current = current(session, id);
                if (current == null || !current.equals(original)) return false;
                incrementable(current, id);
                return rows.updateOne(session, fence(current), new Document("$set", new Document(WRITERS, active))
                        .append("$inc", new Document(REVISION, 1L))).getMatchedCount() == 1;
            });
            if (revoked) {
                if (!garbage.isEmpty()) {
                    Document exact = scoped(new Document("_id", ownedKeys(id)).append(GENERATION, new Document("$in", List.copyOf(garbage))));
                    // Revocation won before deletion; a late writer cannot publish these exact UUIDs.
                    access.verify(); StoreIo.run(id, () -> rows.deleteMany(exact)); access.verify();
                }
                return;
            }
        }
    }

    private Document current(ClientSession session, String id) {
        Document row = rows.find(session, new Document("_id", id)).first();
        if (row == null || !access.inScope(row, id)) return null;
        validate(row, id); return row;
    }
    private Document scoped(Document filter) {
        Document scope = access.stamp(new Document()).get("cacheScope", Document.class);
        return new Document(filter).append("cacheScope.clusterId", scope.getString("clusterId"))
                .append("cacheScope.bindingVersion", scope.getLong("bindingVersion")).append("cacheScope.cacheIncarnation", scope.getString("cacheIncarnation"));
    }
    private static Document fence(Document current) { return new Document(current); }
    private static long revision(Document current) { return current.getLong(REVISION); }
    private static void incrementable(Document current, String id) { if (revision(current) >= Long.MAX_VALUE - 1) throw unreadable(id); }
    private static void validate(Document row, String id) {
        try {
            if (row == null || !id.equals(row.get("_id")) || !(row.get(REVISION) instanceof Long revision) || revision < 1 || revision == Long.MAX_VALUE
                    || !(row.get(WRITERS) instanceof Document writers)) throw unreadable(id);
            for (var writer : writers.entrySet()) if (!canonicalUuid(writer.getKey()) || !(writer.getValue() instanceof Date)) throw unreadable(id);
            if (!row.containsKey(GENERATION)) { if (!row.keySet().equals(PENDING)) throw unreadable(id); return; }
            if (!row.keySet().equals(PUBLISHED) || !Integer.valueOf(MongoSchemaStore.RESOLVED_TYPES).equals(row.get("modelVersion"))
                    || !(row.get("connectorId") instanceof String) || !(row.get("discoveredAt") instanceof Long)
                    || !(row.get("tableCount") instanceof Integer count) || count < 0 || !(row.get(GENERATION) instanceof String generation)
                    || !canonicalUuid(generation)) throw unreadable(id);
        } catch (IllegalArgumentException | NullPointerException | ClassCastException invalid) { throw unreadable(id); }
    }
    private static boolean canonicalUuid(String value) { try { return UUID.fromString(value).toString().equals(value); }
        catch (IllegalArgumentException invalid) { return false; } }
    private static Document ownedKeys(String id) {
        if (id.indexOf('.') >= 0) throw new IllegalArgumentException("connectionId must not contain '.'");
        return new Document("$gte", id + '.').append("$lt", id + '/');
    }
    private static String tableKey(String id, String generation, int order) { return id + '.' + generation + '.' + order; }
    private static TapstateException unreadable(String id) { return MongoCacheMetadataAccess.unreadable(id, "schema"); }
    private static TapstateException contention(String id) { return new TapstateException(IoError.SCHEMA_WRITE_CONTENTION, Map.of("connectionId", id), null); }
}
