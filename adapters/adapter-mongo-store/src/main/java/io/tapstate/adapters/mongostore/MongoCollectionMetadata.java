package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.cursor.TimeoutMode;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/** Reads complete collection descriptions without query operators restricted by managed deployments. */
final class MongoCollectionMetadata {
    private static final ThreadLocal<Attempt> CURRENT = new ThreadLocal<>();

    private MongoCollectionMetadata() {}

    /** Collection descriptions are shared only within one operator transaction attempt. */
    static Attempt operatorAttempt(MongoDatabase control, MongoDatabase target, SystemCollections payload) {
        return new Attempt(control, target, payload);
    }

    static final class Attempt implements AutoCloseable {
        private final Attempt previous = CURRENT.get();
        private final java.util.IdentityHashMap<MongoDatabase, List<Document>> descriptions = new java.util.IdentityHashMap<>();
        private final Set<String> names;

        private Attempt(MongoDatabase control, MongoDatabase target, SystemCollections payload) {
            var selected = new java.util.LinkedHashSet<String>();
            if (payload != null) {
                for (var table : List.of(SystemCollections.CACHE_PIPELINE_STATE, SystemCollections.CACHE_WORKLOAD_CLAIMS,
                        SystemCollections.CACHE_OPERATOR_CLEANUP, SystemCollections.CACHE_NEST_SHAPES, payload))
                    selected.addAll(table.physicalNames());
                descriptions.put(control, null);
                descriptions.put(target, null);
            }
            names = Set.copyOf(selected);
            CURRENT.set(this);
        }

        private List<Document> matching(MongoDatabase database, Set<String> required) {
            List<Document> rows = descriptions.get(database);
            if (rows == null) {
                rows = read(database, names, false);
                descriptions.put(database, rows);
            }
            return rows.stream().filter(row -> required.contains(row.getString("name"))).toList();
        }

        @Override public void close() {
            if (previous == null) CURRENT.remove(); else CURRENT.set(previous);
        }
    }

    static Document first(MongoDatabase database, String name) {
        Attempt attempt = CURRENT.get();
        if (attempt != null && attempt.descriptions.containsKey(database) && attempt.names.contains(name)) {
            List<Document> rows = attempt.matching(database, Set.of(name));
            return rows.isEmpty() ? null : rows.getFirst();
        }
        return database.listCollections().filter(new Document("name", name)).first();
    }

    static List<Document> matching(MongoDatabase database, Collection<String> names) {
        return matching(database, names, false);
    }

    static List<Document> matching(MongoDatabase database, Collection<String> names, boolean bounded) {
        Set<String> required = Set.copyOf(names);
        if (required.isEmpty()) return List.of();
        Attempt attempt = CURRENT.get();
        if (!bounded && attempt != null && attempt.descriptions.containsKey(database)
                && attempt.names.containsAll(required)) return attempt.matching(database, required);
        return read(database, required, bounded);
    }

    private static List<Document> read(MongoDatabase database, Set<String> required, boolean bounded) {
        var result = new ArrayList<Document>();
        // Retain only requested descriptions while consuming every cursor batch and preserving type/options.
        var collections = database.listCollections();
        if (bounded) collections.timeoutMode(TimeoutMode.CURSOR_LIFETIME);
        try (var cursor = collections.iterator()) {
            while (cursor.hasNext()) {
                Document row = cursor.next();
                if (row.get("name") instanceof String name && required.contains(name)) result.add(row);
            }
        }
        return result;
    }
}
