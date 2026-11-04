package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.connection.ClusterType;
import java.util.ArrayList;
import java.util.List;
import org.bson.Document;

/** Bounded facts from one transaction snapshot; ambiguous predicates retain collection-local reads. */
final class MongoBindingReadFacts {
    private static final String SINGLETON_ID = "cluster";
    private static final String BINDING_VERSION = "bindingVersion";
    private static final String ROOT_DOCUMENT = "$$ROOT";
    private static final String LARGE = "large";
    private static final String VALUE = "value";
    private static final int INLINE_BYTES = 64 * 1024;

    record Facts(Document identity, Document setup, Document global, boolean conflictingResource) {}

    private MongoBindingReadFacts() {}

    static boolean supported(MongoClient client) {
        var cluster = client.getClusterDescription();
        // Routers and unknown or older peers retain the original individual-query path.
        return cluster.getType() == ClusterType.REPLICA_SET && !cluster.getServerDescriptions().isEmpty()
                && cluster.getServerDescriptions().stream().allMatch(server -> server.getMaxWireVersion() >= 9);
    }

    static Facts read(ClientSession session, Document binding, MongoCollection<Document> identity,
            MongoCollection<Document> setup, MongoCollection<Document> notes,
            List<MongoCollection<Document>> resources) {
        var pipeline = new ArrayList<Document>();
        pipeline.add(new Document("$match", new Document("_id", SINGLETON_ID)));
        pipeline.add(new Document("$limit", 1));
        pipeline.add(captured());
        pipeline.add(probe(setup, "setup", 2));
        boolean bound = binding != null && binding.get("defaultSourceId") instanceof String
                && binding.get(BINDING_VERSION) instanceof Long;
        if (bound) {
            pipeline.add(probe(notes, "global", 2));
            for (int i = 0; i < resources.size(); i++) pipeline.add(probe(resources.get(i), "resource" + i, 1));
        }
        Document row = identity.aggregate(session, pipeline).first();
        if (row == null) return new Facts(null, null, null, false);
        Document cluster = unpack(session, row, identity, SINGLETON_ID);
        Document stage = singleton(session, row.getList("setup", Document.class), setup, SINGLETON_ID);
        Document global = null;
        boolean conflict = false;
        if (bound) {
            Object noteId = new Document(BINDING_VERSION, binding.get(BINDING_VERSION))
                    .append("namespace", "pdk.global-state");
            global = singleton(session, row.getList("global", Document.class), notes, noteId);
            for (int i = 0; i < resources.size(); i++) {
                // Only emptiness is independent of the aggregate's collation. A populated
                // namespace must evaluate its ID predicate under its own historical collation.
                if (!row.getList("resource" + i, Document.class).isEmpty()
                        && resources.get(i).find(session, new Document("_id", binding.get("defaultSourceId"))).first() != null)
                    conflict = true;
            }
        }
        return new Facts(cluster, stage, global, conflict);
    }

    private static Document probe(MongoCollection<Document> table, String field, int limit) {
        // No foreign predicate uses the root collection's collation. Limit BEFORE projection
        // bounds both examined documents and result size, including populated namespaces.
        return new Document("$lookup", new Document("from", table.getNamespace().getCollectionName())
                .append("pipeline", List.of(new Document("$limit", limit), captured())).append("as", field));
    }

    private static Document captured() {
        // Legal documents near the BSON limit are read separately in the same snapshot.
        return new Document("$project", new Document("_id", 0)
                .append(LARGE, new Document("$gt", List.of(new Document("$bsonSize", ROOT_DOCUMENT), INLINE_BYTES)))
                .append(VALUE, new Document("$cond", new Document("if",
                        new Document("$lte", List.of(new Document("$bsonSize", ROOT_DOCUMENT), INLINE_BYTES)))
                        .append("then", ROOT_DOCUMENT).append("else", null))));
    }

    private static Document singleton(ClientSession session, List<Document> rows,
            MongoCollection<Document> table, Object id) {
        if (rows.isEmpty()) return null;
        if (rows.size() == 1 && !Boolean.TRUE.equals(rows.getFirst().get(LARGE))) {
            Document value = rows.getFirst().get(VALUE, Document.class);
            if (exactId(id, value.get("_id"))) return value;
        }
        return table.find(session, new Document("_id", id)).first();
    }

    private static boolean exactId(Object expected, Object actual) {
        // BSON embedded-document equality includes field order. Document.equals does not.
        if (expected instanceof Document key)
            return actual instanceof Document value && new ArrayList<>(key.keySet()).equals(new ArrayList<>(value.keySet()))
                    && key.equals(value);
        return expected.equals(actual);
    }

    private static Document unpack(ClientSession session, Document row, MongoCollection<Document> table, Object id) {
        return Boolean.TRUE.equals(row.get(LARGE))
                ? table.find(session, new Document("_id", id)).first() : row.get(VALUE, Document.class);
    }
}
