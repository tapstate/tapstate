package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.connection.ClusterType;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.bson.Document;

/** Bounded transport of facts from one CONFIG transaction attempt; validators remain at each caller. */
final class MongoBindingReadFacts {
    private static final String SINGLETON_ID = "cluster";
    private static final String BINDING_VERSION = "bindingVersion";
    private static final String ROOT_DOCUMENT = "$$ROOT";

    private static final int INLINE_BYTES = 64 * 1024;
    record Facts(Document identity, Document setup, Document global, boolean conflictingResource) {}

    private MongoBindingReadFacts() {}

    static boolean supported(MongoClient client) {
        var cluster = client.getClusterDescription();
        // A router may target sharded foreign collections, which cannot participate in this lookup.
        // Unknown peers and older servers retain the existing individual-query path.
        return cluster.getType() == ClusterType.REPLICA_SET && !cluster.getServerDescriptions().isEmpty()
                && cluster.getServerDescriptions().stream().allMatch(server -> server.getMaxWireVersion() >= 9);
    }

    static Facts read(ClientSession session, Document binding, MongoCollection<Document> identity,
            MongoCollection<Document> setup, MongoCollection<Document> notes,
            List<MongoCollection<Document>> otherResources) {
        var pipeline = new ArrayList<Document>();
        pipeline.add(new Document("$match", new Document("_id", SINGLETON_ID)));
        pipeline.add(captured());
        pipeline.add(lookup(setup, SINGLETON_ID, "setup", false));
        boolean bound = binding != null && binding.get("defaultSourceId") instanceof String
                && binding.get(BINDING_VERSION) instanceof Long;
        if (bound) {
            Object noteId = new Document(BINDING_VERSION, binding.get(BINDING_VERSION))
                    .append("namespace", "pdk.global-state");
            pipeline.add(lookup(notes, noteId, "global", false));
            for (int i = 0; i < otherResources.size(); i++)
                pipeline.add(lookup(otherResources.get(i), binding.get("defaultSourceId"), "conflict" + i, true));
        }
        Document row = identity.aggregate(session, pipeline).first();
        if (row == null) return new Facts(null, null, null, false);
        Document cluster = unwrap(session, row, identity, SINGLETON_ID).orElse(null);
        Document stage = unwrap(session, first(row, "setup"), setup, SINGLETON_ID).orElse(null);
        Document global = null;
        boolean conflict = false;
        if (bound) {
            global = unwrap(session, first(row, "global"), notes, new Document(BINDING_VERSION, binding.get(BINDING_VERSION))
                    .append("namespace", "pdk.global-state")).orElse(null);
            for (int i = 0; i < otherResources.size(); i++)
                conflict |= first(row, "conflict" + i) != null;
        }
        return new Facts(cluster, stage, global, conflict);
    }

    private static Document lookup(MongoCollection<Document> table, Object id, String field, boolean existenceOnly) {
        return new Document("$lookup", new Document("from", table.getNamespace().getCollectionName())
                .append("pipeline", List.of(new Document("$match", new Document("_id", id)),
                        existenceOnly ? new Document("$project", new Document("_id", 1)) : captured()))
                .append("as", field));
    }

    private static Document captured() {
        // An existing legal document may be close to the BSON limit. Return a marker, then read
        // that document separately in this same snapshot instead of enlarging the lookup result.
        return new Document("$project", new Document("_id", 0)
                .append("large", new Document("$gt", List.of(new Document("$bsonSize", ROOT_DOCUMENT), INLINE_BYTES)))
                .append("value", new Document("$cond", new Document("if",
                        new Document("$lte", List.of(new Document("$bsonSize", ROOT_DOCUMENT), INLINE_BYTES)))
                        .append("then", ROOT_DOCUMENT).append("else", null))));
    }

    private static Document first(Document row, String field) {
        var values = row.getList(field, Document.class);
        return values.isEmpty() ? null : values.getFirst();
    }

    private static Optional<Document> unwrap(ClientSession session, Document captured, MongoCollection<Document> table, Object id) {
        if (captured == null) return Optional.empty();
        return Optional.ofNullable(Boolean.TRUE.equals(captured.get("large"))
                ? table.find(session, new Document("_id", id)).first() : captured.get("value", Document.class));
    }
}
