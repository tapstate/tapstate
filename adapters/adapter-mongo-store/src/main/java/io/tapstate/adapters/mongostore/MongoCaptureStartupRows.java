package io.tapstate.adapters.mongostore;

import com.mongodb.ClientSessionOptions;
import com.mongodb.ReadPreference;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.bson.Document;

/** Bounded exact-key facts captured by an ordinary shared startup transaction. */
final class MongoCaptureStartupRows {
    private MongoCaptureStartupRows() {}

    static boolean supportsReadSnapshot(MongoClient client, MongoDatabase database) {
        return ReadPreference.primary().equals(database.getReadPreference())
                && !MongoValidationClaimObservation.lacksSnapshotRead(client);
    }

    static <T> T readSnapshot(MongoClient client, Runnable active, Function<ClientSession, T> action) {
        return MongoStructureFailureScope.inspectOnFailure(client, () -> StoreIo.call(() -> {
            active.run();
            try (var session = client.startSession(ClientSessionOptions.builder().snapshot(true).build())) {
                MongoStructureFailureScope.beginCallback();
                return action.apply(session);
            } finally {
                active.run();
            }
        }));
    }

    static Document lookup(SystemCollections table, String slot, Object id) {
        return lookup(table, slot, id, List.of());
    }

    static Document lookup(SystemCollections table, String slot, Object id, List<Document> modifiers) {
        var bytes = new Document("$bsonSize", "$$ROOT");
        var projection = new Document("_id", 1).append("large", new Document("$gt", List.of(bytes, 64 * 1024)))
                .append("value", new Document("$cond", List.of(new Document("$lte", List.of(bytes, 64 * 1024)), "$$ROOT", false)));
        var stages = new ArrayList<>(List.of(new Document("$match", new Document("_id", id)), new Document("$limit", 2)));
        stages.addAll(modifiers);
        stages.add(new Document("$project", projection));
        return new Document("$lookup", new Document("from", table.collectionName()).append("as", slot)
                .append("pipeline", stages));
    }

    static boolean ambiguous(Document row, String slot, Object id) {
        if (row == null) return false;
        var rows = row.getList(slot, Document.class);
        return rows.size() > 1 || !rows.isEmpty() && (Boolean.TRUE.equals(rows.getFirst().get("large"))
                || !exact(id, rows.getFirst().get("_id")));
    }

    static Document get(Document row, String slot) {
        if (row == null) return null;
        var rows = row.getList(slot, Document.class);
        return rows.isEmpty() ? null : rows.getFirst().get("value", Document.class);
    }

    private static boolean exact(Object left, Object right) {
        if (left instanceof Document expected)
            return right instanceof Document actual && new ArrayList<>(expected.keySet()).equals(new ArrayList<>(actual.keySet()))
                    && expected.equals(actual);
        return left.equals(right);
    }
}
