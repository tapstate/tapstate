package io.tapstate.adapters.mongostore.migration;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.ChangeSet;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.adapters.mongostore.schemamigration.LegacySchemaDocuments;
import org.bson.Document;

import java.util.List;
import java.util.regex.Pattern;

/**
 * Splits inline discoveries into table documents using only the observation already stored.
 * No connector is opened: an unavailable source cannot prevent the stored model from being moved.
 * Missing resolved types remain missing and the reader attributes them as unknown.
 */
public final class V5SplitSourceSchemas implements ChangeSet {

    private static final String GENERATION = "migration-v5";
    private static final Document LEGACY = new Document("$or", List.of(
            new Document("tables", new Document("$exists", true)),
            new Document("_id", new Document("$not", Pattern.compile("\\.")))
                    .append("modelVersion", new Document("$ne", 2)),
            new Document("connectorId", new Document("$exists", true))
                    .append("generation", new Document("$exists", false))));

    @Override
    public int version() {
        return 5;
    }

    @Override
    public void up(MongoDatabase database, Fence fence) {
        MongoCollection<Document> sources = SystemCollections.SOURCE_SCHEMAS.on(database);
        try (MongoCursor<Document> cursor = sources.find(LEGACY).iterator()) {
            while (cursor.hasNext()) {
                Document legacy = cursor.next();
                String id = LegacySchemaDocuments.owner(legacy);
                Object version = legacy.get("modelVersion");
                if (version != null && !Integer.valueOf(1).equals(version)) {
                    throw LegacySchemaDocuments.unreadable(id, "modelVersion");
                }
                List<Document> tables = LegacySchemaDocuments.tables(legacy, id);
                for (int order = 0; order < tables.size(); order++) {
                    Document table = tables.get(order);
                    String key = id + "." + GENERATION + "." + order;
                    Document split = new Document(table).append("_id", key)
                            .append("generation", GENERATION).append("order", order);
                    // A deterministic key makes an interrupted copy resumable without orphaning a
                    // new generation on every restart. The envelope remains legacy until all land.
                    Document existing = sources.find(new Document("_id", key)).first();
                    if (existing == null) {
                        fence.requireStillHeld();
                        sources.insertOne(split);
                    } else if (!existing.equals(split)) {
                        // A retry can reuse an identical copy, but a contradictory observation must
                        // remain available for diagnosis rather than being silently overwritten.
                        throw LegacySchemaDocuments.unreadable(key, "tables");
                    }
                }
                Document envelope = new Document(legacy);
                envelope.remove("tables");
                envelope.append("modelVersion", 2).append("generation", GENERATION);
                fence.requireStillHeld();
                sources.replaceOne(new Document("_id", id), envelope);
            }
        }
    }

    @Override
    public String dryRunSummary(MongoDatabase database) {
        return SystemCollections.SOURCE_SCHEMAS.on(database).countDocuments(LEGACY)
                + " stored discovery envelope(s) to split without contacting their sources";
    }
}
