package io.tapstate.adapters.mongostore.migration;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import io.tapstate.adapters.mongostore.ChangeSet;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.adapters.mongostore.migration.schema.LegacySchemaDocuments;
import org.bson.Document;

import java.util.List;
import java.util.regex.Pattern;

/**
 * Moves complete pipeline histories into their step documents before any store consumer starts.
 * The former write-time move now runs under the startup lock, with a fence before each write.
 * Re-derivation cannot recover the older versions that drift compares against, so none are dropped.
 */
public final class V6SplitDerivedSchemas implements ChangeSet {

    private static final Document LEGACY = new Document("$or", List.of(
            new Document("steps", new Document("$exists", true)),
            new Document("_id", new Document("$not", Pattern.compile("\\.")))));

    @Override
    public int version() {
        return 6;
    }

    @Override
    public void up(MongoDatabase database, Fence fence) {
        MongoCollection<Document> histories = SystemCollections.DERIVED_SCHEMAS.on(database);
        try (MongoCursor<Document> cursor = histories.find(LEGACY).iterator()) {
            while (cursor.hasNext()) {
                Document legacy = cursor.next();
                String id = LegacySchemaDocuments.owner(legacy);
                List<Document> steps = LegacySchemaDocuments.steps(legacy, id);
                for (Document step : steps) {
                    String key = id + "." + step.getString("step");
                    List<Document> versions = LegacySchemaDocuments.versions(step.get("versions"), key);
                    Document existing = histories.find(new Document("_id", key)).first();
                    if (existing != null && existing.containsKey("versions")) {
                        // An interrupted older move may already have copied and advanced this step.
                        // Verify that it carries every old version before leaving that history intact.
                        LegacySchemaDocuments.requirePreservedHistory(versions, existing, key);
                    } else {
                        fence.requireStillHeld();
                        histories.updateOne(new Document("_id", key),
                                new Document("$set", new Document("versions", versions)),
                                new UpdateOptions().upsert(true));
                    }
                }
                // A crash before this leaves the authoritative old record available for another
                // pass; a crash after it leaves every version in the already-validated split records.
                fence.requireStillHeld();
                histories.deleteOne(new Document("_id", id));
            }
        }
    }

    @Override
    public String dryRunSummary(MongoDatabase database) {
        return SystemCollections.DERIVED_SCHEMAS.on(database).countDocuments(LEGACY)
                + " pipeline history document(s) to split while preserving every version";
    }
}
