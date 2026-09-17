package io.tapstate.adapters.mongostore.migration;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.ChangeSet;
import io.tapstate.adapters.mongostore.ChangeSet.Fence;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.canonical.CanonicalHash;
import org.bson.Document;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Removes view schema policies written by the released canonical writer. */
public final class V8DiscardViewSchemaPolicies implements ChangeSet {

    private static final String INLINE_SCHEMA = "body.view.schema";
    private static final String REUSABLE_SCHEMA = "body.schema";
    private static final Document EXISTS = new Document("$exists", true);
    private static final Document RETIRED_VIEW_SCHEMAS = new Document("$or", List.of(
            new Document("kind", "pipeline")
                    .append("body.kind", "pipeline")
                    .append(INLINE_SCHEMA, EXISTS),
            new Document("kind", "view")
                    .append("body.kind", "view")
                    .append(REUSABLE_SCHEMA, EXISTS)));

    private static final DslParser PARSER = new DslParser();

    @Override
    public int version() {
        return 8;
    }

    @Override
    public void up(MongoDatabase database, Fence fence) {
        MongoCollection<Document> artifacts = SystemCollections.ARTIFACTS.on(database);
        Map<String, CarriedArtifact> carried = new LinkedHashMap<>();

        // Validate every resulting body before changing any of them. Retiring this one field must not
        // turn another malformed or unknown field into a partially migrated collection.
        try (MongoCursor<Document> cursor = artifacts.find(RETIRED_VIEW_SCHEMAS).iterator()) {
            while (cursor.hasNext()) {
                Document artifact = cursor.next();
                String id = String.valueOf(artifact.get("_id"));
                String schemaPath = "view".equals(artifact.getString("kind"))
                        ? REUSABLE_SCHEMA : INLINE_SCHEMA;
                Document body = withoutSchemaPolicy(artifact, schemaPath);
                Resource resource = PARSER.fromTree(body);
                carried.put(id, new CarriedArtifact(schemaPath, body, CanonicalHash.of(resource)));
            }
        }

        for (Map.Entry<String, CarriedArtifact> entry : carried.entrySet()) {
            fence.requireStillHeld();
            CarriedArtifact artifact = entry.getValue();
            Document stillCarriesSchema = new Document("_id", entry.getKey())
                    .append(artifact.schemaPath(), EXISTS);
            artifacts.updateOne(stillCarriesSchema, new Document("$set", new Document("body", artifact.body())
                    .append("contentHash", artifact.contentHash())));
        }
    }

    private static Document withoutSchemaPolicy(Document artifact, String schemaPath) {
        Document body = new Document(artifact.get("body", Document.class));
        if (REUSABLE_SCHEMA.equals(schemaPath)) {
            body.remove("schema");
            return body;
        }
        Document view = new Document(body.get("view", Document.class));
        view.remove("schema");
        body.put("view", view);
        return body;
    }

    @Override
    public String dryRunSummary(MongoDatabase database) {
        long pending = SystemCollections.ARTIFACTS.on(database).countDocuments(RETIRED_VIEW_SCHEMAS);
        return pending == 0
                ? "no stored view artifact carries a retired schema policy"
                : "removes the retired schema policy from " + pending
                        + " stored view artifact(s) and recomputes their content hashes";
    }

    private record CarriedArtifact(String schemaPath, Document body, String contentHash) {
    }
}
