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
import io.tapstate.core.model.canonical.CanonicalWriter;
import org.bson.Document;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Repairs blank pipeline documents written without their required empty source sequence. */
public final class V7RepairBlankPipelines implements ChangeSet {

    /** The only legacy shape this changeset can repair: a blank pipeline with no source or output. */
    private static final Document BLANK_PIPELINES = new Document("kind", "pipeline")
            .append("body.version", "tapstate/v1")
            .append("body.kind", "pipeline")
            .append("body.source", new Document("$exists", false))
            .append("body.transforms", new Document("$exists", false))
            .append("body.view", new Document("$exists", false))
            .append("body.serve", new Document("$exists", false));

    private static final CanonicalWriter WRITER = new CanonicalWriter();
    private static final DslParser PARSER = new DslParser();

    @Override
    public int version() {
        return 7;
    }

    @Override
    public void up(MongoDatabase database, Fence fence) {
        MongoCollection<Document> artifacts = SystemCollections.ARTIFACTS.on(database);
        Map<String, Document> repaired = new LinkedHashMap<>();
        try (MongoCursor<Document> cursor = artifacts.find(BLANK_PIPELINES).iterator()) {
            while (cursor.hasNext()) {
                Document document = cursor.next();
                String id = String.valueOf(document.get("_id"));
                Document body = new Document(document.get("body", Document.class)).append("source", List.of());
                Resource resource = PARSER.fromTree(body);
                repaired.put(id, new Document("body", new Document(WRITER.tree(resource)))
                        .append("contentHash", CanonicalHash.of(resource)));
            }
        }

        for (Map.Entry<String, Document> entry : repaired.entrySet()) {
            fence.requireStillHeld();
            Document filter = new Document(BLANK_PIPELINES).append("_id", entry.getKey());
            artifacts.updateOne(filter, new Document("$set", entry.getValue()));
        }
    }

    @Override
    public String dryRunSummary(MongoDatabase database) {
        long pending = SystemCollections.ARTIFACTS.on(database).countDocuments(BLANK_PIPELINES);
        return pending == 0
                ? "no blank pipeline artifact is missing its empty source sequence"
                : "repairs " + pending + " blank pipeline artifact(s) by adding source: [] and recomputing "
                        + "their content hashes";
    }
}
