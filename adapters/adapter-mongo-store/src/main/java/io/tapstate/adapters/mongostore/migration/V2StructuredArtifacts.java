package io.tapstate.adapters.mongostore.migration;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.ChangeSet;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.canonical.CanonicalWriter;
import org.bson.Document;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Moves stored artifacts from a canonical text body to a structured one, and re-takes every content
 * hash over the structure rather than over the text.
 *
 * <p>Both halves happen here because they are one change. The hash used to be the digest of the text
 * that is being removed; leaving it alone would leave every stored resource identified by a rendering
 * the store no longer keeps, which nothing could recompute and nothing could check.
 *
 * <p>The text is read for the last time here. This is the one place left that parses a stored body,
 * and it exists only because there are stores that were written before the structure was.
 */
public final class V2StructuredArtifacts implements ChangeSet {

    /** Documents still holding a text body. Selected by the shape they are in, never by a marker. */
    private static final Document STILL_TEXT = new Document("canonical", new Document("$exists", true));

    private static final CanonicalWriter WRITER = new CanonicalWriter();
    private static final DslParser PARSER = new DslParser();

    @Override
    public int version() {
        return 2;
    }

    @Override
    public void up(MongoDatabase database) {
        MongoCollection<Document> artifacts = SystemCollections.ARTIFACTS.on(database);

        // Read and convert everything before writing anything. A document that cannot be read must stop
        // the whole changeset rather than half of it: the version number covers the collection, not a
        // document, so a partly-moved collection that recorded no failure would be read as done. The
        // whole workspace is held in memory for the length of this — these are the resources someone
        // authored, so the count is a human one.
        Map<String, Document> converted = new LinkedHashMap<>();
        List<String> unreadable = new ArrayList<>();
        try (MongoCursor<Document> cursor = artifacts.find(STILL_TEXT).iterator()) {
            while (cursor.hasNext()) {
                Document document = cursor.next();
                String id = String.valueOf(document.get("_id"));
                try {
                    Resource resource = PARSER.parse(document.getString("canonical"));
                    converted.put(id, new Document("body", new Document(WRITER.tree(resource)))
                            .append("contentHash", CanonicalHash.of(resource)));
                } catch (RuntimeException unparsable) {
                    unreadable.add(id);
                }
            }
        }
        if (!unreadable.isEmpty()) {
            // Thrown bare: the runner turns whatever a changeset throws into the coded failure that names
            // the changeset. Every id is listed because an operator fixing them one restart at a time is
            // the difference between one outage and one per bad document.
            throw new IllegalStateException(
                    "cannot read the stored body of " + unreadable.size() + " artifact(s): " + unreadable);
        }

        converted.forEach((id, update) -> artifacts.updateOne(new Document("_id", id),
                new Document("$set", update).append("$unset", new Document("canonical", ""))));

        // The index on kind is declared on the artifacts row but arrives after the row does, so the
        // changeset that built the first indexes has already run wherever this one is needed.
        for (SystemCollections.IndexSpec index : SystemCollections.ARTIFACTS.indexes()) {
            V1BaselineIndexes.build(artifacts, index);
        }
    }

    @Override
    public String dryRunSummary(MongoDatabase database) {
        long pending = SystemCollections.ARTIFACTS.on(database).countDocuments(STILL_TEXT);
        return pending == 0
                ? "no artifact still holds a text body"
                : "rewrites " + pending + " artifact(s) from a text body to a structured one, and re-takes "
                        + "their content hashes -- every hash held by a client before the upgrade stops matching";
    }
}
