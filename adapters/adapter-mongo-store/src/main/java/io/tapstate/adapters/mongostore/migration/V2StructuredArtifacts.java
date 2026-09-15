package io.tapstate.adapters.mongostore.migration;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.ChangeSet;
import io.tapstate.adapters.mongostore.ChangeSet.Fence;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.dsl.DslException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.canonical.CanonicalWriter;
import org.bson.Document;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

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
 *
 * <p>It is read as the release that wrote it wrote it, not as this build would accept it. The two are
 * not the same grammar: a field that release emitted and this one retired appears in every stored
 * document that used it, and reading it with this build's parser would refuse a document that was
 * valid when it was written -- with no way out, since the upgrade is the only reader of that text and
 * a refusal here stops the server from starting at all.
 */
public final class V2StructuredArtifacts implements ChangeSet {

    /** Documents still holding a text body. Selected by the shape they are in, never by a marker. */
    private static final Document STILL_TEXT = new Document("canonical", new Document("$exists", true));

    private static final CanonicalWriter WRITER = new CanonicalWriter();
    private static final DslParser PARSER = new DslParser();

    /**
     * What this build retired, dropped from the stored text before it is read.
     *
     * <p>Every release up to the one this upgrades from accepted {@code options} and emitted it back:
     * a store written by such a release holds canonical text carrying it. This build defines no engine
     * option at all, so there is nothing in the structure being written for the value to become, and
     * dropping it is the whole of what carrying it forward could mean.
     *
     * <p>The list is pinned to this changeset on purpose. It says what one upgrade left behind, which
     * stops being true the moment a later build defines an option of its own -- and a later build's
     * stored text never reaches this changeset, because the store records that it has run.
     */
    private static final Set<String> RETIRED_KEYS = Set.of("options");

    @Override
    public int version() {
        return 2;
    }

    @Override
    public void up(MongoDatabase database, Fence fence) {
        MongoCollection<Document> artifacts = SystemCollections.ARTIFACTS.on(database);

        // Read and convert everything before writing anything. A document that cannot be read must stop
        // the whole changeset rather than half of it: the version number covers the collection, not a
        // document, so a partly-moved collection that recorded no failure would be read as done. The
        // whole workspace is held in memory for the length of this — these are the resources someone
        // authored, so the count is a human one.
        Map<String, Document> converted = new LinkedHashMap<>();
        Map<String, RuntimeException> unreadable = new LinkedHashMap<>();
        try (MongoCursor<Document> cursor = artifacts.find(STILL_TEXT).iterator()) {
            while (cursor.hasNext()) {
                Document document = cursor.next();
                String id = String.valueOf(document.get("_id"));
                try {
                    Resource resource = PARSER.parseDropping(document.getString("canonical"), RETIRED_KEYS);
                    converted.put(id, new Document("body", new Document(WRITER.tree(resource)))
                            .append("contentHash", CanonicalHash.of(resource)));
                } catch (RuntimeException unparsable) {
                    unreadable.put(id, unparsable);
                }
            }
        }
        if (!unreadable.isEmpty()) {
            // Thrown bare: the runner turns whatever a changeset throws into the coded failure that names
            // the changeset. Every id is listed because an operator fixing them one restart at a time is
            // the difference between one outage and one per bad document -- and each is listed with what
            // stopped it, because "could not be read" alone does not separate a corrupt document from one
            // this build newly requires a field of, and that is the distinction deciding what to do next.
            // The first failure travels as the cause so its own position and stack survive the bare throw.
            throw new IllegalStateException(
                    "cannot read the stored body of " + unreadable.size() + " artifact(s): "
                            + unreadable.entrySet().stream().map(V2StructuredArtifacts::describe).toList(),
                    unreadable.values().iterator().next());
        }

        for (Map.Entry<String, Document> entry : converted.entrySet()) {
            fence.requireStillHeld();
            artifacts.updateOne(new Document("_id", entry.getKey()),
                    new Document("$set", entry.getValue()).append("$unset", new Document("canonical", "")));
        }

        // The index on kind is declared on the artifacts row but arrives after the row does, so the
        // changeset that built the first indexes has already run wherever this one is needed.
        for (SystemCollections.IndexSpec index : SystemCollections.ARTIFACTS.indexes()) {
            fence.requireStillHeld();
            V1BaselineIndexes.build(artifacts, index);
        }
    }

    /** One unreadable artifact as {@code id: code at path}, or its own words when it carries no code. */
    private static String describe(Map.Entry<String, RuntimeException> failure) {
        if (!(failure.getValue() instanceof DslException dsl)) {
            return failure.getKey() + ": " + failure.getValue();
        }
        String path = dsl.path() == null || dsl.path().isEmpty() ? "the document root" : dsl.path();
        return failure.getKey() + ": " + dsl.code().code() + " at " + path;
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
