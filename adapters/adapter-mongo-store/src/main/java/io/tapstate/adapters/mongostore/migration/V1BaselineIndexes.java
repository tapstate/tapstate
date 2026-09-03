package io.tapstate.adapters.mongostore.migration;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import io.tapstate.adapters.mongostore.ChangeSet;
import io.tapstate.adapters.mongostore.SystemCollections;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

/**
 * The indexes the product's own queries need, built for the first time.
 *
 * <p>There were none before this. Every collection had its {@code _id} index and nothing else, which
 * was survivable only while the collections were small: the session lookup below runs on every request
 * that carries a credential, and without an index each of those is a scan of every session that has
 * ever been issued. That degrades with use rather than failing, which is why nothing reported it.
 *
 * <p>Which indexes exist is not decided here. Each one is declared beside the collection it belongs
 * to, next to the query shape that needs it, and this walks that list — so a later collection that
 * declares an index gets it from the changeset that introduces the collection, and nobody has to keep
 * two lists agreeing.
 *
 * <p>Re-runnable, as every changeset must be: building an index that already exists with the same keys
 * and options is what the driver treats as nothing to do.
 */
public final class V1BaselineIndexes implements ChangeSet {

    /** How many colliding values to name when a unique index cannot be built. Enough to act on. */
    private static final int DUPLICATE_SAMPLE_LIMIT = 5;

    @Override
    public int version() {
        return 1;
    }

    @Override
    public void up(MongoDatabase database) {
        for (SystemCollections row : SystemCollections.values()) {
            // Only the store database is reached from here. The operator-state collections are in
            // another one and outside this scheme entirely; they declare no index, so this skips
            // nothing today, and it says which database this changeset is holding rather than
            // depending on that staying true.
            if (row.database() != SystemCollections.Database.STORE) {
                continue;
            }
            for (SystemCollections.IndexSpec index : row.indexes()) {
                build(row.indexTargetOn(database), index);
            }
        }
    }

    @Override
    public String dryRunSummary(MongoDatabase database) {
        List<String> planned = new ArrayList<>();
        for (SystemCollections row : SystemCollections.values()) {
            if (row.database() != SystemCollections.Database.STORE) {
                continue;
            }
            for (SystemCollections.IndexSpec index : row.indexes()) {
                planned.add(row.indexTarget() + "." + index.indexName()
                        + (exists(row.indexTargetOn(database), index) ? " (already built)" : " (to build)"));
            }
        }
        return "builds " + planned.size() + " index(es): " + String.join(", ", planned);
    }

    private static boolean exists(MongoCollection<Document> collection, SystemCollections.IndexSpec index) {
        for (Document existing : collection.listIndexes()) {
            if (index.indexName().equals(existing.getString("name"))) {
                return true;
            }
        }
        return false;
    }

    /**
     * Builds one index. A unique one is checked against the data first: the driver's own failure for a
     * collection that already holds duplicates names one of them and stops, which leaves an operator to
     * find the rest by hand, one restart at a time. Package-visible so that check can be put in front
     * of real duplicated data — no collection declares a unique index yet, so the production path
     * through it is the one a future declaration takes, and a guard nothing can put into its failing
     * state is a guard nobody knows still works.
     */
    static void build(MongoCollection<Document> collection, SystemCollections.IndexSpec index) {
        if (index.unique()) {
            refuseOnDuplicates(collection, index);
        }
        Document keys = new Document();
        for (String key : index.keys()) {
            keys.append(key, 1);
        }
        collection.createIndex(keys, new IndexOptions().name(index.indexName()).unique(index.unique()));
    }

    /** Reports the values that would collide, rather than letting the index build report one and stop. */
    private static void refuseOnDuplicates(MongoCollection<Document> collection, SystemCollections.IndexSpec index) {
        Document groupKey = new Document();
        for (String key : index.keys()) {
            groupKey.append(key.replace('.', '_'), "$" + key);
        }
        List<Document> colliding = new ArrayList<>();
        collection.aggregate(List.of(
                        Aggregates.group(groupKey, Accumulators.sum("count", 1)),
                        Aggregates.match(Filters.gt("count", 1)),
                        Aggregates.limit(DUPLICATE_SAMPLE_LIMIT)))
                .into(colliding);
        if (!colliding.isEmpty()) {
            // Thrown bare: the runner turns whatever a changeset throws into the coded failure that
            // names the changeset, so raising a coded one here would only decide which of two codes
            // an operator sees for the same event.
            // No count is claimed: the sample is taken with a limit, so the number collected is the
            // limit long before it is the number of duplicates, and reporting it as one would tell an
            // operator there are five of something there may be thousands of.
            throw new IllegalStateException("cannot build unique index " + index.indexName() + " on "
                    + collection.getNamespace().getCollectionName()
                    + ": duplicated values, up to " + DUPLICATE_SAMPLE_LIMIT + " of them shown: "
                    + colliding.stream().map(document -> String.valueOf(document.get("_id"))).toList());
        }
    }
}
