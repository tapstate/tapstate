package io.tapstate.adapters.mongostore.migration;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.ChangeSet;
import io.tapstate.adapters.mongostore.SystemCollections;
import org.bson.Document;

import java.util.Map;
import java.util.regex.Pattern;

/**
 * Discards the resume positions a released build wrote without ever obtaining one.
 *
 * <p>Up to and including the last release, nothing in the product asked a connector where a read had
 * reached. The wiring module supplied both halves itself: one fixed string standing in for the seam a
 * snapshot samples at its source, and a counter standing in for the watermark a tail advances. Both
 * were stored as though a source had issued them, so a store written by such a build holds positions
 * that name no place in any change stream.
 *
 * <p>This build reads a stored position back through the connector's own codec, which refuses one it
 * cannot decode rather than beginning at the present -- carrying on there would drop every change since
 * the position was recorded, with nothing thrown and nothing logged. Against an invented string that
 * refusal is correct and terminal: the pipeline enters a failed state on every convergence and there is
 * no operator action that clears it, because the value was never wrong to begin with and re-applying
 * the pipeline does not touch it.
 *
 * <p>So the invented strings are erased here, and only those. They are matched by the two shapes that
 * build could produce, never by "this build cannot decode it": a real position this build fails to read
 * is a different fact with a different remedy, and clearing it would re-mine every change since it.
 * Erasing is the whole of the fix -- a chain with no recorded position is the state a chain nothing has
 * read is already in, and the run resolves its own start exactly as it does there.
 *
 * <p>Each position is erased together with the order stored beside it. A token without its order can no
 * longer be ranked against anything and an order without its token is nothing to resume from; the
 * store's own writer moves the two together for that reason, and half an erase would leave behind a
 * record no later comparison can use.
 */
public final class V4DiscardInventedPositions implements ChangeSet {

    /** The seam the released build wrote in place of one sampled at the source. */
    private static final String INVENTED_SEAM = "cdc-start-0";

    /** The counter it advanced in place of a watermark a connector issued: w1, w2, and so on. */
    private static final Pattern INVENTED_WATERMARK = Pattern.compile("w\\d+");

    @Override
    public int version() {
        return 4;
    }

    @Override
    public void up(MongoDatabase database) {
        MongoCollection<Document> chains = SystemCollections.SRS_META.on(database);
        try (MongoCursor<Document> cursor = chains.find().iterator()) {
            while (cursor.hasNext()) {
                Document chain = cursor.next();
                Document unset = invented(chain);
                if (!unset.isEmpty()) {
                    chains.updateOne(new Document("_id", chain.get("_id")), new Document("$unset", unset));
                }
            }
        }
    }

    @Override
    public String dryRunSummary(MongoDatabase database) {
        long chains = 0;
        try (MongoCursor<Document> cursor = SystemCollections.SRS_META.on(database).find().iterator()) {
            while (cursor.hasNext()) {
                if (!invented(cursor.next()).isEmpty()) {
                    chains++;
                }
            }
        }
        return chains + " chain(s) hold a position no source issued";
    }

    /**
     * The fields to remove from one chain record: each invented position, and the order beside it.
     * Empty when the record holds nothing this build put there, which is what makes running this twice
     * the same as running it once -- the second pass selects on the same shape and matches nothing.
     */
    private static Document invented(Document chain) {
        Document unset = new Document();
        if (wasInvented(chain.get("cdcStartPosition"))) {
            unset.append("cdcStartPosition", "").append("snapshotEpoch", "");
        }
        if (wasInvented(chain.get("sourceReadOffset"))) {
            unset.append("sourceReadOffset", "")
                    .append("sourceReadEpoch", "")
                    .append("sourceReadSeq", "")
                    .append("sourceReadAt", "");
        }
        if (chain.get("consumerOffsets") instanceof Document consumers) {
            for (Map.Entry<String, Object> entry : consumers.entrySet()) {
                if (entry.getValue() instanceof Document consumer
                        && wasInvented(consumer.get("sinkAckedSrcpos"))) {
                    // A resource id carries no dot, so each path addresses exactly one consumer.
                    String path = "consumerOffsets." + entry.getKey() + ".";
                    unset.append(path + "sinkAckedSrcpos", "")
                            .append(path + "sinkAckedEpoch", "")
                            .append(path + "sinkAckedSeq", "");
                }
            }
        }
        return unset;
    }

    /** Whether a stored token is one of the two the released build made up rather than obtained. */
    private static boolean wasInvented(Object token) {
        return token instanceof String text
                && (INVENTED_SEAM.equals(text) || INVENTED_WATERMARK.matcher(text).matches());
    }
}
