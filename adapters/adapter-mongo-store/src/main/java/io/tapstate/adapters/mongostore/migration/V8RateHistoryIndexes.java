package io.tapstate.adapters.mongostore.migration;

import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.ChangeSet;
import io.tapstate.adapters.mongostore.ChangeSet.Fence;
import io.tapstate.adapters.mongostore.IndexEnsure;
import io.tapstate.adapters.mongostore.SystemCollections;

import java.util.ArrayList;
import java.util.List;

/**
 * The indexes of the sample history, for a store migrated before that collection existed. The baseline
 * changeset builds every declared index, but only on a store it runs against; a store already past it
 * never sees a collection introduced later, so the collection's own changeset brings the indexes in.
 * Idempotent through the same ensure the baseline uses: created when absent, expiry altered when it
 * differs, left alone when it matches — so a store whose startup already wrote a configured retention
 * onto the index is not refused here for having done so.
 */
public final class V8RateHistoryIndexes implements ChangeSet {

    private static final SystemCollections ROW = SystemCollections.PIPELINE_RATE_HISTORY;

    @Override
    public int version() {
        return 8;
    }

    @Override
    public void up(MongoDatabase database, Fence fence) {
        for (SystemCollections.IndexSpec index : ROW.indexes()) {
            fence.requireStillHeld();
            IndexEnsure.ensure(database, ROW.indexTargetOn(database), index);
        }
    }

    @Override
    public String dryRunSummary(MongoDatabase database) {
        List<String> planned = new ArrayList<>();
        for (SystemCollections.IndexSpec index : ROW.indexes()) {
            planned.add(ROW.indexTarget() + "." + index.indexName()
                    + (IndexEnsure.existing(ROW.indexTargetOn(database), index) != null
                            ? " (already built)" : " (to build)"));
        }
        return "builds " + planned.size() + " index(es): " + String.join(", ", planned);
    }
}
