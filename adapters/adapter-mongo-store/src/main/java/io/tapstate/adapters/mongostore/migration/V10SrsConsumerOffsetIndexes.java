package io.tapstate.adapters.mongostore.migration;

import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.ChangeSet;
import io.tapstate.adapters.mongostore.ChangeSet.Fence;
import io.tapstate.adapters.mongostore.IndexEnsure;
import io.tapstate.adapters.mongostore.SystemCollections;

import java.util.ArrayList;
import java.util.List;

/**
 * The lookup indexes for the per-consumer SRS cursor collection. Stores that already ran the baseline
 * index changeset need this versioned step; new stores see the same declarations in both places, and the
 * shared index ensure makes the second application a no-op.
 */
public final class V10SrsConsumerOffsetIndexes implements ChangeSet {

    private static final SystemCollections ROW = SystemCollections.SRS_CONSUMER_OFFSETS;

    @Override
    public int version() {
        return 10;
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
