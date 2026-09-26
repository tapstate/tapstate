package io.tapstate.adapters.mongostore.migration;

import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.ChangeSet;
import io.tapstate.adapters.mongostore.IndexEnsure;
import io.tapstate.adapters.mongostore.SystemCollections;

/** Creates the age and scoped keyset indexes for bounded pipeline events. */
public final class V12PipelineEventIndexes implements ChangeSet {

    private static final SystemCollections ROW = SystemCollections.PIPELINE_EVENTS;

    @Override
    public int version() {
        return 12;
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
        return "builds " + ROW.indexTarget() + " bounded event indexes";
    }
}
