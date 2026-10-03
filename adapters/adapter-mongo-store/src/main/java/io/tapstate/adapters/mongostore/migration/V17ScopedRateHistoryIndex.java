package io.tapstate.adapters.mongostore.migration;

import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.ChangeSet;
import io.tapstate.adapters.mongostore.IndexEnsure;
import io.tapstate.adapters.mongostore.SystemCollections;

/** Adds a scoped range index without rewriting samples or removing the legacy read index. */
public final class V17ScopedRateHistoryIndex implements ChangeSet {

    private static final SystemCollections ROW = SystemCollections.PIPELINE_RATE_HISTORY;
    private static final SystemCollections.IndexSpec SCOPED_INDEX = ROW.indexes().get(2);

    @Override
    public int version() {
        return 17;
    }

    @Override
    public void up(MongoDatabase database, Fence fence) {
        fence.requireStillHeld();
        IndexEnsure.ensure(database, ROW.indexTargetOn(database), SCOPED_INDEX);
    }

    @Override
    public String dryRunSummary(MongoDatabase database) {
        return "builds " + ROW.indexTarget() + "." + SCOPED_INDEX.indexName()
                + (IndexEnsure.existing(ROW.indexTargetOn(database), SCOPED_INDEX) == null
                        ? " (to build)" : " (already built)");
    }
}
