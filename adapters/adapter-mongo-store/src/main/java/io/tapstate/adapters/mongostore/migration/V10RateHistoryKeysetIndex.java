package io.tapstate.adapters.mongostore.migration;

import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.ChangeSet;
import io.tapstate.adapters.mongostore.IndexEnsure;
import io.tapstate.adapters.mongostore.SystemCollections;

import java.util.List;

/** Adds the generated document id to the rate-history range index for stable keyset pages. */
public final class V10RateHistoryKeysetIndex implements ChangeSet {

    private static final SystemCollections ROW = SystemCollections.PIPELINE_RATE_HISTORY;
    private static final SystemCollections.IndexSpec KEYSET_INDEX = ROW.indexes().get(1);
    private static final SystemCollections.IndexSpec OLD_RANGE_INDEX =
            new SystemCollections.IndexSpec(List.of("pipelineId", "observedAt"), false);

    @Override
    public int version() {
        return 10;
    }

    @Override
    public void up(MongoDatabase database, Fence fence) {
        fence.requireStillHeld();
        IndexEnsure.ensure(database, ROW.indexTargetOn(database), KEYSET_INDEX);
        fence.requireStillHeld();
        if (IndexEnsure.existing(ROW.indexTargetOn(database), OLD_RANGE_INDEX) != null) {
            ROW.indexTargetOn(database).dropIndex(OLD_RANGE_INDEX.indexName());
        }
    }

    @Override
    public String dryRunSummary(MongoDatabase database) {
        return "builds " + ROW.indexTarget() + "." + KEYSET_INDEX.indexName()
                + (IndexEnsure.existing(ROW.indexTargetOn(database), KEYSET_INDEX) == null
                        ? " (to build)" : " (already built)")
                + (IndexEnsure.existing(ROW.indexTargetOn(database), OLD_RANGE_INDEX) == null
                        ? "" : "; drops " + OLD_RANGE_INDEX.indexName());
    }
}
