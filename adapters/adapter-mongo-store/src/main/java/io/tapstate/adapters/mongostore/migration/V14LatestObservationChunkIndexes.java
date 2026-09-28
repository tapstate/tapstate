package io.tapstate.adapters.mongostore.migration;

import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.ChangeSet;
import io.tapstate.adapters.mongostore.IndexEnsure;
import io.tapstate.adapters.mongostore.SystemCollections;

import java.util.ArrayList;
import java.util.List;

/** Installs the ordered-read and retired-chunk indexes for bounded latest observations. */
public final class V14LatestObservationChunkIndexes implements ChangeSet {

    @Override
    public int version() {
        return 14;
    }

    @Override
    public void up(MongoDatabase database, Fence fence) {
        for (SystemCollections collection : List.of(
                SystemCollections.ARTIFACTS, SystemCollections.PIPELINE_OBSERVATION,
                SystemCollections.PIPELINE_OBSERVATION_CHUNKS)) {
            for (SystemCollections.IndexSpec index : collection.indexes()) {
                fence.requireStillHeld();
                IndexEnsure.ensure(database, collection.indexTargetOn(database), index);
            }
        }
    }

    @Override
    public String dryRunSummary(MongoDatabase database) {
        List<String> planned = new ArrayList<>();
        for (SystemCollections collection : List.of(
                SystemCollections.ARTIFACTS, SystemCollections.PIPELINE_OBSERVATION,
                SystemCollections.PIPELINE_OBSERVATION_CHUNKS)) {
            for (SystemCollections.IndexSpec index : collection.indexes()) {
                planned.add(collection.indexTarget() + "." + index.indexName()
                        + (IndexEnsure.existing(collection.indexTargetOn(database), index) != null
                                ? " (already built)" : " (to build)"));
            }
        }
        return "builds " + planned.size() + " index(es): " + String.join(", ", planned);
    }
}
