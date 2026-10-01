package io.tapstate.adapters.mongostore.migration;

import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.ChangeSet;
import io.tapstate.adapters.mongostore.IndexEnsure;
import io.tapstate.adapters.mongostore.SystemCollections;

import java.util.List;

/** Gates phased handoffs and private continuation envelopes without scanning existing payloads. */
public final class V16DurableRebuildHandoff implements ChangeSet {
    @Override public int version() { return 16; }
    @Override public void up(MongoDatabase database, Fence fence) {
        fence.requireStillHeld();
        IndexEnsure.ensure(database, SystemCollections.PIPELINE_OBSERVATION.indexTargetOn(database), privateLeaseIndex());
    }
    @Override public String dryRunSummary(MongoDatabase database) {
        return "recognizes phased handoffs and private continuation envelopes; builds " + privateLeaseIndex().indexName()
                + "; existing documents remain unchanged";
    }

    private static SystemCollections.IndexSpec privateLeaseIndex() {
        return SystemCollections.PIPELINE_OBSERVATION.indexes().stream()
                .filter(index -> index.keys().equals(List.of("continuationPending.publishUntil", "_id")))
                .findFirst().orElseThrow(() -> new IllegalStateException("private continuation lease index is not registered"));
    }
}
