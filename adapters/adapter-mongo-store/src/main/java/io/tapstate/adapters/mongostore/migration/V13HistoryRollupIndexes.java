package io.tapstate.adapters.mongostore.migration;

import io.tapstate.adapters.mongostore.DeclaredCollectionIndexes;
import io.tapstate.adapters.mongostore.SystemCollections;

/** Installs the TTL and scoped range indexes for the derived history cache. */
public final class V13HistoryRollupIndexes extends DeclaredCollectionIndexes {

    public V13HistoryRollupIndexes() {
        super(13, SystemCollections.PIPELINE_HISTORY_ROLLUPS);
    }
}
