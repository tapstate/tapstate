package io.tapstate.adapters.mongostore.migration;

import io.tapstate.adapters.mongostore.DeclaredCollectionIndexes;
import io.tapstate.adapters.mongostore.SystemCollections;

/**
 * The indexes of the sample history, for a store migrated before that collection existed. The baseline
 * changeset builds every declared index, but only on a store it runs against; a store already past it
 * never sees a collection introduced later, so the collection's own changeset brings the indexes in.
 * Idempotent through the same ensure the baseline uses: created when absent, expiry altered when it
 * differs, left alone when it matches — so a store whose startup already wrote a configured retention
 * onto the index is not refused here for having done so.
 */
public final class V9RateHistoryIndexes extends DeclaredCollectionIndexes {

    public V9RateHistoryIndexes() {
        super(9, SystemCollections.PIPELINE_RATE_HISTORY);
    }
}
