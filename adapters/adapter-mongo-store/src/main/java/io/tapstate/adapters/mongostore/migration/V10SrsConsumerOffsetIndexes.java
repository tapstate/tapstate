package io.tapstate.adapters.mongostore.migration;

import io.tapstate.adapters.mongostore.DeclaredCollectionIndexes;
import io.tapstate.adapters.mongostore.SystemCollections;

/**
 * The lookup indexes for the per-consumer SRS cursor collection. Stores that already ran the baseline
 * index changeset need this versioned step; new stores see the same declarations in both places, and the
 * shared index ensure makes the second application a no-op.
 */
public final class V10SrsConsumerOffsetIndexes extends DeclaredCollectionIndexes {

    public V10SrsConsumerOffsetIndexes() {
        super(10, SystemCollections.SRS_CONSUMER_OFFSETS);
    }
}
