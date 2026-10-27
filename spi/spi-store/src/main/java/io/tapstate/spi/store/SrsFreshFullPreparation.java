package io.tapstate.spi.store;

import java.time.Instant;
import java.util.Map;

/** One store-owned frozen inspection. Publication cannot adopt later membership or root revisions. */
public interface SrsFreshFullPreparation {
    SrsChainState originalChain();
    SrsFreshFullReceipt publish(long snapshotEpoch, String sourcePosition,
                               Map<String, SourceTable> actualTables, Instant preparedAt);
}
