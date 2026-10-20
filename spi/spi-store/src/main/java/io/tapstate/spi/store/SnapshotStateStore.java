package io.tapstate.spi.store;

import java.util.Map;
import java.util.OptionalLong;

/** Snapshot ordering and measured row counts are separate resources under one original Pipeline execution. */
public interface SnapshotStateStore {
    /** The explicitly prepared counter; missing history is never interpreted as zero. */
    long order();

    /** One allocation per original execution, after every bounded capture has been admitted. */
    long advanceOrder(long expectedOrder,long retainedFloor,Map<String,CaptureCacheState> originalCaptures);

    void recordCount(String source,String table,long snapshotEpoch,long rows);
    OptionalLong count(String source,String table,long snapshotEpoch);
}
