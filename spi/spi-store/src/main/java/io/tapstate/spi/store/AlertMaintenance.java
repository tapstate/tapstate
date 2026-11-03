package io.tapstate.spi.store;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/** Process-local cursors and measured diagnostics; no additional persistence or delivery authority. */
public interface AlertMaintenance {
    record Cursor(int collection, Instant candidateAt, String id) {
        public Cursor { if (collection < 0 || collection > 11 || id != null && id.length() > 8192) throw new IllegalArgumentException("invalid maintenance cursor"); }
    }
    record Batch(Cursor next, int scanned, int changed, long bytes, Map<String,Long> skipped, Instant oldestCandidate, Instant oldestProtected) {
        public Batch(Cursor next,int scanned,int changed,long bytes,Map<String,Long> skipped,Instant oldestCandidate){this(next,scanned,changed,bytes,skipped,oldestCandidate,null);}
        public Batch { skipped = Map.copyOf(skipped); }
    }
    record Usage(String resource, long rows, long bytes, long reservedBytes, long protectedBytes, Instant oldestCandidate) { }
    record Inventory(Instant measuredAt, List<Usage> resources, long pending, Instant oldestPending, long failed, long exhausted) {
        public Inventory { resources = List.copyOf(resources); }
        public long logicalBytes() { return resources.stream().mapToLong(r -> r.bytes() + r.reservedBytes()).sum(); }
    }
    /** Enables fail-closed admission while the first or a fresh capacity sample is unavailable. */
    default void enableCapacityProtection() { }
    default Map<String,Long> diagnostics() { return Map.of(); }
    /** At most 200 reads and 4 MiB including dependent reads/writes; conflicts and retries consume the same budget. */
    Batch maintain(Cursor after, int maxRows, long maxBytes, java.time.Duration maxTime);
    Inventory inventory();
}
