package io.tapstate.control.core;

/** One materialized collection and the pipeline responsible for keeping it current. */
public record ViewCatalogItem(
        String id, String kind, Location location, String pipelineId, Freshness status,
        Long documents, Long sizeBytes, Long lastAppliedAt) {

    public record Location(String storeId, String database, String collection) {
    }

    /** Event age is measured at target ACK and is independent of observation availability. */
    public record Freshness(String state, Long behindSeconds, Long staleSeconds, Integer percent,
            Long eventAgeSeconds, java.time.Instant observedAt, java.time.Instant refreshedAt,
            Long observationAgeSeconds, String unit, String aggregation, String reason) {
        public Freshness(String state,Long behindSeconds,Long staleSeconds,Integer percent) {
            this(state,behindSeconds,staleSeconds,percent,behindSeconds,null,null,staleSeconds,"s","max-input-table-event-age",null);
        }
    }
}
