package io.tapstate.control.core;

/** One materialized collection and the pipeline responsible for keeping it current. */
public record ViewCatalogItem(
        String id, String kind, Location location, String pipelineId, Freshness status,
        Long documents, Long sizeBytes, Long lastAppliedAt) {

    public record Location(String storeId, String database, String collection) {
    }

    public record Freshness(String state, Long behindSeconds, Long staleSeconds, Integer percent) {
    }
}
