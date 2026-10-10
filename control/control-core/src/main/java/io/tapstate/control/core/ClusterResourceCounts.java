package io.tapstate.control.core;

/** The six configured or computable member resource counts; unknown quantities have no numeric value. */
public record ClusterResourceCounts(long processors, long blockingProcessors, long writers,
        long connectorInstances, long bufferedRecords, long edgeQueueRecords) {}
