package io.tapstate.app;

import io.tapstate.core.lifecycle.ClusterCapacityLimits;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** Fixed per-member aggregate ceilings, shared by normal submission and recovery admission. */
@ConfigurationProperties(prefix = "tapstate.execution.cluster-capacity")
class ClusterCapacityProperties {
    private long processors = 1024;
    private long blockingProcessors = 128;
    private long writers = 64;
    private long connectorInstances = 64;
    private long bufferedRecords = 16_777_216;
    private long edgeQueueRecords = 16_777_216;

    public long getProcessors() { return processors; }
    public void setProcessors(long value) { processors = value; }
    public long getBlockingProcessors() { return blockingProcessors; }
    public void setBlockingProcessors(long value) { blockingProcessors = value; }
    public long getWriters() { return writers; }
    public void setWriters(long value) { writers = value; }
    public long getConnectorInstances() { return connectorInstances; }
    public void setConnectorInstances(long value) { connectorInstances = value; }
    public long getBufferedRecords() { return bufferedRecords; }
    public void setBufferedRecords(long value) { bufferedRecords = value; }
    public long getEdgeQueueRecords() { return edgeQueueRecords; }
    public void setEdgeQueueRecords(long value) { edgeQueueRecords = value; }

    ClusterCapacityLimits limits() {
        return new ClusterCapacityLimits(processors, blockingProcessors, writers, connectorInstances,
                bufferedRecords, edgeQueueRecords);
    }
}
