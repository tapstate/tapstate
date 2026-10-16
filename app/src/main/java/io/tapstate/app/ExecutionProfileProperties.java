package io.tapstate.app;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** Stable heap tier and its minimum; the actual configured JVM heap is included in admission. */
@ConfigurationProperties(prefix = "tapstate.cluster.execution-profile")
class ExecutionProfileProperties {
    private String heapTier = "default";
    private long minimumHeapBytes = 67_108_864;

    public String getHeapTier() { return heapTier; }
    public void setHeapTier(String value) { heapTier = value; }
    public long getMinimumHeapBytes() { return minimumHeapBytes; }
    public void setMinimumHeapBytes(long value) { minimumHeapBytes = value; }
}
