package io.tapstate.spi.store;

/** The coordinate system in which one source node confirms its recovery progress. */
public enum ConsumerProgressKind {
    /** Older progress without evidence that different table orders belong to one source stream. */
    LEGACY,
    /** Independent confirmed positions in the shared log's per-table sequence spaces. */
    SRS,
    /** One isolated database channel whose forwarding order covers every selected table. */
    DIRECT_SOURCE
}
