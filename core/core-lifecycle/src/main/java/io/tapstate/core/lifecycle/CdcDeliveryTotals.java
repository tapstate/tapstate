package io.tapstate.core.lifecycle;

/** Pipeline-wide totals of confirmed CDC output records with measurable event timestamps. */
public record CdcDeliveryTotals(long count, long totalMillis) {

    public static final String COUNT_METRIC = "tapstate.pipeline.cdc.delivery.count";
    public static final String TOTAL_MILLIS_METRIC = "tapstate.pipeline.cdc.delivery.total_millis";
    public static final CdcDeliveryTotals ZERO = new CdcDeliveryTotals(0, 0);
}
