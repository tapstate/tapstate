package io.tapstate.adapters.otel;

import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.metrics.data.Data;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.MetricDataType;
import io.opentelemetry.sdk.resources.Resource;

import java.util.Objects;

/**
 * One metric as the SDK's readers consume it, built from the facts rather than aggregated by the SDK.
 * The SDK's public data interfaces are implemented here directly, so the projection stands on the
 * contract the exporters are written against and on nothing the SDK marks as its own internals.
 */
final class FactMetricData implements MetricData {

    /** Every metric this process exports is scoped to the product, not to a library inside it. */
    static final InstrumentationScopeInfo SCOPE = InstrumentationScopeInfo.create("io.tapstate");

    private final Resource resource;
    private final String name;
    private final String unit;
    private final MetricDataType type;
    private final Data<?> data;

    FactMetricData(Resource resource, String name, String unit, MetricDataType type, Data<?> data) {
        this.resource = Objects.requireNonNull(resource, "resource");
        this.name = Objects.requireNonNull(name, "name");
        this.unit = unit == null ? "" : unit;
        this.type = Objects.requireNonNull(type, "type");
        this.data = Objects.requireNonNull(data, "data");
    }

    @Override
    public Resource getResource() {
        return resource;
    }

    @Override
    public InstrumentationScopeInfo getInstrumentationScopeInfo() {
        return SCOPE;
    }

    @Override
    public String getName() {
        return name;
    }

    /** The facts carry no prose; the name and the unit are the description. */
    @Override
    public String getDescription() {
        return "";
    }

    @Override
    public String getUnit() {
        return unit;
    }

    @Override
    public MetricDataType getType() {
        return type;
    }

    @Override
    public Data<?> getData() {
        return data;
    }

    @Override
    public String toString() {
        return "FactMetricData[" + name + " " + type + " unit=" + unit + " points=" + data.getPoints().size() + "]";
    }
}
