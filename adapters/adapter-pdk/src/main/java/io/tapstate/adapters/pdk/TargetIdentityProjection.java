package io.tapstate.adapters.pdk;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Envelope;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/** Applies an assessed target projection to both images while preserving converted value carriers. */
public final class TargetIdentityProjection {
    private TargetIdentityProjection() { }

    public static Envelope apply(TargetIdentityPlan plan, Envelope event) {
        requireUsable(plan);
        if (!plan.automatic()) return event;
        Set<String> removed = new LinkedHashSet<>();
        for (String field : event.removed()) {
            removed.add(field.equals(plan.sourceField()) ? plan.preservedField() : field);
        }
        return new Envelope(event.op(), event.ts(), event.src(), row(plan, event.before()),
                row(plan, event.after()), event.schema()).withRemoved(removed);
    }

    public static Map<String, Object> row(TargetIdentityPlan plan, Map<String, Object> image) {
        requireUsable(plan);
        if (!plan.automatic() || image == null) return image;
        if (image.containsKey(plan.preservedField())) {
            throw failure(plan, "field-collision");
        }
        if (!image.containsKey(plan.sourceField())) {
            throw failure(plan, "source-identity-missing");
        }
        Map<String, Object> projected = new LinkedHashMap<>();
        image.forEach((field, value) -> projected.put(
                field.equals(plan.sourceField()) ? plan.preservedField() : field, value));
        return projected;
    }

    public static void requireUsable(TargetIdentityPlan plan) {
        if (plan.blocked()) throw failure(plan, plan.reason());
    }

    public static TapstateException failure(TargetIdentityPlan plan, String reason) {
        return new TapstateException(ConnectorError.TARGET_IDENTITY_CONFLICT,
                Map.of("table", plan.target().name(), "field", value(plan.sourceField()),
                        "preserved", value(plan.preservedField()), "reason", reason), null);
    }

    private static String value(String value) { return value == null ? "" : value; }
}
