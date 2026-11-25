package io.tapstate.adapters.pdk;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.ConvertedValue;
import java.math.BigInteger;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/** Applies an assessed target projection to both images while preserving converted value carriers. */
public final class TargetIdentityProjection {
    private static final String UNSUPPORTED_KEY = "key-representation-unsupported";

    private TargetIdentityProjection() { }

    public static Envelope apply(TargetIdentityPlan plan, Envelope event) {
        requireUsable(plan);
        if (!plan.isAutomatic()) return event;
        if (event.before() == null && event.after() == null
                || event.op() == io.tapstate.core.event.Op.DELETE && event.before() == null
                || event.op() != io.tapstate.core.event.Op.DELETE && event.after() == null) {
            throw failure(plan, UNSUPPORTED_KEY);
        }
        Set<String> removed = new LinkedHashSet<>();
        for (String field : event.removed()) {
            removed.add(field.equals(plan.sourceField()) ? plan.preservedField() : field);
        }
        Map<String, Object> before = row(plan, event.before());
        Map<String, Object> after = row(plan, event.after());
        if (plan.requireDocumentKeys() && event.op() == io.tapstate.core.event.Op.UPDATE && before != null && after != null
                && !MongoIdentityKeys.key(plan, before).equals(MongoIdentityKeys.key(plan, after))) {
            // Expansion emits delete/insert for key changes. A type-only change hidden by logical
            // equality would leave an update whose old native predicate cannot match on replay.
            throw failure(plan, UNSUPPORTED_KEY);
        }
        return new Envelope(event.op(), event.ts(), event.src(), before, after, event.schema()).withRemoved(removed);
    }

    public static Map<String, Object> row(TargetIdentityPlan plan, Map<String, Object> image) {
        requireUsable(plan);
        if (!plan.isAutomatic() || image == null) return image;
        if (image.containsKey(plan.preservedField())) {
            throw failure(plan, "field-collision");
        }
        if (!image.containsKey(plan.sourceField())) {
            throw failure(plan, "source-identity-missing");
        }
        Map<String, Object> projected = new LinkedHashMap<>();
        image.forEach((field, value) -> projected.put(
                field.equals(plan.sourceField()) ? plan.preservedField() : field, value));
        // Discovery is sampled; reject runtime drift before a connector can coerce distinct keys.
        if (plan.requireSignedInt64Keys() && plan.writeKey().stream()
                .anyMatch(field -> !signedInt64(ConvertedValue.unwrap(projected.get(field))))) {
            throw failure(plan, UNSUPPORTED_KEY);
        }
        if (plan.requireDocumentKeys()) MongoIdentityKeys.key(plan, projected);
        return projected;
    }

    private static boolean signedInt64(Object value) {
        return value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long
                || value instanceof BigInteger integer && integer.bitLength() <= 63;
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
