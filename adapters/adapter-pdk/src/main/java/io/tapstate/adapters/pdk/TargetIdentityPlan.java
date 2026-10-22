package io.tapstate.adapters.pdk;

import io.tapstate.spi.sink.TargetTable;
import java.util.List;

/** One target's assessed projection; routing continues to use the input model's logical key. */
public record TargetIdentityPlan(String status, String reason, TargetTable target,
        String sourceField, String preservedField, List<String> writeKey) {
    public static final String NOT_REQUIRED = "not-required";
    public static final String AUTOMATIC = "automatic";
    public static final String ACTION_REQUIRED = "action-required";

    public TargetIdentityPlan {
        writeKey = List.copyOf(writeKey);
    }

    public boolean automatic() { return AUTOMATIC.equals(status); }
    public boolean blocked() { return ACTION_REQUIRED.equals(status); }
}
