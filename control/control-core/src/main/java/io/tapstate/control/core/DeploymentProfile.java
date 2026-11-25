package io.tapstate.control.core;

import java.util.Locale;

/** Product deployment mode; OP is the safe default for packaged and local installations. */
public enum DeploymentProfile {
    CLOUD,
    ON_PREM;

    public static DeploymentProfile parse(String value) {
        if (value == null || value.isBlank()) return ON_PREM;
        return switch (value.trim().toLowerCase(Locale.ROOT)) {
            case "cloud" -> CLOUD;
            case "on-prem", "on_prem", "onprem" -> ON_PREM;
            default -> throw new IllegalArgumentException("unsupported deployment profile: " + value);
        };
    }
}
