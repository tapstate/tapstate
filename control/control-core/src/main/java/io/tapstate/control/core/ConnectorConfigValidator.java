package io.tapstate.control.core;

import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.CapabilityRules;
import io.tapstate.core.model.SourceResource;

import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/** Applies the same live connector config rules to Source and connection operations. */
public final class ConnectorConfigValidator {

    private final Supplier<TapstateCatalog> catalog;

    public ConnectorConfigValidator(Supplier<TapstateCatalog> catalog) {
        this.catalog = Objects.requireNonNull(catalog, "catalog");
    }

    /** Validates connector-specific settings without persisting or mutating them. */
    public void validate(String connector, Map<String, Object> settings) {
        Objects.requireNonNull(connector, "connector");
        TapstateCatalog live = Objects.requireNonNull(catalog.get(), "catalog.get()");
        if (!live.ids().contains(connector)) {
            throw new TapstateException(
                    ControlError.MALFORMED_REQUEST,
                    Map.of("reason", "unknown connector: " + connector),
                    null);
        }
        SourceResource candidate = new SourceResource(
                "__config_validation__", null, connector, settings, null, null, null, null);
        CapabilityRules.validateOnline(candidate, live);
    }
}
