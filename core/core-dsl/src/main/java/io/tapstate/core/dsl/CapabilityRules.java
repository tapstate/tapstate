package io.tapstate.core.dsl;

import io.tapstate.core.catalog.ConfigField;
import io.tapstate.core.catalog.ConfigType;
import io.tapstate.core.catalog.ConnectorCatalogEntry;
import io.tapstate.core.catalog.EnumOption;
import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.SourceResource;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Connector capability-matrix validation (plan poc1 C3) — the offline tier that consults the
 * connector catalog (structural + in-batch reference + capability matrix over
 * bundled ∪ cache). The catalog is the build-time projection of each connector's own spec, so the
 * connector's legal modes and config schema are looked up per connector — nothing here hard-codes
 * connector knowledge.
 *
 * <p>Scope is the offline projection:
 * <ul>
 *   <li>a connector absent from the catalog is skipped — its registration is authoritative only on
 *       the server, so offline cannot reject an unknown connector;</li>
 *   <li>mode × connector legality: a source's {@code mode}, when present, must be one of the
 *       connector's declared modes (a write-target supplier has no {@code mode} and is skipped);</li>
 *   <li>config field type / enum: each <em>provided</em> config value is checked against the
 *       connector's declared field. Unknown keys are tolerated (the normalized spec may drop
 *       fields, and {@code ${ENV}} externalization is opaque offline); required-field presence is a
 *       server-side connection concern, not enforced here.</li>
 * </ul>
 */
public final class CapabilityRules {

    private CapabilityRules() {
    }

    static void validate(Collection<Resource> batch, TapstateCatalog catalog) {
        for (Resource r : batch) {
            if (r instanceof SourceResource s) {
                checkSource(s, catalog, false);
            }
        }
    }

    /** Validates one online Source against the live catalog, including active required fields. */
    public static void validateOnline(SourceResource source, TapstateCatalog catalog) {
        checkSource(source, catalog, true);
    }

    private static void checkSource(SourceResource s, TapstateCatalog catalog, boolean requireRequired) {
        if (!catalog.ids().contains(s.connector())) {
            return;   // not in the offline catalog → connector legality is a server-side check
        }
        ConnectorCatalogEntry entry = catalog.byId(s.connector());
        checkMode(s, entry);
        checkConfig(s, entry, requireRequired);
    }

    private static void checkMode(SourceResource s, ConnectorCatalogEntry entry) {
        if (s.mode() == null) {
            return;   // a pure connection supplier (write target) has no read mode to check
        }
        if (entry.modes().isEmpty() || !entry.modesAreTrustworthy()) {
            return;   // catalog has no reliable mode signal for this connector → defer to server
        }
        if (!entry.modes().contains(s.mode())) {
            throw unsupportedMode(s.connector(), s.mode(), entry.modes());
        }
    }

    private static void checkConfig(SourceResource s, ConnectorCatalogEntry entry, boolean requireRequired) {
        Map<String, ConfigField> fields = new LinkedHashMap<>();
        for (ConfigField f : entry.config()) {
            fields.put(f.name(), f);
        }
        for (Map.Entry<String, Object> e : s.config().entrySet()) {
            ConfigField field = fields.get(e.getKey());
            if (field == null || isInterpolated(e.getValue())) {
                continue;   // unknown key (tolerated) or ${ENV} (opaque offline) — no value to judge
            }
            if (!matchesType(field.type(), e.getValue())) {
                throw configTypeMismatch(s.connector(), field);
            }
            if (!field.options().isEmpty()) {
                Object offender = firstNonOption(field, e.getValue());
                if (offender != null) {
                    throw invalidConfigValue(s.connector(), field, offender);
                }
            }
        }
        if (requireRequired) {
            for (ConfigField field : fields.values()) {
                if (field.required() && isVisible(field, fields, s.config())
                        && !hasValue(s.config(), field.name())
                        && field.defaultValue() == null) {
                    throw configRequired(s.connector(), field);
                }
            }
        }
    }

    private static boolean isVisible(ConfigField field, Map<String, ConfigField> fields,
                                     Map<String, Object> config) {
        if (field.visibleWhen() == null) {
            return true;
        }
        String controlling = field.visibleWhen().controllingField();
        Object value = config.get(controlling);
        if (value == null) {
            ConfigField controller = fields.get(controlling);
            value = controller == null ? null : controller.defaultValue();
        }
        if (isInterpolated(value)) {
            return false;
        }
        return value != null && field.visibleWhen().equalsAnyOf().contains(String.valueOf(value));
    }

    private static boolean hasValue(Map<String, Object> config, String field) {
        Object value = config.get(field);
        return value != null && (!(value instanceof String text) || !text.isBlank());
    }

    /**
     * The first value not among the field's declared options, or {@code null} if all are legal. A
     * multi-select array enum is judged element by element (the value is a list of choices, not one
     * choice), so its offending member is named rather than the whole list. Interpolated members are
     * skipped, like scalar {@code ${ENV}} values.
     */
    private static Object firstNonOption(ConfigField field, Object value) {
        if (value instanceof List<?> list) {
            for (Object element : list) {
                if (!isInterpolated(element) && !isOption(field, element)) {
                    return element;
                }
            }
            return null;
        }
        return isOption(field, value) ? null : value;
    }

    private static boolean isOption(ConfigField field, Object value) {
        String v = String.valueOf(value);
        return field.options().stream().anyMatch(o -> o.value().equals(v));
    }

    private static boolean matchesType(ConfigType type, Object value) {
        if (value == null) {
            return false;
        }
        return switch (type) {
            case ARRAY -> value instanceof List;
            case BOOLEAN -> value instanceof Boolean
                    || (value instanceof String s && (s.equals("true") || s.equals("false")));
            case NUMBER -> value instanceof Number || (value instanceof String s && isNumeric(s));
            case STRING -> !(value instanceof List) && !(value instanceof Map);
        };
    }

    private static boolean isNumeric(String s) {
        try {
            Double.parseDouble(s);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private static boolean isInterpolated(Object value) {
        return value instanceof String s && s.contains("${");
    }

    private static DslException unsupportedMode(String connector, SourceMode mode, List<SourceMode> allowed) {
        return new DslException(DslError.UNSUPPORTED_MODE, "mode", 0, 0, null,
                Map.of("connector", connector, "mode", mode.yaml(), "allowed", join(allowed)));
    }

    private static DslException configTypeMismatch(String connector, ConfigField field) {
        return new DslException(DslError.CONFIG_TYPE_MISMATCH, "config." + field.name(), 0, 0, null,
                Map.of("connector", connector, "field", field.name(), "expected", field.type().yaml()));
    }

    private static DslException configRequired(String connector, ConfigField field) {
        return new DslException(DslError.CONFIG_REQUIRED, "config." + field.name(), 0, 0, null,
                Map.of("connector", connector, "field", field.name()));
    }

    private static DslException invalidConfigValue(String connector, ConfigField field, Object value) {
        return new DslException(DslError.INVALID_CONFIG_VALUE, "config." + field.name(), 0, 0, null,
                Map.of("connector", connector, "field", field.name(),
                        "value", String.valueOf(value), "allowed", joinOptions(field)));
    }

    private static String joinOptions(ConfigField field) {
        return field.options().stream().map(EnumOption::value).collect(Collectors.joining(", "));
    }

    private static String join(List<SourceMode> modes) {
        return modes.stream().map(SourceMode::yaml).collect(Collectors.joining(", "));
    }
}
