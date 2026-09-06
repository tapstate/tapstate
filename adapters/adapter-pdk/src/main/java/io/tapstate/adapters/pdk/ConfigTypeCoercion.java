package io.tapstate.adapters.pdk;

import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.TapstateException;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Converts a connection's config values to the types the connector's own connection form declares,
 * so a value spelled as text reaches the connector as the number or boolean its config bean holds.
 *
 * <p>The values arrive as text because that is what the workspace wrote: a connection whose settings
 * came from a form, a flag or a generated document spells {@code port} as {@code "3306"}, while the
 * connector's config bean holds a {@code Number} and casts to it. Without this the cast throws out of
 * the connector's own code, so the operator is handed a {@code ClassCastException} between two JDK
 * types instead of a diagnosis. The boolean shape is worse: it survives the connection test and fails
 * only at the first write, long after the pipeline reported itself running.
 *
 * <p><b>The declaration coerced from is the connector's own</b> — the connection form it ships inside
 * its spec resource ({@code configOptions.connection.properties}), reached through the raw spec text
 * the ref carries. Two facts on a form item declare a non-text type, and both are the connector's:
 *
 * <ul>
 *   <li>the schema {@code type}, when it says {@code boolean}, or {@code number} / {@code integer} /
 *       {@code int};</li>
 *   <li>the {@code InputNumber} component the item renders with. Numeric fields are overwhelmingly
 *       typed {@code string} in shipped forms and declare their numeric nature only through this
 *       component — {@code port} is exactly that shape — so reading {@code type} alone would find no
 *       numeric field at all on most connectors.</li>
 * </ul>
 *
 * <p>Only a text value of a field declared non-text is converted; anything else is handed on exactly as
 * it arrived — a value already of the declared type, a field the form does not declare, a connector
 * carrying no spec. A blank one is handed on too: an empty setting is an absent value rather than a
 * misspelt number, and refusing it would turn an omitted optional field into a hard failure.
 *
 * <p>Runs on the host with the host's json reader over the raw spec text, never under the connector's
 * loader, and reads only the form the connector shipped — no reflection, and no table of field names
 * of our own.
 */
final class ConfigTypeCoercion {

    /** The form component a connector renders a field with when it reads that field's value as a number. */
    private static final String NUMBER_COMPONENT = "InputNumber";

    /**
     * The spellings of a number this accepts: an optionally signed decimal with an optional fraction and
     * exponent. Deliberately narrower than {@code Double.parseDouble}, which also takes a hexadecimal
     * float, a trailing type suffix and the non-finite words — none of which a connection setting means.
     */
    private static final Pattern NUMBER =
            Pattern.compile("[+-]?(\\d+(\\.\\d*)?|\\.\\d+)([eE][+-]?\\d+)?");

    /** A non-text type a connection form can declare for one of its fields. */
    private enum Declared {
        NUMBER("number"),
        BOOLEAN("boolean");

        private final String label;

        Declared(String label) {
            this.label = label;
        }

        /** The type as the diagnostic names it to the operator. */
        String label() {
            return label;
        }
    }

    private ConfigTypeCoercion() {
    }

    /**
     * The config map to hand the connector: {@code settings} with each text value of a field the
     * connector declares non-text converted to that type. Returns {@code settings} itself when there is
     * nothing to do — no settings, no spec, or a spec declaring no non-text field.
     *
     * @throws TapstateException {@code connector.config-type-mismatch} when a value cannot be converted
     *         to the type its field declares
     */
    static Map<String, Object> coerce(String connectorId, String spec, Map<String, Object> settings) {
        if (settings == null || settings.isEmpty() || spec == null) {
            return settings;
        }
        Map<String, Declared> declared = declaredTypes(spec);
        if (declared.isEmpty()) {
            return settings;
        }
        Map<String, Object> coerced = new LinkedHashMap<>(settings);
        for (Map.Entry<String, Object> entry : settings.entrySet()) {
            Declared type = declared.get(entry.getKey());
            if (type == null || !(entry.getValue() instanceof String text) || text.isBlank()) {
                continue;
            }
            coerced.put(entry.getKey(), convert(connectorId, entry.getKey(), type, text));
        }
        return coerced;
    }

    private static Object convert(String connectorId, String field, Declared type, String text) {
        String value = text.trim();
        Object converted = switch (type) {
            case NUMBER -> number(value);
            case BOOLEAN -> bool(value);
        };
        if (converted == null) {
            throw new TapstateException(ConnectorError.CONFIG_TYPE_MISMATCH,
                    Map.of("connector", connectorId, "field", field, "expected", type.label(), "value", text),
                    null);
        }
        return converted;
    }

    /**
     * The number {@code value} spells, or null when it spells none. A whole value that fits becomes a
     * {@code Long} so an integral setting stays integral; everything else becomes a {@code Double}. A
     * connector reads either through {@code Number}, which is what its config bean casts to.
     */
    private static Number number(String value) {
        if (!NUMBER.matcher(value).matches()) {
            return null;
        }
        try {
            return Long.valueOf(value);
        } catch (NumberFormatException fractionalOrTooLarge) {
            return Double.valueOf(value);
        }
    }

    /** The boolean {@code value} spells, or null when it spells none. */
    private static Boolean bool(String value) {
        if ("true".equalsIgnoreCase(value)) {
            return Boolean.TRUE;
        }
        return "false".equalsIgnoreCase(value) ? Boolean.FALSE : null;
    }

    /**
     * Each field the connector's connection form declares as a number or a boolean, keyed by the name
     * the connector reads it under. Container nodes are flattened: a form groups optional fields under a
     * node of its own, while the connector still reads them by their leaf name.
     */
    private static Map<String, Declared> declaredTypes(String spec) {
        Map<String, Declared> declared = new LinkedHashMap<>();
        if (JsonReader.parse(spec) instanceof Map<?, ?> root
                && root.get("configOptions") instanceof Map<?, ?> configOptions
                && configOptions.get("connection") instanceof Map<?, ?> connection
                && connection.get("properties") instanceof Map<?, ?> properties) {
            collect(properties, declared);
        }
        return declared;
    }

    private static void collect(Map<?, ?> properties, Map<String, Declared> out) {
        for (Map.Entry<?, ?> entry : properties.entrySet()) {
            if (!(entry.getKey() instanceof String name) || !(entry.getValue() instanceof Map<?, ?> item)) {
                continue; // a non-object property value is a malformed form item — skip it, don't guess
            }
            if (item.get("properties") instanceof Map<?, ?> nested) {
                collect(nested, out);
                continue;
            }
            Declared declared = declaredType(item);
            if (declared != null) {
                out.put(name, declared);
            }
        }
    }

    /** The non-text type this form item declares, or null when it declares text or nothing. */
    private static Declared declaredType(Map<?, ?> item) {
        if (item.get("type") instanceof String type) {
            // Shipped forms are inconsistent about casing and use a few synonyms ("Boolean", "int").
            Declared byType = switch (type.toLowerCase(Locale.ROOT)) {
                case "boolean" -> Declared.BOOLEAN;
                case "number", "integer", "int" -> Declared.NUMBER;
                default -> null;
            };
            if (byType != null) {
                return byType;
            }
        }
        return NUMBER_COMPONENT.equals(item.get("x-component")) ? Declared.NUMBER : null;
    }
}
