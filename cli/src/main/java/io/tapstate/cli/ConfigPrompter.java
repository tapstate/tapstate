package io.tapstate.cli;

import io.tapstate.core.catalog.ConfigField;
import io.tapstate.core.catalog.ConfigType;
import io.tapstate.core.catalog.EnumOption;
import io.tapstate.core.catalog.VisibleWhen;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The connector config Q&A: walks a connector's config fields and asks for each visible one through
 * the prompter. It mirrors exactly what {@code validate} checks offline (type coercion, enum choices)
 * and nothing more — required is a server concern, so a blank reply simply omits the field. A field
 * gated by {@code visibleWhen} is asked only when its controlling field's answer matches.
 */
final class ConfigPrompter {

    /** The enum choice meaning "leave this field unset". */
    private static final String SKIP = "(skip)";

    Map<String, Object> collect(List<ConfigField> fields, Prompter prompter) {
        Map<String, Object> config = new LinkedHashMap<>();
        for (ConfigField field : fields) {
            if (!visible(field, config)) {
                continue;
            }
            Object value = askField(field, prompter);
            if (value != null) {
                config.put(field.name(), value);
            }
        }
        return config;
    }

    /**
     * The essentials-only walk the guided first run takes: the required fields and the secret ones are
     * answered, every other field keeps its catalog default and is not asked. An answer comes from
     * {@code given} first (the flag form), then from the prompter, and where a field has a default an
     * empty reply - or no prompter - takes it, because that is what the bracketed default promises.
     * A field that can be answered by neither is omitted, as in {@link #collect}.
     *
     * <p>Visibility is read against the defaults too: a gate on an optional field that was never asked
     * (the deployment mode a connector's host sits behind, say) is judged by that field's default, or
     * a required field behind it could never be asked at all. Keys in {@code given} that are not
     * essential fields are carried through as given, so the flag form can say more than it is asked.
     *
     * @param prompter what asks, or null to never ask
     */
    Map<String, Object> collectEssential(List<ConfigField> fields, Map<String, String> given, Prompter prompter) {
        Map<String, Object> config = new LinkedHashMap<>();
        Map<String, String> unread = new LinkedHashMap<>(given);
        for (ConfigField field : fields) {
            if (!(field.required() || field.secret()) || !visibleByDefault(field, fields, given, config)) {
                continue;
            }
            String raw = unread.remove(field.name());
            if (raw == null && prompter != null) {
                raw = field.secret()
                        ? prompter.secret(label(field))
                        : prompter.ask(label(field), field.defaultValue());
            }
            if (raw == null || raw.isBlank()) {
                raw = field.defaultValue();
            }
            if (raw != null && !raw.isBlank()) {
                config.put(field.name(), coerce(field.type(), raw.trim()));
            }
        }
        for (Map.Entry<String, String> extra : unread.entrySet()) {
            ConfigField field = fields.stream().filter(f -> f.name().equals(extra.getKey())).findFirst().orElse(null);
            config.put(extra.getKey(), field == null ? extra.getValue() : coerce(field.type(), extra.getValue()));
        }
        return config;
    }

    private Object askField(ConfigField field, Prompter prompter) {
        if (!field.options().isEmpty()) {
            List<String> options = new ArrayList<>(field.options().stream().map(EnumOption::value).toList());
            options.add(SKIP);
            String chosen = prompter.choose(label(field), options);
            return SKIP.equals(chosen) ? null : coerce(field.type(), chosen);
        }
        String raw = field.secret()
                ? prompter.secret(label(field))
                : prompter.ask(label(field), field.defaultValue());
        if (raw == null || raw.isBlank()) {
            return null;
        }
        return coerce(field.type(), raw.trim());
    }

    private boolean visible(ConfigField field, Map<String, Object> answered) {
        VisibleWhen vw = field.visibleWhen();
        if (vw == null) {
            return true;
        }
        Object controller = answered.get(vw.controllingField());
        return controller != null && vw.equalsAnyOf().contains(String.valueOf(controller));
    }

    /** As {@link #visible}, with an unanswered controller read as its own catalog default. */
    private boolean visibleByDefault(ConfigField field, List<ConfigField> fields, Map<String, String> given,
                                     Map<String, Object> answered) {
        VisibleWhen vw = field.visibleWhen();
        if (vw == null) {
            return true;
        }
        Object controller = given.get(vw.controllingField());
        if (controller == null) {
            controller = answered.get(vw.controllingField());
        }
        if (controller == null) {
            controller = fields.stream()
                    .filter(f -> f.name().equals(vw.controllingField()))
                    .map(ConfigField::defaultValue)
                    .filter(Objects::nonNull)
                    .findFirst()
                    .orElse(null);
        }
        return controller != null && vw.equalsAnyOf().contains(String.valueOf(controller));
    }

    /**
     * Coerces to the declared scalar type so the artifact renders typed ({@code port: 1521}, not
     * {@code "1521"}). A value that does not parse is kept verbatim and left for validate to flag.
     */
    private static Object coerce(ConfigType type, String raw) {
        switch (type) {
            case NUMBER:
                try {
                    return Integer.valueOf(raw);
                } catch (NumberFormatException notInt) {
                    try {
                        return Double.valueOf(raw);
                    } catch (NumberFormatException notNumber) {
                        return raw;
                    }
                }
            case BOOLEAN:
                if (raw.equalsIgnoreCase("true")) {
                    return Boolean.TRUE;
                }
                if (raw.equalsIgnoreCase("false")) {
                    return Boolean.FALSE;
                }
                return raw;
            default:
                return raw;
        }
    }

    private static String label(ConfigField field) {
        String en = field.label().get("en_US");
        return en != null ? en : field.name();
    }
}
