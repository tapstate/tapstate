package io.tapstate.cli;

import io.tapstate.core.catalog.ConfigField;
import io.tapstate.core.catalog.ConfigType;
import io.tapstate.core.catalog.EnumOption;
import io.tapstate.core.catalog.VisibleWhen;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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

    /**
     * Coerces to the declared scalar type so the artifact renders typed ({@code port: 1521}, not
     * {@code "1521"}). A value that does not parse is kept verbatim and left for validate to flag.
     */
    private static Object coerce(ConfigType type, String raw) {
        return SourceScaffold.coerce(type, raw);
    }

    private static String label(ConfigField field) {
        String en = field.label().get("en_US");
        return en != null ? en : field.name();
    }
}
