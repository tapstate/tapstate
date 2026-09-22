package io.tapstate.core.catalog;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Normalizes a connector's already-parsed spec tree (the {@code Map}/{@code List} shape a JSON
 * parser produces) into a {@link NormalizedSpec}: identity, the Formily connection form flattened to
 * config fields with localized labels and best-effort conditional visibility, the DML sink signals,
 * the group guess and any declared {@code tapstate.modes}. Pure and dependency-free — the caller owns
 * parsing (the build-time assembler, or a test), so the core ring needs no JSON/YAML library.
 */
public final class SpecNormalizer {

    /** Matches equality and boolean inequality checks in a Formily visibility expression. */
    private static final Pattern SELF_VALUE =
            Pattern.compile("\\$self\\.value\\s*(===|==|!==|!=)\\s*(?:'([^']*)'|\"([^\"]*)\"|(true|false))");

    /** A form component whose value the connector reads as a number, regardless of schema spelling. */
    private static final String NUMBER_COMPONENT = "InputNumber";

    private static final String DEFAULT_LOCALE = "en_US";

    private SpecNormalizer() {
    }

    public static NormalizedSpec normalize(Map<String, Object> spec) {
        Map<String, Object> properties = asMap(spec.get("properties"));
        String id = str(properties.get("id"));
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("connector spec missing properties.id");
        }
        String name = str(properties.get("name"));
        String realName = str(properties.get("realName"));
        String displayName = realName != null ? realName : name;
        String icon = str(properties.get("icon"));
        ConnectorGroup tagGroup = GroupRules.fromTags(stringList(properties.get("tags")));

        Map<String, Object> messages = asMap(spec.get("messages"));
        String defaultLocale = str(messages.get("default"));
        if (defaultLocale == null) {
            defaultLocale = DEFAULT_LOCALE;
        }
        Map<String, Map<String, Object>> locales = localeMaps(messages);

        Map<String, Object> configOptions = asMap(spec.get("configOptions"));
        Map<String, Object> connectionProps =
                asMap(asMap(configOptions.get("connection")).get("properties"));

        // Two passes: invert all visibility reactions first (a target may precede its controller),
        // then flatten the form to fields and attach each field's rule.
        Map<String, VisibleWhen> reactions = new LinkedHashMap<>();
        collectReactions(connectionProps, reactions);
        List<ConfigField> config = new ArrayList<>();
        collectFields(connectionProps, defaultLocale, locales, reactions, config);

        List<String> dmlInsertAlternatives = dmlInsertAlternatives(configOptions);
        boolean hasDmlUpdatePolicy = hasCapability(configOptions, "dml_update_policy");

        List<String> declaredModes = stringList(asMap(spec.get("tapstate")).get("modes"));

        return new NormalizedSpec(id, name, displayName, icon, tagGroup, config,
                dmlInsertAlternatives, hasDmlUpdatePolicy, declaredModes);
    }

    // ---- reaction inversion ----

    private static void collectReactions(Map<String, Object> props, Map<String, VisibleWhen> out) {
        for (Map.Entry<String, Object> entry : props.entrySet()) {
            if (!(entry.getValue() instanceof Map<?, ?>)) {
                continue; // a non-object property value is malformed Formily — skip, don't invent a field
            }
            Map<String, Object> def = asMap(entry.getValue());
            extractReactions(entry.getKey(), def, out);
            if (isContainer(def)) {
                collectReactions(asMap(def.get("properties")), out);
            }
        }
    }

    private static void extractReactions(String controller, Map<String, Object> def,
                                         Map<String, VisibleWhen> out) {
        for (Map<String, Object> reaction : reactionList(def.get("x-reactions"))) {
            String target = str(reaction.get("target"));
            if (target == null) {
                continue;
            }
            String visible = str(asMap(asMap(reaction.get("fulfill")).get("state")).get("visible"));
            List<String> allowed = parseSelfValueCondition(visible);
            if (allowed == null) {
                continue;
            }
            for (String t : parseTargets(target)) {
                out.put(t, new VisibleWhen(controller, allowed));
            }
        }
    }

    /** Normalizes the {@code x-reactions} value to a list of reaction maps (it may be a list or one). */
    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> reactionList(Object raw) {
        List<Map<String, Object>> result = new ArrayList<>();
        if (raw instanceof List<?> list) {
            for (Object item : list) {
                Map<String, Object> m = asMap(item);
                if (m != null) {
                    result.add(m);
                }
            }
        } else if (raw instanceof Map<?, ?> m) {
            result.add((Map<String, Object>) m);
        }
        return result;
    }

    /** A single target {@code "name"} or a group {@code "*(a,b,c)"} → the list of field names. */
    private static List<String> parseTargets(String target) {
        String t = target.trim();
        if (t.startsWith("*(") && t.endsWith(")")) {
            t = t.substring(2, t.length() - 1);
        }
        List<String> names = new ArrayList<>();
        for (String part : t.split(",")) {
            String name = part.trim();
            if (!name.isEmpty()) {
                names.add(name);
            }
        }
        return names;
    }

    /**
     * Parses the values from a {@code $self.value === 'x'} visibility expression, supporting an
     * {@code ||} disjunction. Returns null for anything else ({@code $deps}/{@code $inputs} context
     * reactions, or a compound {@code &&}) — those are dropped and the field gets no rule.
     */
    private static List<String> parseSelfValueCondition(String visible) {
        if (visible == null || visible.contains("$deps") || visible.contains("$inputs")
                || visible.contains("&&")) {
            return null;
        }
        List<String> values = new ArrayList<>();
        boolean hasEquality = false;
        boolean hasInequality = false;
        Matcher m = SELF_VALUE.matcher(visible);
        while (m.find()) {
            String operator = m.group(1);
            String single = m.group(2);
            String dbl = m.group(3);
            String bool = m.group(4);
            if (operator.startsWith("!")) {
                // The normalized contract can express a boolean complement exactly. For arbitrary
                // strings the complement is open-ended, so retain the original reaction instead of
                // inventing a finite allow-list.
                if (bool == null) {
                    return null;
                }
                if (hasEquality) {
                    return null;
                }
                hasInequality = true;
                values.add(Boolean.parseBoolean(bool) ? "false" : "true");
            } else {
                if (hasInequality) {
                    return null;
                }
                hasEquality = true;
                values.add(single != null ? single : dbl != null ? dbl : bool);
            }
        }
        return values.isEmpty() ? null : values;
    }

    // ---- field flattening ----

    private static void collectFields(Map<String, Object> props, String defaultLocale,
                                      Map<String, Map<String, Object>> locales,
                                      Map<String, VisibleWhen> reactions, List<ConfigField> out) {
        for (Map.Entry<String, Object> entry : props.entrySet()) {
            String key = entry.getKey();
            if (!(entry.getValue() instanceof Map<?, ?>)) {
                continue; // a non-object property value is malformed Formily — skip, don't emit an empty field
            }
            Map<String, Object> def = asMap(entry.getValue());
            if (isContainer(def)) {
                collectFields(asMap(def.get("properties")), defaultLocale, locales, reactions, out);
                continue;
            }
            if ("hidden".equals(str(def.get("x-display")))) {
                continue;
            }
            ConfigType type = mapType(def);
            Map<String, String> label = resolveLabel(str(def.get("title")), key, defaultLocale, locales);
            boolean required = Boolean.TRUE.equals(def.get("required"));
            String defaultValue = textOf(def.get("default"));
            boolean secret = "Password".equals(str(def.get("x-component")));
            List<EnumOption> options = parseEnum(def.get("enum"), defaultLocale, locales);
            out.add(new ConfigField(key, type, label, required, defaultValue, secret, options,
                    reactions.get(key)));
        }
    }

    /** A void node, or an object that holds nested properties, is a container to flatten — not a field. */
    private static boolean isContainer(Map<String, Object> def) {
        String type = str(def.get("type"));
        if (type == null) {
            return false;
        }
        String t = type.toLowerCase(Locale.ROOT); // upstream casing is inconsistent ("Object", "Void")
        if ("void".equals(t)) {
            return true;
        }
        return "object".equals(t) && def.get("properties") != null;
    }

    private static ConfigType mapType(Map<String, Object> def) {
        // Most shipped numeric fields use the Formily string schema type but render through
        // InputNumber, and their connector config beans read Number. The component is therefore the
        // effective type declaration; following the schema token alone publishes those fields as text.
        if (NUMBER_COMPONENT.equals(str(def.get("x-component")))) {
            return ConfigType.NUMBER;
        }
        String formilyType = str(def.get("type"));
        if (formilyType == null) {
            return ConfigType.STRING;
        }
        // Upstream specs are inconsistent about casing and use a few synonyms ("String", "int"); fold
        // case and map the known synonyms. Genuinely unknown tokens fall to STRING (the safe form-input
        // default) — surfacing unrecognized types is the ingest report's job (a later slice).
        return switch (formilyType.toLowerCase(Locale.ROOT)) {
            case "boolean" -> ConfigType.BOOLEAN;
            case "number", "integer", "int" -> ConfigType.NUMBER;
            case "array" -> ConfigType.ARRAY;
            default -> ConfigType.STRING;
        };
    }

    private static List<EnumOption> parseEnum(Object raw, String defaultLocale,
                                              Map<String, Map<String, Object>> locales) {
        List<EnumOption> options = new ArrayList<>();
        if (raw instanceof List<?> list) {
            for (Object item : list) {
                Map<String, Object> m = asMap(item);
                if (m == null) {
                    continue;
                }
                String value = textOf(m.get("value"));
                Map<String, String> label = resolveLabel(str(m.get("label")), value, defaultLocale, locales);
                options.add(new EnumOption(value, label));
            }
        }
        return options;
    }

    // ---- i18n ----

    /**
     * Resolves a Formily {@code title}/label to an English-only label map ({@code {en_US: text}}).
     * tapstate ships English-only labels, so connector-supplied translations are dropped here rather
     * than carried as dead weight. A {@code ${key}} ref is resolved against the en_US bundle (falling
     * back to the spec's declared default locale, then the raw key); a literal title is kept as-is; a
     * missing title falls back to the field name. Flagging unresolved refs for human review is the
     * ingest report's job (a later slice).
     */
    private static Map<String, String> resolveLabel(String title, String fallback,
                                                    String defaultLocale,
                                                    Map<String, Map<String, Object>> locales) {
        Map<String, String> result = new LinkedHashMap<>();
        if (title == null) {
            result.put(DEFAULT_LOCALE, fallback);
            return result;
        }
        if (!(title.startsWith("${") && title.endsWith("}"))) {
            result.put(DEFAULT_LOCALE, title); // literal, not localized
            return result;
        }
        String key = title.substring(2, title.length() - 1);
        String text = messageOf(locales.get(DEFAULT_LOCALE), key);
        if (text == null) {
            text = messageOf(locales.get(defaultLocale), key); // fall back to the spec's declared default
        }
        result.put(DEFAULT_LOCALE, text != null ? text : key);
        return result;
    }

    private static String messageOf(Map<String, Object> bundle, String key) {
        return bundle == null ? null : str(bundle.get(key));
    }

    /** The locale → message-bundle map (entries whose value is a map; skips the {@code default} string). */
    private static Map<String, Map<String, Object>> localeMaps(Map<String, Object> messages) {
        Map<String, Map<String, Object>> result = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : messages.entrySet()) {
            Map<String, Object> bundle = asMap(entry.getValue());
            if (bundle != null) {
                result.put(entry.getKey(), bundle);
            }
        }
        return result;
    }

    // ---- DML sink signals ----

    private static List<String> dmlInsertAlternatives(Map<String, Object> configOptions) {
        for (Map<String, Object> capability : capabilities(configOptions)) {
            if ("dml_insert_policy".equals(str(capability.get("id")))) {
                return stringList(capability.get("alternatives"));
            }
        }
        return List.of();
    }

    private static boolean hasCapability(Map<String, Object> configOptions, String id) {
        for (Map<String, Object> capability : capabilities(configOptions)) {
            if (id.equals(str(capability.get("id")))) {
                return true;
            }
        }
        return false;
    }

    private static List<Map<String, Object>> capabilities(Map<String, Object> configOptions) {
        List<Map<String, Object>> result = new ArrayList<>();
        Object raw = configOptions.get("capabilities");
        if (raw instanceof List<?> list) {
            for (Object item : list) {
                Map<String, Object> m = asMap(item);
                if (m != null) {
                    result.add(m);
                }
            }
        }
        return result;
    }

    // ---- tree accessors (null-tolerant; the empty map keeps callers branch-free) ----

    @SuppressWarnings("unchecked")
    private static Map<String, Object> asMap(Object value) {
        return value instanceof Map<?, ?> m ? (Map<String, Object>) m : Map.of();
    }

    private static String str(Object value) {
        return value instanceof String s ? s : null;
    }

    private static List<String> stringList(Object value) {
        if (!(value instanceof List<?> list)) {
            return null;
        }
        List<String> result = new ArrayList<>();
        for (Object item : list) {
            result.add(String.valueOf(item));
        }
        return result;
    }

    /**
     * Canonical text of a default/value: a scalar is its literal text (numbers and booleans as-is),
     * while an array or object default is serialized to compact JSON so it stays faithful and
     * round-trippable instead of being mangled by Java collection/map {@code toString()}.
     */
    private static String textOf(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Boolean b) {
            return b.toString();
        }
        if (value instanceof Map<?, ?> || value instanceof List<?>) {
            StringBuilder sb = new StringBuilder();
            writeJson(value, sb);
            return sb.toString();
        }
        return String.valueOf(value);
    }

    /** Compact-JSON encodes a value subtree (objects keep their parsed key order) for a structured default. */
    private static void writeJson(Object value, StringBuilder sb) {
        if (value == null) {
            sb.append("null");
        } else if (value instanceof String s) {
            writeJsonString(s, sb);
        } else if (value instanceof Boolean || value instanceof Number) {
            sb.append(value);
        } else if (value instanceof Map<?, ?> map) {
            sb.append('{');
            boolean first = true;
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                if (!first) {
                    sb.append(',');
                }
                first = false;
                writeJsonString(String.valueOf(entry.getKey()), sb);
                sb.append(':');
                writeJson(entry.getValue(), sb);
            }
            sb.append('}');
        } else if (value instanceof List<?> list) {
            sb.append('[');
            boolean first = true;
            for (Object item : list) {
                if (!first) {
                    sb.append(',');
                }
                first = false;
                writeJson(item, sb);
            }
            sb.append(']');
        } else {
            writeJsonString(String.valueOf(value), sb);
        }
    }

    private static void writeJsonString(String s, StringBuilder sb) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                case '\b' -> sb.append("\\b");
                case '\f' -> sb.append("\\f");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        sb.append('"');
    }
}
