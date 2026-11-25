package io.tapstate.adapters.alerting;

import io.tapstate.core.alerting.AlarmTypeCatalog;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Locale;
import java.util.Objects;
import java.util.Properties;

/** Fixed locale text over static type keys. Descriptions do not establish provider availability. */
public final class AlarmTypeText {
    public record Description(String condition, String recovery, String guidance) { }
    private final Map<String, String> text;
    private final Map<String, Map<String, String>> translations;
    public AlarmTypeText(Map<String, String> fixedText) {
        this(fixedText, Map.of());
    }
    private AlarmTypeText(Map<String, String> fixedText, Map<String, Map<String, String>> translations) {
        this.translations = Map.copyOf(translations);
        text = Map.copyOf(fixedText);
        if (text.size() > 512) { throw new IllegalArgumentException("too many fixed alarm text entries"); }
        text.values().forEach(value -> {
            if (value.isBlank() || value.codePointCount(0, value.length()) > 4096
                    || !StandardCharsets.UTF_8.newEncoder().canEncode(value)
                    || value.codePoints().anyMatch(Character::isISOControl)) {
                throw new IllegalArgumentException("invalid fixed alarm text");
            }
        });
    }
    public static AlarmTypeText english() {
        return new AlarmTypeText(load("en"), Map.of("zh-CN", load("zh-CN"), "zh-TW", load("zh-TW")));
    }
    private static Map<String, String> load(String language) {
        var words = new Properties();
        try (var stream = AlarmTypeText.class.getResourceAsStream("/alarm-types/" + language + ".properties")) {
            if (stream == null) { throw new IllegalStateException("missing fixed alarm descriptions"); }
            words.load(new InputStreamReader(stream, StandardCharsets.UTF_8));
        } catch (IOException failure) { throw new IllegalStateException("cannot read fixed alarm descriptions", failure); }
        var values = new HashMap<String, String>();
        words.forEach((key, value) -> values.put((String) key, (String) value));
        new AlarmTypeText(values);
        return Map.copyOf(values);
    }
    /** Statically assembled locale additions for registered types; no expression or template execution. */
    public AlarmTypeText with(Map<String,String> additions) {
        var combined=new HashMap<>(text);combined.putAll(additions);return new AlarmTypeText(combined,translations);
    }
    public Description describe(AlarmTypeCatalog.Type type) {
        return describe(type, Locale.ENGLISH);
    }
    public Description describe(AlarmTypeCatalog.Type type, Locale locale) {
        Objects.requireNonNull(type, "type");
        String language = "zh".equals(locale.getLanguage())
                ? (java.util.Set.of("HK", "MO", "TW").contains(locale.getCountry())
                        || "Hant".equals(locale.getScript()) ? "zh-TW" : "zh-CN") : "en";
        Map<String, String> localized = translations.getOrDefault(language, Map.of());
        return new Description(required(type.conditionTextKey(), localized),
                type.kind() == AlarmTypeCatalog.Kind.EVENT ? null : required(type.recoveryTextKey(), localized),
                required(type.guidanceTextKey(), localized));
    }
    private String required(String key, Map<String, String> localized) {
        String value = localized.getOrDefault(key, text.get(key));
        if (value == null) { throw new IllegalStateException("missing fixed alarm text: " + key); }
        return value;
    }
}
