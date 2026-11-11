package io.tapstate.adapters.alerting;

import io.tapstate.core.alerting.AlarmTypeCatalog;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/** Fixed locale text over static type keys. Descriptions do not establish provider availability. */
public final class AlarmTypeText {
    public record Description(String condition, String recovery, String guidance) { }
    private final Map<String, String> text;
    public AlarmTypeText(Map<String, String> fixedText) {
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
        var words = new Properties();
        try (var stream = AlarmTypeText.class.getResourceAsStream("/alarm-types/en.properties")) {
            if (stream == null) { throw new IllegalStateException("missing fixed alarm descriptions"); }
            words.load(new InputStreamReader(stream, StandardCharsets.UTF_8));
        } catch (IOException failure) { throw new IllegalStateException("cannot read fixed alarm descriptions", failure); }
        var values = new HashMap<String, String>();
        words.forEach((key, value) -> values.put((String) key, (String) value));
        return new AlarmTypeText(values);
    }
    /** Statically assembled locale additions for registered types; no expression or template execution. */
    public AlarmTypeText with(Map<String,String> additions) {
        var combined=new HashMap<>(text);combined.putAll(additions);return new AlarmTypeText(combined);
    }
    public Description describe(AlarmTypeCatalog.Type type) {
        Objects.requireNonNull(type, "type");
        return new Description(required(type.conditionTextKey()), type.kind() == AlarmTypeCatalog.Kind.EVENT ? null : required(type.recoveryTextKey()),
                required(type.guidanceTextKey()));
    }
    private String required(String key) {
        String value = text.get(key);
        if (value == null) { throw new IllegalStateException("missing fixed alarm text: " + key); }
        return value;
    }
}
