package io.tapstate.app;

import java.util.function.Supplier;
import java.util.regex.Pattern;

/** Opt-in test diagnostic that reduces one locally read error field to a fixed category. */
final class CloudSpecPutRefusalClassifier {
    enum Category { NOT_ENABLED, UNCLASSIFIED, SPACE_QUOTA }

    private static final Pattern SPACE_QUOTA = Pattern.compile(
            "you are over your space quota, using [0-9]{1,12}(?:\\.[0-9]{1,3})? "
                    + "(?:B|KB|MB|GB|TB) of [0-9]{1,12}(?:\\.[0-9]{1,3})? (?:B|KB|MB|GB|TB)");

    private CloudSpecPutRefusalClassifier() {}

    static Category classify(boolean explicitlyEnabled, boolean exactSeedScope,
            String operation, String command, int driverCode, Supplier<String> errorField) {
        if (!explicitlyEnabled) return Category.NOT_ENABLED;
        if (!exactSeedScope || !"spec-put".equals(operation) || !"update".equals(command)
                || driverCode != 8000) return Category.UNCLASSIFIED;
        return classifyField(errorField);
    }

    static Category classifyField(Supplier<String> errorField) {
        try {
            String value = errorField.get();
            return value != null && value.length() <= 160 && SPACE_QUOTA.matcher(value).matches()
                    ? Category.SPACE_QUOTA : Category.UNCLASSIFIED;
        } catch (RuntimeException unavailable) {
            return Category.UNCLASSIFIED;
        }
    }
}
