package io.tapstate.core.alerting;

import io.tapstate.core.common.TapstateException;
import java.time.Duration;
import java.util.Map;

final class AlertValidation {
    private AlertValidation() { }
    static void require(boolean condition, String field, String reason) {
        if (!condition) {
            throw new TapstateException(AlertError.INVALID_CONFIGURATION,
                    Map.of("field", field, "reason", reason), null);
        }
    }
    static String text(String value, String field, int limit) {
        require(value != null && !value.isBlank(), field, "required");
        require(value.codePointCount(0, value.length()) <= limit, field, "too-long");
        require(value.codePoints().noneMatch(Character::isISOControl), field, "control-character");
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (Character.isHighSurrogate(c)) {
                require(i + 1 < value.length() && Character.isLowSurrogate(value.charAt(i + 1)),
                        field, "invalid-unicode");
                i++;
            } else {
                require(!Character.isLowSurrogate(c), field, "invalid-unicode");
            }
        }
        return value;
    }
    static Duration duration(Duration value, String field, boolean zeroAllowed, Duration maximum) {
        require(value != null && !value.isNegative() && (zeroAllowed || !value.isZero()),
                field, "invalid-duration");
        require(value.compareTo(maximum) <= 0, field, "duration-too-long");
        return value;
    }
}
