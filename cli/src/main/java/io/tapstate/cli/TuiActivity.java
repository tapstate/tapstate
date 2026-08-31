package io.tapstate.cli;

import java.util.regex.Pattern;

/** Small presentation boundary that keeps command history useful without echoing credentials. */
final class TuiActivity {

    private static final Pattern KEY_VALUE_SECRET = Pattern.compile(
            "(?i)([\"']?(?:password|secret|bearer|token(?:[_-]?(?:id|value))?|access[_-]?token|"
                    + "refresh[_-]?token|session[_-]?token)[\"']?\\s*[:=]\\s*[\"']?)([^\"'\\s,;}]+)");
    private static final Pattern BEARER_SECRET = Pattern.compile("(?i)(\\bBearer\\s+)([^\\s,;]+)");
    private static final Pattern OPTION_SECRET = Pattern.compile(
            "(?i)(--(?:password|secret|token|access-token|refresh-token|session-token)\\s+)([^\\s]+)");
    private static final Pattern LOGIN_SECRET = Pattern.compile("(?i)(\\bauth\\s+login\\s+\\S+\\s+)(\\S+)");
    private static final Pattern REVOKE_SECRET = Pattern.compile("(?i)(\\btoken\\s+revoke\\s+)(\\S+)");

    private TuiActivity() {
    }

    static String command(String value) {
        return redact(compact(value));
    }

    static String result(String value) {
        return redact(compact(value));
    }

    static String redact(String value) {
        if (value == null || value.isBlank()) {
            return "";
        }
        String redacted = KEY_VALUE_SECRET.matcher(value).replaceAll("$1[redacted]");
        redacted = BEARER_SECRET.matcher(redacted).replaceAll("$1[redacted]");
        redacted = OPTION_SECRET.matcher(redacted).replaceAll("$1[redacted]");
        redacted = LOGIN_SECRET.matcher(redacted).replaceAll("$1[redacted]");
        return REVOKE_SECRET.matcher(redacted).replaceAll("$1[redacted]");
    }

    private static String compact(String value) {
        return value == null ? "" : value.replaceAll("\\s+", " ").trim();
    }
}
