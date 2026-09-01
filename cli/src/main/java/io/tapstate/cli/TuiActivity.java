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
    private static final Pattern LOGIN_SECRET = Pattern.compile(
            "(?i)(\\b(?:auth\\s+)?login\\s+\\S+\\s+)(\\S+)");
    private static final Pattern REVOKE_SECRET = Pattern.compile("(?i)(\\btoken\\s+revoke\\s+)(\\S+)");
    private static final Pattern TOKEN_OUTPUT_SECRET = Pattern.compile(
            "(?i)(\\btoken\\s+)(?!revoke\\b)([^\\s,;]+)");
    private static final Pattern ANSI_SEQUENCE = Pattern.compile(
            "\\u001B(?:\\\\[[0-?]*[ -/]*[@-~]|\\\\][^\\u0007]*(?:\\u0007|\\\\u001B\\\\\\\\))");

    private TuiActivity() {
    }

    static String command(String value) {
        if (value == null || value.isBlank()) {
            return "";
        }
        // Preserve a trailing separator while typing so the reducer can distinguish the next argument.
        return redact(value.replaceAll("\\s+", " "));
    }

    static String result(String value) {
        return redact(compact(value));
    }

    static String redact(String value) {
        if (value == null || value.isBlank()) {
            return "";
        }
        String redacted = stripTerminalControls(value);
        redacted = KEY_VALUE_SECRET.matcher(redacted).replaceAll("$1[redacted]");
        redacted = BEARER_SECRET.matcher(redacted).replaceAll("$1[redacted]");
        redacted = OPTION_SECRET.matcher(redacted).replaceAll("$1[redacted]");
        redacted = LOGIN_SECRET.matcher(redacted).replaceAll("$1[redacted]");
        redacted = REVOKE_SECRET.matcher(redacted).replaceAll("$1[redacted]");
        return TOKEN_OUTPUT_SECRET.matcher(redacted).replaceAll("$1[redacted]");
    }

    static String stripTerminalControls(String value) {
        if (value == null || value.isEmpty()) {
            return "";
        }
        String stripped = ANSI_SEQUENCE.matcher(value).replaceAll("");
        StringBuilder safe = new StringBuilder(stripped.length());
        stripped.codePoints().forEach(codePoint -> {
            if (!Character.isISOControl(codePoint) || Character.isWhitespace(codePoint)) {
                safe.appendCodePoint(codePoint);
            } else {
                safe.append(' ');
            }
        });
        return safe.toString();
    }

    private static String compact(String value) {
        return value == null ? "" : value.replaceAll("\\s+", " ").trim();
    }
}
