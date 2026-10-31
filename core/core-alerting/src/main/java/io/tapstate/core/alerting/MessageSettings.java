package io.tapstate.core.alerting;

/** The four literal display fields. No variable, template, HTML or secret expansion is performed. */
public record MessageSettings(String titlePrefix, String titleSuffix, String messageNote, String footerText) {
    public MessageSettings {
        titlePrefix = check(titlePrefix, "titlePrefix", 64, false);
        titleSuffix = check(titleSuffix, "titleSuffix", 64, false);
        messageNote = check(messageNote, "messageNote", 1024, true);
        footerText = check(footerText, "footerText", 1024, true);
    }
    public static MessageSettings empty() { return new MessageSettings("", "", "", ""); }
    private static String check(String value, String field, int limit, boolean multiline) {
        AlertValidation.require(value != null, field, "required");
        AlertValidation.require(value.codePointCount(0, value.length()) <= limit, field, "too-long");
        AlertValidation.require(value.codePoints().noneMatch(c -> Character.isISOControl(c)
                && !(multiline && (c == '\n' || c == '\r' || c == '\t'))), field, "control-character");
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (Character.isHighSurrogate(c)) {
                AlertValidation.require(i + 1 < value.length() && Character.isLowSurrogate(value.charAt(i + 1)), field, "invalid-unicode");
                i++;
            } else { AlertValidation.require(!Character.isLowSurrogate(c), field, "invalid-unicode"); }
        }
        return value;
    }
}
