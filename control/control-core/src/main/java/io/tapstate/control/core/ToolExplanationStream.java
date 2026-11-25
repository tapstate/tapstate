package io.tapstate.control.core;

import io.tapstate.core.common.JsonReader;
import java.util.function.Consumer;

/** Reads only the top-level explanation string while tool arguments are still arriving. */
final class ToolExplanationStream {
    private enum StringKind { KEY, EXPLANATION, OTHER }
    private int depth;
    private boolean expectingKey;
    private boolean explanationValue;
    private boolean escaped;
    private int unicodeDigits;
    private int unicodeValue;
    private char pendingHighSurrogate;
    private String key;
    private StringKind stringKind;
    private final StringBuilder keyLiteral = new StringBuilder();

    void accept(String fragment, Consumer<String> text) {
        StringBuilder visible = new StringBuilder();
        for (int index = 0; index < fragment.length(); index++) {
            char value = fragment.charAt(index);
            if (stringKind != null) {
                if (stringKind == StringKind.KEY) keyLiteral.append(value);
                if (stringKind == StringKind.EXPLANATION) {
                    explanationCharacter(value, visible);
                } else if (escaped) {
                    escaped = false;
                } else if (value == '\\') {
                    escaped = true;
                } else if (value == '"') {
                    if (stringKind == StringKind.KEY) key = (String) JsonReader.parse(keyLiteral.toString());
                    stringKind = null;
                }
                continue;
            }
            switch (value) {
                case '{', '[' -> {
                    depth++;
                    if (depth == 1) expectingKey = true;
                }
                case '}', ']' -> depth--;
                case ',' -> {
                    if (depth == 1) {
                        expectingKey = true;
                        explanationValue = false;
                        key = null;
                    }
                }
                case ':' -> {
                    if (depth == 1) {
                        explanationValue = "explanation".equals(key);
                        expectingKey = false;
                    }
                }
                case '"' -> {
                    stringKind = depth == 1 && expectingKey ? StringKind.KEY
                            : depth == 1 && explanationValue ? StringKind.EXPLANATION : StringKind.OTHER;
                    if (stringKind == StringKind.KEY) {
                        keyLiteral.setLength(0);
                        keyLiteral.append('"');
                    }
                    escaped = false;
                }
                default -> { }
            }
        }
        if (!visible.isEmpty()) text.accept(visible.toString());
    }

    private void explanationCharacter(char value, StringBuilder visible) {
        if (unicodeDigits > 0) {
            int digit = Character.digit(value, 16);
            if (digit < 0) throw new IllegalArgumentException("Invalid Unicode escape");
            unicodeValue = unicodeValue * 16 + digit;
            if (--unicodeDigits == 0) append((char) unicodeValue, visible);
        } else if (escaped) {
            escaped = false;
            if (value == 'u') {
                unicodeDigits = 4;
                unicodeValue = 0;
            } else {
                append(switch (value) {
                    case '"', '\\', '/' -> value;
                    case 'b' -> '\b';
                    case 'f' -> '\f';
                    case 'n' -> '\n';
                    case 'r' -> '\r';
                    case 't' -> '\t';
                    default -> throw new IllegalArgumentException("Invalid JSON string escape");
                }, visible);
            }
        } else if (value == '\\') {
            escaped = true;
        } else if (value == '"') {
            if (pendingHighSurrogate != 0) throw new IllegalArgumentException("Incomplete Unicode surrogate");
            stringKind = null;
            explanationValue = false;
        } else {
            if (value < 0x20) throw new IllegalArgumentException("Invalid JSON string character");
            append(value, visible);
        }
    }

    private void append(char value, StringBuilder visible) {
        if (pendingHighSurrogate != 0) {
            if (!Character.isLowSurrogate(value)) throw new IllegalArgumentException("Invalid Unicode surrogate");
            visible.append(pendingHighSurrogate).append(value);
            pendingHighSurrogate = 0;
        } else if (Character.isHighSurrogate(value)) {
            pendingHighSurrogate = value;
        } else {
            if (Character.isLowSurrogate(value)) throw new IllegalArgumentException("Invalid Unicode surrogate");
            visible.append(value);
        }
    }
}
