package io.tapstate.cli;

import java.util.Arrays;

/** Wipeable password input that exposes only its length and masked rendering while editing. */
final class TuiSecretInput implements AutoCloseable {

    private char[] value = new char[8];
    private int length;

    void appendCodePoint(int codePoint) {
        if (!Character.isValidCodePoint(codePoint)) {
            return;
        }
        char[] codePointChars = Character.toChars(codePoint);
        ensureCapacity(length + codePointChars.length);
        System.arraycopy(codePointChars, 0, value, length, codePointChars.length);
        length += codePointChars.length;
    }

    void deleteLastCodePoint() {
        if (length == 0) {
            return;
        }
        int last = length - 1;
        int start = last > 0 && Character.isLowSurrogate(value[last])
                && Character.isHighSurrogate(value[last - 1]) ? last - 1 : last;
        Arrays.fill(value, start, length, '\0');
        length = start;
    }

    int codePointCount() {
        return Character.codePointCount(value, 0, length);
    }

    String masked() {
        return "•".repeat(codePointCount());
    }

    String take() {
        String result = new String(value, 0, length);
        clear();
        return result;
    }

    void clear() {
        Arrays.fill(value, '\0');
        length = 0;
    }

    @Override
    public void close() {
        clear();
    }

    @Override
    public String toString() {
        return "TuiSecretInput[length=" + codePointCount() + ']';
    }

    private void ensureCapacity(int required) {
        if (required <= value.length) {
            return;
        }
        char[] expanded = Arrays.copyOf(value, Math.max(required, value.length * 2));
        Arrays.fill(value, '\0');
        value = expanded;
    }
}
