package io.tapstate.cli;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.Function;

/** Session-scoped mutable secret text that can be cleared after one guarded use. */
final class SecretBuffer implements AutoCloseable {

    private char[] value = new char[16];
    private int length;
    private boolean closed;

    void append(int codePoint) {
        ensureOpen();
        if (Character.isISOControl(codePoint)) {
            return;
        }
        char[] chars = Character.toChars(codePoint);
        ensureCapacity(length + chars.length);
        System.arraycopy(chars, 0, value, length, chars.length);
        length += chars.length;
        Arrays.fill(chars, '\0');
    }

    void append(String text) {
        Objects.requireNonNull(text, "text").codePoints().forEach(this::append);
    }

    void deleteLast() {
        ensureOpen();
        if (length == 0) {
            return;
        }
        int width = Character.isLowSurrogate(value[length - 1]) && length > 1
                && Character.isHighSurrogate(value[length - 2]) ? 2 : 1;
        Arrays.fill(value, length - width, length, '\0');
        length -= width;
    }

    int length() {
        ensureOpen();
        return valueLengthInCodePoints();
    }

    String mask() {
        return "*".repeat(length());
    }

    <T> T consume(Function<String, T> action) {
        Objects.requireNonNull(action, "action");
        ensureOpen();
        try {
            return action.apply(new String(value, 0, length));
        } finally {
            close();
        }
    }

    @Override
    public void close() {
        Arrays.fill(value, '\0');
        length = 0;
        closed = true;
    }

    boolean cleared() {
        return closed && Arrays.equals(value, new char[value.length]);
    }

    @Override
    public String toString() {
        return "SecretBuffer[redacted]";
    }

    private int valueLengthInCodePoints() {
        return Character.codePointCount(value, 0, length);
    }

    private void ensureCapacity(int needed) {
        if (needed <= value.length) {
            return;
        }
        char[] replacement = Arrays.copyOf(value, Math.max(needed, value.length * 2));
        Arrays.fill(value, '\0');
        value = replacement;
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("Secret buffer is closed");
        }
    }
}
