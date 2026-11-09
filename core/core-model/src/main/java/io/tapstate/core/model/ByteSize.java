package io.tapstate.core.model;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A size in bytes as an author writes it: a whole number followed by {@code B}, {@code KiB}, {@code MiB} or
 * {@code GiB}, the units binary (1 KiB = 1,024 bytes). A bare whole number is a count of bytes.
 *
 * <p>The written form is kept in the largest unit that states the size exactly, so {@code 1024KiB} and
 * {@code 1MiB} are one size and one spelling, and an artifact that spells it either way has the same content.
 */
public final class ByteSize {

    private static final Pattern WRITTEN = Pattern.compile("([0-9]+)\\s*(B|KiB|MiB|GiB)?");
    private static final String[] UNITS = {"GiB", "MiB", "KiB"};
    private static final long[] FACTORS = {1L << 30, 1L << 20, 1L << 10};

    private ByteSize() {
    }

    /**
     * The bytes {@code written} states, or -1 when it is not a size: not of the form above, zero, or more
     * bytes than a long holds.
     */
    public static long bytes(String written) {
        Objects.requireNonNull(written, "written");
        Matcher matched = WRITTEN.matcher(written.strip());
        if (!matched.matches()) {
            return -1;
        }
        long number;
        try {
            number = Long.parseLong(matched.group(1));
        } catch (NumberFormatException tooLong) {
            return -1;
        }
        long factor = factor(matched.group(2));
        if (number <= 0 || number > Long.MAX_VALUE / factor) {
            return -1;
        }
        return number * factor;
    }

    /** {@code bytes} in the largest unit that states it exactly. */
    public static String written(long bytes) {
        if (bytes <= 0) {
            throw new IllegalArgumentException("a size is positive: " + bytes);
        }
        for (int i = 0; i < UNITS.length; i++) {
            if (bytes % FACTORS[i] == 0) {
                return bytes / FACTORS[i] + UNITS[i];
            }
        }
        return bytes + "B";
    }

    private static long factor(String unit) {
        if (unit == null || unit.equals("B")) {
            return 1L;
        }
        for (int i = 0; i < UNITS.length; i++) {
            if (UNITS[i].equals(unit)) {
                return FACTORS[i];
            }
        }
        throw new IllegalStateException("unit outside the pattern: " + unit);
    }
}
