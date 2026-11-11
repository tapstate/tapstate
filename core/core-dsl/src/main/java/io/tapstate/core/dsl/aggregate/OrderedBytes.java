package io.tapstate.core.dsl.aggregate;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.Year;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.List;

/**
 * Values written as bytes whose unsigned, left-to-right order is the order {@link Values} gives the values
 * themselves, so a store that sorts keys as bytes finds a least or greatest value without reading the rest.
 *
 * <p>Whatever {@link Values#compare} orders, these bytes order the same way: numbers by numeric value across
 * integer, decimal and floating representations ({@code 2.50} and {@code 2.5} are the same bytes), strings by
 * code point, temporals by instant where they name one. Two values {@code compare} refuses to order still get
 * a fixed order here, by kind; refusing such a pair is the business of whoever checks a value against its
 * declared type, not of the encoding.
 *
 * <p>Every encoded value ends where it ends: no value's bytes are the beginning of another's. Values can
 * therefore be written one after another, and a list compares as its entries do, left to right.
 */
public final class OrderedBytes {

    private static final int NULL = 0x00;
    private static final int NEGATIVE_INFINITY = 0x10;
    private static final int NEGATIVE = 0x11;
    private static final int ZERO = 0x12;
    private static final int POSITIVE = 0x13;
    private static final int POSITIVE_INFINITY = 0x14;
    private static final int NOT_A_NUMBER = 0x15;
    private static final int TEXT = 0x20;
    private static final int INSTANT = 0x30;
    private static final int LOCAL_DATE_TIME = 0x31;
    private static final int LOCAL_DATE = 0x32;
    private static final int LOCAL_TIME = 0x33;
    private static final int YEAR = 0x34;
    private static final int BOOLEAN = 0x40;
    private static final int OTHER = 0x50;

    private OrderedBytes() {
    }

    /** One non-null value. */
    public static byte[] of(Object value) {
        if (value == null) {
            throw new IllegalArgumentException("a null has no place in an order; only a key may hold one");
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        write(out, value);
        return out.toByteArray();
    }

    /** Non-null values one after another, ordered as {@link Values#compareAll} orders lists of one length. */
    public static byte[] ofAll(List<Object> values) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (Object value : values) {
            if (value == null) {
                throw new IllegalArgumentException("a null has no place in an order; only a key may hold one");
            }
            write(out, value);
        }
        return out.toByteArray();
    }

    /**
     * An element key, ordered as {@link Values#compareKeys} orders keys of one length: a null sorts before
     * every value.
     */
    public static byte[] ofKey(List<Object> key) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (Object part : key) {
            if (part == null) {
                out.write(NULL);
            } else {
                write(out, part);
            }
        }
        return out.toByteArray();
    }

    private static void write(ByteArrayOutputStream out, Object value) {
        switch (value) {
            case Number number -> number(out, number);
            case CharSequence text -> {
                out.write(TEXT);
                text(out, text.toString());
            }
            case Instant instant -> instant(out, instant);
            case OffsetDateTime at -> instant(out, at.toInstant());
            case ZonedDateTime at -> instant(out, at.toInstant());
            case Date date -> instant(out, date.toInstant());
            case LocalDateTime at -> {
                out.write(LOCAL_DATE_TIME);
                signed(out, at.toEpochSecond(ZoneOffset.UTC));
                unsigned32(out, at.getNano());
            }
            case LocalDate date -> {
                out.write(LOCAL_DATE);
                signed(out, date.toEpochDay());
            }
            case LocalTime time -> {
                out.write(LOCAL_TIME);
                signed(out, time.toNanoOfDay());
            }
            case Year year -> {
                out.write(YEAR);
                signed(out, year.getValue());
            }
            case Boolean flag -> {
                out.write(BOOLEAN);
                out.write(flag ? 1 : 0);
            }
            default -> {
                out.write(OTHER);
                text(out, String.valueOf(value));
            }
        }
    }

    /**
     * A number by its value, whatever it was held as. A finite number is its sign, then the power of ten of
     * its first digit, then its digits without trailing zeros; a negative number writes the last two inverted,
     * so a greater magnitude sorts first.
     */
    private static void number(ByteArrayOutputStream out, Number number) {
        if (!Values.finite(number)) {
            double d = number.doubleValue();
            out.write(Double.isNaN(d) ? NOT_A_NUMBER : d > 0 ? POSITIVE_INFINITY : NEGATIVE_INFINITY);
            return;
        }
        BigDecimal exact = Values.decimal(number);
        if (exact.signum() == 0) {
            out.write(ZERO);
            return;
        }
        boolean negative = exact.signum() < 0;
        BigDecimal magnitude = exact.abs().stripTrailingZeros();
        String digits = magnitude.unscaledValue().toString();
        int firstDigitPower = digits.length() - magnitude.scale();
        int flip = negative ? 0xFF : 0x00;
        out.write(negative ? NEGATIVE : POSITIVE);
        int biased = firstDigitPower ^ 0x80000000;
        for (int shift = 24; shift >= 0; shift -= 8) {
            out.write(((biased >>> shift) & 0xFF) ^ flip);
        }
        for (int i = 0; i < digits.length(); i++) {
            out.write((digits.charAt(i) - '0' + 1) ^ flip);
        }
        out.write(flip);
    }

    /**
     * Text by code point: each code point in the UTF-8 form, an unpaired surrogate included as the code point
     * it is, and a zero byte escaped so that the two zero bytes ending the text occur nowhere inside it.
     */
    private static void text(ByteArrayOutputStream out, String text) {
        for (int i = 0; i < text.length(); ) {
            int point = text.codePointAt(i);
            i += Character.charCount(point);
            if (point == 0) {
                out.write(0x00);
                out.write(0x01);
            } else if (point < 0x80) {
                out.write(point);
            } else if (point < 0x800) {
                out.write(0xC0 | (point >>> 6));
                out.write(0x80 | (point & 0x3F));
            } else if (point < 0x10000) {
                out.write(0xE0 | (point >>> 12));
                out.write(0x80 | ((point >>> 6) & 0x3F));
                out.write(0x80 | (point & 0x3F));
            } else {
                out.write(0xF0 | (point >>> 18));
                out.write(0x80 | ((point >>> 12) & 0x3F));
                out.write(0x80 | ((point >>> 6) & 0x3F));
                out.write(0x80 | (point & 0x3F));
            }
        }
        out.write(0x00);
        out.write(0x00);
    }

    private static void instant(ByteArrayOutputStream out, Instant instant) {
        out.write(INSTANT);
        signed(out, instant.getEpochSecond());
        unsigned32(out, instant.getNano());
    }

    /** Eight bytes, most significant first, with the sign bit turned so negatives sort first. */
    private static void signed(ByteArrayOutputStream out, long value) {
        long biased = value ^ Long.MIN_VALUE;
        for (int shift = 56; shift >= 0; shift -= 8) {
            out.write((int) (biased >>> shift) & 0xFF);
        }
    }

    private static void unsigned32(ByteArrayOutputStream out, int value) {
        for (int shift = 24; shift >= 0; shift -= 8) {
            out.write((value >>> shift) & 0xFF);
        }
    }
}
