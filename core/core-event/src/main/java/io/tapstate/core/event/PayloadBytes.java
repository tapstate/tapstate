package io.tapstate.core.event;

import io.tapstate.core.common.TapstateType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.Year;
import java.time.temporal.Temporal;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.Objects;

/**
 * How many bytes of payload an event carries: the logical size of the row images in it, decided by
 * tapstate and by nothing else.
 *
 * <p><b>This is a definition, not a measurement.</b> Nothing in a running pipeline has a byte count to
 * read: a row is a map of values in memory, and every number that could be read off it belongs to
 * somebody else — a driver's wire format, a transport serializer's output, the heap the objects sit on.
 * Publishing one of those as the product's own figure would put a line on a user's chart that moves when
 * a dependency changes an encoding, with nothing anywhere going red. So the figure is defined here,
 * against the tapstate type namespace, and the rules below are the whole of it.
 *
 * <p><b>What the number is.</b> The sum, over each row image the event carries, of every field's name
 * and value. A name costs its own text, because a row is a mapping and the name is half of each pair —
 * a keyed target stores it per row, and leaving it out would report a row of fifty flags as fifty bytes.
 * A value costs the logical width of its kind: its own content where the kind has content of its own
 * (text, a byte string, an exact decimal, a container's contents), and one declared width where the kind
 * does not (a whole number, a floating point number, a date or a time). The declared width is a decision
 * and not an observation — a small integer costs the same as a large one — which is what keeps a chart
 * of this tracking the shape of the rows rather than the magnitude of the digits in them.
 *
 * <p><b>What the number is not.</b> Not compressed bytes, not bytes on a wire, not the memory the row
 * occupies. Each of those is a real quantity, and each is a different one: the first two belong to
 * whatever moves the row and change when it does, and the last answers a question about this process
 * rather than about the data.
 *
 * <p><b>Adding a member to the type namespace does not compile until its width is decided.</b> The
 * switch below is exhaustive with no default arm, which is deliberate and is the point where the
 * decision is forced: a new kind of column with no logical width would otherwise fall into whichever
 * arm was nearest and be charged as something it is not.
 *
 * <p><b>Nothing here throws on a value it does not recognise.</b> This runs on the data path, once per
 * row, and a pipeline must not die because a byte count could not be worked out — the opposite trade
 * from the content hash, which crashes on an unhashable value because giving two different resources
 * one identity is worse than stopping. A value no rule names is charged by how it writes itself down,
 * which is never nothing: a value charged zero would be a column silently absent from the figure.
 */
public final class PayloadBytes {

    /**
     * What one value of a fixed-width kind costs: a whole number, a floating point number, a date, a
     * time, a date and time together, a year. Eight because that is the width the type namespace speaks
     * for the two numeric kinds, and a moment in time is one of those numbers however it is spelled.
     */
    static final long FIXED_WIDTH_BYTES = 8L;

    /** What one boolean costs. The smallest thing a serialization can spend on a value that has two. */
    static final long BOOLEAN_BYTES = 1L;

    /** What a value whose written form cannot be obtained costs, so that it is present rather than free. */
    static final long UNWRITABLE_BYTES = FIXED_WIDTH_BYTES;

    private PayloadBytes() {
    }

    /**
     * The payload {@code event} carries: its before image plus its after image, each charged by
     * {@link #ofRow}, and nothing else in the envelope.
     *
     * <p>Both images, when both are there. An update carries two halves of one row and both of them
     * crossed the boundary; charging one would make the figure depend on which half was picked rather
     * than on what moved. It follows that a source configured to report full before images reports more
     * bytes for the same change than one that reports only the key — which is true, and is the sort of
     * thing this figure exists to show.
     *
     * <p>Everything else in the envelope is about the row rather than part of it, and is charged
     * nothing: the schema describes it, the positions say where it sat on its chains, the removed names
     * say which of its fields are gone, and the op, the event time and the stream name frame it. Two
     * consequences are worth stating rather than discovering: a schema change carries no row and
     * therefore costs zero bytes while still counting as a record, and so does an event that only says
     * a field has gone. A pipeline doing nothing but schema work shows records rising with bytes flat.
     */
    public static long of(Envelope event) {
        Objects.requireNonNull(event, "event");
        return ofRow(event.before()) + ofRow(event.after());
    }

    /**
     * What one row image costs: every field's name and value. An absent or empty image costs nothing.
     *
     * <p>A field whose value is absent still costs its name. The name arrived, a keyed target writes it,
     * and a row of nothing but absent fields is not a row of no size.
     */
    public static long ofRow(Map<String, Object> row) {
        return ofMap(row);
    }

    /**
     * Which rule charges {@code value}, named in the tapstate type namespace — the vocabulary the width
     * table below is keyed by, and the answer to "why is this row that many bytes".
     *
     * <p>Carriers are seen through: a value the source's connector converted for travel is charged as
     * the value inside it, never as the carrier. That is not an optimisation. A carrier holds the value
     * and the name of the type it came out of, and charging the carrier would put a schema's spelling
     * into a figure about data — so the same column would cost more on a source whose types have longer
     * names, for rows that are identical.
     *
     * <p>{@link TapstateType#JSON} is never the answer. A document reaches a row as text or as a nested
     * row, and is charged as whichever it arrives as; the namespace names it because a column can be
     * declared that way, and a declared type is not what is being classified here.
     *
     * @throws NullPointerException if {@code value} is null — an absent value has no type, and is
     *     charged nothing by {@link #ofRow} without ever reaching a rule
     */
    public static TapstateType kindOf(Object value) {
        Objects.requireNonNull(value, "value");
        Object plain = carried(value);
        return switch (plain) {
            case CharSequence ignored -> TapstateType.STRING;
            case BigDecimal ignored -> TapstateType.DECIMAL;
            // A whole number too wide for the namespace's own width is charged by its digits rather
            // than by that width, which it does not fit. Anything narrower has arrived as a long.
            case BigInteger ignored -> TapstateType.DECIMAL;
            case Long ignored -> TapstateType.INT64;
            case Integer ignored -> TapstateType.INT64;
            case Short ignored -> TapstateType.INT64;
            case Byte ignored -> TapstateType.INT64;
            case Double ignored -> TapstateType.DOUBLE;
            case Float ignored -> TapstateType.DOUBLE;
            case Boolean ignored -> TapstateType.BOOLEAN;
            case Bytes ignored -> TapstateType.BINARY;
            case byte[] ignored -> TapstateType.BINARY;
            case Map<?, ?> ignored -> TapstateType.MAP;
            case Collection<?> ignored -> TapstateType.ARRAY;
            case LocalDate ignored -> TapstateType.DATE;
            case LocalTime ignored -> TapstateType.TIME;
            case OffsetTime ignored -> TapstateType.TIME;
            case Year ignored -> TapstateType.YEAR;
            case Date ignored -> TapstateType.DATETIME;
            // Every remaining moment in time - an instant, a local or offset date and time, a zoned
            // one - is a date and a time together. One that names neither a date nor a time on its own
            // lands here too: it is still a point on a calendar, and charging it by its written form
            // would make a month cost fewer bytes than a day.
            case Temporal ignored -> TapstateType.DATETIME;
            default -> TapstateType.UNKNOWN;
        };
    }

    /**
     * What one value costs, name excluded. An absent value costs nothing; every present one costs the
     * width of its kind.
     */
    public static long ofValue(Object value) {
        if (value == null) {
            return 0L;
        }
        Object plain = carried(value);
        if (plain == null) {
            return 0L;
        }
        return switch (kindOf(plain)) {
            // A document is charged as the text or the row it arrives as, which is why it shares its
            // arm: the namespace has a name for the column, and the value has no separate shape.
            case STRING, JSON -> utf8Length(plain.toString());
            case DECIMAL -> utf8Length(decimalText(plain));
            case INT64, DOUBLE, DATE, TIME, DATETIME, YEAR -> FIXED_WIDTH_BYTES;
            case BOOLEAN -> BOOLEAN_BYTES;
            case BINARY -> binaryLength(plain);
            case MAP -> ofMap((Map<?, ?>) plain);
            case ARRAY -> ofElements((Collection<?>) plain);
            case UNKNOWN -> writtenFormLength(plain);
        };
    }

    /**
     * The value inside however many carriers are wrapped around it. Looped rather than unwrapped once,
     * for the reason the carrier's own walk loops: one carrier holding another is a shape nothing
     * produces today and nothing prevents, and a single unwrap would charge the inner carrier's
     * declared type name as though it were data.
     */
    private static Object carried(Object value) {
        Object plain = value;
        while (plain instanceof ConvertedValue carrier) {
            plain = carrier.value();
        }
        return plain;
    }

    /**
     * A container costs what is in it and nothing for holding it. The punctuation a format spends on
     * brackets and separators belongs to that format, and charging a notional amount for it would put a
     * guess about somebody else's syntax into a figure that is otherwise all data.
     */
    private static long ofMap(Map<?, ?> row) {
        if (row == null || row.isEmpty()) {
            return 0L;
        }
        long total = 0L;
        for (Map.Entry<?, ?> field : row.entrySet()) {
            Object name = field.getKey();
            total += name == null ? 0L : utf8Length(name.toString());
            total += ofValue(field.getValue());
        }
        return total;
    }

    private static long ofElements(Collection<?> elements) {
        long total = 0L;
        for (Object element : elements) {
            total += ofValue(element);
        }
        return total;
    }

    /** An exact number's digits, sign and point, written out without an exponent. */
    private static String decimalText(Object value) {
        return value instanceof BigDecimal decimal ? decimal.toPlainString() : value.toString();
    }

    private static long binaryLength(Object value) {
        return value instanceof Bytes bytes ? bytes.value().length : ((byte[]) value).length;
    }

    /**
     * How a value with no rule of its own writes itself down. A value that cannot even do that is
     * charged a fixed amount rather than nothing, so it stays visible in the figure: a throwing or
     * absent written form is a defect in whatever put the value in the row, and one that reads as a
     * column of size zero is a defect nobody will ever look for.
     */
    private static long writtenFormLength(Object value) {
        String text;
        try {
            text = value.toString();
        } catch (RuntimeException failed) {
            return UNWRITABLE_BYTES;
        }
        return text == null ? UNWRITABLE_BYTES : utf8Length(text);
    }

    /**
     * The number of bytes {@code text} occupies as UTF-8, counted rather than encoded: this runs once
     * per text value on the data path, and encoding to find a length would allocate the bytes of every
     * row a pipeline moves in order to throw them away.
     *
     * <p>An unpaired surrogate counts three, which is what it encodes as once it becomes the
     * replacement character - the only form it can take in UTF-8 at all.
     */
    private static long utf8Length(CharSequence text) {
        long bytes = 0L;
        int length = text.length();
        for (int index = 0; index < length; index++) {
            char character = text.charAt(index);
            if (character < 0x80) {
                bytes += 1L;
            } else if (character < 0x800) {
                bytes += 2L;
            } else if (Character.isHighSurrogate(character)
                    && index + 1 < length
                    && Character.isLowSurrogate(text.charAt(index + 1))) {
                bytes += 4L;
                index++;
            } else {
                bytes += 3L;
            }
        }
        return bytes;
    }
}
