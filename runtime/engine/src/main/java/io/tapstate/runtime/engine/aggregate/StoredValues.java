package io.tapstate.runtime.engine.aggregate;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.Year;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Row values written into operator state and read back as the same values: a value comes back equal to what
 * went in, of the same numeric kind and scale, so a result computed after a restart is the result that
 * would have been computed without one.
 *
 * <p>Integers narrower than 64 bits are read back as 64-bit, a float as a double, and a date-time with a
 * zone or an old-style date as the instant it names, which is how the function library treats them
 * anyway. A value of a kind not listed here is refused rather than stored as its text.
 */
final class StoredValues {

    private static final int NULL = 0;
    private static final int LONG = 1;
    private static final int DECIMAL = 2;
    private static final int DOUBLE = 3;
    private static final int TEXT = 4;
    private static final int BOOLEAN = 5;
    private static final int INSTANT = 6;
    private static final int LOCAL_DATE_TIME = 7;
    private static final int LOCAL_DATE = 8;
    private static final int LOCAL_TIME = 9;
    private static final int YEAR = 10;
    private static final int OFFSET_DATE_TIME = 11;
    private static final int BYTES = 12;
    private static final int LIST = 13;
    private static final int MAP = 14;

    private StoredValues() {
    }

    static void write(DataOutputStream out, Object value) throws IOException {
        switch (value) {
            case null -> out.writeByte(NULL);
            case Long l -> number(out, l);
            case Integer i -> number(out, i);
            case Short s -> number(out, s);
            case Byte b -> number(out, b);
            case BigDecimal d -> {
                out.writeByte(DECIMAL);
                out.writeInt(d.scale());
                bytes(out, d.unscaledValue().toByteArray());
            }
            case BigInteger i -> {
                out.writeByte(DECIMAL);
                out.writeInt(0);
                bytes(out, i.toByteArray());
            }
            case Double d -> {
                out.writeByte(DOUBLE);
                out.writeDouble(d);
            }
            case Float f -> {
                out.writeByte(DOUBLE);
                out.writeDouble(f);
            }
            case CharSequence text -> {
                out.writeByte(TEXT);
                bytes(out, text.toString().getBytes(java.nio.charset.StandardCharsets.UTF_16BE));
            }
            case Boolean flag -> {
                out.writeByte(BOOLEAN);
                out.writeBoolean(flag);
            }
            case Instant instant -> instant(out, instant);
            case ZonedDateTime at -> instant(out, at.toInstant());
            case Date date -> instant(out, date.toInstant());
            case OffsetDateTime at -> {
                out.writeByte(OFFSET_DATE_TIME);
                out.writeLong(at.toLocalDateTime().toEpochSecond(ZoneOffset.UTC));
                out.writeInt(at.getNano());
                out.writeInt(at.getOffset().getTotalSeconds());
            }
            case LocalDateTime at -> {
                out.writeByte(LOCAL_DATE_TIME);
                out.writeLong(at.toEpochSecond(ZoneOffset.UTC));
                out.writeInt(at.getNano());
            }
            case LocalDate date -> {
                out.writeByte(LOCAL_DATE);
                out.writeLong(date.toEpochDay());
            }
            case LocalTime time -> {
                out.writeByte(LOCAL_TIME);
                out.writeLong(time.toNanoOfDay());
            }
            case Year year -> {
                out.writeByte(YEAR);
                out.writeInt(year.getValue());
            }
            case byte[] raw -> {
                out.writeByte(BYTES);
                bytes(out, raw);
            }
            case List<?> list -> {
                out.writeByte(LIST);
                out.writeInt(list.size());
                for (Object element : list) {
                    write(out, element);
                }
            }
            case Map<?, ?> map -> {
                out.writeByte(MAP);
                out.writeInt(map.size());
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    write(out, String.valueOf(entry.getKey()));
                    write(out, entry.getValue());
                }
            }
            default -> throw new IllegalArgumentException(
                    "a value of " + value.getClass().getName() + " cannot be kept in aggregate state");
        }
    }

    static Object read(DataInputStream in) throws IOException {
        int kind = in.readUnsignedByte();
        return switch (kind) {
            case NULL -> null;
            case LONG -> in.readLong();
            case DECIMAL -> {
                int scale = in.readInt();
                yield new BigDecimal(new BigInteger(bytes(in)), scale);
            }
            case DOUBLE -> in.readDouble();
            case TEXT -> new String(bytes(in), java.nio.charset.StandardCharsets.UTF_16BE);
            case BOOLEAN -> in.readBoolean();
            case INSTANT -> Instant.ofEpochSecond(in.readLong(), in.readInt());
            case OFFSET_DATE_TIME -> {
                LocalDateTime local = LocalDateTime.ofEpochSecond(in.readLong(), in.readInt(), ZoneOffset.UTC);
                yield OffsetDateTime.of(local, ZoneOffset.ofTotalSeconds(in.readInt()));
            }
            case LOCAL_DATE_TIME -> LocalDateTime.ofEpochSecond(in.readLong(), in.readInt(), ZoneOffset.UTC);
            case LOCAL_DATE -> LocalDate.ofEpochDay(in.readLong());
            case LOCAL_TIME -> LocalTime.ofNanoOfDay(in.readLong());
            case YEAR -> Year.of(in.readInt());
            case BYTES -> bytes(in);
            case LIST -> {
                int size = in.readInt();
                List<Object> list = new ArrayList<>();
                for (int i = 0; i < size; i++) {
                    list.add(read(in));
                }
                yield list;
            }
            case MAP -> {
                int size = in.readInt();
                Map<String, Object> map = new LinkedHashMap<>();
                for (int i = 0; i < size; i++) {
                    String name = (String) read(in);
                    map.put(name, read(in));
                }
                yield map;
            }
            default -> throw new IOException("aggregate state holds a value of unknown kind " + kind);
        };
    }

    static void writeAll(DataOutputStream out, List<Object> values) throws IOException {
        out.writeInt(values.size());
        for (Object value : values) {
            write(out, value);
        }
    }

    static List<Object> readAll(DataInputStream in) throws IOException {
        int size = in.readInt();
        List<Object> values = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            values.add(read(in));
        }
        return values;
    }

    static void bytes(DataOutputStream out, byte[] bytes) throws IOException {
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    static byte[] bytes(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length < 0 || length > in.available()) {
            throw new IOException("aggregate state names " + length + " bytes where fewer are left");
        }
        return in.readNBytes(length);
    }

    private static void number(DataOutputStream out, long value) throws IOException {
        out.writeByte(LONG);
        out.writeLong(value);
    }

    private static void instant(DataOutputStream out, Instant instant) throws IOException {
        out.writeByte(INSTANT);
        out.writeLong(instant.getEpochSecond());
        out.writeInt(instant.getNano());
    }
}
