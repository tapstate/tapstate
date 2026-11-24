package io.tapstate.app;

import io.tapstate.core.validation.ValidationValues;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Private encoding for already-owned rows in disposable validation sort files. */
final class ValidationSpillRows {
    private ValidationSpillRows() {}

    static void write(DataOutputStream out, Object value) throws IOException {
        switch (value) {
            case null -> out.writeByte(0);
            case String text -> { out.writeByte(1); text(out, text); }
            case Boolean bool -> { out.writeByte(2); out.writeBoolean(bool); }
            case Byte number -> { out.writeByte(3); out.writeByte(number); }
            case Short number -> { out.writeByte(4); out.writeShort(number); }
            case Integer number -> { out.writeByte(5); out.writeInt(number); }
            case Long number -> { out.writeByte(6); out.writeLong(number); }
            case Float number -> { out.writeByte(7); out.writeInt(Float.floatToRawIntBits(number)); }
            case Double number -> { out.writeByte(8); out.writeLong(Double.doubleToRawLongBits(number)); }
            case BigInteger number -> { out.writeByte(9); bytes(out, number.toByteArray()); }
            case BigDecimal number -> {
                out.writeByte(10);
                out.writeInt(number.scale());
                bytes(out, number.unscaledValue().toByteArray());
            }
            case Instant instant -> { out.writeByte(11); out.writeLong(instant.getEpochSecond()); out.writeInt(instant.getNano()); }
            case LocalDate date -> { out.writeByte(12); out.writeLong(date.toEpochDay()); }
            case LocalTime time -> { out.writeByte(13); out.writeLong(time.toNanoOfDay()); }
            case LocalDateTime dateTime -> {
                out.writeByte(14);
                out.writeLong(dateTime.toLocalDate().toEpochDay());
                out.writeLong(dateTime.toLocalTime().toNanoOfDay());
            }
            case ValidationValues.Binary binary -> { out.writeByte(15); out.writeInt(binary.subtype()); bytes(out, binary.bytes()); }
            case ValidationValues.Typed typed -> {
                out.writeByte(16);
                text(out, typed.logicalType());
                out.writeBoolean(typed.nativeType() != null);
                if (typed.nativeType() != null) text(out, typed.nativeType());
                text(out, typed.codecBinding());
                text(out, typed.conversionOrigin());
                write(out, typed.value());
            }
            case Map<?, ?> map -> {
                out.writeByte(17);
                out.writeInt(map.size());
                for (var entry : map.entrySet()) {
                    text(out, (String) entry.getKey());
                    write(out, entry.getValue());
                }
            }
            case List<?> list -> {
                out.writeByte(18);
                out.writeInt(list.size());
                for (Object child : list) write(out, child);
            }
            default -> throw new IllegalArgumentException("unowned validation spill value");
        }
    }

    static ValidationValues.Row readRow(DataInputStream in, ValidationValues.Limits limits) throws IOException {
        Object value = read(in, limits, 1);
        if (!(value instanceof Map<?, ?> map)) throw new IOException("invalid spilled row");
        @SuppressWarnings("unchecked")
        var fields = (Map<String, Object>) map;
        return ValidationValues.ownRow(fields, limits);
    }

    private static Object read(DataInputStream in, ValidationValues.Limits limits, int depth) throws IOException {
        if (depth > limits.depth()) throw new IOException("invalid spilled value depth");
        return switch (in.readUnsignedByte()) {
            case 0 -> null;
            case 1 -> text(in, limits);
            case 2 -> in.readBoolean();
            case 3 -> in.readByte();
            case 4 -> in.readShort();
            case 5 -> in.readInt();
            case 6 -> in.readLong();
            case 7 -> Float.intBitsToFloat(in.readInt());
            case 8 -> Double.longBitsToDouble(in.readLong());
            case 9 -> new BigInteger(bytes(in, limits));
            case 10 -> {
                int scale = in.readInt();
                yield new BigDecimal(new BigInteger(bytes(in, limits)), scale);
            }
            case 11 -> Instant.ofEpochSecond(in.readLong(), in.readInt());
            case 12 -> LocalDate.ofEpochDay(in.readLong());
            case 13 -> LocalTime.ofNanoOfDay(in.readLong());
            case 14 -> LocalDate.ofEpochDay(in.readLong()).atTime(LocalTime.ofNanoOfDay(in.readLong()));
            case 15 -> new ValidationValues.Binary(in.readInt(), bytes(in, limits));
            case 16 -> new ValidationValues.Typed(text(in, limits), in.readBoolean() ? text(in, limits) : null,
                    text(in, limits), text(in, limits), read(in, limits, depth + 1));
            case 17 -> {
                int count = size(in, limits.nodes());
                Map<String, Object> map = new LinkedHashMap<>();
                for (int i = 0; i < count; i++) map.put(text(in, limits), read(in, limits, depth + 1));
                yield map;
            }
            case 18 -> {
                int count = size(in, limits.nodes());
                List<Object> list = new ArrayList<>();
                for (int i = 0; i < count; i++) list.add(read(in, limits, depth + 1));
                yield list;
            }
            default -> throw new IOException("invalid spilled value tag");
        };
    }

    private static void text(DataOutputStream out, String text) throws IOException {
        out.writeInt(text.length());
        out.writeChars(text);
    }

    private static String text(DataInputStream in, ValidationValues.Limits limits) throws IOException {
        char[] chars = new char[size(in, limits.bytes() / 2)];
        for (int i = 0; i < chars.length; i++) chars[i] = in.readChar();
        return new String(chars);
    }

    private static void bytes(DataOutputStream out, byte[] bytes) throws IOException {
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static byte[] bytes(DataInputStream in, ValidationValues.Limits limits) throws IOException {
        var bytes = new byte[size(in, limits.bytes())];
        in.readFully(bytes);
        return bytes;
    }

    private static int size(DataInputStream in, long limit) throws IOException {
        int size = in.readInt();
        if (size < 0 || size > limit) throw new IOException("invalid spilled value length");
        return size;
    }
}
