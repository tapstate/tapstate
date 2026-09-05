package io.tapstate.adapters.pdk;

import io.tapstate.core.common.TapstateException;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The bytes a connector's own notes are stored as: a format version, a type tag, and a payload.
 *
 * <p>The tag is the whole point. What a connector writes for itself is read back with a runtime type
 * check - {@code instanceof byte[]} for a compressed schema history, {@code instanceof Long} for a
 * checkpoint - and an encoding that widens or narrows a value fails those checks silently. A history
 * that came back as text is not an error anywhere; it is a change stream that quietly starts over. So
 * the set of types is closed and a value outside it is refused where it is written, loudly, rather than
 * stringified into something that reads back plausibly.
 *
 * <p>Every value is self-delimiting, so a list or a map holds its elements inline with no separate
 * length table. The type set grows by taking a new tag and never by changing what an issued tag means;
 * the version byte moves only if an issued tag's payload layout has to change, which is what keeps
 * bytes already on disk readable.
 */
final class ConnectorStateCodec {

    /** The format the payload layouts below belong to. Only bumped if an issued tag's layout changes. */
    private static final byte VERSION = 0x01;

    private static final byte STRING = 0x01;
    private static final byte LONG = 0x02;
    private static final byte INTEGER = 0x03;
    private static final byte BOOLEAN = 0x04;
    private static final byte DOUBLE = 0x05;
    private static final byte BYTES = 0x06;
    private static final byte LIST = 0x07;
    private static final byte MAP = 0x08;

    private ConnectorStateCodec() {
    }

    /** The bytes for {@code value}. Refuses a type outside the closed set rather than coercing it. */
    static byte[] encode(Object value) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeByte(VERSION);
            writeValue(out, value);
        } catch (IOException e) {
            // A ByteArrayOutputStream does not do IO; this cannot happen and is not a coded condition.
            throw new UncheckedIOException(e);
        }
        return bytes.toByteArray();
    }

    /** The value {@code encoded} was made from, as the type it was written as. */
    static Object decode(byte[] encoded) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(encoded))) {
            byte version = in.readByte();
            if (version != VERSION) {
                throw unreadable("format version " + version + " was written by a later build");
            }
            return readValue(in);
        } catch (IOException e) {
            throw unreadable("truncated: " + e);
        }
    }

    private static void writeValue(DataOutputStream out, Object value) throws IOException {
        switch (value) {
            // A null never reaches here: storing one is a removal, decided a layer up. Named anyway, so
            // a caller that gets it wrong is refused by code rather than by a bare NPE from the switch.
            case null -> throw unsupported(null);
            case String s -> {
                out.writeByte(STRING);
                writeBytes(out, s.getBytes(StandardCharsets.UTF_8));
            }
            case Long l -> {
                out.writeByte(LONG);
                out.writeLong(l);
            }
            case Integer i -> {
                out.writeByte(INTEGER);
                out.writeInt(i);
            }
            case Boolean b -> {
                out.writeByte(BOOLEAN);
                out.writeBoolean(b);
            }
            case Double d -> {
                out.writeByte(DOUBLE);
                out.writeDouble(d);
            }
            case byte[] b -> {
                out.writeByte(BYTES);
                writeBytes(out, b);
            }
            case List<?> list -> {
                out.writeByte(LIST);
                out.writeInt(list.size());
                for (Object element : list) {
                    writeValue(out, element);
                }
            }
            case Map<?, ?> map -> {
                out.writeByte(MAP);
                out.writeInt(map.size());
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    // A key that is not text has no rendering this codec is entitled to invent.
                    if (!(entry.getKey() instanceof String key)) {
                        throw unsupported(entry.getKey());
                    }
                    writeBytes(out, key.getBytes(StandardCharsets.UTF_8));
                    writeValue(out, entry.getValue());
                }
            }
            default -> throw unsupported(value);
        }
    }

    private static Object readValue(DataInputStream in) throws IOException {
        byte tag = in.readByte();
        return switch (tag) {
            case STRING -> new String(readBytes(in), StandardCharsets.UTF_8);
            case LONG -> in.readLong();
            case INTEGER -> in.readInt();
            case BOOLEAN -> in.readBoolean();
            case DOUBLE -> in.readDouble();
            case BYTES -> readBytes(in);
            case LIST -> {
                int size = readSize(in);
                List<Object> list = new ArrayList<>(size);
                for (int i = 0; i < size; i++) {
                    list.add(readValue(in));
                }
                yield List.copyOf(list);
            }
            case MAP -> {
                int size = readSize(in);
                Map<String, Object> map = new LinkedHashMap<>();
                for (int i = 0; i < size; i++) {
                    map.put(new String(readBytes(in), StandardCharsets.UTF_8), readValue(in));
                }
                yield map;
            }
            default -> throw unreadable("type tag " + tag + " was written by a later build");
        };
    }

    private static void writeBytes(DataOutputStream out, byte[] bytes) throws IOException {
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static byte[] readBytes(DataInputStream in) throws IOException {
        int size = readSize(in);
        byte[] bytes = in.readNBytes(size);
        // readNBytes stops short rather than throwing, so a truncated payload would otherwise come back
        // as a shorter value that looks like what was written.
        if (bytes.length != size) {
            throw unreadable("truncated: wanted " + size + " bytes, got " + bytes.length);
        }
        return bytes;
    }

    /**
     * A length read out of the bytes, checked before it is used to size anything. A corrupt or hostile
     * length would otherwise be an allocation of whatever it says.
     */
    private static int readSize(DataInputStream in) throws IOException {
        int size = in.readInt();
        if (size < 0) {
            throw unreadable("negative length " + size);
        }
        return size;
    }

    private static TapstateException unsupported(Object value) {
        return new TapstateException(ConnectorError.STATE_VALUE_UNSUPPORTED,
                Map.of("type", value == null ? "null" : value.getClass().getName()), null);
    }

    private static TapstateException unreadable(String detail) {
        return new TapstateException(ConnectorError.STATE_UNREADABLE, Map.of("detail", detail), null);
    }
}
