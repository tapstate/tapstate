package io.tapstate.adapters.mongostore;

import java.nio.ByteBuffer;
import java.util.Base64;

/** Lossless opaque UTF-16 text in BSON string fields, including unpaired code units. */
final class OperatorStateText {
    private OperatorStateText() {}

    static String encode(String value) {
        var bytes = ByteBuffer.allocate(Math.multiplyExact(value.length(), 2));
        for (int index = 0; index < value.length(); index++) bytes.putChar(value.charAt(index));
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes.array());
    }

    static String decode(String encoded) {
        byte[] bytes = Base64.getUrlDecoder().decode(encoded);
        if (bytes.length % 2 != 0 || !Base64.getUrlEncoder().withoutPadding().encodeToString(bytes).equals(encoded)) {
            throw new IllegalArgumentException("invalid opaque text");
        }
        var buffer = ByteBuffer.wrap(bytes); char[] units = new char[bytes.length / 2];
        for (int index = 0; index < units.length; index++) units[index] = buffer.getChar();
        return new String(units);
    }
}
