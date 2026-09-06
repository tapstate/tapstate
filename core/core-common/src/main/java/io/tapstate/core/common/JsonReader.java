package io.tapstate.core.common;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A minimal, dependency-free JSON reader for the core ring. The core ring ships no third-party
 * library (rule R1), so this hand-rolls the small parser runtime loaders need. It produces the same
 * tree shape a JSON library would
 * ({@code Map}/{@code List}/{@code String}/{@code Long}/{@code Double}/{@code BigInteger}/{@code BigDecimal}/
 * {@code Boolean}/{@code null}),
 * preserving object key order ({@link LinkedHashMap}) so consumers stay deterministic. Lives in
 * core-common because more than one core module needs it (rule R1 admission) and it carries no
 * business semantics.
 */
public final class JsonReader {

    private JsonReader() {
    }

    /** Parses a JSON document into a {@code Map}/{@code List}/scalar tree. */
    public static Object parse(String json) {
        Parser parser = new Parser(json);
        parser.skipWhitespace();
        Object value = parser.readValue(0);
        parser.skipWhitespace();
        if (!parser.atEnd()) {
            throw parser.error("trailing content after JSON value");
        }
        return value;
    }

    /** A single-use recursive-descent cursor over the input. */
    private static final class Parser {

        /** Bounds recursion so pathological nesting fails with an IllegalArgumentException, not a
         *  StackOverflowError. The documents read here are shallow; this is generous headroom. */
        private static final int MAX_DEPTH = 200;

        private final String src;
        private int pos;

        Parser(String src) {
            this.src = src;
        }

        Object readValue(int depth) {
            if (atEnd()) {
                throw error("unexpected end of input");
            }
            char c = src.charAt(pos);
            return switch (c) {
                case '{' -> readObject(depth);
                case '[' -> readArray(depth);
                case '"' -> readString();
                case 't', 'f' -> readBoolean();
                case 'n' -> readNull();
                default -> readNumber(c);
            };
        }

        private Map<String, Object> readObject(int depth) {
            if (depth >= MAX_DEPTH) {
                throw error("nesting too deep");
            }
            expect('{');
            Map<String, Object> object = new LinkedHashMap<>();
            skipWhitespace();
            if (peek() == '}') {
                pos++;
                return object;
            }
            while (true) {
                skipWhitespace();
                if (peek() != '"') {
                    throw error("expected object key string");
                }
                String key = readString();
                skipWhitespace();
                expect(':');
                skipWhitespace();
                object.put(key, readValue(depth + 1));
                skipWhitespace();
                char c = next();
                if (c == '}') {
                    return object;
                }
                if (c != ',') {
                    throw error("expected ',' or '}' in object");
                }
            }
        }

        private List<Object> readArray(int depth) {
            if (depth >= MAX_DEPTH) {
                throw error("nesting too deep");
            }
            expect('[');
            List<Object> array = new ArrayList<>();
            skipWhitespace();
            if (peek() == ']') {
                pos++;
                return array;
            }
            while (true) {
                skipWhitespace();
                array.add(readValue(depth + 1));
                skipWhitespace();
                char c = next();
                if (c == ']') {
                    return array;
                }
                if (c != ',') {
                    throw error("expected ',' or ']' in array");
                }
            }
        }

        private String readString() {
            expect('"');
            StringBuilder sb = new StringBuilder();
            while (true) {
                if (atEnd()) {
                    throw error("unterminated string");
                }
                char c = src.charAt(pos++);
                if (c == '"') {
                    return sb.toString();
                }
                if (c == '\\') {
                    sb.append(readEscape());
                } else {
                    sb.append(c);
                }
            }
        }

        private char readEscape() {
            if (atEnd()) {
                throw error("unterminated escape");
            }
            char c = src.charAt(pos++);
            return switch (c) {
                case '"' -> '"';
                case '\\' -> '\\';
                case '/' -> '/';
                case 'b' -> '\b';
                case 'f' -> '\f';
                case 'n' -> '\n';
                case 'r' -> '\r';
                case 't' -> '\t';
                case 'u' -> readUnicodeEscape();
                default -> throw error("invalid string escape \\" + c);
            };
        }

        private char readUnicodeEscape() {
            if (pos + 4 > src.length()) {
                throw error("truncated \\u escape");
            }
            String hex = src.substring(pos, pos + 4);
            pos += 4;
            try {
                return (char) Integer.parseInt(hex, 16);
            } catch (NumberFormatException e) {
                throw error("invalid \\u escape: " + hex);
            }
        }

        private Boolean readBoolean() {
            if (src.startsWith("true", pos)) {
                pos += 4;
                return Boolean.TRUE;
            }
            if (src.startsWith("false", pos)) {
                pos += 5;
                return Boolean.FALSE;
            }
            throw error("invalid literal");
        }

        private Object readNull() {
            if (src.startsWith("null", pos)) {
                pos += 4;
                return null;
            }
            throw error("invalid literal");
        }

        private Object readNumber(char first) {
            if (first != '-' && (first < '0' || first > '9')) {
                throw error("unexpected character '" + first + "'");
            }
            int start = pos;
            boolean floating = false;
            while (!atEnd()) {
                char c = src.charAt(pos);
                if (c == '.' || c == 'e' || c == 'E') {
                    floating = true;
                    pos++;
                } else if ((c >= '0' && c <= '9') || c == '-' || c == '+') {
                    pos++;
                } else {
                    break;
                }
            }
            String number = src.substring(start, pos);
            try {
                if (floating) {
                    double value = Double.parseDouble(number);
                    // A magnitude past what a double holds is kept exactly, the same call this makes one
                    // line down for an integer past long. Left as the infinity the parse produces, its
                    // text form is the word "Infinity" - not a number at all - and whatever reads the
                    // value back rejects it, dropping the thing it was part of rather than the value.
                    return Double.isInfinite(value) ? new java.math.BigDecimal(number) : value;
                }
                try {
                    return Long.parseLong(number);
                } catch (NumberFormatException overflow) {
                    // An integer beyond Long's range (e.g. an unsigned-64-bit spec default) is kept
                    // exactly as a BigInteger rather than failing or losing precision.
                    return new java.math.BigInteger(number);
                }
            } catch (NumberFormatException e) {
                throw error("invalid number '" + number + "'");
            }
        }

        void skipWhitespace() {
            while (!atEnd()) {
                char c = src.charAt(pos);
                if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
                    pos++;
                } else {
                    break;
                }
            }
        }

        boolean atEnd() {
            return pos >= src.length();
        }

        private char peek() {
            if (atEnd()) {
                throw error("unexpected end of input");
            }
            return src.charAt(pos);
        }

        private char next() {
            if (atEnd()) {
                throw error("unexpected end of input");
            }
            return src.charAt(pos++);
        }

        private void expect(char c) {
            if (atEnd() || src.charAt(pos) != c) {
                throw error("expected '" + c + "'");
            }
            pos++;
        }

        IllegalArgumentException error(String message) {
            return new IllegalArgumentException(message + " at position " + pos);
        }
    }
}
