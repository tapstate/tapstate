package io.tapstate.core.common;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The compact, dependency-free JSON writer that is the core ring's counterpart to {@link JsonReader}:
 * it renders a {@code Map}/{@code List}/scalar tree back to a compact JSON string (no indentation, no
 * inter-token spaces), preserving object key order, so a frame serialised here parses back to the same
 * tree through {@link JsonReader}. The escaping cases are the load-bearing part — an arbitrary log
 * message must survive the round trip byte-for-byte.
 */
class JsonWriterTest {

    @Test
    void writesStringScalarQuotedAndUnescapedWhenPlain() {
        assertThat(JsonWriter.write("hello")).isEqualTo("\"hello\"");
    }

    @Test
    void writesLongAndDoubleAndBooleanAndNullScalars() {
        assertThat(JsonWriter.write(1_700_000_000_000L)).isEqualTo("1700000000000");
        assertThat(JsonWriter.write(42)).isEqualTo("42");
        assertThat(JsonWriter.write(Boolean.TRUE)).isEqualTo("true");
        assertThat(JsonWriter.write(Boolean.FALSE)).isEqualTo("false");
        assertThat(JsonWriter.write(null)).isEqualTo("null");
    }

    @Test
    void escapesTheJsonMetacharactersInAString() {
        // quote, backslash, and the named control escapes
        assertThat(JsonWriter.write("a\"b")).isEqualTo("\"a\\\"b\"");
        assertThat(JsonWriter.write("a\\b")).isEqualTo("\"a\\\\b\"");
        assertThat(JsonWriter.write("line1\nline2")).isEqualTo("\"line1\\nline2\"");
        assertThat(JsonWriter.write("a\tb")).isEqualTo("\"a\\tb\"");
        assertThat(JsonWriter.write("a\rb\bc\fd")).isEqualTo("\"a\\rb\\bc\\fd\"");
    }

    @Test
    void escapesOtherControlCharactersAsFourDigitUnicode() {
        // a control character with no named escape (U+0001) escapes as four hex digits, lower-case and zero-padded
        assertThat(JsonWriter.write("x\u0001y")).isEqualTo("\"x\\u0001y\"");
    }

    @Test
    void writesAnObjectCompactPreservingKeyOrder() {
        Map<String, Object> object = new LinkedHashMap<>();
        object.put("pipelineId", "orders");
        object.put("state", "RUNNING");
        assertThat(JsonWriter.write(object)).isEqualTo("{\"pipelineId\":\"orders\",\"state\":\"RUNNING\"}");
    }

    @Test
    void writesAnArrayCompact() {
        assertThat(JsonWriter.write(List.of("a", "b"))).isEqualTo("[\"a\",\"b\"]");
    }

    @Test
    void writesEmptyObjectAndEmptyArray() {
        assertThat(JsonWriter.write(new LinkedHashMap<>())).isEqualTo("{}");
        assertThat(JsonWriter.write(List.of())).isEqualTo("[]");
    }

    @Test
    void writesNestedStructure() {
        Map<String, Object> line = new LinkedHashMap<>();
        line.put("timestampMillis", 1L);
        line.put("level", "INFO");
        line.put("message", "started");
        Map<String, Object> frame = new LinkedHashMap<>();
        frame.put("pipelineId", "p");
        frame.put("lines", List.of(line));
        assertThat(JsonWriter.write(frame)).isEqualTo(
                "{\"pipelineId\":\"p\",\"lines\":[{\"timestampMillis\":1,\"level\":\"INFO\",\"message\":\"started\"}]}");
    }

    @Test
    void roundTripsAnArbitraryMessageThroughTheReader() {
        String message = "tab\there, quote\", backslash\\, newline\n, ctrl\u0002, unicode é end";
        Map<String, Object> line = new LinkedHashMap<>();
        line.put("timestampMillis", 1_700_000_000_123L);
        line.put("level", "WARN");
        line.put("message", message);

        Object reparsed = JsonReader.parse(JsonWriter.write(line));

        assertThat(reparsed).isInstanceOf(Map.class);
        assertThat(((Map<?, ?>) reparsed).get("message")).isEqualTo(message);
        assertThat(((Map<?, ?>) reparsed).get("timestampMillis")).isEqualTo(1_700_000_000_123L);
    }

    @Test
    void rejectsAnUnsupportedValueType() {
        assertThatThrownBy(() -> JsonWriter.write(new Object()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void writesADecimalBeyondDoublesRangeBackAsANumberThatParses() {
        // The round trip this class promises has to hold for every type the reader produces, and a
        // magnitude past a double is one of them. Written from the infinity it used to parse as, the
        // output was the bare word "Infinity", which is not json at all.
        Object parsed = JsonReader.parse("1E+6145");

        assertThat(JsonReader.parse(JsonWriter.write(parsed))).isEqualTo(parsed);
    }
}
