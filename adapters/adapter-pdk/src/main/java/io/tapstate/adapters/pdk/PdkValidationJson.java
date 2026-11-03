package io.tapstate.adapters.pdk;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.core.exc.StreamConstraintsException;
import com.fasterxml.jackson.core.util.JsonRecyclerPools;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationValues;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Strict structural decoding only for text already declared JSON by the selected logical rule.
 * Ordinary text never enters this parser. Integers and decimals stay exact; object order is retained
 * for diagnostics and array order is never changed. The caller separately admits the raw text,
 * parser working buffers and subsequent row ownership. These local charges bound the parsed tree,
 * not a driver's earlier allocation or the precise heap footprint of the parser.
 */
final class PdkValidationJson {
    private PdkValidationJson() {}

    static Object parse(String text, String connector, String field, ValidationValues.Limits limits) {
        Objects.requireNonNull(text, "text");
        Objects.requireNonNull(connector, "connector");
        Objects.requireNonNull(field, "field");
        Objects.requireNonNull(limits, "limits");
        if (2L * text.length() > limits.bytes()) throw limit("jsonInputBytes");
        int tokenCharacters = (int) Math.max(1, Math.min(limits.bytes() / 2, Integer.MAX_VALUE));
        JsonFactory factory = JsonFactory.builder()
                // A previous large row must not leave buffers retained on a shared decoding worker
                // after its run's work reservation has been released.
                .recyclerPool(JsonRecyclerPools.nonRecyclingPool())
                .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
                .disable(StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION)
                .disable(JsonFactory.Feature.INTERN_FIELD_NAMES)
                .disable(JsonFactory.Feature.CANONICALIZE_FIELD_NAMES)
                .streamReadConstraints(StreamReadConstraints.builder()
                        .maxNestingDepth(limits.depth()).maxDocumentLength(text.length())
                        .maxTokenCount(3L * limits.nodes() + 2)
                        .maxNumberLength(Math.min(tokenCharacters, 4096))
                        .maxStringLength(tokenCharacters).maxNameLength(tokenCharacters).build())
                .build();
        try (JsonParser parser = factory.createParser(text)) {
            JsonToken first = parser.nextToken();
            if (first == null) throw new IOException("JSON has no value");
            Object value = read(parser, first, 1, new Meter(limits));
            if (parser.nextToken() != null) throw new IOException("JSON has trailing content");
            return value;
        } catch (StreamConstraintsException exceeded) {
            throw new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED, Map.of("limit", "jsonParser"), exceeded);
        } catch (IOException | NumberFormatException invalid) {
            throw new TapstateException(ValidationError.CODEC_FAILED,
                    Map.of("connector", connector, "field", field), invalid);
        }
    }

    private static Object read(JsonParser parser, JsonToken token, int depth, Meter meter) throws IOException {
        meter.node(depth);
        return switch (token) {
            case VALUE_NULL -> null;
            case VALUE_TRUE -> { meter.charge(48); yield true; }
            case VALUE_FALSE -> { meter.charge(48); yield false; }
            case VALUE_STRING -> {
                // Text length is checked before asking Jackson to materialize the String value.
                meter.charge(24L + 2L * parser.getTextLength());
                String value = parser.getText();
                unicode(value);
                yield value;
            }
            case VALUE_NUMBER_INT -> {
                meter.charge(32L + parser.getTextLength());
                BigInteger value = parser.getBigIntegerValue();
                yield value;
            }
            case VALUE_NUMBER_FLOAT -> {
                meter.charge(48L + parser.getTextLength());
                BigDecimal value = parser.getDecimalValue();
                // A short exponent must not admit an unbounded expansion at later numeric boundaries.
                long digits = Math.max(value.precision(), Math.max(Math.abs((long) value.scale()),
                        (long) value.precision() - value.scale()));
                if (digits > meter.limits.bytes() / 2) throw limit("jsonNumberMagnitude");
                yield value;
            }
            case START_OBJECT -> {
                meter.charge(64);
                Map<String, Object> values = new LinkedHashMap<>();
                for (JsonToken next = parser.nextToken(); next != JsonToken.END_OBJECT; next = parser.nextToken()) {
                    if (next != JsonToken.FIELD_NAME) throw new IOException("JSON object has no field name");
                    String name = parser.currentName();
                    meter.charge(72L + 2L * name.length());
                    unicode(name);
                    if (values.containsKey(name)) throw new IOException("JSON has a duplicate field name");
                    JsonToken child = parser.nextToken();
                    if (child == null) throw new IOException("JSON object has no field value");
                    values.put(name, read(parser, child, depth + 1, meter));
                }
                yield Collections.unmodifiableMap(values);
            }
            case START_ARRAY -> {
                meter.charge(32);
                ArrayList<Object> values = new ArrayList<>();
                for (JsonToken next = parser.nextToken(); next != JsonToken.END_ARRAY; next = parser.nextToken()) {
                    if (next == null) throw new IOException("JSON array is incomplete");
                    meter.charge(8);
                    values.add(read(parser, next, depth + 1, meter));
                }
                yield Collections.unmodifiableList(values);
            }
            default -> throw new IOException("Unexpected JSON token");
        };
    }

    private static void unicode(String text) throws IOException {
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (Character.isHighSurrogate(c)) {
                if (++i == text.length() || !Character.isLowSurrogate(text.charAt(i)))
                    throw new IOException("JSON contains an unpaired surrogate");
            } else if (Character.isLowSurrogate(c)) throw new IOException("JSON contains an unpaired surrogate");
        }
    }

    private static TapstateException limit(String dimension) {
        return new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED, Map.of("limit", dimension), null);
    }

    private static final class Meter {
        private final ValidationValues.Limits limits;
        private long bytes;
        private int nodes;
        private Meter(ValidationValues.Limits limits) { this.limits = limits; }
        private void node(int depth) {
            if (depth > limits.depth()) throw limit("depth");
            if (++nodes > limits.nodes()) throw limit("nodes");
            charge(16);
        }
        private void charge(long amount) {
            if (amount > limits.bytes() - bytes) throw limit("bytes");
            bytes += amount;
        }
    }
}
