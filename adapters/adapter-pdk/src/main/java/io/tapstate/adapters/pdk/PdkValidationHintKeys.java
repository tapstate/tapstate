package io.tapstate.adapters.pdk;

import io.tapdata.entity.schema.TapTable;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationKey;
import io.tapstate.core.validation.ValidationKeyBinding;
import io.tapstate.core.validation.ValidationRootHintBatch.Change;
import io.tapstate.core.validation.ValidationRootHintBatch.ChangeKind;
import io.tapstate.core.validation.ValidationValues;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Scalar physical-column key extraction from the actual supplied change images. Only selected fields
 * reach the independent codecs; unrelated business values are neither traversed nor retained. Each
 * selected value is owned before returning. Missing old keys are never recovered from a new image,
 * current source query, target, or query request. No production transformation is an expected answer.
 *
 * <p>The caller admits the actual capture/model/codec contract and its allocation/time envelope first.
 * A decoded event alone does not prove that a custom capture codec retained the original key facts.
 * Do not install arbitrary registered codecs on a capture thread without a bounded nonblocking
 * contract. This helper does not subscribe, transport, establish coverage, or acknowledge source work.
 */
final class PdkValidationHintKeys {
    private final String node;
    private final ValidationKeyBinding binding;
    private final List<String> fields;
    private final PdkValidationRows decoder;
    private final long reservationBytes;

    PdkValidationHintKeys(String node, TapTable table, PdkValidationCodecs codecs,
                          ValidationKeyBinding binding, Map<String, PdkValidationRows.Rule> rules,
                          ValidationValues.Limits limits) {
        this.node = ValidationError.fieldName(node, "hint.node");
        if (node.isBlank()) throw invalid("hint.node");
        this.binding = Objects.requireNonNull(binding, "binding");
        Objects.requireNonNull(table, "table"); Objects.requireNonNull(rules, "rules");
        var selected = new LinkedHashMap<String, PdkValidationRows.Rule>();
        for (var component : binding.components()) {
            if (component.path().size() != 1) throw unsupported(component.path().toString(), "hint.scalar-column-required");
            String field = component.path().getFirst();
            if (table.getNameFieldMap() == null || table.getNameFieldMap().get(field) == null) {
                throw unsupported(field, "hint.key-model-missing");
            }
            PdkValidationRows.Rule rule = rules.get(field);
            if (rule == null || !compatible(component, rule.shape())) throw unsupported(field, "hint.explicit-key-shape-required");
            selected.put(field, rule);
        }
        fields = List.copyOf(selected.keySet());
        Map<String, PdkValidationRows.Rule> frozen = Map.copyOf(selected);
        decoder = new PdkValidationRows(table, codecs, path -> {
            if (path.size() != 1 || !(path.getFirst() instanceof String name) || !frozen.containsKey(name)) {
                throw unsupported(path.toString(), "hint.scalar-column-required");
            }
            return frozen.get(name);
        }, limits);
        // Images are decoded sequentially; only the first owned key survives decoding the second.
        reservationBytes = PdkValidationRows.workReservationBytes(limits) + 4L * ValidationKey.MAX_KEY_BYTES + 64L * 1024;
    }

    long workReservationBytes() { return reservationBytes; }

    Change extract(ChangeKind kind, Map<String, Object> before, Map<String, Object> after) {
        Objects.requireNonNull(kind, "kind");
        return switch (kind) {
            case INSERT -> new Change(kind, null, read(after, "after"));
            case UPDATE -> new Change(kind, read(before, "before"), read(after, "after"));
            case DELETE -> new Change(kind, read(before, "before"), null);
        };
    }

    private ValidationKey read(Map<String, Object> image, String side) {
        if (image == null) throw unsupported(side, "hint.image-missing");
        var selected = new LinkedHashMap<String, Object>();
        for (String field : fields) if (image.containsKey(field)) selected.put(field, image.get(field));
        var result = binding.read(decoder.decode(selected));
        if (result instanceof ValidationKeyBinding.Bound bound) return bound.key();
        var problem = ((ValidationKeyBinding.Invalid) result).problems().getFirst();
        throw unsupported(side + ":" + problem.path(), "hint.key-" + problem.kind().name().toLowerCase(java.util.Locale.ROOT));
    }

    private static boolean compatible(ValidationKeyBinding.Component component, PdkValidationRows.Shape shape) {
        if (component.domain() == ValidationKey.Kind.EXACT_NUMBER) {
            return shape == PdkValidationRows.Shape.EXACT_NUMBER
                    || component.numericPolicy() == ValidationKeyBinding.NumericPolicy.EXACT_BINARY_FLOAT
                    && (shape == PdkValidationRows.Shape.FLOAT32 || shape == PdkValidationRows.Shape.FLOAT64);
        }
        return switch (component.domain()) {
            case TEXT -> shape == PdkValidationRows.Shape.TEXT;
            case BOOLEAN -> shape == PdkValidationRows.Shape.BOOLEAN;
            case BINARY -> shape == PdkValidationRows.Shape.BINARY;
            case DATE -> shape == PdkValidationRows.Shape.DATE;
            case TIME -> shape == PdkValidationRows.Shape.TIME;
            case LOCAL_DATE_TIME -> shape == PdkValidationRows.Shape.LOCAL_DATE_TIME;
            case INSTANT -> shape == PdkValidationRows.Shape.INSTANT;
            case EXACT_NUMBER -> throw new IllegalStateException("numeric domain must be handled separately");
        };
    }
    private TapstateException unsupported(String field, String rule) {
        return new TapstateException(ValidationError.RULE_UNSUPPORTED, Map.of("node", node, "field", field, "rule", rule), null);
    }
    private static TapstateException invalid(String field) {
        return new TapstateException(ValidationError.INVALID_CONTRACT, Map.of("field", field), null);
    }
}
