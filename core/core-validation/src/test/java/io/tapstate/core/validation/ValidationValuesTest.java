package io.tapstate.core.validation;

import io.tapstate.core.common.TapstateException;
import java.math.BigDecimal;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class ValidationValuesTest {
    private static final ValidationValues.Limits LIMITS = new ValidationValues.Limits(65536, 1000, 16);

    @Test void duplicateTextualFieldNamesCannotOverwriteFactsIncludingNull() {
        var duplicate = new IdentityHashMap<String, Object>();
        duplicate.put(new String("id"), null);
        duplicate.put(new String("id"), 2);
        assertThat(duplicate).hasSize(2);
        assertThatThrownBy(() -> ValidationValues.ownRow(duplicate, LIMITS))
                .isInstanceOfSatisfying(TapstateException.class,
                        e -> assertThat(e.args()).containsEntry("field", "value.duplicateField"));
    }

    @Test void ownsNestedRowsWithoutChangingPresencePrecisionOrCodecProvenance() {
        Map<String, Object> child = new LinkedHashMap<>();
        child.put("presentNull", null);
        child.put("price", new ValidationValues.Typed("exact-number", "NUMERIC", "pg-codec-v1",
                new BigDecimal("9007199254740993.1200")));
        var children = new ArrayList<Object>();
        children.add(child);
        children.add(null);
        var row = ValidationValues.ownRow(Map.of("children", children), LIMITS);
        child.put("price", 0);
        children.clear();
        var frozenChildren = (List<?>) row.fields().get("children");
        var frozenChild = (Map<?, ?>) frozenChildren.getFirst();
        assertThat(frozenChildren).hasSize(2);
        assertThat(frozenChildren.get(1)).isNull();
        assertThat(frozenChild.containsKey("presentNull")).isTrue();
        assertThat(frozenChild.containsKey("missing")).isFalse();
        assertThat(frozenChild.get("price")).isEqualTo(new ValidationValues.Typed(
                "exact-number", "NUMERIC", "pg-codec-v1", new BigDecimal("9007199254740993.1200")));
        assertThatThrownBy(() -> row.fields().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(frozenChildren::clear).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(frozenChild::clear).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test void immutableBinaryKeepsSubtypeAndDefendsBothInputAndOutput() {
        byte[] bytes = {1, 2, 3};
        var binary = new ValidationValues.Binary(4, bytes);
        var row = ValidationValues.ownRow(Map.of("uuid", binary), LIMITS);
        bytes[0] = 9;
        binary.bytes()[1] = 9;
        var retained = (ValidationValues.Binary) row.fields().get("uuid");
        assertThat(retained.subtype()).isEqualTo(4);
        assertThat(retained.bytes()).containsExactly(1, 2, 3);
    }

    @Test void noDriverMutableNumberOrByteArrayCanEscapeAsPortableFacts() {
        for (Object unsupported : List.of(new Object(), new AtomicInteger(1), new byte[]{1})) {
            assertThatThrownBy(() -> ValidationValues.ownRow(Map.of("field", unsupported), LIMITS))
                    .isInstanceOfSatisfying(TapstateException.class,
                            e -> assertThat(e.code()).isEqualTo(ValidationError.VALUE_TYPE_UNSUPPORTED));
        }
    }

    @Test void largeContainerIsRejectedBeforeItsChildrenAreReadOrCopied() {
        List<Object> large = new AbstractList<>() {
            @Override public int size() { return Integer.MAX_VALUE; }
            @Override public Object get(int index) { throw new AssertionError("must reject before reading children"); }
        };
        assertThatThrownBy(() -> ValidationValues.ownRow(Map.of("items", large), LIMITS))
                .isInstanceOfSatisfying(TapstateException.class,
                        e -> assertThat(e.code()).isEqualTo(ValidationError.VALUE_LIMIT_EXCEEDED));
    }

    @Test void cyclesDepthAndCombinedBytesAreRefusedInsteadOfTruncated() {
        var cycle = new LinkedHashMap<String, Object>();
        cycle.put("self", cycle);
        assertThatThrownBy(() -> ValidationValues.ownRow(cycle, LIMITS))
                .isInstanceOfSatisfying(TapstateException.class,
                        e -> assertThat(e.args()).containsEntry("field", "value.cycle"));
        assertThatThrownBy(() -> ValidationValues.ownRow(Map.of("a", Map.of("b", 1)),
                new ValidationValues.Limits(1000, 100, 2))).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> ValidationValues.ownRow(Map.of("a", "x".repeat(1000), "b", "x".repeat(1000)),
                new ValidationValues.Limits(3000, 100, 8))).isInstanceOf(TapstateException.class);
    }

    @Test void repeatedReferencesAreCopiedAndChargedWithoutBeingMistakenForCycles() {
        var child = Map.of("amount", new BigDecimal("1.50"));
        var one = ValidationValues.ownRow(Map.of("a", child), LIMITS);
        var two = ValidationValues.ownRow(Map.of("a", child, "b", child), LIMITS);
        assertThat(two.accountedBytes()).isGreaterThan(one.accountedBytes());
        assertThat(two.nodes()).isGreaterThan(one.nodes());
        assertThat(two.fields().get("a")).isEqualTo(two.fields().get("b"));
        assertThat(two.toString()).doesNotContain("amount", "1.50");
    }
}
