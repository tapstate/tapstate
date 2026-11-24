package io.tapstate.app;

import io.tapstate.core.validation.ValidationKey;
import io.tapstate.core.validation.ValidationValues;
import io.tapstate.spi.store.ValidationRowCursor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ValidationSortedRowsTest {
    private static final ValidationValues.Limits LIMITS = new ValidationValues.Limits(64 * 1024, 8192, 32);

    @Test void selectsLocalSortOnlyBelowTheThreshold() {
        assertThat(NativeValidationPdkRows.shouldSortLocally(OptionalLong.of(0))).isTrue();
        assertThat(NativeValidationPdkRows.shouldSortLocally(OptionalLong.of(9_999_999))).isTrue();
        assertThat(NativeValidationPdkRows.shouldSortLocally(OptionalLong.of(10_000_000))).isFalse();
        assertThat(NativeValidationPdkRows.shouldSortLocally(OptionalLong.of(10_000_001))).isFalse();
        assertThat(NativeValidationPdkRows.shouldSortLocally(OptionalLong.empty())).isFalse();
    }

    @Test void mergesMultipleSpillsWithoutDroppingDuplicatesOrPayloads() {
        var input = new Rows(List.of("B", "a", "B", "Z", "0", "a", "X", "1", "B"));
        var received = new ArrayList<String>();
        try (var sorted = new ValidationSortedRows(input, ValidationSortedRowsTest::key, LIMITS, () -> {}, 1)) {
            for (var next = sorted.next(); next.isPresent(); next = sorted.next()) {
                try (var row = next.orElseThrow()) {
                    assertThat(row.ordinal()).isEqualTo(received.size());
                    assertThat(row.values().fields().get("value")).isInstanceOf(ValidationValues.Typed.class);
                    received.add((String) row.values().fields().get("key"));
                }
            }
        }
        assertThat(received).containsExactly("0", "1", "B", "B", "B", "X", "Z", "a", "a");
        assertThat(input.closed).isTrue();
    }

    private static ValidationKey key(ValidationValues.Row row) {
        return new ValidationKey(List.of(ValidationKey.Part.text((String) row.fields().get("key"))));
    }

    private static final class Rows implements ValidationRowCursor {
        private final List<String> keys;
        private boolean closed;
        private int index;

        Rows(List<String> keys) { this.keys = keys; }

        @Override public Optional<Row> next() {
            if (index == keys.size()) return Optional.empty();
            int position = index++;
            var row = ValidationValues.ownRow(Map.of("key", keys.get(position), "value",
                    new ValidationValues.Typed("text", null, "binding", "test", keys.get(position))), LIMITS);
            return Optional.of(new Row() {
                @Override public long ordinal() { return position; }
                @Override public ValidationValues.Row values() { return row; }
                @Override public void close() {}
            });
        }

        @Override public void close() { closed = true; }
    }
}
