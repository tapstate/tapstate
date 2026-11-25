package io.tapstate.core.event;

import io.tapstate.core.common.TapstateException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RowMapKeysTest {
    @Test
    void rejectsEveryNonStringKeyWithoutRenderingIt() {
        Object hostile = new Object() {
            @Override
            public String toString() {
                throw new AssertionError("a rejected key must never be rendered");
            }
        };
        for (Object key : new Object[] {7, 7L, null, hostile}) {
            Map<Object, Object> nested = new LinkedHashMap<>();
            nested.put(key, "value");
            assertThatThrownBy(() -> RowMapKeys.validate(Map.of("dict", nested)))
                    .isInstanceOfSatisfying(TapstateException.class, failure -> {
                        assertThat(failure.code()).isEqualTo(RowValueError.NON_STRING_MAP_KEY);
                        assertThat(failure.args()).containsExactlyInAnyOrderEntriesOf(Map.of(
                                "path", "$/dict", "keyType", key == null ? "null" : key.getClass().getName()));
                    });
            assertThat(nested).hasSize(1);
        }
    }

    @Test
    void visitsMapsInsideCarriersCollectionsAndObjectArraysWithEscapedPaths() {
        Object value = Map.of("a/b~c", new ConvertedValue(
                List.of((Object) new Object[] {Map.of(7, "value")}), "array"));
        assertThatThrownBy(() -> RowMapKeys.validate(value))
                .isInstanceOfSatisfying(TapstateException.class, failure -> {
                    assertThat(failure.code()).isEqualTo(RowValueError.NON_STRING_MAP_KEY);
                    assertThat(failure.args()).containsEntry("path", "$/a~1b~0c/0/0");
                });
    }

    @Test
    void preservesLegalValuesAndDoesNotMaterializeJson() {
        Map<String, Object> row = new LinkedHashMap<>();
        ConvertedValue value = new ConvertedValue(Map.of("", List.of(new int[] {1, 2})), "object");
        row.put("dict", value);
        row.put("nullable", null);
        row.put("empty", Map.of());
        assertThatCode(() -> RowMapKeys.validate(row)).doesNotThrowAnyException();
        assertThat(row.get("dict")).isSameAs(value);
        assertThat(row).containsEntry("nullable", null).containsEntry("empty", Map.of());
    }
}
