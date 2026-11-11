package io.tapstate.core.dsl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.common.TapstateType;
import io.tapstate.core.dsl.aggregate.AggregateError;
import io.tapstate.core.dsl.aggregate.AggregateGroupKey;
import io.tapstate.core.dsl.aggregate.AggregateStepSpec;
import io.tapstate.core.dsl.aggregate.OrderedBytes;
import io.tapstate.core.dsl.aggregate.ValueType;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class AggregateGroupKeyTest {

    private static AggregateStepSpec spec(Map<String, ValueType> types) {
        Map<String, String> groupBy = new LinkedHashMap<>();
        types.keySet().forEach(name -> groupBy.put(name, "$" + name));
        return new AggregateStepSpec("by_day", List.of("id"), null, groupBy, types, List.of());
    }

    private static Map<String, ValueType> types() {
        Map<String, ValueType> types = new LinkedHashMap<>();
        types.put("region", ValueType.of(TapstateType.STRING));
        types.put("tier", ValueType.INT64);
        types.put("rate", ValueType.decimal(2));
        types.put("day", ValueType.of(TapstateType.DATE));
        types.put("seen", ValueType.of(TapstateType.DATETIME));
        types.put("local", ValueType.of(TapstateType.DATETIME));
        types.put("active", ValueType.of(TapstateType.BOOLEAN));
        types.put("note", ValueType.of(TapstateType.STRING));
        return types;
    }

    @Test
    @DisplayName("a group read back from the text it was shown as is the same group, value by value")
    void whatIsShownReadsBackAsTheSameGroup() {
        AggregateStepSpec spec = spec(types());
        List<Object> group = Arrays.asList("EU", 3L, new BigDecimal("2.50"), LocalDate.parse("2026-11-10"),
                Instant.parse("2026-11-10T08:00:00Z"), LocalDateTime.parse("2026-11-10T08:00:00"), true, null);

        Map<String, String> shown = AggregateGroupKey.shown(spec, group);
        List<Object> read = AggregateGroupKey.parse(spec, shown);

        assertThat(shown).containsEntry("region", "EU").containsEntry("rate", "2.50").containsEntry("note", null);
        assertThat(OrderedBytes.ofKey(read)).as("stored under the same bytes").isEqualTo(OrderedBytes.ofKey(group));
    }

    @Test
    @DisplayName("a name the step does not group by, a missing name and an unreadable value are each refused by reason")
    void whatDoesNotNameAGroupIsRefused() {
        AggregateStepSpec spec = spec(Map.of("tier", ValueType.INT64));

        assertRefused(spec, Map.of("level", "3"), "`level` is not one of its group names");
        assertRefused(spec, Map.of(), "no value is given for `tier`");
        assertRefused(spec, Map.of("tier", "three"), "`three` does not read as the int64 value of `tier`");
    }

    private static void assertRefused(AggregateStepSpec spec, Map<String, String> named, String reason) {
        assertThatThrownBy(() -> AggregateGroupKey.parse(spec, named))
                .isInstanceOfSatisfying(TapstateException.class, refused -> {
                    assertThat(refused.code()).isEqualTo(AggregateError.GROUP_INVALID);
                    assertThat(String.valueOf(refused.args().get("reason"))).contains(reason);
                    assertThat(refused.args()).containsEntry("step", "by_day");
                });
    }
}
