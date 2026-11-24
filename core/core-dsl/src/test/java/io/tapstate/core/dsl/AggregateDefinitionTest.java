package io.tapstate.core.dsl;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.dsl.aggregate.AggregateDefinition;
import io.tapstate.core.dsl.aggregate.AggregateStepSpec;
import io.tapstate.core.dsl.aggregate.RollupSpec;
import io.tapstate.core.dsl.aggregate.ValueType;
import io.tapstate.core.model.RollupFunction;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class AggregateDefinitionTest {

    private static RollupSpec count(String field, String filter) {
        return new RollupSpec(field, RollupFunction.COUNT, null, List.of(), filter,
                ValueType.INT64, ValueType.INT64, null, null, null);
    }

    private static AggregateStepSpec spec(List<String> key, String where, String group, RollupSpec... fields) {
        Map<String, String> groupBy = new LinkedHashMap<>();
        groupBy.put("region", group);
        return new AggregateStepSpec("by_region", key, where, groupBy,
                Map.of("region", ValueType.INT64), List.of(fields));
    }

    private static final AggregateDefinition BUILT =
            AggregateDefinition.of(List.of("orders"), spec(List.of("id"), null, "$region", count("orders", null)));

    @Test
    @DisplayName("the same step read from the same input is the same definition, also after it was stored")
    void theSameStepIsUnchanged() {
        AggregateDefinition again =
                AggregateDefinition.of(List.of("orders"), spec(List.of("id"), null, "$region", count("orders", null)));

        assertThat(again.changedSince(BUILT)).isEmpty();
        assertThat(again.changedSince(AggregateDefinition.read(BUILT.stored()))).isEmpty();
    }

    @Test
    @DisplayName("each part a user writes is told apart, and a change names exactly the part that changed")
    void aChangeNamesItsPart() {
        assertThat(AggregateDefinition.of(List.of("refunds"),
                spec(List.of("id"), null, "$region", count("orders", null))).changedSince(BUILT))
                .containsExactly("from");
        assertThat(AggregateDefinition.of(List.of("orders"),
                spec(List.of("id", "shard"), null, "$region", count("orders", null))).changedSince(BUILT))
                .containsExactly("key");
        assertThat(AggregateDefinition.of(List.of("orders"),
                spec(List.of("id"), "$status == 'open'", "$region", count("orders", null))).changedSince(BUILT))
                .containsExactly("where");
        assertThat(AggregateDefinition.of(List.of("orders"),
                spec(List.of("id"), null, "$country", count("orders", null))).changedSince(BUILT))
                .containsExactly("groupBy");
        assertThat(AggregateDefinition.of(List.of("orders"),
                spec(List.of("id"), null, "$region", count("orders", "$qty > 1"))).changedSince(BUILT))
                .containsExactly("fields");
    }

    @Test
    @DisplayName("two lists that read the same when joined are still different definitions")
    void itemsAreNotConfusedByJoining() {
        AggregateDefinition split = AggregateDefinition.of(List.of("orders"),
                spec(List.of("a", "b"), null, "$region", count("orders", null)));
        AggregateDefinition joined = AggregateDefinition.of(List.of("orders"),
                spec(List.of("ab"), null, "$region", count("orders", null)));

        assertThat(split.changedSince(joined)).containsExactly("key");
    }
}
