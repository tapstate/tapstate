package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.TransformBody;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

class UnwindColumnCollisionTest {
    @ParameterizedTest
    @CsvSource({"region,,include_array_index,region", ",region,element_key,region",
            "items,,include_array_index,items", ",items,element_key,items",
            "sku,sku,element_key,sku"})
    void generatedColumnsCannotReplaceParentColumnsOrEachOther(
            String ordinal, String elementKey, String option, String column) {
        Throwable failure = catchThrowable(() -> derive(ordinal, elementKey));
        assertThat(failure).isInstanceOf(TapstateException.class);
        TapstateException refusal = (TapstateException) failure;
        assertThat(refusal.code().code()).isEqualTo("dsl.unwind-column-already-exists");
        assertThat(refusal.args()).containsEntry("column", column).containsEntry("option", option);
    }

    @Test
    void distinctGeneratedColumnsKeepParentColumnsAndReplaceOnlyTheArray() {
        NodeColumns output = derive("item_index", "sku");
        assertThat(output.columns()).containsKeys("id", "region", "items", "item_index", "sku");
        assertThat(output.columns().get("region")).isEqualTo("STRING NULL");
    }

    private static NodeColumns derive(String ordinal, String elementKey) {
        return NodeColumns.of(new TransformBody.Unwind("items", ordinal, false, elementKey, null),
                Map.of("in", NodeColumns.known(Map.of("id", "INT64 NOT NULL",
                        "region", "STRING NULL", "items", "ARRAY NULL"))), null);
    }
}
