package io.tapstate.e2e;

import io.tapstate.core.common.JsonWriter;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkReturnWorkBudgetDefinitionsTest {
    @Test void an_independent_material_plan_bounds_escaped_json_before_output_preparation() {
        var plan = plan(100, 8, 64, 512);
        var tree = Map.of("text", "quote\"\\\u0001\n", "values", List.of(Long.MIN_VALUE, Double.MAX_VALUE, true));
        var checked = BenchmarkReturnWorkBudgetDefinitions.requireOutputTree(tree, plan);
        byte[] actual = JsonWriter.write(tree).getBytes(StandardCharsets.UTF_8);
        assertThat((long) actual.length).isLessThanOrEqualTo(plan.encodedUpperBytes());
        assertThat(checked.nodes()).isEqualTo(8);
        assertThat(checked.primitiveElements()).isEqualTo(4);
        assertThat(checked.transientUtf8CopyUpperBytes()).isGreaterThanOrEqualTo(checked.stringUtf8Bytes());
    }

    @Test void an_oversized_leaf_refuses_before_the_serializer_is_called() {
        var prior = plan(100, 8, 16, 512); var serializer = new AtomicInteger();
        var tree = Map.of("value", "x".repeat(17));
        assertThatThrownBy(() -> {
            BenchmarkReturnWorkBudgetDefinitions.requireOutputTree(tree, prior);
            serializer.incrementAndGet(); JsonWriter.write(tree);
        }).isInstanceOf(AssertionError.class).hasMessageContaining("prior length");
        assertThat(serializer).hasValue(0);
    }

    @Test void aggregate_strings_and_key_material_are_separate_prior_constraints() {
        var tree = Map.of("value", "1234567");
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetDefinitions.requireOutputTree(tree, plan(100, 8, 16, 10)))
                .isInstanceOf(AssertionError.class).hasMessageContaining("aggregate byte");
        var keyPlan = new BenchmarkReturnWorkBudgetDefinitions.OutputPlan(100, 8, 64, 512, 4, 24);
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetDefinitions.requireOutputTree(tree, keyPlan))
                .isInstanceOf(AssertionError.class).hasMessageContaining("prior length");
    }

    @Test void node_depth_and_cycle_refusals_do_not_need_the_json_writer() {
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetDefinitions.requireOutputTree(List.of(1, 2, 3), plan(3, 8, 64, 512)))
                .isInstanceOf(AssertionError.class).hasMessageContaining("node bound");
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetDefinitions.requireOutputTree(List.of(List.of(1)), plan(100, 2, 64, 512)))
                .isInstanceOf(AssertionError.class).hasMessageContaining("depth bound");
        var cycle = new LinkedHashMap<String, Object>(); cycle.put("self", cycle);
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetDefinitions.requireOutputTree(cycle, plan(100, 8, 64, 512)))
                .isInstanceOf(AssertionError.class).hasMessageContaining("cycle");
    }

    @Test void shared_immutable_children_are_counted_each_time_the_writer_will_visit_them() {
        var child = Map.of("value", "same");
        var checked = BenchmarkReturnWorkBudgetDefinitions.requireOutputTree(List.of(child, child), plan(100, 8, 64, 512));
        assertThat(checked.nodes()).isEqualTo(7); assertThat(checked.stringUtf8Bytes()).isEqualTo(18);
        assertThat(checked.primitiveElements()).isEqualTo(2);
    }

    @Test void unknown_number_implementations_are_refused_without_invoking_their_text_provider() {
        var provider = new AtomicInteger();
        Number unknown = new Number() {
            @Override public int intValue() { return 1; }
            @Override public long longValue() { return 1; }
            @Override public float floatValue() { return 1; }
            @Override public double doubleValue() { return 1; }
            @Override public String toString() { provider.incrementAndGet(); return "x".repeat(1000000); }
        };
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetDefinitions.requireOutputTree(unknown, plan(100, 8, 64, 512)))
                .isInstanceOf(AssertionError.class).hasMessageContaining("unsupported implementation");
        assertThat(provider).hasValue(0);
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetDefinitions.requireOutputTree(Double.NaN, plan(100, 8, 64, 512)))
                .isInstanceOf(AssertionError.class).hasMessageContaining("not finite");
    }

    @Test void later_caller_mutations_cannot_change_the_checked_tree_given_to_the_encoder() {
        var list = new java.util.ArrayList<Object>(); list.add("before"); list.add(null);
        var original = new LinkedHashMap<String, Object>(); original.put("items", list);
        var frozen = BenchmarkReturnWorkBudgetDefinitions.freezeOutputTree(original, plan(100, 8, 64, 512));
        String expected = JsonWriter.write(frozen.value());
        list.set(0, "x".repeat(1000)); original.put("unexpected", List.of(1, 2, 3));
        assertThat(JsonWriter.write(frozen.value())).isEqualTo(expected).isEqualTo("{\"items\":[\"before\",null]}");
        assertThat(frozen.work().nodes()).isEqualTo(5);
    }

    @Test void invalid_material_plans_and_checked_formula_overflow_are_refused() {
        assertThatThrownBy(() -> plan(0, 8, 64, 512)).isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> plan(Long.MAX_VALUE, 8, 64, Long.MAX_VALUE)).isInstanceOf(ArithmeticException.class);
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetDefinitions.requireOutputTree(Map.of(), null))
                .isInstanceOf(AssertionError.class).hasMessageContaining("plan is missing");
    }

    private static BenchmarkReturnWorkBudgetDefinitions.OutputPlan plan(long nodes, int depth, int leafBytes, long stringBytes) {
        return new BenchmarkReturnWorkBudgetDefinitions.OutputPlan(nodes, depth, leafBytes, stringBytes, 32, 24);
    }
}
