package io.tapstate.e2e;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkSharedMongoCacheBudgetTest {
    @Test void tiny_scientific_input_is_refused_before_plain_decimal_expansion() {
        var expansions = new AtomicInteger();
        BigDecimal budget = new BigDecimal("1E-1000000000") {
            @Override public String toPlainString() {
                expansions.incrementAndGet();
                throw new AssertionError("plain decimal expansion must not be entered");
            }
        };
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "scientific-cache")) {
            assertThatThrownBy(() -> SharedMongo.cacheArgument(budget)).isInstanceOf(AssertionError.class)
                    .hasMessageContaining("frozen value or scale");
            assertThat(expansions).hasValue(0);
            assertThatThrownBy(() -> SharedMongo.parseCacheBudget("1E-1000000000"))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("frozen value or scale");
            assertThat(scope.snapshot()).containsEntry("state", "UNKNOWN").containsEntry("pendingOperations", 0L);
        }
    }

    @Test void active_cache_value_and_scale_are_exact_before_normalization_or_formatting() {
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "fixed-cache")) {
            scope.bindRoot(17, 23);
            assertThat(SharedMongo.parseCacheBudget("0.5")).isEqualTo(new BigDecimal("0.5"));
            assertThat(SharedMongo.cacheArgument(new BigDecimal("0.5"))).isEqualTo("--wiredTigerCacheSizeGB=0.5");
            assertThat(new BigDecimal("0.50").compareTo(new BigDecimal("0.5"))).isZero();
            assertThatThrownBy(() -> SharedMongo.parseCacheBudget("0.50")).isInstanceOf(AssertionError.class)
                    .hasMessageContaining("frozen value or scale");
            assertThatThrownBy(() -> SharedMongo.cacheArgument(new BigDecimal("0.50"))).isInstanceOf(AssertionError.class)
                    .hasMessageContaining("frozen value or scale");
            assertThat(scope.snapshot()).containsEntry("state", "UNKNOWN");
        }
    }

    @Test void absent_scope_keeps_ordinary_normalization_and_calls_the_original_formatter_once() {
        assertThat(BenchmarkReturnWorkBudget.current()).isNull();
        assertThat(SharedMongo.parseCacheBudget("0.50")).isEqualTo(new BigDecimal("0.5"));
        assertThat(SharedMongo.cacheArgument(new BigDecimal("0.75"))).isEqualTo("--wiredTigerCacheSizeGB=0.75");
        var parsed = SharedMongo.parseCacheBudget("1E-1000000000");
        assertThat(parsed.scale()).isEqualTo(1000000000);
        assertThat(parsed.unscaledValue()).isEqualTo(BigInteger.ONE);
        var expansions = new AtomicInteger();
        BigDecimal controlled = new BigDecimal("1E-1000000000") {
            @Override public String toPlainString() { expansions.incrementAndGet(); return "controlled-ordinary"; }
        };
        assertThat(SharedMongo.cacheArgument(controlled)).isEqualTo("--wiredTigerCacheSizeGB=controlled-ordinary");
        assertThat(expansions).hasValue(1);
    }

    private static BenchmarkReturnWorkBudget.Definition definition() {
        var upper = new EnumMap<BenchmarkReturnWorkBudget.Activity, BenchmarkReturnWorkBudget.Work>(BenchmarkReturnWorkBudget.Activity.class);
        upper.put(BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT, BenchmarkSharedSupportSourceWork.sharedUpper());
        var excluded = EnumSet.allOf(BenchmarkReturnWorkBudget.Activity.class);
        excluded.removeAll(upper.keySet());
        return new BenchmarkReturnWorkBudget.Definition("controlled-cache-bound", "a".repeat(64), Map.of(), upper, excluded);
    }
}
