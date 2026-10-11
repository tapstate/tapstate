package io.tapstate.e2e;

import java.math.BigDecimal;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkSharedMongoCachePresenceTest {
    @Test void cached_empty_retains_unscoped_absence_but_refuses_the_active_frozen_recipe() {
        Optional<BigDecimal> cached = Optional.empty();
        var reads = new AtomicInteger(); var commands = new AtomicInteger();
        java.util.function.Supplier<String> property = () -> {
            reads.incrementAndGet(); throw new AssertionError("cached input must not reread the property");
        };
        assertThat(BenchmarkReturnWorkBudget.current()).isNull();
        assertThat(SharedMongo.configuredCacheBudgetUsing(cached, property)).isSameAs(cached);
        assertThat(reads).hasValue(0);
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "cached-empty-cache")) {
            assertThatThrownBy(() -> BenchmarkSharedSupportSourceWork.entered(
                    BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT, BenchmarkSharedSupportSourceWork.Kind.MONGO_CONFIGURATION,
                    () -> { var value = SharedMongo.configuredCacheBudgetUsing(cached, property); commands.incrementAndGet(); return value; }))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("frozen cache budget is absent");
            assertThat(reads).hasValue(0); assertThat(commands).hasValue(0);
            assertThat(cached).isEmpty();
            assertThat(scope.snapshot()).containsEntry("state", "UNKNOWN").containsEntry("pendingOperations", 0L);
        }
    }

    @Test void fresh_absence_reads_once_per_entry_and_only_the_active_recipe_refuses() {
        var reads = new AtomicInteger(); var commands = new AtomicInteger();
        java.util.function.Supplier<String> property = () -> { reads.incrementAndGet(); return null; };
        assertThat(BenchmarkReturnWorkBudget.current()).isNull();
        assertThat(SharedMongo.configuredCacheBudgetUsing(null, property)).isEmpty();
        assertThat(reads).hasValue(1);
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "fresh-absent-cache")) {
            assertThatThrownBy(() -> BenchmarkSharedSupportSourceWork.entered(
                    BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT, BenchmarkSharedSupportSourceWork.Kind.MONGO_CONFIGURATION,
                    () -> { var value = SharedMongo.configuredCacheBudgetUsing(null, property); commands.incrementAndGet(); return value; }))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("frozen cache budget is absent");
            assertThat(reads).hasValue(2); assertThat(commands).hasValue(0);
            assertThat(scope.snapshot()).containsEntry("state", "UNKNOWN").containsEntry("pendingOperations", 0L);
        }
    }

    private static BenchmarkReturnWorkBudget.Definition definition() {
        var upper = new EnumMap<BenchmarkReturnWorkBudget.Activity, BenchmarkReturnWorkBudget.Work>(BenchmarkReturnWorkBudget.Activity.class);
        upper.put(BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT, BenchmarkSharedSupportSourceWork.sharedUpper());
        var excluded = EnumSet.allOf(BenchmarkReturnWorkBudget.Activity.class);
        excluded.removeAll(upper.keySet());
        return new BenchmarkReturnWorkBudget.Definition("controlled-cache-presence", "a".repeat(64), Map.of(), upper, excluded);
    }
}
