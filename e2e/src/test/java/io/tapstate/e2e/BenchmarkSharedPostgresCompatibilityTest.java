package io.tapstate.e2e;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkSharedPostgresCompatibilityTest {
    @Test void absent_scope_preserves_the_original_getter_and_conversion_interleaving() throws Exception {
        assertThat(BenchmarkReturnWorkBudget.current()).isNull();
        var events = new ArrayList<String>();
        var values = Map.of("host", "127.0.0.1", "port", "5432", "database", "controlled",
                "username", "user", "password", "secret");
        var settings = new java.util.LinkedHashMap<String, Object>() {
            @Override public Object get(Object key) {
                String name = (String) key; events.add("get:" + name);
                return new Object() {
                    @Override public String toString() { events.add("convert:" + name); return values.get(name); }
                };
            }
        };
        SharedPostgres.connect(settings, (uri, user, password) -> {
            events.add("open");
            assertThat(uri).isEqualTo("jdbc:postgresql://127.0.0.1:5432/controlled");
            assertThat(user).isEqualTo("user"); assertThat(password).isEqualTo("secret");
            return null;
        });
        assertThat(events).containsExactly("get:host", "convert:host", "get:port", "convert:port",
                "get:database", "convert:database", "get:username", "convert:username",
                "get:password", "convert:password", "open");
    }

    @Test void active_scalar_refusal_precedes_later_getters_and_the_connection_provider() {
        var events = new ArrayList<String>(); var opens = new AtomicInteger();
        var settings = new java.util.LinkedHashMap<String, Object>() {
            @Override public Object get(Object key) {
                events.add("get:" + key);
                if (key.equals("host")) return "127.0.0.1";
                if (key.equals("port")) return new Object();
                throw new AssertionError("later scalar getter must not be entered");
            }
        };
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "guarded-connect-order")) {
            assertThatThrownBy(() -> SharedPostgres.connect(settings, (uri, user, password) -> {
                opens.incrementAndGet(); return null;
            })).isInstanceOf(AssertionError.class).hasMessageContaining("no source-level byte formula");
            assertThat(events).containsExactly("get:host", "get:port"); assertThat(opens).hasValue(0);
            assertThat(scope.snapshot()).containsEntry("state", "UNKNOWN").containsEntry("pendingOperations", 0L);
        }
    }

    private static BenchmarkReturnWorkBudget.Definition definition() {
        var upper = new EnumMap<BenchmarkReturnWorkBudget.Activity, BenchmarkReturnWorkBudget.Work>(BenchmarkReturnWorkBudget.Activity.class);
        upper.put(BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT, BenchmarkSharedSupportSourceWork.sharedUpper());
        var excluded = EnumSet.allOf(BenchmarkReturnWorkBudget.Activity.class);
        excluded.removeAll(upper.keySet());
        return new BenchmarkReturnWorkBudget.Definition("controlled-connect-order", "a".repeat(64), Map.of(), upper, excluded);
    }
}
