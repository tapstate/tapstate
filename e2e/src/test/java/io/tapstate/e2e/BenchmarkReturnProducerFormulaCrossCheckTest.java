package io.tapstate.e2e;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The controller re-derives the owned producer's allocation table, transport descriptor and formula hash on its
 * own side, and the producer refuses an input whose formula hash differs from its own. Nothing else compares the
 * two copies before a real fork reaches them, so a one-sided change surfaces only as a refused producer input or
 * an unequal source sum at the end of a measured fork.
 */
class BenchmarkReturnProducerFormulaCrossCheckTest {
    private static final String PRODUCER = "io.tapstate.adapters.pdk.PdkWriteReturnWorkBudget";

    @Test void the_controller_formula_hash_is_the_producers_own() throws Exception {
        Class<?> producer = Class.forName(PRODUCER);
        String own = (String) value(producer, "SOURCE_FORMULA_SHA256", null);
        assertThat(BenchmarkReturnWorkBudgetProducerAllocations.FORMULA_SHA256).isEqualTo(own);
        assertThat(BenchmarkReturnWorkBudgetProducerSeal.FORMULA_SHA256).isEqualTo(own);
        assertThat(value(BenchmarkReturnWorkBudgetProducerAllocations.class, "TRANSPORT_SOURCE", null))
                .isEqualTo(value(producer, "TRANSPORT_SOURCE", null));
    }

    @Test void every_controller_activity_upper_is_the_producers_own() throws Exception {
        Class<?> producer = Class.forName(PRODUCER);
        Class<?> activity = Class.forName(PRODUCER + "$Activity");
        Field upper = activity.getDeclaredField("upper"); upper.setAccessible(true);
        var own = new LinkedHashMap<String, String>();
        for (Object constant : activity.getEnumConstants()) {
            own.put(((Enum<?>) constant).name(), Arrays.toString((long[]) upper.get(constant)));
        }
        var mirrored = new LinkedHashMap<String, String>();
        for (var entry : ((Map<?, ?>) value(BenchmarkReturnWorkBudgetProducerAllocations.class, "TABLE", null)).entrySet()) {
            var work = (BenchmarkReturnWorkBudget.Work) entry.getValue();
            mirrored.put((String) entry.getKey(), Arrays.toString(new long[]{work.calls(), work.requests(), work.records(),
                    work.frames(), work.bytes(), work.passes(), work.elements(), work.updates()}));
        }
        assertThat(producer).isNotNull();
        assertThat(mirrored).hasSize(own.size()).containsExactlyInAnyOrderEntriesOf(own);
    }

    private static Object value(Class<?> owner, String name, Object instance) throws Exception {
        Field field = owner.getDeclaredField(name); field.setAccessible(true); return field.get(instance);
    }
}
