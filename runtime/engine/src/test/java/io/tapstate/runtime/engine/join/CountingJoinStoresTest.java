package io.tapstate.runtime.engine.join;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import static org.assertj.core.api.Assertions.assertThat;

class CountingJoinStoresTest {

    @Test
    void pageCountRequestsAreMeasuredEvenWhenTheBucketDoesNotChange() throws IllegalAccessException {
        CountingJoinStores stores = new CountingJoinStores(2);
        stores.indexAdd("customers", "ada", "order-1");
        stores.indexAdd("customers", "ada", "order-2");
        stores.indexAdd("customers", "ada", "order-3");
        stores.forgetCounts();
        long before = recordedOperations(stores);

        assertThat(stores.indexPageCount("customers", "ada")).isEqualTo(2);
        assertThat(stores.indexPageCount("customers", "ada")).isEqualTo(2);
        assertThat(stores.indexPageCount("customers", "missing")).isZero();

        assertThat(recordedOperations(stores) - before)
                .as("three page-count requests must be measured, including repeated and empty buckets")
                .isEqualTo(3);
        assertThat(stores.batchReads).isZero();
        assertThat(stores.singleReads).isZero();
        assertThat(stores.keysRead).isZero();
        assertThat(stores.writes).isZero();

        stores.forgetCounts();
        assertThat(recordedOperations(stores)).isZero();
    }

    // Inspect numeric counters without prescribing a name for the missing measurement.
    private static long recordedOperations(CountingJoinStores stores) throws IllegalAccessException {
        long total = 0;
        for (Field field : CountingJoinStores.class.getDeclaredFields()) {
            if (!Modifier.isStatic(field.getModifiers())
                    && (field.getType() == int.class || field.getType() == long.class)) {
                field.setAccessible(true);
                total += field.getLong(stores);
            }
        }
        return total;
    }
}
