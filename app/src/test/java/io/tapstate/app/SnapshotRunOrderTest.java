package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class SnapshotRunOrderTest {

    @Test
    void eachPhysicalRunTakesTheNextDurableGeneration() {
        InMemoryKeyedStateStore store = new InMemoryKeyedStateStore();

        assertThat(SnapshotRunOrder.next(store, "p", 0L)).isEqualTo(1L);
        assertThat(SnapshotRunOrder.next(store, "p", 0L)).isEqualTo(2L);
        assertThat(SnapshotRunOrder.current(store, "p")).isEqualTo(2L);
    }

    @Test
    void firstSnapshotOnlyRunStartsAboveRetainedChainGeneration() {
        InMemoryKeyedStateStore store = new InMemoryKeyedStateStore();

        assertThat(SnapshotRunOrder.next(store, "p", 7L)).isEqualTo(8L);
        assertThat(SnapshotRunOrder.next(store, "p", 3L)).isEqualTo(9L);
    }

    @Test
    void clearingThePipelinesStateClearsItsGeneration() {
        InMemoryKeyedStateStore store = new InMemoryKeyedStateStore();
        SnapshotRunOrder.next(store, "p", 0L);

        store.dropNamespace(SnapshotRunOrder.namespaceOf("p"));

        assertThat(SnapshotRunOrder.next(store, "p", 0L)).isEqualTo(1L);
    }
}
