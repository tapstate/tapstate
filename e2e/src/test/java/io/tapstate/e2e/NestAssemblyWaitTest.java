package io.tapstate.e2e;

import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class NestAssemblyWaitTest {
    @Test
    void waitsThroughParentFirstSnapshotsUntilCustomerAndBothChildrenArePresent() {
        List<List<Document>> snapshots = List.of(
                List.of(),
                List.of(new Document("customer", "expected")),
                List.of(assembly("expected")),
                List.of(assembly("expected", "sku-1")),
                List.of(assembly("expected", "sku-1", "sku-1")),
                List.of(assembly("other", "sku-1", "sku-2")),
                List.of(assembly("expected", "sku-2", "sku-1")));
        var reads = new AtomicInteger();

        NestStateOutlivesARunAndIsDiscardedBetweenThemIT.awaitCustomer(
                () -> snapshots.get(Math.min(reads.getAndIncrement(), snapshots.size() - 1)),
                "expected", "the completed assembly after a parent-first schedule");

        assertThat(reads.get()).as("every incomplete snapshot must be polled past").isEqualTo(snapshots.size());
    }

    private static Document assembly(String customer, String... skus) {
        return new Document("customer", customer)
                .append("items", java.util.Arrays.stream(skus).map(sku -> new Document("sku", sku)).toList());
    }
}
