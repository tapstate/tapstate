package io.tapstate.runtime.engine.nest;

import io.tapstate.core.model.EmbedAs;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.tapstate.runtime.engine.nest.NestFixtures.at;
import static io.tapstate.runtime.engine.nest.NestFixtures.noPositions;
import static io.tapstate.runtime.engine.nest.NestFixtures.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

class RootAssemblyTest {

    private static final String ITEMS = "items";
    private static final List<EmbedSlot> ITEMS_ARRAY = List.of(new EmbedSlot(ITEMS, EmbedAs.ARRAY, List.of()));
    private static final List<EmbedSlot> ITEMS_OBJECT = List.of(new EmbedSlot(ITEMS, EmbedAs.OBJECT, List.of()));

    /** One element of the single array embed under the root, identified by {@code value}. */
    private static ElementRef item(Object value) {
        return new ElementRef(List.of(ITEMS), null, List.of(value), null);
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> itemsOf(Optional<Map<String, Object>> document) {
        return (List<Map<String, Object>>) document.orElseThrow().get(ITEMS);
    }

    @Test
    void theDocumentCarriesTheRootFieldsAndItsElements() {
        RootAssembly assembly = new RootAssembly();
        assembly.applyRoot(row("id", 7, "customer", "ann"), at(1));
        assembly.applyElement(item("i1"), row("sku", "a"), at(2), noPositions());

        Map<String, Object> document = assembly.render(ITEMS_ARRAY).orElseThrow();
        assertThat(document).containsEntry("id", 7).containsEntry("customer", "ann");
        assertThat(itemsOf(Optional.of(document))).containsExactly(row("sku", "a"));
    }

    @Test
    void noDocumentUntilTheRootArrives() {
        RootAssembly assembly = new RootAssembly();
        assembly.applyElement(item("i1"), row("sku", "a"), at(1), noPositions());

        assertThat(assembly.render(ITEMS_ARRAY)).isEmpty();

        assembly.applyRoot(row("id", 7), at(2));
        assertThat(itemsOf(assembly.render(ITEMS_ARRAY))).containsExactly(row("sku", "a"));
    }

    @Test
    void anOlderRootUpdateDoesNotOverwriteANewerOne() {
        RootAssembly assembly = new RootAssembly();
        assembly.applyRoot(row("id", 7, "customer", "new"), at(5));

        assertThat(assembly.applyRoot(row("id", 7, "customer", "old"), at(4))).isFalse();
        assertThat(assembly.render(ITEMS_ARRAY).orElseThrow()).containsEntry("customer", "new");
    }

    @Test
    void aTieKeepsWhatIsAlreadyThere() {
        RootAssembly assembly = new RootAssembly();
        assembly.applyRoot(row("id", 7, "customer", "first"), at(5));

        assertThat(assembly.applyRoot(row("id", 7, "customer", "second"), at(5))).isFalse();
        assertThat(assembly.render(ITEMS_ARRAY).orElseThrow()).containsEntry("customer", "first");
    }

    @Test
    void deletingTheRootKeepsTheElementsForItsReturn() {
        RootAssembly assembly = new RootAssembly();
        assembly.applyRoot(row("id", 7), at(1));
        assembly.applyElement(item("i1"), row("sku", "a"), at(2), noPositions());
        assembly.applyElement(item("i2"), row("sku", "b"), at(3), noPositions());

        assembly.deleteRoot(at(4));
        assertThat(assembly.rootPresent()).isFalse();
        assertThat(assembly.render(ITEMS_ARRAY)).isEmpty();

        assembly.applyRoot(row("id", 7), at(5));
        assertThat(itemsOf(assembly.render(ITEMS_ARRAY)))
                .containsExactly(row("sku", "a"), row("sku", "b"));
    }

    @Test
    void aDeletedRootRendersNoDocumentWhateverArrivesNext() {
        RootAssembly assembly = new RootAssembly();
        assembly.applyRoot(row("id", 7), at(1));
        assembly.deleteRoot(at(2));

        assembly.applyElement(item("i1"), row("sku", "a"), at(3), noPositions());
        assertThat(assembly.render(ITEMS_ARRAY)).isEmpty();
    }

    @Test
    void aReplayedRootInsertBeneathTheTombstoneStaysDeleted() {
        RootAssembly assembly = new RootAssembly();
        assembly.applyRoot(row("id", 7), at(10));
        assembly.deleteRoot(at(11));

        assertThat(assembly.applyRoot(row("id", 7), at(10))).isFalse();
        assertThat(assembly.render(ITEMS_ARRAY)).isEmpty();
    }

    @Test
    void anElementUpdateStaysInItsPlace() {
        RootAssembly assembly = new RootAssembly();
        assembly.applyRoot(row("id", 7), at(1));
        assembly.applyElement(item("i1"), row("sku", "a"), at(2), noPositions());
        assembly.applyElement(item("i2"), row("sku", "b"), at(3), noPositions());
        assembly.applyElement(item("i3"), row("sku", "c"), at(4), noPositions());

        assembly.applyElement(item("i2"), row("sku", "b2"), at(5), noPositions());

        assertThat(itemsOf(assembly.render(ITEMS_ARRAY)))
                .containsExactly(row("sku", "a"), row("sku", "b2"), row("sku", "c"));
    }

    @Test
    void anOlderElementUpdateDoesNotOverwriteANewerOne() {
        RootAssembly assembly = new RootAssembly();
        assembly.applyRoot(row("id", 7), at(1));
        assembly.applyElement(item("i1"), row("sku", "new"), at(5), noPositions());

        assertThat(assembly.applyElement(item("i1"), row("sku", "old"), at(4), noPositions())).isFalse();
        assertThat(itemsOf(assembly.render(ITEMS_ARRAY))).containsExactly(row("sku", "new"));
    }

    @Test
    void deletingAnElementLeavesTheRestUntouched() {
        RootAssembly assembly = new RootAssembly();
        assembly.applyRoot(row("id", 7), at(1));
        assembly.applyElement(item("i1"), row("sku", "a"), at(2), noPositions());
        assembly.applyElement(item("i2"), row("sku", "b"), at(3), noPositions());
        assembly.applyElement(item("i3"), row("sku", "c"), at(4), noPositions());

        assembly.deleteElement(item("i2"), at(5), noPositions());

        assertThat(itemsOf(assembly.render(ITEMS_ARRAY)))
                .containsExactly(row("sku", "a"), row("sku", "c"));
    }

    @Test
    void aReplayedInsertBeneathTheElementTombstoneStaysDeleted() {
        RootAssembly assembly = new RootAssembly();
        assembly.applyRoot(row("id", 7), at(1));
        assembly.applyElement(item("i1"), row("sku", "a"), at(2), noPositions());
        assembly.deleteElement(item("i1"), at(10), noPositions());

        assertThat(assembly.applyElement(item("i1"), row("sku", "a"), at(9), noPositions())).isFalse();
        assertThat(itemsOf(assembly.render(ITEMS_ARRAY))).isEmpty();
    }

    @Test
    void aRebuildAboveTheElementTombstoneBringsTheElementBack() {
        RootAssembly assembly = new RootAssembly();
        assembly.applyRoot(row("id", 7), at(1));
        assembly.applyElement(item("i1"), row("sku", "a"), at(2), noPositions());
        assembly.deleteElement(item("i1"), at(10), noPositions());

        assertThat(assembly.applyElement(item("i1"), row("sku", "rebuilt"), at(11), noPositions())).isTrue();
        assertThat(itemsOf(assembly.render(ITEMS_ARRAY))).containsExactly(row("sku", "rebuilt"));
    }

    @Test
    void aReplayedDeleteBeneathTheElementTombstoneChangesNothing() {
        RootAssembly assembly = new RootAssembly();
        assembly.applyRoot(row("id", 7), at(1));
        assembly.applyElement(item("i1"), row("sku", "a"), at(2), noPositions());
        assembly.deleteElement(item("i1"), at(10), noPositions());
        assembly.applyElement(item("i1"), row("sku", "rebuilt"), at(11), noPositions());

        assertThat(assembly.deleteElement(item("i1"), at(10), noPositions())).isFalse();
        assertThat(itemsOf(assembly.render(ITEMS_ARRAY))).containsExactly(row("sku", "rebuilt"));
    }

    @Test
    void anArrayEmbedWithNoElementsRendersAnEmptyArray() {
        RootAssembly assembly = new RootAssembly();
        assembly.applyRoot(row("id", 7), at(1));

        Map<String, Object> document = assembly.render(ITEMS_ARRAY).orElseThrow();
        assertThat(document).containsKey(ITEMS);
        assertThat(itemsOf(Optional.of(document))).isEmpty();
    }

    @Test
    void anObjectEmbedWithNoElementOmitsTheField() {
        RootAssembly assembly = new RootAssembly();
        assembly.applyRoot(row("id", 7), at(1));

        assertThat(assembly.render(ITEMS_OBJECT).orElseThrow()).doesNotContainKey(ITEMS);
    }

    @Test
    void anObjectEmbedRendersTheElementItselfNotAnArray() {
        RootAssembly assembly = new RootAssembly();
        assembly.applyRoot(row("id", 7), at(1));
        assembly.applyElement(item("i1"), row("sku", "a"), at(2), noPositions());

        assertThat(assembly.render(ITEMS_OBJECT).orElseThrow()).containsEntry(ITEMS, row("sku", "a"));
    }

    @Test
    void anObjectEmbedHoldingSeveralRowsShowsTheLatestOrdered() {
        RootAssembly assembly = new RootAssembly();
        assembly.applyRoot(row("id", 7), at(1));
        // Arrival order disagrees with the order the events carry, in both directions: the winner is
        // neither the first nor the last to arrive, so neither can pass for "the latest".
        assembly.applyElement(item("i1"), row("sku", "earliest"), at(2), noPositions());
        assembly.applyElement(item("i3"), row("sku", "latest"), at(9), noPositions());
        assembly.applyElement(item("i2"), row("sku", "middle"), at(5), noPositions());

        assertThat(assembly.render(ITEMS_OBJECT).orElseThrow())
                .containsEntry(ITEMS, row("sku", "latest"));
    }

    @Test
    void everyAppliedEventCarriesAnOrder() {
        RootAssembly assembly = new RootAssembly();
        assertThatNullPointerException().isThrownBy(() -> assembly.applyRoot(row("id", 7), null));
        assertThatNullPointerException().isThrownBy(() -> assembly.deleteRoot(null));
        assertThatNullPointerException()
                .isThrownBy(() -> assembly.applyElement(item("i1"), row("sku", "a"), null, noPositions()));
        assertThatNullPointerException()
                .isThrownBy(() -> assembly.deleteElement(item("i1"), null, noPositions()));
    }

    @Test
    void aRenderedDocumentDoesNotShareStateWithTheAssembly() {
        RootAssembly assembly = new RootAssembly();
        assembly.applyRoot(row("id", 7), at(1));
        assembly.applyElement(item("i1"), row("sku", "a"), at(2), noPositions());

        Map<String, Object> document = assembly.render(ITEMS_ARRAY).orElseThrow();
        document.remove("id");
        itemsOf(Optional.of(document)).clear();

        Map<String, Object> again = assembly.render(ITEMS_ARRAY).orElseThrow();
        assertThat(again).containsEntry("id", 7);
        assertThat(itemsOf(Optional.of(again))).containsExactly(row("sku", "a"));
    }

    @Test
    void theAssemblyRoundTripsThroughSerializationWithItsTombstones() throws Exception {
        RootAssembly assembly = new RootAssembly();
        assembly.applyRoot(row("id", 7, "customer", "ann"), at(1));
        assembly.applyElement(item("i1"), row("sku", "a"), at(2), noPositions());
        assembly.applyElement(item("i2"), row("sku", "b"), at(3), noPositions());
        assembly.deleteElement(item("i2"), at(10), noPositions());

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(assembly);
        }
        RootAssembly restored;
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            restored = (RootAssembly) in.readObject();
        }

        assertThat(itemsOf(restored.render(ITEMS_ARRAY))).containsExactly(row("sku", "a"));
        assertThat(restored.applyElement(item("i2"), row("sku", "b"), at(9), noPositions())).isFalse();
        assertThat(itemsOf(restored.render(ITEMS_ARRAY))).containsExactly(row("sku", "a"));
    }
}
