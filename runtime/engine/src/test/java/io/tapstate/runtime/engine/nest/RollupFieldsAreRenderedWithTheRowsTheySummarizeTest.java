package io.tapstate.runtime.engine.nest;

import io.tapstate.core.model.Embed;
import io.tapstate.core.model.EmbedAs;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.tapstate.runtime.engine.nest.NestFixtures.at;
import static io.tapstate.runtime.engine.nest.NestFixtures.noPositions;
import static io.tapstate.runtime.engine.nest.NestFixtures.row;
import static io.tapstate.runtime.engine.nest.RollupFieldsAreCheckedWithTheTreeTest.compile;
import static io.tapstate.runtime.engine.nest.RollupFieldsAreCheckedWithTheTreeTest.itemRollups;
import static io.tapstate.runtime.engine.nest.RollupFieldsAreCheckedWithTheTreeTest.items;
import static io.tapstate.runtime.engine.nest.RollupFieldsAreCheckedWithTheTreeTest.ratings;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Rollup fields are computed in the render that writes the rows they summarize, from those rows as rendered:
 * after references resolve, in the same document, so an array and its total can never be seen disagreeing.
 */
class RollupFieldsAreRenderedWithTheRowsTheySummarizeTest {

    private static final NestTopology TREE = compile(items(itemRollups()), ratings());
    private static final String PRIZES = TREE.slots().getFirst().children().getFirst().lookupMap();
    private static final List<String> ITEMS = List.of("item_movement");
    private static final List<String> RATINGS = List.of("$rollup[r]");

    @Test
    void aTransactionCarriesItsTotalsBesideItsItemsAndItsLatestRatingWithoutTheRatings() {
        RootAssembly t1 = transaction();
        Map<String, Object> document = t1.render(TREE.slots(), prizes(5L, 3L), "t1").orElseThrow();

        assertThat((List<?>) document.get("item_movement")).hasSize(2);
        assertThat(document).containsEntry("qty", 3L).containsEntry("amount", 13L)
                .containsEntry("reward_qty", 2L).containsEntry("latest_score", 690L)
                .doesNotContainKey("$rollup[r]");
        assertThat(document.keySet()).containsExactly("id", "player_id", "item_movement", "qty", "amount",
                "reward_qty", "latest_score");
    }

    @Test
    void aChangeToAReferencedRowChangesTheTotalOnTheNextRender() {
        Map<String, Object> document = transaction().render(TREE.slots(), prizes(10L, 3L), "t1").orElseThrow();
        assertThat(document).containsEntry("amount", 23L).containsEntry("qty", 3L);
    }

    @Test
    void aReferenceAnItemNamesButThatIsMissingMakesTheDependentTotalNullAndLeavesTheOthers() {
        Map<String, Map<Object, Map<String, Object>>> onlyOne = new HashMap<>();
        onlyOne.put(PRIZES, Map.of(List.of(1L), row("id", 1L, "prize_code", "reward-points", "prize_value", 5L)));
        Map<String, Object> document = transaction().render(TREE.slots(), onlyOne, "t1").orElseThrow();
        assertThat(document).containsEntry("amount", null).containsEntry("reward_qty", null)
                .containsEntry("qty", 3L);
    }

    @Test
    void anItemThatNamesNoReferenceAtAllIsNotMissingOne() {
        RootAssembly t1 = new RootAssembly();
        t1.applyRoot(row("id", 1L, "player_id", 9L), at(1));
        t1.applyElement(item(1L), row("id", 1L, "transaction_id", 1L, "prize_definition_id", null, "amount", 4L),
                at(2), noPositions());
        Map<String, Object> document = t1.render(TREE.slots(), prizes(5L, 3L), "t1").orElseThrow();
        assertThat(document).containsEntry("qty", 4L).containsEntry("amount", 0L).containsEntry("reward_qty", null);
    }

    @Test
    void aDeletedItemLeavesTheTotalsAndTheLastOneLeavesTheDefaults() {
        RootAssembly t1 = transaction();
        t1.deleteElement(item(2L), at(10), noPositions());
        assertThat(t1.render(TREE.slots(), prizes(5L, 3L), "t1").orElseThrow())
                .containsEntry("qty", 2L).containsEntry("amount", 10L);
        t1.deleteElement(item(1L), at(11), noPositions());
        assertThat(t1.render(TREE.slots(), prizes(5L, 3L), "t1").orElseThrow())
                .containsEntry("qty", 0L).containsEntry("amount", 0L).containsEntry("reward_qty", null);
    }

    @Test
    void anItemMovedToAnotherTransactionTakesItsShareOfTheTotalsWithIt() {
        RootAssembly t1 = transaction();
        RootAssembly t2 = new RootAssembly();
        t2.applyRoot(row("id", 2L, "player_id", 9L), at(1));
        // a re-parent arrives as the old document losing the element and the new one gaining it
        t1.deleteElement(item(2L), at(10), noPositions());
        t2.applyElement(item(2L), itemRow(2L, 2L, 2L, 1L), at(10), noPositions());
        assertThat(t1.render(TREE.slots(), prizes(5L, 3L), "t1").orElseThrow())
                .containsEntry("qty", 2L).containsEntry("amount", 10L);
        assertThat(t2.render(TREE.slots(), prizes(5L, 3L), "t2").orElseThrow())
                .containsEntry("qty", 1L).containsEntry("amount", 3L).containsEntry("latest_score", null);
    }

    @Test
    void theLatestRatingFallsBackWhenTheWinnerIsDeleted() {
        RootAssembly t1 = transaction();
        t1.deleteElement(rating("B"), at(10), noPositions());
        assertThat(t1.render(TREE.slots(), prizes(5L, 3L), "t1").orElseThrow()).containsEntry("latest_score", 650L);
    }

    /**
     * A rollup level stores its elements as whole rows, exactly as an array does: what is kept never depends on
     * how the level renders or which fields are computed over it, so adding a field or turning an array into a
     * rollup level reads rows that hold every column.
     */
    @Test
    void theStoredElementIsTheSameWhateverTheLevelRendersAs() throws IOException {
        NestTopology plain = compile(new Embed("i", Map.of("transaction_id", "id"), EmbedAs.ARRAY,
                "item_movement", null, List.of("id"), null, null,
                List.of(RollupFieldsAreCheckedWithTheTreeTest.prize()), null));
        RootAssembly withRollups = transaction();
        RootAssembly withoutRollups = transaction();
        withRollups.render(TREE.slots(), prizes(5L, 3L), "t1");
        withoutRollups.render(plain.slots(), prizes(5L, 3L), "t1");
        assertThat(serialized(withRollups)).isEqualTo(serialized(withoutRollups));
    }

    // ---- fixtures -------------------------------------------------------------------------------------

    /** Transaction 1: items (amount 2, prize 1) and (amount 1, prize 2); ratings A and B on one day and A earlier. */
    private static RootAssembly transaction() {
        RootAssembly t1 = new RootAssembly();
        t1.applyRoot(row("id", 1L, "player_id", 9L), at(1));
        t1.applyElement(item(1L), itemRow(1L, 1L, 1L, 2L), at(2), noPositions());
        t1.applyElement(item(2L), itemRow(2L, 1L, 2L, 1L), at(3), noPositions());
        t1.applyElement(rating("B"), ratingRow("B", "2026-03-05", 690L), at(4), noPositions());
        t1.applyElement(rating("A"), ratingRow("A", "2026-03-05", 650L), at(5), noPositions());
        return t1;
    }

    private static ElementRef item(long id) {
        return new ElementRef(ITEMS, null, List.of(id), id);
    }

    private static ElementRef rating(String type) {
        return new ElementRef(RATINGS, null, List.of(1L, type), List.of(1L, type));
    }

    private static Map<String, Object> itemRow(long id, long transaction, long prize, long amount) {
        return row("id", id, "transaction_id", transaction, "prize_definition_id", prize, "amount", amount);
    }

    private static Map<String, Object> ratingRow(String type, String day, long score) {
        return row("trans_id", 1L, "rating_type", type, "model_date", LocalDate.parse(day), "score", score);
    }

    /** Prize 1 is reward points worth {@code first}; prize 2 is cash worth {@code second}. */
    private static Map<String, Map<Object, Map<String, Object>>> prizes(long first, long second) {
        return Map.of(PRIZES, Map.of(
                List.of(1L), row("id", 1L, "prize_code", "reward-points", "prize_value", first),
                List.of(2L), row("id", 2L, "prize_code", "cash", "prize_value", second)));
    }

    private static byte[] serialized(RootAssembly assembly) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(assembly);
        }
        return bytes.toByteArray();
    }
}
