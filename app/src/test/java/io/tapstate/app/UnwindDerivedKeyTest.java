package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.model.FieldRule;
import io.tapstate.core.model.TransformBody;
import io.tapstate.spi.sink.TargetField;
import io.tapstate.spi.sink.TargetTable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Which columns a published target is keyed on once a node in the chain expands a list.
 *
 * <p>Every node before this one produced one output row per input row, so the rows a target
 * received were identified by whatever of the source table's own key still travelled - one rule,
 * applied where the target model is published, reading nothing from the nodes themselves. An
 * expansion breaks that: the parent's key is the same value on all of one row's output rows, and
 * the thing that tells them apart is a column the source table never had. Nothing in the derivation
 * vocabulary could say so, so the column arrived as an ordinary one and the target came out keyed
 * on the parent alone - which an upsert target answers by collapsing every expanded row of a parent
 * into one, filling the table, reporting nothing, and looking exactly like a step that did not run.
 *
 * <p><b>So the derivation now carries what each node adds to the key, and the published rule
 * composes.</b> The two halves are held together here on purpose: adding the expansion's column is
 * worth nothing if the seven kinds that add none start keying differently, and a run over rows that
 * happen to be unique either way tells the two apart nowhere.
 */
class UnwindDerivedKeyTest {

    /** A source table as the sink sees it: the store's own type tokens, one key column. */
    private static TargetTable orders() {
        return new TargetTable("orders", List.of(
                new TargetField("o_id", "bigint", true),
                new TargetField("o_region", "text", false),
                new TargetField("items", "json", false)));
    }

    private static NodeColumns atTheSource() {
        return shared("o_id", "INT64 NOT NULL", "o_region", "STRING NULL", "items", "ARRAY NULL");
    }

    private static NodeColumns shared(String... namesAndTypes) {
        Map<String, String> columns = new LinkedHashMap<>();
        for (int i = 0; i < namesAndTypes.length; i += 2) {
            columns.put(namesAndTypes[i], namesAndTypes[i + 1]);
        }
        return NodeColumns.known(columns);
    }

    private static Map<String, NodeColumns> one(NodeColumns upstream) {
        return Map.of("in", upstream);
    }

    private static TransformBody.Unwind unwind(String includeArrayIndex, String elementKey) {
        return new TransformBody.Unwind("items", includeArrayIndex, false, elementKey, null);
    }

    private static List<String> keyOf(TargetTable published) {
        return published.fields().stream()
                .filter(TargetField::primaryKey).map(TargetField::name).toList();
    }

    /** The whole chain in one line: what a node works out, published against the source's target. */
    private static List<String> publishedKey(NodeColumns produced) {
        return keyOf(StoreBackedDagSource.publishedAs(orders(), produced, atTheSource()));
    }

    @Test
    @DisplayName("the ordinal column an expansion invents is part of the key it publishes")
    void anInventedOrdinalColumnReachesTheTargetKey() {
        NodeColumns produced = NodeColumns.of(unwind("item_no", null), one(atTheSource()), null);

        assertThat(produced.columns()).containsKey("item_no");
        assertThat(publishedKey(produced)).containsExactly("o_id", "item_no");
    }

    /**
     * An element's own field identifies the row it becomes, and the element sits inside one column,
     * so the field is written beside it as a column of its own. Without that the declared key names
     * something no target table has: the published rule matches key names against the fields it is
     * publishing and passes over one it cannot find, which leaves the parent's key alone as the key
     * and says nothing.
     */
    @Test
    @DisplayName("an element's own key field is published as a column and keyed on")
    void anElementKeyIsLiftedIntoAColumnOfItsOwn() {
        NodeColumns produced = NodeColumns.of(unwind(null, "sku"), one(atTheSource()), null);

        assertThat(produced.columns()).containsKey("sku");
        assertThat(publishedKey(produced)).containsExactly("o_id", "sku");
    }

    /**
     * Declaring both is not a conflict and not two key columns: the element's own field is the
     * identity, the ordinal is an ordinary column beside it. Keying on the ordinal too would undo
     * the reason an author reaches for a stable field - insert an element ahead of another and
     * every later row's key moves.
     */
    @Test
    @DisplayName("declaring both keys on the element's field, and the ordinal is an ordinary column")
    void theOrdinalIsNotPartOfTheKeyWhenTheElementCarriesOne() {
        NodeColumns produced = NodeColumns.of(unwind("item_no", "sku"), one(atTheSource()), null);

        assertThat(produced.columns()).containsKeys("item_no", "sku");
        assertThat(publishedKey(produced)).containsExactly("o_id", "sku");
    }

    /**
     * A declaration naming neither is refused where it is written, so nothing here has to cope with
     * it - but inventing a key for one would turn a refusal into a pipeline that runs and collapses,
     * which is the failure this whole rule exists to remove.
     */
    @Test
    @DisplayName("an expansion naming no element locator adds nothing to the key")
    void namingNoLocatorAddsNothing() {
        NodeColumns produced = NodeColumns.of(unwind(null, null), one(atTheSource()), null);

        assertThat(publishedKey(produced)).containsExactly("o_id");
    }

    /**
     * The reverse half, and the more important one. Every kind that does not expand adds nothing, so
     * the published key is what it has always been - whatever of the table's own key still travels.
     */
    @Test
    @DisplayName("a kind that expands nothing publishes the key it always did")
    void theKindsThatExpandNothingKeyExactlyAsBefore() {
        assertThat(publishedKey(NodeColumns.of(
                new TransformBody.Filter("after.o_id > 1"), one(atTheSource()), null)))
                .containsExactly("o_id");
        assertThat(publishedKey(NodeColumns.of(
                new TransformBody.Union(), one(atTheSource()), null)))
                .containsExactly("o_id");
        assertThat(publishedKey(NodeColumns.of(
                new TransformBody.MapProjection(Map.of("region", FieldRule.rename("o_region"))),
                one(atTheSource()), null)))
                .containsExactly("o_id");
        // A projection that drops the key column publishes rows with nothing to match on, which is
        // reported at the sink against the table being written rather than invented back here.
        assertThat(publishedKey(NodeColumns.of(
                new TransformBody.MapProjection(Map.of("o_id", FieldRule.drop())),
                one(atTheSource()), null)))
                .isEmpty();
    }

    /**
     * A key column is a claim about the rows a node emits, so it survives only as long as the column
     * does. A projection downstream of an expansion that drops the ordinal leaves rows keyed on the
     * parent alone - the same collapse as before, and naming a column the target has not got would
     * hide it behind a key that reads complete.
     */
    @Test
    @DisplayName("a projection that drops the expansion's column drops it from the key too")
    void aDroppedColumnLeavesTheKey() {
        NodeColumns expanded = NodeColumns.of(unwind("item_no", null), one(atTheSource()), null);

        NodeColumns projected = NodeColumns.of(
                new TransformBody.MapProjection(Map.of("item_no", FieldRule.drop())),
                one(expanded), null);

        assertThat(projected.columns()).doesNotContainKey("item_no");
        assertThat(publishedKey(projected)).containsExactly("o_id");
    }

    /** A projection that keeps it keeps it: the pass-through half of the case above. */
    @Test
    @DisplayName("a projection downstream of an expansion carries the expansion's key on")
    void aProjectionCarriesTheKeyOn() {
        NodeColumns expanded = NodeColumns.of(unwind("item_no", null), one(atTheSource()), null);

        NodeColumns projected = NodeColumns.of(
                new TransformBody.MapProjection(Map.of("o_region", FieldRule.drop())),
                one(expanded), null);

        assertThat(publishedKey(projected)).containsExactly("o_id", "item_no");
    }

    /**
     * Two expansions in a row identify a row by both of their locators. The second does not fall
     * back to the table's own key, which would be the parent's alone and would collapse every row
     * the first one produced.
     */
    @Test
    @DisplayName("a second expansion keys on both locators, not on the table's key alone")
    void aSecondExpansionInheritsTheFirstsKey() {
        NodeColumns first = NodeColumns.of(unwind("item_no", null), one(atTheSource()), null);

        NodeColumns second = NodeColumns.of(
                new TransformBody.Unwind("items", "tag_no", false, null, null), one(first), null);

        assertThat(publishedKey(second)).containsExactly("o_id", "item_no", "tag_no");
    }

    /**
     * A rename is not a loss. The column is the same column under another name, and forgetting that
     * leaves the rows an expansion made distinct keyed on the parent alone again - the collapse this
     * rule exists to remove, arriving through a rule that only renamed something.
     */
    @Test
    @DisplayName("a projection that renames the expansion's column keys on the new name")
    void aRenamedColumnIsFollowedIntoTheKey() {
        NodeColumns expanded = NodeColumns.of(unwind("item_no", null), one(atTheSource()), null);

        NodeColumns projected = NodeColumns.of(
                new TransformBody.MapProjection(Map.of("line_no", FieldRule.rename("item_no"))),
                one(expanded), null);

        assertThat(projected.columns()).containsKey("line_no").doesNotContainKey("item_no");
        assertThat(publishedKey(projected)).containsExactly("o_id", "line_no");
    }

    /**
     * A merge forwards each input's rows as they arrive rather than reshaping them, so an added key
     * column is a claim about every row leaving the node only where every input makes it. One branch
     * that expanded a list and one that did not produce rows told apart by different things, and
     * keying on either branch's answer describes the other branch's rows wrongly.
     */
    @Test
    @DisplayName("a merge keeps an added key column only where every input adds the same one")
    void aMergeCarriesOnlyTheKeyItsInputsAgreeOn() {
        NodeColumns expanded = NodeColumns.of(unwind("item_no", null), one(atTheSource()), null);

        Map<String, NodeColumns> disagreeing = new LinkedHashMap<>();
        disagreeing.put("a", expanded);
        disagreeing.put("b", atTheSource());
        assertThat(publishedKey(NodeColumns.of(new TransformBody.Union(), disagreeing, null)))
                .containsExactly("o_id");

        Map<String, NodeColumns> agreeing = new LinkedHashMap<>();
        agreeing.put("a", expanded);
        agreeing.put("b", expanded);
        assertThat(publishedKey(NodeColumns.of(new TransformBody.Union(), agreeing, null)))
                .containsExactly("o_id", "item_no");
    }

    /** An unknown upstream is still unknown, and an unknown never reaches the published rule. */
    @Test
    @DisplayName("an expansion over a node nobody can describe stays undescribed")
    void anExpansionOverAnUnknownStaysUnknown() {
        NodeColumns dark = NodeColumns.unknown("js: nobody can say");

        assertThat(NodeColumns.of(unwind("item_no", "sku"), one(dark), null)).isEqualTo(dark);
    }
}
