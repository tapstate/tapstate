package io.tapstate.runtime.engine.nest;

import static io.tapstate.runtime.engine.nest.NestTreeFixtures.embed;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.keyed;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.nest;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.tables;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.TransformBody;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * What identifies one row of a level is a question that always has an answer, or the tree does not run.
 *
 * <p>Four places can answer it and they are asked in order: the key the level declares, the table's
 * primary key, a single unique index, and then nothing - which refuses the tree by name rather than
 * carrying an unknown identity into the assembly. The order matters in one direction only: a declared
 * key is taken as written and the schema is not consulted to second-guess it, because a business
 * identity and a physical primary key are allowed to differ and only the author knows whether they do.
 *
 * <p>The last two cases are a pair and neither stands without the other. Taking one unique index when
 * there is exactly one is safe; taking one when there are two makes the identity of the level depend on
 * which index the source happened to report first, which is the same silent wrongness this whole
 * direction exists to remove - it just moves it from the author to the catalog.
 */
class ANestAlwaysKnowsWhatIdentifiesALevelsRowsTest {

    private static final String PIPELINE = "orders_to_docs";
    private static final String NODE = "assemble";

    private static NestTopology compile(TransformBody.Nest tree) {
        return NestTopology.compile(PIPELINE, NODE, tree, tables());
    }

    /** An embed of {@code alias} under an order, declaring {@code key} as what identifies its rows. */
    private static TransformBody.Nest treeEmbedding(String alias, List<String> key) {
        return nest("order", List.of("order_id"),
                keyed(embed(alias, "order_id", "order_id", EmbedAs.ARRAY, "rows", null), key));
    }

    @Test
    void aLevelThatDeclaresItsKeyIsTakenAtItsWord() {
        // items declares item_id as its primary key, and the author says the level is identified by
        // something else. The declared one wins and nothing goes looking for a second opinion: a
        // business identity that differs from the physical key is the case this rung exists for.
        NestTopology topology = compile(treeEmbedding("item", List.of("sku_code")));

        assertThat(topology.vertices()).isNotEmpty();
        assertThat(topology.streams()).anySatisfy(stream ->
                assertThat(stream.arrayKey()).isEqualTo(List.of("sku_code")));
    }

    @Test
    void aLevelThatDeclaresNothingTakesTheTablesPrimaryKey() {
        NestTopology topology = compile(treeEmbedding("item", null));

        assertThat(topology.streams()).anySatisfy(stream ->
                assertThat(stream.arrayKey()).isEqualTo(List.of("item_id")));
    }

    @Test
    void aLevelWithNoPrimaryKeyTakesItsOneUniqueIndex() {
        // No declared key and no primary key, but exactly one unique index: that index is the only
        // thing on the table claiming to identify a row, so there is nothing to choose between.
        NestTopology topology = compile(treeEmbedding("uniquelyIndexed", null));

        assertThat(topology.streams()).anySatisfy(stream ->
                assertThat(stream.arrayKey()).isEqualTo(List.of("serial_no")));
    }

    @Test
    void aLevelWithTwoUniqueIndexesIsRefusedRatherThanPickingOne() {
        // Both indexes identify a row and neither is more the identity than the other. Picking would
        // make the answer depend on catalog order; the refusal names both and points at the one rung
        // that can settle it - writing the key on the level itself.
        Throwable thrown = catchThrowable(() -> compile(treeEmbedding("twiceIndexed", null)));

        assertThat(thrown).isInstanceOf(TapstateException.class);
        TapstateException refused = (TapstateException) thrown;
        assertThat(refused.code()).isEqualTo(NestError.KEY_AMBIGUOUS);
        assertThat(refused.args()).containsEntry("table", "twice_indexed");
        assertThat(String.valueOf(refused.args().get("candidates")))
                .contains("serial_no")
                .contains("batch_no");
    }

    @Test
    void aLevelWithNothingToIdentifyItByIsRefusedByName() {
        Throwable thrown = catchThrowable(() -> compile(treeEmbedding("keyless", null)));

        assertThat(thrown).isInstanceOf(TapstateException.class);
        TapstateException refused = (TapstateException) thrown;
        assertThat(refused.code()).isEqualTo(NestError.ARRAY_KEY_UNRESOLVABLE);
        assertThat(refused.args()).containsEntry("table", "keyless_rows");
    }
}
