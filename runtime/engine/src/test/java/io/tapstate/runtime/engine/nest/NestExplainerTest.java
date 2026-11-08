package io.tapstate.runtime.engine.nest;

import io.tapstate.core.lifecycle.NestTree;
import io.tapstate.core.model.Embed;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.model.TransformBody;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The tree a nest step is explained with is what the compiler decided, not a second reading of the
 * definition: an embed whose child carries the parent's key is owned, one whose parent carries the child's key
 * is a shared row with the per-row limit it runs under, and the element limit is the nest's own where it
 * writes one.
 */
class NestExplainerTest {

    private static final Embed ITEMS = new Embed("i", Map.of("order_id", "id"), EmbedAs.ARRAY, "items",
            null, List.of("id"), null, null, List.of(
                    new Embed("n", Map.of("item_id", "id"), EmbedAs.ARRAY, "notes", null, List.of("id"), null,
                            null, null)));
    private static final Embed CUSTOMER = new Embed("c", Map.of("id", "customer_id"), EmbedAs.OBJECT, "customer",
            null, null, null, null, null);

    private static final Map<String, NestTable> TABLES = Map.of(
            "o", new NestTable("orders", List.of("id"), List.of(), List.of("id", "customer_id")),
            "i", new NestTable("order_items", List.of("id"), List.of(), List.of("id", "order_id")),
            "n", new NestTable("item_notes", List.of("id"), List.of(), List.of("id", "item_id")),
            "c", new NestTable("customers", List.of("id"), List.of(), List.of("id", "name")));

    @Test
    void eachEmbedReadsAsOwnedOrSharedTheWayItCompiled() {
        TransformBody.Nest nest = new TransformBody.Nest(null, null,
                new NestRoot("o", List.of("id"), null, null, List.of(ITEMS, CUSTOMER)));
        NestTopology topology = NestTopology.compile("p", "doc", nest, TABLES::get);

        NestTree tree = NestExplainer.tree("p", "doc", nest, topology, NestSettings.defaults());

        assertThat(tree.rootAlias()).isEqualTo("o");
        assertThat(tree.elementLimit()).isEqualTo(NestSettings.DEFAULT_ELEMENT_LIMIT);
        assertThat(tree.embeds()).extracting(NestTree.Level::path, NestTree.Level::owned, NestTree.Level::as)
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple("items", true, "array"),
                        org.assertj.core.groups.Tuple.tuple("customer", false, "object"));
        assertThat(tree.embeds().getFirst().referrerLimit()).isZero();
        assertThat(tree.embeds().getLast().referrerLimit()).isEqualTo(NestSettings.DEFAULT_REFERENCE_FANOUT_LIMIT);
        assertThat(tree.embeds().getFirst().children()).singleElement()
                .satisfies(notes -> assertThat(notes.path()).isEqualTo("items.notes"));
    }

    @Test
    void aNestThatWritesItsOwnElementLimitIsExplainedWithIt() {
        TransformBody.Nest nest = new TransformBody.Nest(null, null, null, 500,
                new NestRoot("o", List.of("id"), null, null, List.of(ITEMS)));

        NestTree tree = NestExplainer.tree("p", "doc", nest, NestTopology.compile("p", "doc", nest, TABLES::get),
                NestSettings.defaults());

        assertThat(tree.elementLimit()).isEqualTo(500);
    }
}
