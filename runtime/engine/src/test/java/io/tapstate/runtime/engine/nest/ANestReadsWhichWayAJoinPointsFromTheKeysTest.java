package io.tapstate.runtime.engine.nest;

import static io.tapstate.runtime.engine.nest.NestTreeFixtures.embed;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.nest;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.tables;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.Embed;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.TransformBody;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Which way an embed points is read off the keys, not written by the author.
 *
 * <p>The two directions are written identically - a pair of column names either way - so the pair alone
 * cannot say which. What can say is which side names its own table's key: a column that identifies a row
 * names one row, and a column that does not names however many share it. So the side that is its own
 * table's key is the side being pointed at.
 *
 * <p>Nothing here declares a direction, which is the point. Every tree below is one an author would have
 * written before any of this existed, and the ones that used to assemble documents grouped on a column
 * many rows share are now refused instead - with every count where it should be, that was the failure
 * nobody could see.
 */
class ANestReadsWhichWayAJoinPointsFromTheKeysTest {

    private static final String PIPELINE = "orders_to_docs";
    private static final String NODE = "assemble";

    private static NestTopology compile(TransformBody.Nest tree) {
        return NestTopology.compile(PIPELINE, NODE, tree, tables());
    }

    /**
     * The paths whose <em>rows</em> arrive on the vertex that assembles or resolves {@code pathId}.
     *
     * <p>Word that a pointed-at row was edited arrives on an edge of its own and is deliberately not
     * counted here. That edge carries no row - it names an identity and nothing else - so it says nothing
     * about which way the join was read, which is the only question these cases ask. Counting it would
     * make every pointed-at level look like a gathered one.
     */
    private static List<List<String>> rowEdgesInto(NestTopology topology, List<String> pathId) {
        return topology.vertices().stream()
                .filter(vertex -> vertex.pathId().equals(pathId))
                .flatMap(vertex -> vertex.inbound().stream())
                .filter(edge -> !edge.carriesTouches())
                .map(NestInbound::pathId)
                .toList();
    }

    /** The paths this vertex is told about edits to - the levels it points at rather than gathers. */
    private static List<List<String>> touchEdgesInto(NestTopology topology, List<String> pathId) {
        return topology.vertices().stream()
                .filter(vertex -> vertex.pathId().equals(pathId))
                .flatMap(vertex -> vertex.inbound().stream())
                .filter(NestInbound::carriesTouches)
                .map(NestInbound::pathId)
                .toList();
    }

    @Test
    void anOrderCarryingACustomersIdentityPointsAtThatCustomer() {
        // orders.customer_id is not what identifies an order; customers.customer_id is what identifies a
        // customer. Only one reading survives that: the order names the customer, so the customer row is
        // fetched by key rather than routed to the order, and no edge into the document carries it.
        NestTopology topology = compile(nest("order", List.of("order_id"),
                embed("customer", "customer_id", "customer_id", EmbedAs.OBJECT, "customer", null)));

        assertThat(topology.isPassthrough()).isFalse();
        assertThat(rowEdgesInto(topology, List.of()))
                .describedAs("no customer row is routed to a document: one customer sits behind thousands "
                        + "of them, so there is no document to route it to")
                .doesNotContain(List.of("customer"));
        assertThat(touchEdgesInto(topology, List.of()))
                .describedAs("what does arrive is word that such a row was edited, carrying an identity "
                        + "and no fields. Without it the direction is read right and then never propagates")
                .contains(List.of("customer"));
    }

    @Test
    void aRowPointedAtIsFiledUnderItsOwnIdentityRatherThanReachingAnyDocument() {
        NestTopology topology = compile(nest("order", List.of("order_id"),
                embed("customer", "customer_id", "customer_id", EmbedAs.OBJECT, "customer", null)));

        assertThat(topology.lookups()).hasSize(1);
        assertThat(topology.lookups().get(0).partitionKey())
                .describedAs("filed under what identifies the customer, which is what a reference names")
                .containsExactly("customer_id");
        assertThat(topology.streamAt(List.of("customer")).entryVertex())
                .describedAs("the stream enters the vertex that files it, not one holding documents")
                .isEqualTo(topology.lookups().get(0).name());
        assertThat(topology.stateNamespaces())
                .describedAs("its namespace is named, or a tree taken down leaves every row it held")
                .contains(topology.lookups().get(0).mapName());
        assertThat(topology.stateNamespaces())
                .describedAs("and so is the record of what points at those rows, which no vertex is named "
                        + "for and no rendering ever reads - so nothing else would ever go looking for it, "
                        + "and a tree taken down without naming it leaves it standing for good")
                .contains(topology.lookups().get(0).referencesMapName());
    }

    @Test
    void aRowPointedAtMayNotCarryEmbedsOfItsOwn() {
        // Left to compile, this puts the customer's own rows and the rows of the level beneath it into one
        // namespace under one name - two unrelated kinds of state in the same place - while every document
        // still goes out looking whole. So the assertion is that it is refused, not that it assembles.
        Throwable refused = catchThrowable(() -> compile(nest("order", List.of("order_id"),
                embed("customer", "customer_id", "customer_id", EmbedAs.OBJECT, "customer", null,
                        embed("profile", "customer_id", "customer_id", EmbedAs.OBJECT, "profile", null)))));

        assertThat(refused)
                .describedAs("a level the document points at cannot hold children of its own")
                .isInstanceOf(TapstateException.class);
        assertThat(refused.getMessage()).contains("customer").contains("profile");
    }

    @Test
    void anOrderLineCarryingItsOrdersIdentityHangsUnderThatOrder() {
        // The direction every embed written so far means, and it still compiles without saying so:
        // items.order_id is not what identifies an item, and orders.order_id is what identifies an order.
        NestTopology topology = compile(nest("order", List.of("order_id"),
                embed("item", "order_id", "order_id", EmbedAs.ARRAY, "items", List.of("item_id"))));

        assertThat(rowEdgesInto(topology, List.of())).contains(List.of("items"));
    }

    @Test
    void aJoinWhoseBothSidesAreKeysKeepsTheDirectionItAlwaysHad() {
        // profiles.customer_id identifies a profile and customers.customer_id identifies a customer, so
        // the join is one-to-one and renders the same document read either way. Taking the existing
        // direction is what makes such a tree compile to exactly what it compiled to before.
        NestTopology topology = compile(nest("customer", List.of("customer_id"),
                embed("profile", "customer_id", "customer_id", EmbedAs.OBJECT, "profile", null)));

        assertThat(rowEdgesInto(topology, List.of())).contains(List.of("profile"));
    }

    @Test
    void aJoinWithAKeyOnNeitherSideIsRefusedBecauseNothingSaysWhichWayItPoints() {
        // documents.customer_id is not what identifies a document, and orders.customer_id is not what
        // identifies an order. Read either way this groups rows on a column many of them share. Today it
        // compiles, runs, and assembles wrong documents while every count stays where it should be.
        Throwable thrown = catchThrowable(() -> compile(nest("order", List.of("order_id"),
                embed("document", "customer_id", "customer_id", EmbedAs.ARRAY, "docs",
                        List.of("document_id")))));

        assertThat(thrown).isInstanceOf(TapstateException.class);
        TapstateException refused = (TapstateException) thrown;
        assertThat(refused.code()).isEqualTo(NestError.EMBED_TARGET_NOT_PARENT_KEY);
        assertThat(refused.args())
                .containsEntry("embedPath", "docs")
                .containsEntry("fields", "customer_id")
                .containsEntry("parentKey", "order_id");
    }

    @Test
    void anEmbedTheParentSideAlreadyAnswersIsNotAskedForItsOwnKey() {
        // keyless_rows declares nothing that identifies a row, and nothing discovered one either. It does
        // not matter: orders.order_id identifies an order, so the direction is settled without asking. A
        // tree already answered must not be refused for want of something the answer does not depend on -
        // and every stream nobody discovered lands here, which is most of them in an unbound artifact.
        NestTopology topology = compile(nest("order", List.of("order_id"),
                embed("keyless", "order_id", "order_id", EmbedAs.ARRAY, "rows", List.of("some_id"))));

        assertThat(rowEdgesInto(topology, List.of())).contains(List.of("rows"));
    }

    /**
     * And the same holds when the level nothing identifies is the one being hung under, which is the half
     * the case above does not reach: there the unanswerable side is the embed's own, and the parent's key
     * settles the direction before it is ever asked for. Here the parent is the unanswerable one, so the
     * question is put to a level that cannot answer it at all.
     *
     * <p><b>Reading a direction is not a reason to demand a key that was never needed.</b> A level fed by
     * an earlier step of the pipeline carries no primary key and no index - nothing discovered one - and
     * embeds have always been allowed to hang under it. Asking such a level to identify its rows before
     * any embed below it may compile turns every one of those artifacts into a refusal, and the refusal
     * names an array key, which is not what the author got wrong.
     */
    @Test
    void anEmbedUnderALevelNothingIdentifiesKeepsTheDirectionItAlwaysHad() {
        Embed owner = embed("customer", "customer_id", "customer_id", EmbedAs.OBJECT, "owner", null);
        NestTopology topology = compile(nest("order", List.of("order_id"),
                embed("keyless", "order_id", "order_id", EmbedAs.ARRAY, "rows", List.of("some_id"), owner)));

        assertThat(rowEdgesInto(topology, List.of("rows")))
                .describedAs("the embed under it is a child level as it always was - its rows arrive at "
                        + "the level above, which is what the other direction would not do")
                .contains(List.of("rows", "owner"));
    }

    @Test
    void anOrderLinePointingAtAProductIsReadTheSameWayAtAnyDepth() {
        // The deep case walks the same two questions with a different parent: the level above an embed is
        // an alias and a key whether it is the root or another embed, so nothing here knows about depth.
        Embed product = embed("customer", "customer_id", "customer_id", EmbedAs.OBJECT, "owner", null);
        NestTopology topology = compile(nest("order", List.of("order_id"),
                embed("item", "order_id", "order_id", EmbedAs.ARRAY, "items", List.of("item_id"), product)));

        assertThat(topology.isPassthrough()).isFalse();
        assertThat(rowEdgesInto(topology, List.of("items")))
                .doesNotContain(List.of("items", "owner"));
        assertThat(touchEdgesInto(topology, List.of("items")))
                .describedAs("and the word of an edit lands on the level that points at it, whatever depth "
                        + "that level sits at - the same edge the root case gets, one level down")
                .contains(List.of("items", "owner"));
    }
}
