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

    /** The paths arriving on the vertex that assembles or resolves {@code pathId}. */
    private static List<List<String>> edgesInto(NestTopology topology, List<String> pathId) {
        return topology.vertices().stream()
                .filter(vertex -> vertex.pathId().equals(pathId))
                .flatMap(vertex -> vertex.inbound().stream())
                .map(NestInbound::pathId)
                .toList();
    }

    @Test
    void anOrderCarryingACustomersIdentityPointsAtThatCustomer() {
        // orders.customer_id is not what identifies an order; customers.customer_id is what identifies a
        // customer. Only one reading survives that: the order names the customer, so the customer is
        // fetched by key rather than routed to the order - it never arrives on an edge at all.
        NestTopology topology = compile(nest("order", List.of("order_id"),
                embed("customer", "customer_id", "customer_id", EmbedAs.OBJECT, "customer", null)));

        assertThat(topology.isPassthrough()).isFalse();
        assertThat(edgesInto(topology, List.of())).doesNotContain(List.of("customer"));
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

        assertThat(edgesInto(topology, List.of())).contains(List.of("items"));
    }

    @Test
    void aJoinWhoseBothSidesAreKeysKeepsTheDirectionItAlwaysHad() {
        // profiles.customer_id identifies a profile and customers.customer_id identifies a customer, so
        // the join is one-to-one and renders the same document read either way. Taking the existing
        // direction is what makes such a tree compile to exactly what it compiled to before.
        NestTopology topology = compile(nest("customer", List.of("customer_id"),
                embed("profile", "customer_id", "customer_id", EmbedAs.OBJECT, "profile", null)));

        assertThat(edgesInto(topology, List.of())).contains(List.of("profile"));
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

        assertThat(edgesInto(topology, List.of())).contains(List.of("rows"));
    }

    @Test
    void anOrderLinePointingAtAProductIsReadTheSameWayAtAnyDepth() {
        // The deep case walks the same two questions with a different parent: the level above an embed is
        // an alias and a key whether it is the root or another embed, so nothing here knows about depth.
        Embed product = embed("customer", "customer_id", "customer_id", EmbedAs.OBJECT, "owner", null);
        NestTopology topology = compile(nest("order", List.of("order_id"),
                embed("item", "order_id", "order_id", EmbedAs.ARRAY, "items", List.of("item_id"), product)));

        assertThat(topology.isPassthrough()).isFalse();
        assertThat(edgesInto(topology, List.of("items"))).doesNotContain(List.of("items", "owner"));
    }
}
