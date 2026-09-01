package io.tapstate.runtime.engine.nest;

import static io.tapstate.runtime.engine.nest.NestTreeFixtures.embed;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.nest;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.referencing;
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
 * A tree is refused for the direction it declares, not for the direction it happens to be read as.
 *
 * <p>The same join is correct one way and wrong the other, and it is written the same both times.
 * {@code order.customer_id -> customer.customer_id} embeds one customer under an order; read the other
 * way it embeds every order of that customer under one order. Nothing in the pair of names says which,
 * so each direction has its own question to answer, and the answers are mirror images: one asks whether
 * the field the embedded rows carry identifies the row they hang under, the other whether it identifies
 * the embedded row itself.
 *
 * <p>Asking only one of them is how this went wrong before. A tree that names the wrong side compiles,
 * runs, and assembles documents keyed on a column many rows share - every count stays where it should
 * be, and the documents are wrong.
 */
class ANestSaysWhichSideOfAJoinCarriesTheOtherTest {

    private static final String PIPELINE = "orders_to_docs";
    private static final String NODE = "assemble";

    /** {@code customer_id} on both sides: one customer under each order, or every order under a customer. */
    private static Embed customerUnderOrder() {
        return embed("customer", "customer_id", "customer_id", EmbedAs.OBJECT, "customer", null);
    }

    private static Embed ordersUnderCustomer() {
        return embed("order", "customer_id", "customer_id", EmbedAs.ARRAY, "orders", List.of("order_id"));
    }

    private static NestTopology compile(TransformBody.Nest tree) {
        return NestTopology.compile(PIPELINE, NODE, tree, tables());
    }

    @Test
    void anOrderCarryingItsCustomersIdentityEmbedsThatOneCustomer() {
        // customer_id identifies a customer, so it names one row to fetch. The order rows are not
        // regrouped by it: the root stays keyed on its own key, which is what says this compiles at all.
        NestTopology topology =
                compile(nest("order", List.of("order_id"), referencing(customerUnderOrder())));

        assertThat(topology.isPassthrough()).isFalse();
    }

    @Test
    void theSameJoinReadTheOtherWayIsRefused() {
        // Byte for byte the tree above, minus the one word. Now the embed claims customer_id identifies
        // an order, and the root says its key is order_id - two answers to one question.
        Throwable thrown =
                catchThrowable(() -> compile(nest("order", List.of("order_id"), customerUnderOrder())));

        assertThat(thrown).isInstanceOf(TapstateException.class);
        assertThat(((TapstateException) thrown).code())
                .isEqualTo(NestError.SIBLING_EMBEDS_TARGET_DIFFERENT_PARENT_KEYS);
    }

    @Test
    void ordersHangingUnderTheirCustomerAreEmbeddedTheWayTheyAlwaysWere() {
        // The direction every embed written so far means, and it still compiles without saying so.
        assertThat(compile(nest("customer", List.of("customer_id"), ordersUnderCustomer())).isPassthrough())
                .isFalse();
    }

    @Test
    void theSameJoinCalledAReferenceIsRefusedBecauseItNamesNoSingleOrder() {
        // The mirror of the case above: customer_id does not identify an order, so a customer row
        // carrying one names every order that customer ever placed, not the one to embed.
        Throwable thrown = catchThrowable(
                () -> compile(nest("customer", List.of("customer_id"), referencing(ordersUnderCustomer()))));

        assertThat(thrown).isInstanceOf(TapstateException.class);
        TapstateException e = (TapstateException) thrown;
        assertThat(e.code()).isEqualTo(NestError.EMBED_REFERENCE_NOT_OWN_KEY);
        assertThat(e.args())
                .containsEntry("embedPath", "orders")
                .containsEntry("fields", "customer_id")
                .containsEntry("referencedKey", "order_id");
    }

    @Test
    void aReferenceIntoATableThatDeclaresNoKeyIsLetThrough() {
        // The existing ceiling, kept deliberately rather than tightened. Only a declared key can say
        // whether a column identifies a row, so a table that declares none is left alone - the same
        // pass every embed already gets, and pipelines relying on it are running today.
        Embed keyless = referencing(
                embed("keyless", "some_id", "some_ref", EmbedAs.OBJECT, "thing", List.of("some_id")));

        assertThat(compile(nest("customer", List.of("customer_id"), keyless)).isPassthrough()).isFalse();
    }
}
