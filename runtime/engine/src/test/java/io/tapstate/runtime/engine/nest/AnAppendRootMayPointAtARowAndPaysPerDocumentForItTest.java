package io.tapstate.runtime.engine.nest;

import static io.tapstate.runtime.engine.nest.NestTreeFixtures.tables;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.Embed;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.model.TransformBody;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * That an append-only root may point at a row, and that what it costs is written down rather than
 * refused.
 *
 * <p><b>The refusal beside it is what makes this worth a case.</b> An append-only root under a tree that
 * tracks structural key changes is refused outright, because moving a subtree holds emissions back until
 * it lands and holding emissions back is the one thing append-only forbids. Pointing at a row looks like
 * the same kind of thing from a distance - both are about a document changing for a reason outside its
 * own stream - and reading across from one to the other would produce a refusal nobody decided on. They
 * differ in the direction that matters: a rename reaching the documents that point at the row produces
 * records those documents genuinely owed, where a suppressed emission produces one fewer record than
 * there should be.
 *
 * <p>So the pair is asserted together. On its own, the tree that compiles says only that today's code
 * lets it through; next to the tree that does not, it says which of the two the rule is about.
 */
class AnAppendRootMayPointAtARowAndPaysPerDocumentForItTest {

    private static final String PIPELINE = "orders_to_docs";
    private static final String NODE = "assemble";

    /** An append-only root whose documents point at a customer row. */
    private static TransformBody.Nest appendRootPointingAtACustomer(Boolean trackKeyChanges) {
        Embed customer = new Embed("customer", Map.of("customer_id", "cust_ref"), EmbedAs.OBJECT,
                "customer", null, null, trackKeyChanges, null);
        return new TransformBody.Nest(null, null,
                new NestRoot("order", List.of("order_id"), "append", null, List.of(customer)));
    }

    @Test
    @DisplayName("an append-only root pointing at a row compiles, and folds nothing")
    void anAppendRootMayPointAtARowAndSendsEveryChangeOfItsOwn() {
        NestTopology topology = NestTopology.compile(
                PIPELINE, NODE, appendRootPointingAtACustomer(null), tables());

        assertThat(topology.lookups())
                .describedAs("the tree compiled with its pointed-at level intact rather than being "
                        + "refused, which is the whole of the first half")
                .hasSize(1);
        assertThat(topology.foldingAllowed())
                .describedAs("an append reader is owed every change as its own record, so nothing here "
                        + "may be merged into anything else. This is what makes one edit to a row cost "
                        + "one record per document pointing at it - the cost this shape accepts, and the "
                        + "reason it is worth being explicit about rather than discovering in production")
                .isFalse();
    }

    @Test
    @DisplayName("the same root is still refused for tracking structural key changes")
    void trackingKeyChangesUnderAnAppendRootIsStillRefused() {
        assertThatThrownBy(() -> NestTopology.compile(
                        PIPELINE, NODE, appendRootPointingAtACustomer(true), tables()))
                .describedAs("this is the refusal the case above must not be read across from. If this "
                        + "one ever stops throwing, the case above stops meaning 'pointing at a row is "
                        + "allowed' and starts meaning 'nothing is checked here at all'")
                .isInstanceOf(TapstateException.class)
                .hasMessageContaining(NestError.APPEND_MODE_CONFLICTS_WITH_KEY_TRACKING.code());
    }

    @Test
    @DisplayName("an upsert root with the same shape is allowed to fold")
    void anUpsertRootWithTheSameShapeMayFold() {
        TransformBody.Nest upsert = new TransformBody.Nest(null, null,
                new NestRoot("order", List.of("order_id"), null, null,
                        List.of(new Embed("customer", Map.of("customer_id", "cust_ref"), EmbedAs.OBJECT,
                                "customer", null, null, null, null))));

        assertThat(NestTopology.compile(PIPELINE, NODE, upsert, tables()).foldingAllowed())
                .describedAs("the control: the same tree without append folds, so what the case above "
                        + "reads is the write mode and not something true of every tree with a pointer "
                        + "in it")
                .isTrue();
    }
}
