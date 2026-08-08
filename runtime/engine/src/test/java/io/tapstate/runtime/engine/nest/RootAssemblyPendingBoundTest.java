package io.tapstate.runtime.engine.nest;

import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.model.EmbedAs;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Map;

import static io.tapstate.runtime.engine.nest.NestFixtures.at;
import static io.tapstate.runtime.engine.nest.NestFixtures.element;
import static io.tapstate.runtime.engine.nest.NestFixtures.noPositions;
import static io.tapstate.runtime.engine.nest.NestFixtures.row;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * What an assembly owes the durable frontier: every change it has taken in and not yet sent on inside a
 * document. Two quite different things are held that way and both must be reported, because the frontier
 * passing either one loses data that nothing anywhere reports.
 *
 * <p>An element whose ancestor has not arrived has been taken off the stream and put nowhere a sink can
 * see it. So has an element absorbed into a document whose root is absent — it is attached, correctly,
 * and then nothing goes out at all, because a rootless document is a ghost downstream. The second is the
 * easier one to miss: the element is in the tree, in its right place, and looks for all the world like it
 * has been dealt with.
 *
 * <p>The bound is only ever a position an element really arrived with, and both halves of it travel
 * together — the order to compare on, the token to persist and hand back to the connector.
 */
class RootAssemblyPendingBoundTest {

    private static final List<String> POLICIES = List.of("policies");
    private static final List<String> CLAIMS = List.of("policies", "claims");
    private static final List<String> DOCUMENTS = List.of("policies", "claims", "documents");
    private static final List<EmbedSlot> SHAPE = List.of(new EmbedSlot("policies", EmbedAs.ARRAY,
            List.of(new EmbedSlot("claims", EmbedAs.ARRAY, List.of()))));

    private static Map<String, ChainPosition> on(String chain, long seq, String token) {
        return Map.of(chain, new ChainPosition(at(seq), token));
    }

    private static RootAssembly customer() {
        RootAssembly assembly = new RootAssembly();
        assembly.applyRoot(row("customer_id", "C1"), at(1));
        return assembly;
    }

    @Test
    void anElementWaitingForAnAncestorReportsThePositionItArrivedWith() {
        RootAssembly assembly = customer();

        assembly.applyElement(element(CLAIMS, "P1", "CL1", "CL1"), row("claim_no", "CL1"), at(5),
                on("claim", 5, "t5"));

        assertThat(assembly.lowestHeldByChain())
                .containsExactly(Map.entry("claim", new ChainPosition(at(5), "t5")));
    }

    @Test
    void anElementThatFoundItsAncestorStopsHoldingTheFrontierBack() {
        RootAssembly assembly = customer();
        assembly.applyElement(element(CLAIMS, "P1", "CL1", "CL1"), row("claim_no", "CL1"), at(5),
                on("claim", 5, "t5"));

        assembly.applyElement(element(POLICIES, null, "P1", "P1"), row("policy_no", "P1"), at(6),
                on("policy", 6, "t6"));
        assembly.documentSent();

        assertThat(assembly.lowestHeldByChain()).isEmpty();
    }

    @Test
    void theLowestPositionOnAChainIsReported() {
        RootAssembly assembly = customer();

        // Out of order on purpose: the lowest is neither the first to arrive nor the last.
        assembly.applyElement(element(CLAIMS, "P1", "CL1", "CL1"), row("claim_no", "CL1"), at(7),
                on("claim", 7, "t7"));
        assembly.applyElement(element(CLAIMS, "P1", "CL2", "CL2"), row("claim_no", "CL2"), at(5),
                on("claim", 5, "t5"));
        assembly.applyElement(element(CLAIMS, "P1", "CL3", "CL3"), row("claim_no", "CL3"), at(9),
                on("claim", 9, "t9"));

        assertThat(assembly.lowestHeldByChain())
                .containsExactly(Map.entry("claim", new ChainPosition(at(5), "t5")));
    }

    @Test
    void everyChainReportsItsOwnBoundAndTheyAreNeverCompared() {
        RootAssembly assembly = customer();

        assembly.applyElement(element(CLAIMS, "P1", "CL1", "CL1"), row("claim_no", "CL1"), at(9),
                on("claim", 9, "t9"));
        assembly.applyElement(element(DOCUMENTS, "CL9", "D1", null), row("document_no", "D1"), at(4),
                on("document", 4, "t4"));

        assertThat(assembly.lowestHeldByChain()).containsOnly(
                Map.entry("claim", new ChainPosition(at(9), "t9")),
                Map.entry("document", new ChainPosition(at(4), "t4")));
    }

    @Test
    void aDeletionWaitingForAnAncestorHoldsTheFrontierBackJustAsARowDoes() {
        RootAssembly assembly = customer();

        assembly.deleteElement(element(CLAIMS, "P1", "CL1", "CL1"), at(5), on("claim", 5, "t5"));

        assertThat(assembly.lowestHeldByChain())
                .containsExactly(Map.entry("claim", new ChainPosition(at(5), "t5")));
    }

    @Test
    void anElementMovedToAParentThatHasNotArrivedHoldsTheFrontierBack() {
        RootAssembly assembly = customer();
        assembly.applyElement(element(POLICIES, null, "P1", "P1"), row("policy_no", "P1"), at(2),
                noPositions());
        assembly.applyElement(element(CLAIMS, "P1", "CL1", "CL1"), row("claim_no", "CL1"), at(3),
                noPositions());

        assembly.reparentElement(element(CLAIMS, "P1", "CL1", "CL1"), element(CLAIMS, "P9", "CL1", "CL1"),
                row("claim_no", "CL1"), at(8), on("claim", 8, "t8"));

        assertThat(assembly.lowestHeldByChain())
                .containsExactly(Map.entry("claim", new ChainPosition(at(8), "t8")));
    }

    @Test
    void aWaitingElementKeepsItsPositionAcrossBeingStoredAndRestored() throws Exception {
        RootAssembly assembly = customer();
        assembly.applyElement(element(CLAIMS, "P1", "CL1", "CL1"), row("claim_no", "CL1"), at(5),
                on("claim", 5, "t5"));

        assertThat(restored(assembly).lowestHeldByChain())
                .containsExactly(Map.entry("claim", new ChainPosition(at(5), "t5")));
    }

    @Test
    void anAssemblyThatHasTakenInNothingReportsNoBoundAtAll() {
        assertThat(new RootAssembly().lowestHeldByChain()).isEmpty();
    }

    @Test
    void anElementAbsorbedWhileTheRootIsAbsentHoldsTheFrontierBack() {
        RootAssembly assembly = new RootAssembly();

        assembly.applyElement(element(POLICIES, null, "P1", "P1"), row("policy_no", "P1"), at(5),
                on("policy", 5, "t5"));

        // Attached, in its right place, and going nowhere: a rootless document is never rendered.
        assertThat(assembly.render(SHAPE)).isEmpty();
        assertThat(assembly.lowestHeldByChain())
                .containsExactly(Map.entry("policy", new ChainPosition(at(5), "t5")));
    }

    @Test
    void theDocumentGoingOutIsWhatReleasesWhatItCarried() {
        RootAssembly assembly = new RootAssembly();
        assembly.applyElement(element(POLICIES, null, "P1", "P1"), row("policy_no", "P1"), at(5),
                on("policy", 5, "t5"));

        assembly.applyRoot(row("customer_id", "C1"), at(6));
        assertThat(assembly.render(SHAPE)).isPresent();
        assembly.documentSent();

        assertThat(assembly.lowestHeldByChain()).isEmpty();
    }

    @Test
    void aDocumentRenderedButNotYetSentIsStillHoldingWhatItCarries() {
        RootAssembly assembly = new RootAssembly();
        assembly.applyElement(element(POLICIES, null, "P1", "P1"), row("policy_no", "P1"), at(5),
                on("policy", 5, "t5"));
        assembly.applyRoot(row("customer_id", "C1"), at(6));

        assembly.render(SHAPE);

        // Rendering is a question, not an act: what was asked for may still fail to leave the outbox.
        assertThat(assembly.lowestHeldByChain())
                .containsExactly(Map.entry("policy", new ChainPosition(at(5), "t5")));
    }

    @Test
    void aDocumentSentDoesNotReleaseWhatIsStillWaitingForItsAncestor() {
        RootAssembly assembly = customer();
        assembly.applyElement(element(CLAIMS, "P1", "CL1", "CL1"), row("claim_no", "CL1"), at(5),
                on("claim", 5, "t5"));

        assertThat(assembly.render(SHAPE)).isPresent();
        assembly.documentSent();

        // The document that went out had no claim in it: its policy has never arrived.
        assertThat(assembly.lowestHeldByChain())
                .containsExactly(Map.entry("claim", new ChainPosition(at(5), "t5")));
    }

    @Test
    void whatWasAbsorbedBeforeTheRootWasDeletedIsHeldOnTo() {
        RootAssembly assembly = customer();

        assembly.applyElement(element(POLICIES, null, "P1", "P1"), row("policy_no", "P1"), at(5),
                on("policy", 5, "t5"));
        assembly.deleteRoot(at(6));

        // What goes out for a deleted root is the key, so a sink can remove the document — not the
        // document, so the element that arrived alongside has still been shown to nobody.
        assertThat(assembly.render(SHAPE)).isEmpty();
        assertThat(assembly.lowestHeldByChain())
                .containsExactly(Map.entry("policy", new ChainPosition(at(5), "t5")));
    }

    @Test
    void anUpdateOfAnElementAlreadyThereIsHeldJustAsItsArrivalWas() {
        RootAssembly assembly = customer();
        assembly.applyElement(element(POLICIES, null, "P1", "P1"), row("policy_no", "PN-1"), at(5),
                on("policy", 5, "t5"));
        assembly.documentSent();

        assembly.applyElement(element(POLICIES, null, "P1", "P1"), row("policy_no", "PN-2"), at(7),
                on("policy", 7, "t7"));

        assertThat(assembly.lowestHeldByChain())
                .containsExactly(Map.entry("policy", new ChainPosition(at(7), "t7")));
    }

    @Test
    void anElementMovedWithinARootlessDocumentIsHeldWhereItLands() {
        RootAssembly assembly = new RootAssembly();
        assembly.applyElement(element(POLICIES, null, "P1", "P1"), row("policy_no", "PN-1"), at(2),
                noPositions());
        assembly.applyElement(element(POLICIES, null, "P9", "P9"), row("policy_no", "PN-9"), at(3),
                noPositions());
        assembly.applyElement(element(CLAIMS, "P1", "CL1", "CL1"), row("claim_no", "CL1"), at(4),
                noPositions());

        assembly.reparentElement(element(CLAIMS, "P1", "CL1", "CL1"), element(CLAIMS, "P9", "CL1", "CL1"),
                row("claim_no", "CL1"), at(8), on("claim", 8, "t8"));

        // Its new parent is right here, so it is attached rather than held for one - and still nothing
        // goes out, because the root is absent.
        assertThat(assembly.render(SHAPE)).isEmpty();
        assertThat(assembly.lowestHeldByChain())
                .containsExactly(Map.entry("claim", new ChainPosition(at(8), "t8")));
    }

    /**
     * The one path where leaving the waiting bucket is not the same as leaving at all. A moved element
     * waits for the parent it was moved to, and a document that goes out meanwhile carries no sign of it -
     * so when that parent finally arrives and the whole node is attached, the only thing left reporting it
     * is the attachment itself. Here the root is gone by then, so nothing goes out to release it either.
     */
    @Test
    void aMovedElementAttachedIntoARootlessDocumentIsStillHeld() {
        RootAssembly assembly = customer();
        assembly.applyElement(element(POLICIES, null, "P1", "P1"), row("policy_no", "PN-1"), at(2),
                noPositions());
        assembly.applyElement(element(CLAIMS, "P1", "CL1", "CL1"), row("claim_no", "CL1"), at(3),
                noPositions());
        assembly.reparentElement(element(CLAIMS, "P1", "CL1", "CL1"), element(CLAIMS, "P9", "CL1", "CL1"),
                row("claim_no", "CL1"), at(8), on("claim", 8, "t8"));
        assembly.documentSent();
        assembly.deleteRoot(at(9));

        assembly.applyElement(element(POLICIES, null, "P9", "P9"), row("policy_no", "PN-9"), at(10),
                noPositions());

        assertThat(assembly.lowestHeldByChain())
                .containsExactly(Map.entry("claim", new ChainPosition(at(8), "t8")));
    }

    @Test
    void whatAnAbsentRootIsHoldingSurvivesBeingStoredAndRestored() throws Exception {
        RootAssembly assembly = new RootAssembly();
        assembly.applyElement(element(POLICIES, null, "P1", "P1"), row("policy_no", "P1"), at(5),
                on("policy", 5, "t5"));

        // A restart re-derives what an instance had promised, but not what its state is holding: that
        // travels with the state, or the frontier walks past it the moment the job comes back up.
        assertThat(restored(assembly).lowestHeldByChain())
                .containsExactly(Map.entry("policy", new ChainPosition(at(5), "t5")));
    }

    private static RootAssembly restored(RootAssembly assembly) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(assembly);
        }
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            return (RootAssembly) in.readObject();
        }
    }
}
