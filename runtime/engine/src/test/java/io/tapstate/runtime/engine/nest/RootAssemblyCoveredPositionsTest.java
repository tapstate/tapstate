package io.tapstate.runtime.engine.nest;

import io.tapstate.core.event.ChainPosition;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static io.tapstate.runtime.engine.nest.NestFixtures.at;
import static io.tapstate.runtime.engine.nest.NestFixtures.element;
import static io.tapstate.runtime.engine.nest.NestFixtures.row;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * What a document going out covers, which is the other half of what an assembly owes the durable frontier.
 * The held side keeps the frontier below everything still here; this side is what lets it move at all —
 * a document is the only thing that ever leaves an assembler, so if it says nothing about where its
 * contents came from, no sink can ever ack a chain that ran through a nest.
 *
 * <p><b>The highest of each chain is covered, not the lowest.</b> A document going out carries everything
 * it took in, so every position it took in on a chain has now been shown. Reporting the lowest instead
 * would leave the ones above it reported by nobody and the frontier trailing them for good. Nothing here
 * needs to check what is still held beneath: a position covered is only a candidate, and the bound
 * combined across every instance is what decides how far the frontier may actually go.
 *
 * <p><b>What goes out for a deleted root is its key, and a key carries no element.</b> So the deletion
 * covers the root's own change and nothing else, and everything absorbed alongside it goes on being held
 * — the same asymmetry the held side already keeps, seen from the other direction. Leaving the root's own
 * change out of it too would be worse than conservative: a root deleted and never seen again would pin
 * its chain for as long as the job runs.
 */
class RootAssemblyCoveredPositionsTest {

    private static final List<String> POLICIES = List.of("policies");
    private static final List<String> CLAIMS = List.of("policies", "claims");

    private static Map<String, ChainPosition> on(String chain, long seq, String token) {
        return Map.of(chain, new ChainPosition(at(seq), token));
    }

    private static RootAssembly customer() {
        RootAssembly assembly = new RootAssembly();
        assembly.applyRoot(row("customer_id", "C1"), at(1), on("customer", 1, "t1"));
        return assembly;
    }

    @Test
    void aDocumentCoversTheHighestPositionItTookInOnEachChain() {
        RootAssembly assembly = customer();
        assembly.applyElement(element(POLICIES, null, "P1", "P1"), row("policy_no", "P1"), at(4),
                on("policy", 4, "t4"));
        assembly.applyElement(element(CLAIMS, "P1", "CL1", "CL1"), row("claim_no", "CL1"), at(9),
                on("claim", 9, "t9"));
        assembly.applyElement(element(CLAIMS, "P1", "CL2", "CL2"), row("claim_no", "CL2"), at(5),
                on("claim", 5, "t5"));

        assertThat(assembly.covered())
                .describedAs("both halves travel: the order to compare on, the token to persist")
                .containsOnly(
                        Map.entry("customer", new ChainPosition(at(1), "t1")),
                        Map.entry("policy", new ChainPosition(at(4), "t4")),
                        Map.entry("claim", new ChainPosition(at(9), "t9")));
    }

    @Test
    void whatADocumentCoveredIsForgottenOnceItHasGoneOut() {
        RootAssembly assembly = customer();
        assembly.applyElement(element(POLICIES, null, "P1", "P1"), row("policy_no", "P1"), at(4),
                on("policy", 4, "t4"));

        assembly.documentSent();

        assertThat(assembly.covered())
                .describedAs("a second document covering the same positions would ack them twice over")
                .isEmpty();
    }

    @Test
    void anElementStillWaitingForItsAncestorIsCoveredByNothing() {
        RootAssembly assembly = customer();

        assembly.applyElement(element(CLAIMS, "P1", "CL1", "CL1"), row("claim_no", "CL1"), at(9),
                on("claim", 9, "t9"));

        assertThat(assembly.covered())
                .describedAs("it was taken off the stream and put where no sink can see it")
                .containsOnlyKeys("customer");
        assertThat(assembly.lowestHeldByChain()).containsOnlyKeys("customer", "claim");
    }

    @Test
    void aDeletedRootsKeyRowCoversTheDeletionAndNotWhatItLeavesBehind() {
        RootAssembly assembly = customer();
        assembly.documentSent();
        assembly.applyElement(element(POLICIES, null, "P1", "P1"), row("policy_no", "P1"), at(4),
                on("policy", 4, "t4"));

        assembly.deleteRoot(at(7), on("customer", 7, "t7"));

        assertThat(assembly.coveredByADeletion())
                .containsExactly(Map.entry("customer", new ChainPosition(at(7), "t7")));
        assertThat(assembly.covered())
                .describedAs("the element went out in no document and is still owed")
                .containsOnly(
                        Map.entry("customer", new ChainPosition(at(7), "t7")),
                        Map.entry("policy", new ChainPosition(at(4), "t4")));
    }

    @Test
    void aDeletionGoingOutReleasesTheRootsOwnChangeAndNothingElse() {
        RootAssembly assembly = customer();
        assembly.documentSent();
        assembly.applyElement(element(POLICIES, null, "P1", "P1"), row("policy_no", "P1"), at(4),
                on("policy", 4, "t4"));
        assembly.deleteRoot(at(7), on("customer", 7, "t7"));

        assembly.deletionSent();

        assertThat(assembly.coveredByADeletion()).isEmpty();
        assertThat(assembly.covered())
                .describedAs("a root that never comes back would otherwise pin its own chain for good")
                .containsExactly(Map.entry("policy", new ChainPosition(at(4), "t4")));
        assertThat(assembly.lowestHeldByChain())
                .describedAs("what the key row could not carry is still held")
                .containsExactly(Map.entry("policy", new ChainPosition(at(4), "t4")));
    }

    @Test
    void aRootChangeRefusedAsTooOldCoversNothing() {
        RootAssembly assembly = customer();
        assembly.documentSent();

        assertThat(assembly.applyRoot(row("customer_id", "C1"), at(1), on("customer", 1, "t1"))).isFalse();

        assertThat(assembly.covered())
                .describedAs("a change that changed nothing here was still shown by whoever sent it first")
                .isEmpty();
    }
}
