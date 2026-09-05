package io.tapstate.runtime.engine.nest;

import static io.tapstate.runtime.engine.nest.NestTreeFixtures.embed;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.nest;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.tables;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.tracking;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.Embed;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.model.TransformBody;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * What a nest tree has to be for the runtime to be allowed to assume it. Each of these is refused while
 * compiling rather than discovered while running, because every one of them fails silently: elements land
 * in the wrong document, or in no document, and the job stays green throughout.
 */
class NestTreeWellFormednessTest {

    private static NestTopology compile(TransformBody.Nest tree) {
        return NestTopology.compile("p", "n", tree, tables());
    }

    private static NestTopology compile(TransformBody.Nest tree, int vertexLimit) {
        return NestTopology.compile("p", "n", tree, tables(), vertexLimit);
    }

    private static NestError codeOf(TransformBody.Nest tree) {
        return (NestError) catchThrowableOfType(() -> compile(tree), TapstateException.class).code();
    }

    @Test
    void fillsInAMissingRootKeyFromTheTablesOwnKey() {
        // The root is asked what identifies one of its rows exactly as any embed is, so a root over a
        // table that declares a key does not have to repeat it.
        TransformBody.Nest tree = nest("customer", null,
                embed("policy", "customer_id", "customer_id", EmbedAs.ARRAY, "policies", List.of("policy_no")));

        assertThat(compile(tree).isPassthrough()).isFalse();
    }

    @Test
    void refusesARootWithNoDeclaredKeyAndNoTableKeyToTakeOneFrom() {
        // Nothing identifies a root row, so its documents have no identity to be grouped under and
        // nothing to partition the assembled documents by.
        //
        // And it is refused as a root rather than as a level. Asking the root the same question as every
        // other level is what lets a root over a keyed table stay silent, but the refusal at the end of
        // that question is written for an embed: it names an array key the author never wrote and a path,
        // and a root has no path - the slot would carry the internal name of the root's own namespace.
        // So the code that names the root and its alias is the one thrown, which is also the only thing
        // keeping it reachable at all.
        TransformBody.Nest tree = nest("keyless", null,
                embed("policy", "customer_id", "customer_id", EmbedAs.ARRAY, "policies", List.of("policy_no")));

        assertThatThrownBy(() -> compile(tree))
                .isInstanceOf(TapstateException.class)
                .hasMessageContaining("nest.root-key-required")
                .hasMessageContaining("rootAlias=keyless");
    }

    @Test
    void refusesTheSiblingThatPointsAtSomethingWhichIdentifiesNeitherSide() {
        // policy_no identifies neither a policy - policies are identified by policy_id - nor a document.
        // Read either way this groups rows on a column that names a set, so there is nothing to embed by.
        TransformBody.Nest tree = nest("customer", List.of("customer_id"),
                embed("policy", "customer_id", "customer_id", EmbedAs.ARRAY, "policies", List.of("policy_no"),
                        embed("claim", "policy_id", "policy_id", EmbedAs.ARRAY, "claims", List.of("claim_id")),
                        embed("document", "policy_no", "policy_no", EmbedAs.ARRAY, "docs",
                                List.of("document_id"))));

        assertThatThrownBy(() -> compile(tree))
                .describedAs("each embed answers for itself before anything compares it with a sibling,"
                        + " so the one at fault is named rather than the level they disagree under")
                .isInstanceOf(TapstateException.class)
                .hasMessageContaining("nest.embed-target-not-parent-key")
                .hasMessageContaining("embedPath=policies.docs")
                .hasMessageContaining("fields=policy_no")
                .hasMessageContaining("parentKey=policy_id");
    }

    @Test
    void refusesADepthOneEmbedJoiningOnAnythingButTheRootKey() {
        // cust_ref does not identify a policy and legacy_id does not identify a customer, so neither
        // reading names one row. The root is asked the same two questions as any other level.
        TransformBody.Nest tree = nest("customer", List.of("customer_id"),
                embed("policy", "cust_ref", "legacy_id", EmbedAs.ARRAY, "policies", List.of("policy_no")));

        assertThatThrownBy(() -> compile(tree))
                .describedAs("the assembler is keyed by the root key and can look up nothing else")
                .isInstanceOf(TapstateException.class)
                .hasMessageContaining("nest.embed-target-not-parent-key")
                .hasMessageContaining("embedPath=policies")
                .hasMessageContaining("fields=legacy_id")
                .hasMessageContaining("parentKey=customer_id");
    }

    @Test
    void anOnlyChildJoiningOnAColumnItsParentPointsAtIsThatOtherDirection() {
        // This tree used to be refused for joining on a column that does not identify a policy - and the
        // reason given was that an only child has nobody to disagree with, so a wrong column goes
        // unchallenged. The column was never wrong: author_id identifies a document, so a policy carrying
        // one names the single document it points at. Refusing it was the defect, not the tree.
        TransformBody.Nest tree = nest("customer", List.of("customer_id"),
                embed("policy", "customer_id", "customer_id", EmbedAs.ARRAY, "policies", List.of("policy_no"),
                        embed("document", "document_id", "author_id", EmbedAs.OBJECT, "author", null)));

        NestTopology topology = compile(tree);

        // Pointed at rather than hanging under: it is fetched by key where the document is rendered, so no
        // row of it is ever routed to the level that names it. The one edge that does mention it carries
        // word of an edit and no row at all, which is how such a row reaches a document that is already out.
        assertThat(topology.vertices()).anySatisfy(vertex -> {
            assertThat(vertex.pathId()).isEqualTo(List.of("policies"));
            assertThat(vertex.inbound()).noneSatisfy(edge -> {
                assertThat(edge.pathId()).isEqualTo(List.of("policies", "author"));
                assertThat(edge.carriesTouches()).isFalse();
            });
        });
    }

    @Test
    void refusesTwoEmbedsClaimingTheSamePlaceInTheDocument() {
        TransformBody.Nest tree = nest("customer", List.of("customer_id"),
                embed("policy", "customer_id", "customer_id", EmbedAs.ARRAY, "rows", List.of("policy_no")),
                embed("order", "customer_id", "customer_id", EmbedAs.ARRAY, "rows", List.of("order_id")));

        assertThat(codeOf(tree)).isEqualTo(NestError.EMBED_PATH_CONFLICT);
    }

    @Test
    void refusesAnEmbedClaimingAPlaceInsideAnothersPath() {
        TransformBody.Nest tree = nest("customer", List.of("customer_id"),
                embed("policy", "customer_id", "customer_id", EmbedAs.ARRAY, "book", List.of("policy_no")),
                embed("order", "customer_id", "customer_id", EmbedAs.ARRAY, "book.orders", List.of("order_id")));

        assertThatThrownBy(() -> compile(tree))
                .describedAs("one would overwrite the other, and their names from the root would collide")
                .isInstanceOf(TapstateException.class)
                .hasMessageContaining("nest.embed-path-conflict")
                .hasMessageContaining("path=book");
    }

    @Test
    void fillsInAMissingElementIdentityFromTheTablesOwnKey() {
        TransformBody.Nest tree = nest("customer", List.of("customer_id"),
                embed("policy", "customer_id", "customer_id", EmbedAs.ARRAY, "policies", null));

        assertThat(compile(tree).streamAt(List.of("policies")).arrayKey()).containsExactly("policy_id");
    }

    @Test
    void refusesAnEmbedWithNoElementIdentityAndNoTableKeyToTakeOneFrom() {
        TransformBody.Nest tree = nest("customer", List.of("customer_id"),
                embed("keyless", "customer_id", "customer_id", EmbedAs.ARRAY, "rows", null));

        assertThatThrownBy(() -> compile(tree))
                .describedAs("without an element identity every update piles up as another duplicate")
                .isInstanceOf(TapstateException.class)
                .hasMessageContaining("nest.array-key-unresolvable")
                .hasMessageContaining("table=keyless_rows");
    }

    @Test
    void bareThrowsWhenAnAliasResolvesToNoTableAtAll() {
        TransformBody.Nest tree = nest("customer", List.of("customer_id"),
                embed("nowhere", "customer_id", "customer_id", EmbedAs.ARRAY, "rows", null));

        assertThatThrownBy(() -> compile(tree))
                .describedAs("a stream that does not resolve is a wiring bug, not something an author can fix")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("nowhere");
    }

    @Test
    void refusesKeyTrackingAnywhereUnderAnAppendOnlyRoot() {
        Embed policy = embed("policy", "customer_id", "customer_id", EmbedAs.ARRAY, "policies",
                List.of("policy_no"),
                tracking(embed("claim", "policy_id", "policy_id", EmbedAs.ARRAY, "claims", List.of("claim_id"))));
        TransformBody.Nest tree = nest(new NestRoot("customer", List.of("customer_id"), "append", null,
                List.of(policy)));

        assertThatThrownBy(() -> compile(tree))
                .describedAs("moving a subtree holds emissions back, which is what append-only forbids")
                .isInstanceOf(TapstateException.class)
                .hasMessageContaining("nest.append-mode-conflicts-with-key-tracking")
                .hasMessageContaining("embedPath=policies.claims");
    }

    @Test
    void refusesAnAppendOnlyRootThatTracksItsOwnKey() {
        TransformBody.Nest tree = nest(new NestRoot("customer", List.of("customer_id"), "append", true,
                List.of(embed("policy", "customer_id", "customer_id", EmbedAs.ARRAY, "policies",
                        List.of("policy_no")))));

        assertThat(codeOf(tree)).isEqualTo(NestError.APPEND_MODE_CONFLICTS_WITH_KEY_TRACKING);
    }

    @Test
    void acceptsAnAppendOnlyRootThatTracksNothing() {
        TransformBody.Nest tree = nest(new NestRoot("customer", List.of("customer_id"), "append", null,
                List.of(embed("policy", "customer_id", "customer_id", EmbedAs.ARRAY, "policies",
                        List.of("policy_no")))));

        assertThat(compile(tree).assembler().partitionKey()).containsExactly("customer_id");
    }

    @Test
    void refusesATreeCompilingToMoreResolversThanTheLimitAllows() {
        TransformBody.Nest tree = nest("customer", List.of("customer_id"),
                embed("policy", "customer_id", "customer_id", EmbedAs.ARRAY, "policies", List.of("policy_no"),
                        embed("claim", "policy_id", "policy_id", EmbedAs.ARRAY, "claims", List.of("claim_id"),
                                embed("document", "claim_id", "claim_id", EmbedAs.ARRAY, "docs",
                                        List.of("document_id")))));

        assertThatThrownBy(() -> compile(tree, 1))
                .describedAs("each resolver takes a thread of its own, so the count is a real budget")
                .isInstanceOf(TapstateException.class)
                .hasMessageContaining("nest.resolver-vertex-limit-exceeded")
                .hasMessageContaining("vertices=2")
                .hasMessageContaining("limit=1");
        assertThat(compile(tree, 2).resolvers()).hasSize(2);
    }
}
