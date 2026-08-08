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
    void refusesARootThatDeclaresNoKey() {
        TransformBody.Nest tree = nest("customer", null,
                embed("policy", "customer_id", "customer_id", EmbedAs.ARRAY, "policies", List.of("policy_no")));

        assertThatThrownBy(() -> compile(tree))
                .isInstanceOf(TapstateException.class)
                .hasMessageContaining("nest.root-key-required")
                .hasMessageContaining("rootAlias=customer");
    }

    @Test
    void refusesSiblingEmbedsPointingAtDifferentColumnsOfTheirParent() {
        TransformBody.Nest tree = nest("customer", List.of("customer_id"),
                embed("policy", "customer_id", "customer_id", EmbedAs.ARRAY, "policies", List.of("policy_no"),
                        embed("claim", "policy_id", "policy_id", EmbedAs.ARRAY, "claims", List.of("claim_id")),
                        embed("document", "policy_no", "policy_no", EmbedAs.ARRAY, "docs",
                                List.of("document_id"))));

        assertThat(codeOf(tree)).isEqualTo(NestError.SIBLING_EMBEDS_TARGET_DIFFERENT_PARENT_KEYS);
    }

    @Test
    void refusesADepthOneEmbedJoiningOnAnythingButTheRootKey() {
        TransformBody.Nest tree = nest("customer", List.of("customer_id"),
                embed("policy", "cust_ref", "legacy_id", EmbedAs.ARRAY, "policies", List.of("policy_no")));

        assertThatThrownBy(() -> compile(tree))
                .describedAs("the assembler is keyed by the root key and can look up nothing else")
                .isInstanceOf(TapstateException.class)
                .hasMessageContaining("nest.sibling-embeds-target-different-parent-keys")
                .hasMessageContaining("embedPath=$root")
                .hasMessageContaining("customer_id, legacy_id");
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
