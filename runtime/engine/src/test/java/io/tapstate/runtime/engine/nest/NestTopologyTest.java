package io.tapstate.runtime.engine.nest;

import static io.tapstate.runtime.engine.nest.NestTreeFixtures.compositeEmbed;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.embed;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.nest;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.tables;
import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.TransformBody;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The shape a nest tree compiles to. Every number here - how many vertices, which key each one
 * partitions by, how many edges arrive and how many hops a row takes - is settled while compiling
 * and never at runtime, so a test can read the whole topology off a tree without running a job.
 *
 * <p>The tree under test is the one worth pinning: it has depth <em>and</em> branching. A chain
 * cannot tell "one vertex per level" from "one vertex per non-leaf embed" - the two agree on a
 * chain and disagree the moment two embeds sit side by side.
 *
 * <pre>
 * customer                                    root, key = customer_id
 * |- policies[]   non-leaf, identity policy_id, arrayKey policy_no
 * |  \- claims[]      non-leaf, identity claim_id
 * |     \- documents[]    leaf
 * |- orders[]     non-leaf, identity order_id
 * |  \- items[]       leaf
 * \- profile      leaf, as:object
 * </pre>
 */
class NestTopologyTest {

    private static final String PIPELINE = "orders_to_docs";
    private static final String NODE = "assemble";

    private static TransformBody.Nest branchingTree() {
        return nest("customer", List.of("customer_id"),
                embed("policy", "customer_id", "customer_id", EmbedAs.ARRAY, "policies", List.of("policy_no"),
                        embed("claim", "policy_id", "policy_id", EmbedAs.ARRAY, "claims", List.of("claim_id"),
                                embed("document", "claim_id", "claim_id", EmbedAs.ARRAY, "documents",
                                        List.of("document_id")))),
                embed("order", "customer_id", "customer_id", EmbedAs.ARRAY, "orders", List.of("order_id"),
                        embed("item", "order_id", "order_id", EmbedAs.ARRAY, "items", List.of("item_id"))),
                embed("profile", "customer_id", "customer_id", EmbedAs.OBJECT, "profile", List.of("customer_id")));
    }

    private static NestTopology compiled() {
        return NestTopology.compile(PIPELINE, NODE, branchingTree(), tables());
    }

    @Test
    void buildsOneResolverPerNonLeafEmbedAndOneAssembler() {
        NestTopology topology = compiled();

        assertThat(topology.resolvers())
                .describedAs("one resolver per non-leaf embed - policies, claims and orders")
                .extracting(NestVertex::pathId)
                .containsExactlyInAnyOrder(
                        List.of("policies"), List.of("policies", "claims"), List.of("orders"));
        assertThat(topology.assembler().pathId()).isEmpty();
        assertThat(topology.isPassthrough()).isFalse();
    }

    @Test
    void ordersResolversSoEveryCascadeSourceComesBeforeItsDestination() {
        NestTopology topology = compiled();
        List<NestVertex> vertices = topology.vertices();

        List<List<String>> paths = vertices.stream().map(NestVertex::pathId).toList();
        for (NestVertex vertex : vertices) {
            for (NestInbound inbound : vertex.inbound()) {
                if (inbound.isCascade()) {
                    assertThat(paths.indexOf(inbound.pathId()))
                            .describedAs("cascade source %s precedes %s", inbound.pathId(), vertex.pathId())
                            .isLessThan(paths.indexOf(vertex.pathId()));
                }
            }
        }
        assertThat(vertices.get(vertices.size() - 1)).isSameAs(topology.assembler());
    }

    @Test
    void namesEveryVertexAndItsMapAfterThePathFromTheRoot() {
        NestTopology topology = compiled();

        NestVertex claims = topology.vertexAt(List.of("policies", "claims"));
        assertThat(claims.name()).isEqualTo("nest:assemble:policies.claims");
        assertThat(claims.mapName()).isEqualTo("nest.orders_to_docs.assemble.policies.claims");

        assertThat(topology.assembler().name()).isEqualTo("nest:assemble");
        assertThat(topology.assembler().mapName()).isEqualTo("nest.orders_to_docs.assemble.$root");
    }

    @Test
    void keysEachResolverByTheColumnItsOwnChildrenPointAt() {
        NestTopology topology = compiled();

        assertThat(topology.vertexAt(List.of("policies")).partitionKey())
                .describedAs("claims join on policy_id, so that is what the policies vertex is keyed by")
                .containsExactly("policy_id");
        assertThat(topology.vertexAt(List.of("policies", "claims")).partitionKey()).containsExactly("claim_id");
        assertThat(topology.vertexAt(List.of("orders")).partitionKey()).containsExactly("order_id");
        assertThat(topology.assembler().partitionKey())
                .describedAs("the assembler is keyed by the root key, never by anything else")
                .containsExactly("customer_id");
    }

    @Test
    void separatesTheKeyElementsAreIdentifiedByFromTheKeyChildrenPointAt() {
        NestTopology topology = compiled();

        assertThat(topology.streamAt(List.of("policies")).arrayKey())
                .describedAs("the document shows a business key while child rows carry the surrogate one")
                .containsExactly("policy_no");
        assertThat(topology.vertexAt(List.of("policies")).partitionKey()).containsExactly("policy_id");
    }

    @Test
    void givesEveryVertexOneInboundEdgePerChildEmbedPlusItsOwnRows() {
        NestTopology topology = compiled();

        assertThat(topology.vertexAt(List.of("policies")).inbound())
                .describedAs("its own policy rows, and claims cascading in")
                .hasSize(2);
        assertThat(topology.vertexAt(List.of("policies", "claims")).inbound())
                .describedAs("its own claim rows, and leaf documents arriving side-on")
                .hasSize(2);
        assertThat(topology.assembler().inbound())
                .describedAs("root rows, leaf profile, and two cascades - never the two the chain suggests")
                .hasSize(4);
    }

    @Test
    void readsThePartitionKeyOffWhicheverFieldTheEdgeCarriesItIn() {
        NestVertex policies = compiled().vertexAt(List.of("policies"));

        NestInbound own = policies.inboundFor(List.of("policies"));
        assertThat(own.alias()).isEqualTo("policy");
        assertThat(own.keyFields())
                .describedAs("a policy row declares its own identity, so the key is read off policy_id")
                .containsExactly("policy_id");

        NestInbound cascade = policies.inboundFor(List.of("policies", "claims"));
        assertThat(cascade.isCascade()).isTrue();
        assertThat(cascade.keyFields())
                .describedAs("a cascading row was already stamped with its parent key upstream")
                .isEmpty();
    }

    @Test
    void readsALeafRowsKeyOffTheJoinFieldItCarries() {
        NestVertex claims = compiled().vertexAt(List.of("policies", "claims"));

        NestInbound documents = claims.inboundFor(List.of("policies", "claims", "documents"));
        assertThat(documents.alias()).isEqualTo("document");
        assertThat(documents.isCascade()).isFalse();
        assertThat(documents.keyFields())
                .describedAs("documents join on claim_id, which is what the claims vertex is keyed by")
                .containsExactly("claim_id");
    }

    @Test
    void sendsLeafRowsStraightIntoTheirParentsVertexOneHopShortOfTheirDepth() {
        NestTopology topology = compiled();

        assertThat(topology.streamAt(List.of()).hops()).isZero();
        assertThat(topology.streamAt(List.of("policies")).hops()).isEqualTo(1);
        assertThat(topology.streamAt(List.of("policies", "claims")).hops()).isEqualTo(2);
        assertThat(topology.streamAt(List.of("policies", "claims", "documents")).hops())
                .describedAs("a leaf has no mapping to declare, so it starts at its parent's vertex")
                .isEqualTo(2);
        assertThat(topology.streamAt(List.of("orders", "items")).hops()).isEqualTo(1);
        assertThat(topology.streamAt(List.of("profile")).hops())
                .describedAs("a depth-one leaf goes straight to the assembler")
                .isZero();
    }

    @Test
    void entersEachStreamAtTheVertexItsFirstHopLandsOn() {
        NestTopology topology = compiled();

        assertThat(topology.streamAt(List.of("policies", "claims", "documents")).entryVertex())
                .isEqualTo("nest:assemble:policies.claims");
        assertThat(topology.streamAt(List.of("profile")).entryVertex()).isEqualTo("nest:assemble");
        assertThat(topology.streamAt(List.of()).entryVertex()).isEqualTo("nest:assemble");
    }

    @Test
    void namesEveryStreamTheTreeConsumesExactlyOnce() {
        assertThat(compiled().streams())
                .extracting(NestStream::alias)
                .containsExactlyInAnyOrder("customer", "policy", "claim", "document", "order", "item", "profile");
    }

    @Test
    void tellsEachResolverWhereOnItsOwnRowsTheKeyOfTheLevelAboveSits() {
        NestTopology topology = compiled();

        assertThat(topology.vertexAt(List.of("policies")).parentKeyFields())
                .describedAs("a policy row is filed under policy_id and answers with its customer_id")
                .containsExactly("customer_id");
        assertThat(topology.vertexAt(List.of("policies", "claims")).parentKeyFields())
                .containsExactly("policy_id");
        assertThat(topology.assembler().parentKeyFields())
                .describedAs("the root hangs from nothing")
                .isEmpty();
    }

    @Test
    void carriesTheShapeTheAssemblerRendersDocumentsInto() {
        List<EmbedSlot> slots = compiled().slots();

        assertThat(slots).extracting(EmbedSlot::path).containsExactly("policies", "orders", "profile");
        assertThat(slots.get(2).as()).isEqualTo(EmbedAs.OBJECT);
        assertThat(slots.get(0).children()).extracting(EmbedSlot::path).containsExactly("claims");
        assertThat(slots.get(0).children().get(0).children()).extracting(EmbedSlot::path)
                .containsExactly("documents");
    }

    @Test
    void compilesARootWithNothingEmbeddedToAPassthrough() {
        NestTopology topology = NestTopology.compile(PIPELINE, NODE,
                nest("customer", List.of("customer_id")), tables());

        assertThat(topology.isPassthrough())
                .describedAs("it would pay for a map, a vertex and a thread while assembling nothing")
                .isTrue();
        assertThat(topology.vertices()).isEmpty();
        assertThat(topology.streams()).isEmpty();
    }

    @Test
    void namesOneStateNamespacePerVertexAndNoneForALeaf() {
        NestTopology topology = compiled();

        // A leaf keeps no namespace of its own - its elements are filed inside its parent's state - so a
        // set naming leaves too would name places nothing was ever stored, and a drop by it would look
        // like it had let go of more than it had.
        assertThat(topology.stateNamespaces()).containsExactlyInAnyOrderElementsOf(
                topology.vertices().stream().map(NestVertex::mapName).toList());
        assertThat(topology.stateNamespaces()).hasSize(topology.resolvers().size() + 1);
    }

    @Test
    void aPassthroughKeepsStateNowhereAtAll() {
        NestTopology topology = NestTopology.compile(PIPELINE, NODE,
                nest("customer", List.of("customer_id")), tables());

        assertThat(topology.stateNamespaces())
                .describedAs("it holds no map, so a stop of it has nothing to name")
                .isEmpty();
    }

    @Test
    void buildsNoResolverWhenEveryEmbedIsALeaf() {
        NestTopology topology = NestTopology.compile(PIPELINE, NODE,
                nest("customer", List.of("customer_id"),
                        embed("policy", "customer_id", "customer_id", EmbedAs.ARRAY, "policies",
                                List.of("policy_no")),
                        embed("profile", "customer_id", "customer_id", EmbedAs.OBJECT, "profile",
                                List.of("customer_id"))),
                tables());

        assertThat(topology.isPassthrough())
                .describedAs("a tree of leaves still assembles documents, so it is not a passthrough")
                .isFalse();
        assertThat(topology.resolvers()).isEmpty();
        assertThat(topology.assembler().inbound()).hasSize(3);
        assertThat(topology.streamAt(List.of("policies")).hops()).isZero();
    }

    @Test
    void alignsACompositeJoinKeyWithTheOrderItsParentIsPartitionedBy() {
        NestTopology topology = NestTopology.compile(PIPELINE, NODE,
                nest("customer", List.of("tenant_id", "customer_id"),
                        compositeEmbed("policy", "policies", List.of("policy_no"),
                                "cust", "customer_id", "tenant", "tenant_id")),
                tables());

        assertThat(topology.assembler().partitionKey()).containsExactly("tenant_id", "customer_id");
        assertThat(topology.assembler().inboundFor(List.of("policies")).keyFields())
                .describedAs("declared the other way round, but read off in the order the parent is keyed by")
                .containsExactly("tenant", "cust");
    }
}
