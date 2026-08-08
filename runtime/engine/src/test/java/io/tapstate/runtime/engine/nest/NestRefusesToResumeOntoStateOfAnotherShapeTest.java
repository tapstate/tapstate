package io.tapstate.runtime.engine.nest;

import static io.tapstate.runtime.engine.nest.NestTreeFixtures.embed;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.nest;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.tables;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.TransformBody;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/**
 * A nest addresses its stored state by the path each embed sits at, so editing a path renames the place
 * the state is kept without moving anything into it. What was written stays where nothing reads it, the
 * new path answers nothing, and the tree is rebuilt from empty while the pipeline reports that it
 * resumed - the whole failure is silent, and its only symptom is documents that quietly lost their
 * elements.
 *
 * <p>This is what makes it worth a refusal rather than a warning: nothing downstream can tell the
 * difference between "resumed onto its own state" and "resumed onto nobody's", so the one moment the two
 * can be told apart is before the job is built, by comparing what the tree compiles to against what the
 * last run wrote.
 *
 * <p>The leaf case is why the comparison is over paths rather than over the maps the state lives in. A
 * leaf embed gets no map of its own - its elements are filed inside its parent's state under the leaf's
 * own path - so renaming one abandons everything stored for it while every map keeps its name. That case
 * carries its own assertion that the map names really do stay put, because a comparison narrowed to them
 * would pass every other test here.
 */
class NestRefusesToResumeOntoStateOfAnotherShapeTest {

    private static final String PIPELINE = "orders_to_docs";
    private static final String NODE = "assemble";

    /** The tree as it was when the state was written: items at {@code lines}, under {@code orders}. */
    private static NestTopology written() {
        return compile("orders", "lines");
    }

    /** The same tree with the order embed at {@code orderPath} and its leaf of items at {@code itemPath}. */
    private static NestTopology compile(String orderPath, String itemPath) {
        TransformBody.Nest tree = nest("customer", List.of("customer_id"),
                embed("policy", "customer_id", "customer_id", EmbedAs.ARRAY, "policies", List.of("policy_no"),
                        embed("claim", "policy_id", "policy_id", EmbedAs.ARRAY, "claims", List.of("claim_id"))),
                embed("order", "customer_id", "customer_id", EmbedAs.ARRAY, orderPath, List.of("order_id"),
                        embed("item", "order_id", "order_id", EmbedAs.ARRAY, itemPath, List.of("item_id"))));
        return NestTopology.compile(PIPELINE, NODE, tree, tables());
    }

    private static Set<String> mapNames(NestTopology topology) {
        return topology.vertices().stream().map(NestVertex::mapName).collect(Collectors.toSet());
    }

    @Test
    void aTreeThatHasNeverRunRecordsWhereItWillKeepItsState() {
        RecordingLedger ledger = new RecordingLedger();

        NestStateLedger.reconcile(ledger, PIPELINE, NODE, written().statePaths());

        assertThat(ledger.recorded.get(PIPELINE + "/" + NODE))
                .describedAs("the first run is what a later one is compared against, so it has to be written down")
                .isEqualTo(written().statePaths())
                .isNotEmpty();
    }

    @Test
    void renamingALeafEmbedPathRefusesTheResumeThoughEveryMapKeepsItsName() {
        RecordingLedger ledger = new RecordingLedger().holding(PIPELINE, NODE, written().statePaths());
        NestTopology renamedLeaf = compile("orders", "items");

        assertThat(mapNames(renamedLeaf))
                .describedAs("a leaf has no map of its own, so a comparison over map names sees nothing "
                        + "here - and its elements are still filed under the path that just changed")
                .isEqualTo(mapNames(written()));
        assertThatThrownBy(() -> NestStateLedger.reconcile(ledger, PIPELINE, NODE, renamedLeaf.statePaths()))
                .isInstanceOf(TapstateException.class)
                .extracting(thrown -> ((TapstateException) thrown).code().code())
                .isEqualTo("nest.state-paths-changed");
    }

    @Test
    void renamingAnEmbedThatKeepsAMapOfItsOwnRefusesTheResume() {
        RecordingLedger ledger = new RecordingLedger().holding(PIPELINE, NODE, written().statePaths());
        NestTopology renamedBranch = compile("purchases", "lines");

        assertThat(mapNames(renamedBranch))
                .describedAs("this one does move a map, which is the case the record is most obviously about")
                .isNotEqualTo(mapNames(written()));
        assertThatThrownBy(() -> NestStateLedger.reconcile(ledger, PIPELINE, NODE, renamedBranch.statePaths()))
                .isInstanceOf(TapstateException.class)
                .extracting(thrown -> ((TapstateException) thrown).code().code())
                .isEqualTo("nest.state-paths-changed");
    }

    @Test
    void insertingALevelRefusesTheResume() {
        RecordingLedger ledger = new RecordingLedger().holding(PIPELINE, NODE, written().statePaths());
        TransformBody.Nest deeper = nest("customer", List.of("customer_id"),
                embed("order", "customer_id", "customer_id", EmbedAs.ARRAY, "orders", List.of("order_id"),
                        embed("policy", "order_id", "order_id", EmbedAs.ARRAY, "policies", List.of("policy_no"),
                                embed("item", "policy_id", "policy_id", EmbedAs.ARRAY, "lines",
                                        List.of("item_id")))));

        assertThatThrownBy(() -> NestStateLedger.reconcile(ledger, PIPELINE, NODE,
                NestTopology.compile(PIPELINE, NODE, deeper, tables()).statePaths()))
                .describedAs("the same path under a new parent is a different place, and the old one is "
                        + "left with nobody to read it")
                .isInstanceOf(TapstateException.class);
    }

    @Test
    void aRefusedResumeLeavesTheRecordNamingWhereTheStateActuallyIs() {
        RecordingLedger ledger = new RecordingLedger().holding(PIPELINE, NODE, written().statePaths());

        assertThatThrownBy(() ->
                NestStateLedger.reconcile(ledger, PIPELINE, NODE, compile("orders", "items").statePaths()))
                .isInstanceOf(TapstateException.class);

        assertThat(ledger.recorded.get(PIPELINE + "/" + NODE))
                .describedAs("overwriting it on the way out would make the second attempt succeed onto "
                        + "state the first one refused to touch")
                .isEqualTo(written().statePaths());
    }

    @Test
    void theSameTreeResumesOntoItsOwnState() {
        RecordingLedger ledger = new RecordingLedger().holding(PIPELINE, NODE, written().statePaths());

        assertThat(written().statePaths())
                .describedAs("a tree that compiled to nothing would pass this without comparing anything")
                .isNotEmpty();
        assertThatCode(() -> NestStateLedger.reconcile(ledger, PIPELINE, NODE, written().statePaths()))
                .doesNotThrowAnyException();
        assertThat(ledger.recorded.get(PIPELINE + "/" + NODE)).isEqualTo(written().statePaths());
    }

    @Test
    void removingEveryEmbedRefusesRatherThanRunningAsAPassthroughOverAbandonedState() {
        RecordingLedger ledger = new RecordingLedger().holding(PIPELINE, NODE, written().statePaths());
        NestTopology passthrough = NestTopology.compile(PIPELINE, NODE,
                nest("customer", List.of("customer_id")), tables());

        assertThat(passthrough.isPassthrough()).isTrue();
        assertThatThrownBy(() ->
                NestStateLedger.reconcile(ledger, PIPELINE, NODE, passthrough.statePaths()))
                .describedAs("a tree with nothing left to keep still has everything it kept before")
                .isInstanceOf(TapstateException.class);
    }

    @Test
    void aNestWithNoLedgerBehindItRunsAsItAlwaysHas() {
        NestStateLedger.reconcile(NestStateLedger.NONE, PIPELINE, NODE, written().statePaths());
        NestStateLedger.reconcile(NestStateLedger.NONE, PIPELINE, NODE, compile("orders", "items").statePaths());
    }

    /** A ledger that keeps what it is told in memory, so a test can see both what it read and what it wrote. */
    private static final class RecordingLedger implements NestStateLedger {

        private static final long serialVersionUID = 1L;

        private final Map<String, Set<String>> recorded = new LinkedHashMap<>();

        RecordingLedger holding(String pipelineId, String nodeId, Set<String> paths) {
            recorded.put(pipelineId + "/" + nodeId, new LinkedHashSet<>(paths));
            return this;
        }

        @Override
        public Set<String> recall(String pipelineId, String nodeId) {
            return recorded.getOrDefault(pipelineId + "/" + nodeId, Set.of());
        }

        @Override
        public void record(String pipelineId, String nodeId, Set<String> paths) {
            recorded.put(pipelineId + "/" + nodeId, new LinkedHashSet<>(paths));
        }
    }
}
