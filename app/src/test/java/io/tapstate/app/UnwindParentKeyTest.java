package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.FieldRule;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.Step;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.core.model.TransformBody;
import io.tapstate.spi.transform.TransformPort;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * What an expansion is told the rows reaching it are identified by.
 *
 * <p>Every other stateless kind is a function of its own body: the whole of what a projection does
 * is in the projection. An expansion is not - to tell one parent's expanded rows from another's it
 * has to know what identifies the parent, and that is a property of the chain above it. It cannot
 * be worked out on the member either, which is where the port is rebuilt, so it is resolved here
 * and captured.
 *
 * <p><b>Getting it wrong is not visible as a failure.</b> An expansion handed no parent key pairs
 * an update's old rows against its new ones on the element alone, so two parents whose lists share
 * an element look like one row to it; handed a stale key it pairs nothing and rewrites every row of
 * every parent on every update. Both run, both fill the target, and the rows that were overwritten
 * are counted nowhere.
 */
class UnwindParentKeyTest {

    /** The discovered key of the one table here; every other reference names no table. */
    private static final Function<FromRef, List<String>> ORDERS_KEYED_ON_O_ID =
            ref -> ref instanceof FromRef.Literal literal && literal.ref().equals("orders")
                    ? List.of("o_id")
                    : List.of();

    private static Step.Inline step(String id, String from, TransformBody body) {
        return new Step.Inline(id, new FromClause.Flow(List.of(new FromRef.Literal(from))), body,
                null);
    }

    private static TransformBody.Unwind unwind(String includeArrayIndex, String elementKey) {
        return new TransformBody.Unwind("items", includeArrayIndex, false, elementKey, null);
    }

    private static Map<String, Step.Inline> chain(Step.Inline... steps) {
        Map<String, Step.Inline> byId = new LinkedHashMap<>();
        for (Step.Inline step : steps) {
            byId.put(step.id(), step);
        }
        return byId;
    }

    private static List<String> keyReaching(Step.Inline step, Step.Inline... chain) {
        return StoreBackedDagSource.parentKeysReaching(step, chain(chain), ref -> {
            List<String> key = ORDERS_KEYED_ON_O_ID.apply(ref);
            return key.isEmpty() ? Map.of() : Map.of("orders", key);
        }).getOrDefault("orders", List.of());
    }

    @Test
    @DisplayName("an expansion reading a table directly is told that table's discovered key")
    void aTableIsReadForItsDiscoveredKey() {
        Step.Inline expand = step("expand", "orders", unwind("item_no", null));

        assertThat(keyReaching(expand, expand)).containsExactly("o_id");
    }

    /**
     * A projection or a predicate between the table and the expansion changes neither how many rows
     * there are nor which of them is which, so the answer is the table's still. Walking has to reach
     * past them: stopping at the first step that is not a table would hand the expansion nothing.
     */
    @Test
    @DisplayName("a projection between the table and the expansion is walked through")
    void aProjectionInBetweenIsWalkedThrough() {
        Step.Inline pick = step("pick",
                "orders", new TransformBody.MapProjection(Map.of("o_region", FieldRule.drop())));
        Step.Inline keep = step("keep", "pick", new TransformBody.Filter("after.o_id > 1"));
        Step.Inline expand = step("expand", "keep", unwind("item_no", null));

        assertThat(keyReaching(expand, pick, keep, expand)).containsExactly("o_id");
    }

    @Test
    void aRenamedParentKeyReachesThePortUnderItsCurrentName() {
        Step.Inline rename = step("rename", "orders", new TransformBody.MapProjection(
                Map.of("order_id", FieldRule.rename("o_id"))));
        Step.Inline expand = step("expand", "rename", unwind(null, "sku"));
        List<String> keys = keyReaching(expand, rename, expand);

        assertThat(keys).containsExactly("order_id");
        TransformPort port = StoreBackedDagSource.transformPort(expand, keys).get();
        List<Envelope> out = port.transform(new Envelope(Op.UPDATE, 1L, "orders",
                Map.of("order_id", 7L, "items", List.of(Map.of("sku", "a"))),
                Map.of("order_id", 8L, "items", List.of(Map.of("sku", "a"))), null));
        assertThat(out).extracting(Envelope::op).containsExactly(Op.DELETE, Op.INSERT);
        assertThat(out.getFirst().before()).containsEntry("order_id", 7L);
        assertThat(out.getLast().after()).containsEntry("order_id", 8L);
    }

    @Test
    void aSecondExpansionUsesRenamedParentAndEarlierLocatorColumns() {
        Step.Inline outer = step("outer", "orders", unwind("item_no", null));
        Step.Inline rename = step("rename", "outer", new TransformBody.MapProjection(
                Map.of("order_id", FieldRule.rename("o_id"),
                        "line_no", FieldRule.rename("item_no"))));
        Step.Inline inner = step("inner", "rename",
                new TransformBody.Unwind("tags", "tag_no", false, null, null));

        assertThat(keyReaching(inner, outer, rename, inner)).containsExactly("order_id", "line_no");
    }

    @Test
    void droppingARenamedParentKeyDoesNotSilentlyRemoveItsRequirement() {
        Step.Inline rename = step("rename", "orders", new TransformBody.MapProjection(
                Map.of("order_id", FieldRule.rename("o_id"))));
        Step.Inline drop = step("drop", "rename", new TransformBody.MapProjection(
                Map.of("order_id", FieldRule.drop())));
        Step.Inline expand = step("expand", "drop", unwind(null, "sku"));
        List<String> keys = keyReaching(expand, rename, drop, expand);

        assertThat(keys).containsExactly("order_id");
        TransformPort port = StoreBackedDagSource.transformPort(expand, keys).get();
        assertThatThrownBy(() -> port.transform(new Envelope(Op.DELETE, 1L, "orders",
                Map.of("region", "east", "items", List.of(Map.of("sku", "a"))), null, null)))
                .isInstanceOf(TapstateException.class)
                .hasMessageContaining("transform.unwind-needs-a-complete-before-image");
    }

    /**
     * The case the walk exists for. Under a first expansion the rows are no longer one per parent,
     * so a second one told only the table's key would pair every row the first produced as one row -
     * and on any update to a list, delete all but one of them.
     */
    @Test
    @DisplayName("a second expansion is told the first one's locator as well")
    void aSecondExpansionIsToldTheFirstsLocator() {
        Step.Inline outer = step("outer", "orders", unwind("item_no", null));
        Step.Inline inner = step("inner", "outer",
                new TransformBody.Unwind("tags", "tag_no", false, null, null));

        // What the inner one is told is the OUTER one's locator beside the table's key. Its own
        // (tag_no) is not in it - the port adds that itself, being the only thing that can read it.
        assertThat(keyReaching(inner, outer, inner)).containsExactly("o_id", "item_no");
        assertThat(keyReaching(inner, outer, inner)).doesNotContain("tag_no");
    }

    /** An expansion identified by a field of its element contributes that column, same as an ordinal. */
    @Test
    @DisplayName("an expansion keyed on its element's own field contributes that column")
    void anElementKeyContributesItsColumn() {
        Step.Inline outer = step("outer", "orders", unwind(null, "sku"));
        Step.Inline inner = step("inner", "outer",
                new TransformBody.Unwind("tags", "tag_no", false, null, null));

        assertThat(keyReaching(inner, outer, inner)).containsExactly("o_id", "sku");
    }

    /**
     * A declaration naming neither is refused where it is written. Adding nothing here is what keeps
     * a chain under one readable rather than keyed on a column called null.
     */
    @Test
    @DisplayName("an expansion above that names no locator adds nothing")
    void anExpansionWithNoLocatorAddsNothing() {
        Step.Inline outer = step("outer", "orders", unwind(null, null));
        Step.Inline inner = step("inner", "outer",
                new TransformBody.Unwind("tags", "tag_no", false, null, null));

        assertThat(keyReaching(inner, outer, inner)).containsExactly("o_id");
    }

    /**
     * A reference naming nothing this pipeline has resolves to no key rather than looping - and to
     * no key rather than to the locator alone. An expansion adds to a key and does not make one, so
     * a key with no parent column in it would say two parents sharing an element are one row, and
     * would say it as confidently as a real answer. Empty is the one way of saying nobody could say.
     */
    @Test
    @DisplayName("a chain that leads nowhere, or back to itself, answers with no key")
    void aChainThatLeadsNowhereAnswersEmpty() {
        Step.Inline orphan = step("orphan", "no_such_thing", unwind("item_no", null));
        assertThat(keyReaching(orphan, orphan)).isEmpty();

        Step.Inline looped = step("looped", "looped", unwind("item_no", null));
        assertThat(keyReaching(looped, looped)).isEmpty();
    }

    /**
     * The seam between the two halves: what the walk resolves has to arrive in the port the factory
     * builds. Nothing about that is checked by the compiler - a factory handed an empty list builds
     * and runs, and pairs an update's rows on the element alone, so two parents whose lists share an
     * element become one row and nothing anywhere says so.
     *
     * <p>The discriminating case is a parent that changes its key without touching its list: told the
     * parent's key, the port sees two different rows and deletes the old one; told nothing, it sees
     * one unchanged row and leaves the old key's row behind for good.
     */
    @Test
    @DisplayName("the port an expansion is built into carries the parent key the walk resolved")
    void theResolvedParentKeyReachesThePortTheFactoryBuilds() {
        Step.Inline expand = step("expand", "orders", unwind("item_no", null));

        TransformPort port = StoreBackedDagSource
                .transformPort(expand, keyReaching(expand, expand))
                .get();

        List<Envelope> out = port.transform(new Envelope(Op.UPDATE, 1L, "orders",
                orderRow(7L), orderRow(8L), null));

        assertThat(out).extracting(Envelope::op).containsExactlyInAnyOrder(Op.DELETE, Op.INSERT);
        assertThat(out).filteredOn(e -> e.op() == Op.DELETE).singleElement()
                .satisfies(e -> assertThat(e.before()).containsEntry("o_id", 7L));
        assertThat(out).filteredOn(e -> e.op() == Op.INSERT).singleElement()
                .satisfies(e -> assertThat(e.after()).containsEntry("o_id", 8L));
    }

    @Test
    void conflictingParentKeysFromTheSameStreamAreRefused() {
        Step.Inline left = step("left", "orders", new TransformBody.MapProjection(
                Map.of("left_id", FieldRule.rename("o_id"))));
        Step.Inline right = step("right", "orders", new TransformBody.MapProjection(
                Map.of("right_id", FieldRule.rename("o_id"))));
        Step.Inline merged = Step.inline("expand",
                FromClause.list(FromRef.literal("left"), FromRef.literal("right")), unwind("item_no", null), null);
        assertThatThrownBy(() -> StoreBackedDagSource.parentKeysReaching(merged, chain(left, right, merged),
                ref -> ref.equals(FromRef.literal("orders")) ? Map.of("orders", List.of("o_id")) : Map.of()))
                .isInstanceOf(TapstateException.class)
                .hasMessageContaining("actuation.unwind-parent-key-unresolved");
    }

    @Test
    void anUndiscoveredParentKeyCannotBecomeAnElementOnlyIdentity() {
        Step.Inline expand = step("expand", "orders", unwind("item_no", null));
        assertThatThrownBy(() -> StoreBackedDagSource.transformPortByStream(
                expand, Map.of("orders", List.of()), null))
                .isInstanceOf(TapstateException.class)
                .hasMessageContaining("actuation.unwind-parent-key-unresolved");
    }

    @Test
    void streamDispatchPassesDdlAndRejectsUnexpectedDataStreams() throws Exception {
        Step.Inline expand = step("expand", "orders", unwind("item_no", null));
        TransformPort port = StoreBackedDagSource.transformPortByStream(
                expand, Map.of("orders", List.of("o_id")), null).get();
        Envelope ddl = new Envelope(Op.DDL, 1L, "schema", null, null, Map.of("change", "add"));
        assertThat(port.transform(ddl)).containsExactly(ddl);
        assertThatThrownBy(() -> port.transform(new Envelope(Op.READ, 1L, "unknown", null, orderRow(1L), null)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void streamSpecificFactoriesSurviveSerializationForDagShipping() throws Exception {
        Step.Inline expand = step("expand", "orders", unwind("item_no", null));
        var supplier = StoreBackedDagSource.transformPortByStream(
                expand, Map.of("orders", List.of("o_id"), "customers", List.of("c_id")), null);
        var bytes = new java.io.ByteArrayOutputStream();
        try (var output = new java.io.ObjectOutputStream(bytes)) {
            output.writeObject(supplier);
        }
        try (var input = new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(bytes.toByteArray()))) {
            var restored = (com.hazelcast.function.SupplierEx<?>) input.readObject();
            TransformPort port = (TransformPort) restored.get();
            assertThat(port.transform(new Envelope(Op.DELETE, 1L, "customers",
                    Map.of("c_id", 1L, "items", List.of("a")), null, null)))
                    .singleElement().satisfies(row -> assertThat(row.before()).containsEntry("c_id", 1L));
        }
    }

    @Test
    void guardingAJsStepPreservesItsDdlProcessing() throws Exception {
        Step.Inline script = step("script", "orders", new TransformBody.Js(
                "function process(r, ctx) { r.schema.touched = true; return r; }"));
        TransformPort port = StoreBackedDagSource.transformPortByStream(
                script, Map.of("orders", List.of("o_id")), "items").get();
        Envelope ddl = Envelope.ddl(1L, "schema", Map.of("change", "add"));
        assertThat(port.transform(ddl)).singleElement()
                .satisfies(row -> assertThat(row.schema()).containsEntry("touched", true));
    }

    /** One order carrying one element, under whichever key it is given. */
    private static Map<String, Object> orderRow(long id) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("o_id", id);
        row.put("items", List.of(Map.of("sku", "a")));
        return row;
    }
}
