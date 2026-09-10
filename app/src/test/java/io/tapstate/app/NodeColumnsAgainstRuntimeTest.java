package io.tapstate.app;

import io.tapstate.adapters.transform.MapSpec;
import io.tapstate.adapters.transform.StatelessTransforms;
import io.tapstate.core.common.TapstateType;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.model.FieldRule;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PushElement;
import io.tapstate.core.model.PushFormat;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.model.ViewBlock;
import io.tapstate.spi.transform.TransformPort;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What a node is worked out to produce, held against the row that node's runtime actually produces.
 *
 * <p><b>Why this is a separate claim from the derivation's own cases.</b> Those state what each arm
 * answers; every one of them stays green if the whole model drifts away from the rows the pipeline
 * moves, because both sides of such a case are written from the same reading of what a node does. The
 * model is what a target table is later built to, so a model that no longer matches the rows fails at
 * the write - as a column the target has no place for, or a column of the target nothing ever fills -
 * and neither failure points back here. These cases put the derivation and the runtime in one
 * assertion so that a change to either alone reddens.
 *
 * <p><b>What plays the part of "the runtime" differs by node, and only three of them run rows.</b> A
 * projection, a predicate and a script are ports that take a row and hand rows back, so those cases
 * run the real port over a real event and read the row that comes out. The rest do not reshape rows
 * at all: a merge forwards each input's rows as they arrive and a view stores what it is given, so
 * the row their runtime produces is the row that reached them, and that is what they are held to
 * here. A push carrying per-field rules is the same projection a map step is, so it is held to the
 * row that projection builds; a push whose body is one expression is held to the value that
 * expression yields.
 *
 * <p><b>Two nodes are compared elsewhere and are not repeated here</b> - a nest against the assembled
 * target model the sink is handed, and a join against the record a start is held to. Both are in the
 * derivation's own class, next to the arms they belong to, because for those two the thing worth
 * comparing against is another artifact this tree already ships rather than a row.
 *
 * <p>The comparison is the column set, the order, and the type of every value that arrived - the
 * types being read from the values by this class rather than by the code under test, so that a
 * mapping wrong on both sides does not agree with itself.
 */
class NodeColumnsAgainstRuntimeTest {

    @Test
    @DisplayName("a map's columns are the row its port builds, rule for rule and in the same order")
    void aMapsColumnsAreTheRowItsPortBuilds() {
        Map<String, FieldRule> rules = projection();

        NodeColumns derived = NodeColumns.of(new TransformBody.MapProjection(rules), one(upstream()), null);
        Map<String, Object> produced = mapped(rules, row());

        agree(derived, produced);
        // Said out loud so that a projection that quietly produced nothing would not satisfy the
        // comparison above by leaving both sides empty.
        assertThat(produced.keySet()).containsExactly("order_id", "channel", "doubled", "qty");
    }

    @Test
    @DisplayName("a filter's columns are the row it lets through, untouched")
    void aFiltersColumnsAreTheRowItLetsThrough() {
        TransformPort filter = StatelessTransforms.filter("after.qty > 1");

        NodeColumns derived =
                NodeColumns.of(new TransformBody.Filter("after.qty > 1"), one(upstream()), null);
        List<Envelope> out = filter.transform(insert(row()));

        // The predicate has to have kept the row: a filter that dropped it would leave nothing to
        // compare, and an empty output must not read as agreement.
        assertThat(out).hasSize(1);
        agree(derived, out.get(0).after());
    }

    @Test
    @DisplayName("a script's row carries a column the derivation refuses to name, which is why it refuses")
    void aScriptsRowCarriesAColumnTheDerivationRefusesToName() {
        String script =
                "function process(r, ctx) { r.after.tier = r.after.qty > 1 ? 'gold' : 'plain'; return r; }";

        NodeColumns derived = NodeColumns.of(new TransformBody.Js(script), one(upstream()), null);
        Map<String, Object> produced =
                StatelessTransforms.js(script).transform(insert(row())).get(0).after();

        // The row really did change shape - without this the case would hold against a script that did
        // nothing, where refusing to answer and answering the upstream model are indistinguishable.
        assertThat(produced).containsKey("tier");
        assertThat(upstream().columns()).doesNotContainKey("tier");
        // So handing the upstream model on, which is what every other single-input arm does, would be
        // a model naming a column that is not in the row and missing one that is.
        assertThat(derived.known()).isFalse();
        assertThat(derived.unknownBecause()).startsWith("js:");
    }

    @Test
    @DisplayName("a merge's columns hold a row from either input, and the type only one of them carries")
    void aMergesColumnsHoldARowFromEitherInput() {
        NodeColumns orders = upstream();
        // Declared NOT NULL by the input that carries it, so that the merge has to take that away
        // rather than being agreed with by accident - the assertion below is about the downgrade, and
        // over a column its own input already called nullable it would hold whether or not one happened.
        NodeColumns returns = known("id", "INT64 NOT NULL", "reason", "STRING NOT NULL");
        Map<String, Object> fromOrders = row();
        Map<String, Object> fromReturns = new LinkedHashMap<>(Map.of("id", 8L, "reason", "damaged"));

        NodeColumns derived = NodeColumns.of(new TransformBody.Union(),
                inputs(orders, returns), null);

        // A merge forwards each row as it arrives, so what it produces is these two rows and the model
        // has to hold both. Containment rather than equality is the whole point: neither row on its own
        // carries every column, and a model equal to either one drops the other's values at the write.
        assertThat(derived.columns().keySet()).containsAll(fromOrders.keySet());
        assertThat(derived.columns().keySet()).containsAll(fromReturns.keySet());
        holdsTypes(derived, fromOrders);
        holdsTypes(derived, fromReturns);
        // And a column only one input carries has to be nullable however sure that input was of it,
        // because the other input's rows - which really do arrive, as this case just showed - carry
        // nothing there.
        assertThat(derived.columns().get("reason")).isEqualTo("STRING NULL");
        assertThat(derived.columns().get("id")).isEqualTo("INT64 NOT NULL");
    }

    @Test
    @DisplayName("a per-field push's columns are the row that same projection builds")
    void aPerFieldPushsColumnsAreTheRowThatProjectionBuilds() {
        Map<String, FieldRule> rules = projection();

        NodeColumns derived = NodeColumns.of(push(PushFormat.fields(rules)), upstream());
        Map<String, Object> produced = mapped(rules, row());

        // The rules a push carries are the rules a map carries, and the row is built from them the same
        // way. A push answered by walking the transform steps instead would answer nothing at all here.
        agree(derived, produced);
    }

    @Test
    @DisplayName("a push whose body is one expression yields one value, so it names no column")
    void aPushWhoseBodyIsOneExpressionYieldsOneValue() {
        NodeColumns derived = NodeColumns.of(push(PushFormat.cel("after.region")), upstream());

        // What that body produces at run time is one value: the same expression, evaluated the one way
        // this tree evaluates expressions over a row, yields a single value under whatever name it is
        // asked to put it - never a set of names of its own.
        Map<String, Object> produced =
                mapped(rules("body", FieldRule.computed("after.region")), row());

        assertThat(produced).containsEntry("body", "eu");
        assertThat(derived.known()).isFalse();
        assertThat(derived.unknownBecause()).startsWith("serve.push cel:");
    }

    @Test
    @DisplayName("a view's columns are the row it is handed, whatever built that row")
    void aViewsColumnsAreTheRowItIsHanded() {
        // A view stores rows rather than reshaping them, so the row its runtime produces is the row
        // that reached it. Building that row with a real projection rather than writing one down keeps
        // the case honest about where the columns come from: the upstream model and the row are then
        // two views of one thing rather than two hand-written lists that agree by construction.
        Map<String, FieldRule> rules = projection();
        NodeColumns reachingTheView =
                NodeColumns.of(new TransformBody.MapProjection(rules), one(upstream()), null);
        Map<String, Object> handedToTheView = mapped(rules, row());

        NodeColumns derived = NodeColumns.of(
                new ViewBlock.Inline("v", FromRef.literal("orders"), "order_id", null, null),
                reachingTheView);

        agree(derived, handedToTheView);
    }

    /** The columns and the order agree with the row, and every value arrived as its column's type. */
    private static void agree(NodeColumns derived, Map<String, Object> row) {
        assertThat(derived.known()).isTrue();
        assertThat(derived.columns().keySet()).containsExactlyElementsOf(row.keySet());
        holdsTypes(derived, row);
    }

    /** Every value in the row is of the type the model declares for its column. */
    private static void holdsTypes(NodeColumns derived, Map<String, Object> row) {
        row.forEach((name, value) -> {
            if (value != null) {
                assertThat(JoinSchemaDrift.typeOf(derived.columns().get(name)))
                        .as("the declared type of '%s'", name)
                        .isEqualTo(typeOf(value));
            }
        });
    }

    /**
     * What type a value that actually arrived is of. Written here rather than taken from the code under
     * test on purpose: a mapping the derivation got wrong would map the value the same wrong way and
     * the comparison would pass. A value of a shape no case seeds fails rather than being skipped -
     * skipping is how a comparison stops comparing anything without saying so.
     */
    private static TapstateType typeOf(Object value) {
        return switch (value) {
            case Boolean ignored -> TapstateType.BOOLEAN;
            case String ignored -> TapstateType.STRING;
            case Integer ignored -> TapstateType.INT64;
            case Long ignored -> TapstateType.INT64;
            case Double ignored -> TapstateType.DOUBLE;
            default -> throw new AssertionError(
                    "a case seeded a value this comparison has no type for: " + value.getClass());
        };
    }

    /** The row a map port builds from these rules, which is what the runtime hands downstream. */
    private static Map<String, Object> mapped(Map<String, FieldRule> rules, Map<String, Object> after) {
        TransformPort map = StatelessTransforms.map(
                MapSpec.from(new TransformBody.MapProjection(rules)));
        return map.transform(insert(after)).get(0).after();
    }

    private static Envelope insert(Map<String, Object> after) {
        return Envelope.insert(1L, "orders", new LinkedHashMap<>(after), null);
    }

    /**
     * One projection exercised by several cases: a rename, a drop, a written-down value and a computed
     * one - the four rules there are, so that no case rests on the one kind that happens to be easiest.
     */
    private static Map<String, FieldRule> projection() {
        Map<String, FieldRule> rules = new LinkedHashMap<>();
        rules.put("order_id", FieldRule.rename("id"));
        rules.put("region", FieldRule.drop());
        rules.put("channel", FieldRule.literal("web"));
        rules.put("doubled", FieldRule.computed("after.qty * 2"));
        return rules;
    }

    /** The model reaching a node, and below it the row that model describes. */
    private static NodeColumns upstream() {
        return known("id", "INT64 NOT NULL", "qty", "INT64 NULL", "region", "STRING NULL");
    }

    private static Map<String, Object> row() {
        Map<String, Object> after = new LinkedHashMap<>();
        after.put("id", 7L);
        after.put("qty", 3L);
        after.put("region", "eu");
        return after;
    }

    private static NodeColumns known(String... nameThenType) {
        Map<String, String> columns = new LinkedHashMap<>();
        for (int i = 0; i < nameThenType.length; i += 2) {
            columns.put(nameThenType[i], nameThenType[i + 1]);
        }
        return NodeColumns.known(columns);
    }

    private static Map<String, FieldRule> rules(String output, FieldRule rule) {
        Map<String, FieldRule> rules = new LinkedHashMap<>();
        rules.put(output, rule);
        return rules;
    }

    private static Map<String, NodeColumns> one(NodeColumns upstream) {
        return Map.of("in", upstream);
    }

    private static Map<String, NodeColumns> inputs(NodeColumns... streams) {
        Map<String, NodeColumns> byName = new LinkedHashMap<>();
        for (int i = 0; i < streams.length; i++) {
            byName.put("in" + i, streams[i]);
        }
        return byName;
    }

    private static PushElement push(PushFormat format) {
        return new PushElement(null, "orders_src", "topic", format);
    }
}
