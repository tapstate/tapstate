package io.tapstate.app;

import io.tapstate.core.model.FieldRule;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.JoinEngine;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.model.PushElement;
import io.tapstate.core.model.PushFormat;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.model.ViewBlock;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The dispatch every node of a pipeline is worked out through, the answers the single-input nodes
 * give, and the guarantee that a node nobody wrote an answer for cannot reach a run.
 *
 * <p>Part of what is under test here is not a behaviour - it is that a ninth kind of node cannot be
 * added without the compiler stopping whoever added it. That guarantee has no runtime shape, so two
 * cases cover the things around it that do: every kind alive today reaches an arm of its own, and the
 * number of kinds each face carries is the number this class answers for. The second is what survives
 * somebody silencing the compiler with a catch-all instead of writing the arm.
 *
 * <p>The rest is the shape each node hands on. <b>A projection's answer has to be the row the
 * projection actually builds</b>, rule for rule and in the same order, because that answer is what a
 * target table is built to later - so the cases below are written against what the projection does to
 * a row rather than against a paraphrase of it.
 */
class NodeColumnsTest {

    @Test
    @DisplayName("every kind of transform reaches an arm of its own")
    void everyKindOfTransformReachesAnArmOfItsOwn() {
        NodeColumns upstream = upstream();

        // The two a single-input step answers for. A filter's answer is the upstream itself, so what
        // makes it an answer rather than a silence is that it is known at all.
        assertThat(NodeColumns.of(map(rules("area", FieldRule.rename("region"))), upstream).known())
                .isTrue();
        assertThat(NodeColumns.of(new TransformBody.Filter("after.qty > 1"), upstream).known()).isTrue();

        List<TransformBody> unanswered = List.of(
                new TransformBody.Js("emit(record)"),
                new TransformBody.Union(),
                new TransformBody.Nest("id", null, new NestRoot("orders", null, null, null, null)),
                new TransformBody.Join(JoinEngine.BUILTIN, "select 1"));
        Set<String> reasons = new LinkedHashSet<>();
        for (TransformBody body : unanswered) {
            NodeColumns answer = NodeColumns.of(body, upstream);
            assertThat(answer.known()).isFalse();
            assertThat(answer.unknownBecause()).startsWith(body.type() + ":");
            reasons.add(answer.unknownBecause());
        }
        // One arm per kind, not one arm shared by four: a single answer covering the lot would read as
        // four answers here unless the reasons are counted.
        assertThat(reasons).hasSameSizeAs(unanswered);
    }

    @Test
    @DisplayName("each push format is answered on its own terms, and carrying none is a format too")
    void eachPushFormatIsAnsweredOnItsOwnTerms() {
        NodeColumns upstream = upstream();

        NodeColumns envelope = NodeColumns.of(push(null), upstream);
        NodeColumns fields =
                NodeColumns.of(push(PushFormat.fields(rules("region", FieldRule.drop()))), upstream);
        NodeColumns cel = NodeColumns.of(push(PushFormat.cel("after.region")), upstream);

        // A push element with no format of its own is not a missing node - it publishes the envelope,
        // which wraps the row in metadata rather than reshaping it, so the row travels on as it was.
        assertThat(envelope.columns()).containsExactlyEntriesOf(upstream.columns());
        assertThat(fields.columns().keySet()).containsExactly("id", "qty");
        // One expression produces the whole body, so there is a type but no set of names. The type is
        // said out loud because it is the whole of what can be said.
        assertThat(cel.known()).isFalse();
        assertThat(cel.unknownBecause()).startsWith("serve.push cel:").contains("STRING");
    }

    @Test
    @DisplayName("a view stores the rows it is given, so the columns it is given")
    void aViewStoresWhatItIsGiven() {
        ViewBlock.Inline view = new ViewBlock.Inline("v", FromRef.literal("orders"), "id", null, null);

        NodeColumns answer = NodeColumns.of(view, upstream());

        assertThat(answer.known()).isTrue();
        assertThat(answer.columns()).containsExactlyEntriesOf(upstream().columns());
    }

    @Test
    @DisplayName("a predicate decides which rows travel on, never which columns they carry")
    void aFilterHandsTheColumnsOnUntouched() {
        NodeColumns answer = NodeColumns.of(new TransformBody.Filter("after.qty > 1"), upstream());

        assertThat(answer.columns()).containsExactlyEntriesOf(upstream().columns());
    }

    @Test
    @DisplayName("a projection puts its declared rules first, then what it did not speak for")
    void aProjectionOrdersItsRulesFirstThenThePassThrough() {
        Map<String, FieldRule> rules = new LinkedHashMap<>();
        rules.put("tag", FieldRule.literal("eu"));
        rules.put("area", FieldRule.rename("region"));

        NodeColumns answer = NodeColumns.of(map(rules), upstream());

        // Declared order is the output order, and a rename consumes the column it took - so 'region'
        // is not also passed through under its old name. What is left arrives in the order it arrived.
        assertThat(answer.columns().keySet()).containsExactly("tag", "area", "id", "qty");
        assertThat(answer.columns()).containsEntry("area", "STRING NULL");
    }

    @Test
    @DisplayName("a rename whose source is not there produces nothing, not an empty column")
    void aRenameOfAnAbsentColumnProducesNothing() {
        NodeColumns answer =
                NodeColumns.of(map(rules("copy", FieldRule.rename("no_such_column"))), upstream());

        // The projection itself produces no output for a source it cannot find, so a column here would
        // be one the rows never carry - and a target table built with it would take a column that is
        // empty on every row.
        assertThat(answer.columns()).doesNotContainKey("copy");
        assertThat(answer.columns().keySet()).containsExactly("id", "qty", "region");
    }

    @Test
    @DisplayName("a drop removes a column, and a rule's output beats a column of the same name")
    void aDropRemovesAndARuleBeatsASameNamedColumn() {
        Map<String, FieldRule> rules = new LinkedHashMap<>();
        rules.put("qty", FieldRule.drop());
        rules.put("region", FieldRule.literal(7));

        NodeColumns answer = NodeColumns.of(map(rules), upstream());

        assertThat(answer.columns()).doesNotContainKey("qty");
        // The upstream 'region' is a string; the rule that produced one of the same name wins, exactly
        // as it wins in the row the projection builds.
        assertThat(answer.columns()).containsEntry("region", "INT64 NOT NULL");
        assertThat(answer.columns().keySet()).containsExactly("region", "id");
    }

    @Test
    @DisplayName("a literal is typed by what was written down, and is the one column known to be there")
    void aLiteralIsTypedByWhatWasWrittenDown() {
        Map<String, FieldRule> rules = new LinkedHashMap<>();
        rules.put("flag", FieldRule.literal(true));
        rules.put("label", FieldRule.literal("eu"));
        rules.put("count", FieldRule.literal(3));
        rules.put("ratio", FieldRule.literal(1.5d));
        rules.put("odd", FieldRule.literal(List.of("a")));

        Map<String, String> columns = NodeColumns.of(map(rules), upstream()).columns();

        // NOT NULL because a literal is the same written-down value on every row - the only column
        // here nothing can make absent.
        assertThat(columns).containsEntry("flag", "BOOLEAN NOT NULL");
        assertThat(columns).containsEntry("label", "STRING NOT NULL");
        assertThat(columns).containsEntry("count", "INT64 NOT NULL");
        assertThat(columns).containsEntry("ratio", "DOUBLE NOT NULL");
        // A shape that is none of those is unknown rather than the nearest of them.
        assertThat(columns).containsEntry("odd", "UNKNOWN NOT NULL");
    }

    @Test
    @DisplayName("a computed value is typed by the expression, and one no column can hold is unknown")
    void aComputedValueIsTypedByTheExpression() {
        Map<String, FieldRule> rules = new LinkedHashMap<>();
        rules.put("doubled", FieldRule.computed("after.qty * 2"));
        rules.put("joined", FieldRule.computed("after.region + '!'"));
        rules.put("stamped", FieldRule.computed("now()"));

        Map<String, String> columns = NodeColumns.of(map(rules), upstream()).columns();

        // The upstream types reach the expression: 'qty' is a whole number there, so the arithmetic
        // over it is one too. Nullable throughout - an expression over a column that may be absent may
        // itself yield nothing.
        assertThat(columns).containsEntry("doubled", "INT64 NULL");
        assertThat(columns).containsEntry("joined", "STRING NULL");
        // A timestamp is a type the expression language produces and no column holds. It goes down the
        // same road every other thing nobody can name goes down, rather than being rounded to the
        // nearest column type this side happens to have.
        assertThat(columns).containsEntry("stamped", "UNKNOWN NULL");
    }

    @Test
    @DisplayName("the per-field push format is the same projection the map step is")
    void thePushFieldsFormatProjectsTheSameWayAMapDoes() {
        Map<String, FieldRule> rules = new LinkedHashMap<>();
        rules.put("area", FieldRule.rename("region"));
        rules.put("qty", FieldRule.drop());
        rules.put("total", FieldRule.computed("after.id * 2"));

        NodeColumns viaMap = NodeColumns.of(map(rules), upstream());
        NodeColumns viaPush = NodeColumns.of(push(PushFormat.fields(rules)), upstream());

        // The two share one rule vocabulary in the grammar, so an answer computed twice would be two
        // answers to maintain - and the one that drifts is the one nobody is looking at.
        assertThat(viaPush.columns()).containsExactlyEntriesOf(viaMap.columns());
        assertThat(viaPush.columns().keySet()).containsExactly("area", "total", "id");
    }

    @Test
    @DisplayName("an unknown is handed on as it was written, naming the step that actually went dark")
    void anUnknownIsHandedOnUnreworded() {
        NodeColumns dark = NodeColumns.of(new TransformBody.Js("emit(record)"), upstream());
        ViewBlock.Inline view = new ViewBlock.Inline("v", FromRef.literal("orders"), "id", null, null);
        Map<String, FieldRule> rules = rules("tag", FieldRule.literal("eu"));

        // Every step below a dark one could say only that it cannot see past it, and a chain of those
        // buries the one fact worth having: which step stopped being able to answer.
        assertThat(NodeColumns.of(new TransformBody.Filter("true"), dark)).isEqualTo(dark);
        assertThat(NodeColumns.of(map(rules), dark)).isEqualTo(dark);
        assertThat(NodeColumns.of(view, dark)).isEqualTo(dark);
        assertThat(NodeColumns.of(push(null), dark)).isEqualTo(dark);
        assertThat(NodeColumns.of(push(PushFormat.cel("1")), dark)).isEqualTo(dark);
        assertThat(NodeColumns.of(push(PushFormat.fields(rules)), dark)).isEqualTo(dark);
    }

    @Test
    @DisplayName("a serve block hands back every push node it carries, and none when it carries none")
    void aServeBlockHandsBackEveryPushNodeItCarries() {
        PushElement first = push(PushFormat.cel("record"));
        PushElement second = push(null);

        assertThat(NodeColumns.pushNodesOf(serve(List.of(first, second))))
                .containsExactly(first, second);
        assertThat(NodeColumns.pushNodesOf(serve(null))).isEmpty();
    }

    @Test
    @DisplayName("a use-reference crashes bare: the derivation runs on a pipeline whose references are expanded")
    void aUseReferenceCrashesBare() {
        ViewBlock.Use view = new ViewBlock.Use(null, "shared_view", FromRef.literal("orders"));
        ServeBlock.Use serve = new ServeBlock.Use(null, "shared_serve", FromRef.literal("orders"));

        // Not a coded error: reaching here with a reference means the caller skipped the expansion,
        // which is a defect on this side and nothing an author can act on. It crashes even where the
        // upstream is itself unknown - a defect on this side is not something to hand on quietly.
        assertThatThrownBy(() -> NodeColumns.of(view, NodeColumns.unknown("js: dark")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("shared_view");
        assertThatThrownBy(() -> NodeColumns.pushNodesOf(serve))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("shared_serve");
    }

    @Test
    @DisplayName("a ninth kind of node cannot land without an arm here")
    void aNinthKindOfNodeCannotLandWithoutAnArmHere() {
        // The compiler is what actually enforces this - every switch over these faces is exhaustive and
        // carries no catch-all, so a new variant stops the build until somebody answers for it. This
        // case covers the one way that guarantee is lost without the build ever going red: a catch-all
        // added to make the error go away. The counts are the arms; a variant absorbed by a catch-all
        // moves the count and not the arms.
        assertThat(TransformBody.class.getPermittedSubclasses()).hasSize(6);
        assertThat(PushFormat.class.getPermittedSubclasses()).hasSize(2);
        assertThat(ViewBlock.class.getPermittedSubclasses()).hasSize(2);
        assertThat(ServeBlock.class.getPermittedSubclasses()).hasSize(2);
    }

    @Test
    @DisplayName("a known answer carries columns and no reason; an unknown one carries a reason and no columns")
    void aKnownAnswerCarriesColumnsAndAnUnknownOneCarriesAReason() {
        // Declared in an order a hash map does not keep, because the declared order is the output order:
        // a node rebuilt with its columns reordered records a schema that differs from the one before it
        // for a reason nothing about the pipeline moved.
        Map<String, String> declared = new LinkedHashMap<>();
        declared.put("zip", "STRING NULL");
        declared.put("id", "INT64 NOT NULL");
        declared.put("amount", "DECIMAL NULL");
        declared.put("created_at", "DATETIME NULL");
        NodeColumns known = NodeColumns.known(declared);
        NodeColumns unknown = NodeColumns.unknown("js: nobody can say");

        assertThat(known.known()).isTrue();
        assertThat(known.columns()).containsExactlyEntriesOf(declared);
        assertThat(known.unknownBecause()).isNull();
        // Empty rather than null, so a caller walking the columns of whatever it was handed does not
        // have to ask which of the two it got before it can walk nothing.
        assertThat(unknown.known()).isFalse();
        assertThat(unknown.columns()).isEmpty();
    }

    /** Three columns in a fixed order, spanning a type arithmetic reaches and one it does not. */
    private static NodeColumns upstream() {
        Map<String, String> columns = new LinkedHashMap<>();
        columns.put("id", "INT64 NOT NULL");
        columns.put("qty", "INT64 NULL");
        columns.put("region", "STRING NULL");
        return NodeColumns.known(columns);
    }

    private static TransformBody.MapProjection map(Map<String, FieldRule> rules) {
        return new TransformBody.MapProjection(rules);
    }

    private static Map<String, FieldRule> rules(String output, FieldRule rule) {
        Map<String, FieldRule> rules = new LinkedHashMap<>();
        rules.put(output, rule);
        return rules;
    }

    private static PushElement push(PushFormat format) {
        return new PushElement(null, "orders_src", "topic", format, null);
    }

    private static ServeBlock.Inline serve(List<PushElement> push) {
        return new ServeBlock.Inline("s", FromRef.literal("orders"), null, null, push);
    }
}
