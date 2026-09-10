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
import io.tapstate.core.sql.JoinPlan;
import io.tapstate.core.sql.SourceColumn;
import io.tapstate.core.sql.SourceTable;
import io.tapstate.core.sql.SqlFrontEnd;
import io.tapstate.spi.sink.TargetField;
import io.tapstate.spi.sink.TargetTable;
import io.tapstate.core.common.TapstateType;
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
        // One set of inputs the six kinds all answer over, chosen so that each arm's answer differs
        // from the others': the root the nest names carries a column the sibling stream does not, and
        // the projection renames one of them.
        Map<String, NodeColumns> inputs = new LinkedHashMap<>();
        inputs.put("orders", known("o_id", "INT64 NOT NULL", "o_total", "DECIMAL NULL"));
        inputs.put("lines", known("l_id", "INT64 NOT NULL"));

        Set<NodeColumns> answers = new LinkedHashSet<>();
        for (TransformBody body : List.of(
                new TransformBody.Js("emit(record)"),
                map(rules("amount", FieldRule.rename("o_total"))),
                new TransformBody.Filter("after.o_id > 1"),
                new TransformBody.Union(),
                nest("orders", List.of("o_total")),
                new TransformBody.Join(JoinEngine.BUILTIN, JOIN_SQL))) {
            answers.add(NodeColumns.of(body, inputs, joinPlan()));
        }

        // Five, not six, and the missing one is on purpose: a predicate and a merge are one function
        // on columns, asserted as such in its own case. Every other pair of arms answers differently,
        // so folding any of them into another - or into a catch-all - drops this count.
        assertThat(answers).hasSize(5);
        assertThat(NodeColumns.of(new TransformBody.Filter("after.o_id > 1"), inputs, null))
                .isEqualTo(NodeColumns.of(new TransformBody.Union(), inputs, null));
    }

    @Test
    @DisplayName("a script is the one kind that answers with a reason instead of columns")
    void aScriptAnswersWithAReasonInsteadOfColumns() {
        NodeColumns answer = NodeColumns.of(new TransformBody.Js("emit(record)"), one(upstream()), null);

        assertThat(answer.known()).isFalse();
        assertThat(answer.unknownBecause()).startsWith("js:");
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
        NodeColumns answer = NodeColumns.of(new TransformBody.Filter("after.qty > 1"), one(upstream()), null);

        assertThat(answer.columns()).containsExactlyEntriesOf(upstream().columns());
    }

    @Test
    @DisplayName("a projection puts its declared rules first, then what it did not speak for")
    void aProjectionOrdersItsRulesFirstThenThePassThrough() {
        Map<String, FieldRule> rules = new LinkedHashMap<>();
        rules.put("tag", FieldRule.literal("eu"));
        rules.put("area", FieldRule.rename("region"));

        NodeColumns answer = NodeColumns.of(map(rules), one(upstream()), null);

        // Declared order is the output order, and a rename consumes the column it took - so 'region'
        // is not also passed through under its old name. What is left arrives in the order it arrived.
        assertThat(answer.columns().keySet()).containsExactly("tag", "area", "id", "qty");
        assertThat(answer.columns()).containsEntry("area", "STRING NULL");
    }

    @Test
    @DisplayName("a rename whose source is not there produces nothing, not an empty column")
    void aRenameOfAnAbsentColumnProducesNothing() {
        NodeColumns answer =
                NodeColumns.of(map(rules("copy", FieldRule.rename("no_such_column"))), one(upstream()), null);

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

        NodeColumns answer = NodeColumns.of(map(rules), one(upstream()), null);

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

        Map<String, String> columns = NodeColumns.of(map(rules), one(upstream()), null).columns();

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
        rules.put("copied", FieldRule.computed("after.region"));
        rules.put("doubled", FieldRule.computed("after.qty * 2"));
        rules.put("joined", FieldRule.computed("after.region + '!'"));
        rules.put("stamped", FieldRule.computed("now()"));

        Map<String, String> columns = NodeColumns.of(map(rules), one(upstream()), null).columns();

        // A bare column read is the assertion that carries the weight: it is the only one here whose
        // answer differs depending on whether the upstream types reached the expression at all. The
        // two below it do not discriminate on their own - an untyped column is checked as dyn, which
        // unifies with the other operand, so arithmetic over an unmodelled column still checks out as
        // arithmetic and a concatenation still checks out as text. Both were green against a build
        // that fed the expression nothing.
        assertThat(columns).containsEntry("copied", "STRING NULL");
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

        NodeColumns viaMap = NodeColumns.of(map(rules), one(upstream()), null);
        NodeColumns viaPush = NodeColumns.of(push(PushFormat.fields(rules)), upstream());

        // The two share one rule vocabulary in the grammar, so an answer computed twice would be two
        // answers to maintain - and the one that drifts is the one nobody is looking at.
        assertThat(viaPush.columns()).containsExactlyEntriesOf(viaMap.columns());
        assertThat(viaPush.columns().keySet()).containsExactly("area", "total", "id");
    }

    @Test
    @DisplayName("an unknown is handed on as it was written, naming the step that actually went dark")
    void anUnknownIsHandedOnUnreworded() {
        NodeColumns dark = NodeColumns.of(new TransformBody.Js("emit(record)"), one(upstream()), null);
        ViewBlock.Inline view = new ViewBlock.Inline("v", FromRef.literal("orders"), "id", null, null);
        Map<String, FieldRule> rules = rules("tag", FieldRule.literal("eu"));

        // Every step below a dark one could say only that it cannot see past it, and a chain of those
        // buries the one fact worth having: which step stopped being able to answer.
        assertThat(NodeColumns.of(new TransformBody.Filter("true"), one(dark), null)).isEqualTo(dark);
        assertThat(NodeColumns.of(map(rules), one(dark), null)).isEqualTo(dark);
        assertThat(NodeColumns.of(view, dark)).isEqualTo(dark);
        assertThat(NodeColumns.of(push(null), dark)).isEqualTo(dark);
        assertThat(NodeColumns.of(push(PushFormat.cel("1")), dark)).isEqualTo(dark);
        assertThat(NodeColumns.of(push(PushFormat.fields(rules)), dark)).isEqualTo(dark);
    }

    @Test
    @DisplayName("down a whole chain, one column nobody can type and one step that went dark stay apart")
    void aChainKeepsTheColumnItCannotTypeApartFromTheStepThatWentDark() {
        // What this adds over the case above, which walks one hop off a dark node: a chain, and the
        // two unknowns a pipeline actually produces standing in it at once. They are different in
        // kind and are acted on differently - one column of an otherwise answered model that no type
        // covers sends a reader to the expression that wrote it, while a step that settles its own
        // columns while it runs sends them to the step. Folded into one answer, the first reads as
        // the second and the whole model below it reads as unknowable.
        NodeColumns stamped = NodeColumns.of(map(rules("stamped", FieldRule.computed("now()"))),
                one(upstream()), null);
        NodeColumns script = NodeColumns.of(
                new TransformBody.Js("function process(r, ctx) { return r; }"), one(stamped), null);
        NodeColumns end = NodeColumns.of(map(rules("area", FieldRule.rename("region"))), one(script), null);

        // A timestamp is outside every type a column holds, so that one column is unknown while the
        // model around it is still answered - the columns beside it keep the types they arrived with.
        assertThat(stamped.known()).isTrue();
        assertThat(stamped.columns()).containsEntry("stamped", "UNKNOWN NULL");
        assertThat(stamped.columns()).containsEntry("region", "STRING NULL");
        // The script is the other kind: not a column, the whole step.
        assertThat(script.known()).isFalse();
        assertThat(script.unknownBecause()).startsWith("js:");
        // And at the end of the chain the reason is still the script's own, word for word, rather
        // than the last step's - which is the step a reader would otherwise be sent to.
        assertThat(end.known()).isFalse();
        assertThat(end.unknownBecause()).isEqualTo(script.unknownBecause());
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
        // The projection switches over the field rules the same way and under the same ban, so a
        // fifth kind of rule is held here too - it is the one face on this list that is not a node.
        assertThat(FieldRule.class.getPermittedSubclasses()).hasSize(4);
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
    // ---- several streams reaching one node ----

    @Test
    @DisplayName("a merge publishes every column any input carries, never only the ones they share")
    void aMergePublishesEveryColumnAnyInputCarries() {
        // Nothing strips a column off a row because a sibling stream has no such column, so the
        // intersection would describe a row that is never produced - and describe it one column short,
        // which is the direction whose values are dropped at the write with nothing reporting it.
        NodeColumns left = known("id", "INT64 NOT NULL", "qty", "INT64 NULL");
        NodeColumns right = known("id", "INT64 NOT NULL", "region", "STRING NULL");

        NodeColumns answer = NodeColumns.of(new TransformBody.Union(), inputs(left, right), null);

        assertThat(answer.columns()).containsOnlyKeys("id", "qty", "region");
    }

    @Test
    @DisplayName("a column only one input carries comes back nullable, whatever that input said")
    void aColumnOnlyOneInputCarriesComesBackNullable() {
        // The load-bearing half of the merge: a row arriving from the input without it carries nothing
        // there, so NOT NULL would be a promise the other stream breaks on its first row.
        NodeColumns left = known("id", "INT64 NOT NULL", "qty", "INT64 NOT NULL");
        NodeColumns right = known("id", "INT64 NOT NULL");

        Map<String, String> columns = NodeColumns.of(
                new TransformBody.Union(), inputs(left, right), null).columns();

        assertThat(columns).containsEntry("qty", "INT64 NULL").containsEntry("id", "INT64 NOT NULL");
    }

    @Test
    @DisplayName("inputs that disagree about a column's type publish it as UNKNOWN")
    void inputsThatDisagreeAboutATypePublishUnknown() {
        // Picking either answer states a type that is wrong for the other input's rows, and the two
        // are equally wrong - so the column is the one thing this vocabulary has for "nobody can say".
        NodeColumns left = known("code", "INT64 NOT NULL");
        NodeColumns right = known("code", "STRING NOT NULL");

        Map<String, String> columns = NodeColumns.of(
                new TransformBody.Union(), inputs(left, right), null).columns();

        assertThat(columns).containsEntry("code", "UNKNOWN NOT NULL");
    }

    @Test
    @DisplayName("a merge keeps the order it first met each column in")
    void aMergeKeepsTheOrderItFirstMetEachColumnIn() {
        NodeColumns left = known("id", "INT64 NOT NULL", "qty", "INT64 NULL");
        NodeColumns right = known("region", "STRING NULL", "id", "INT64 NOT NULL");

        Map<String, String> columns = NodeColumns.of(
                new TransformBody.Union(), inputs(left, right), null).columns();

        assertThat(columns.keySet()).containsExactly("id", "qty", "region");
    }

    @Test
    @DisplayName("one input nobody can describe carries the whole merge, unreworded")
    void oneDarkInputCarriesTheWholeMerge() {
        NodeColumns dark = NodeColumns.of(new TransformBody.Js("emit(record)"), one(upstream()), null);

        NodeColumns answer = NodeColumns.of(
                new TransformBody.Union(), inputs(upstream(), dark), null);

        assertThat(answer).isEqualTo(dark);
    }

    @Test
    @DisplayName("a predicate and a merge answer the same thing over the same inputs")
    void aPredicateAndAMergeAnswerTheSameThing() {
        // They are one function on columns, because a predicate happens not to touch any. Written as
        // two arms all the same: a later change to what one of them does must not become a change to
        // the other by having been folded into it.
        Map<String, NodeColumns> inputs = inputs(
                known("id", "INT64 NOT NULL"), known("region", "STRING NULL"));

        assertThat(NodeColumns.of(new TransformBody.Filter("after.id > 1"), inputs, null))
                .isEqualTo(NodeColumns.of(new TransformBody.Union(), inputs, null));
    }

    @Test
    @DisplayName("a projection over several inputs projects what they merged to")
    void aProjectionOverSeveralInputsProjectsTheMerge() {
        // The rename takes the merged column's type, nullability included - so a column one input does
        // not carry arrives under its new name still admitting the rows that carry nothing there.
        NodeColumns left = known("id", "INT64 NOT NULL", "qty", "INT64 NOT NULL");
        NodeColumns right = known("id", "INT64 NOT NULL");

        Map<String, String> columns = NodeColumns.of(
                map(rules("amount", FieldRule.rename("qty"))), inputs(left, right), null).columns();

        assertThat(columns).containsEntry("amount", "INT64 NULL");
    }

    @Test
    @DisplayName("a node reached with no input at all crashes bare")
    void aNodeWithNoInputCrashesBare() {
        // Every node reads at least one stream, so this is a caller that wired none - a defect on this
        // side, which an unknown would file away as a stream that merely could not be described.
        assertThatThrownBy(() -> NodeColumns.of(new TransformBody.Union(), Map.of(), null))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("no input");
    }

    // ---- nest ----

    @Test
    @DisplayName("a nest publishes its root stream's columns, the root's key leading")
    void aNestPublishesItsRootsColumnsKeyLeading() {
        Map<String, NodeColumns> inputs = new LinkedHashMap<>();
        inputs.put("orders", known("o_total", "DECIMAL NULL", "o_id", "INT64 NOT NULL"));
        inputs.put("lines", known("l_id", "INT64 NOT NULL", "l_qty", "INT64 NULL"));

        Map<String, String> columns = NodeColumns.of(nest("orders", List.of("o_id")), inputs, null)
                .columns();

        // The embedded stream contributes nothing: its rows sit inside the documents rather than
        // beside them, so a column of theirs appearing here would be a target table column no write
        // ever fills.
        assertThat(columns.keySet()).containsExactly("o_id", "o_total");
    }

    @Test
    @DisplayName("a nest key naming a column the root does not carry is left out, not invented")
    void aNestKeyNamingAnAbsentColumnIsLeftOut() {
        Map<String, NodeColumns> inputs = new LinkedHashMap<>();
        inputs.put("orders", known("o_id", "INT64 NOT NULL"));

        Map<String, String> columns =
                NodeColumns.of(nest("orders", List.of("o_id", "no_such_column")), inputs, null)
                        .columns();

        assertThat(columns.keySet()).containsExactly("o_id");
    }

    @Test
    @DisplayName("a nest whose root is not among the inputs crashes bare")
    void aNestWhoseRootIsNotAmongTheInputsCrashesBare() {
        Map<String, NodeColumns> inputs = Map.of("lines", known("l_id", "INT64 NOT NULL"));

        assertThatThrownBy(() -> NodeColumns.of(nest("orders", List.of("o_id")), inputs, null))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("orders");
    }

    @Test
    @DisplayName("a nest orders its columns exactly as the assembled target model does")
    void aNestOrdersItsColumnsAsTheAssembledTargetModelDoes() {
        // The two answers have to stay one answer. The assembled model is still what the sink is
        // handed today, so until that path reads this one there are two orderings of the same columns
        // in the tree, and nothing but this compares them.
        //
        // Names and order only: the assembled model carries the target store's own type token for a
        // column while this carries the shared type and its nullability, so the types are not the
        // same statement about the column and comparing them would assert a coincidence.
        List<String> key = List.of("o_id", "o_region");
        TargetTable model = new TargetTable("orders", List.of(
                new TargetField("o_total", "decimal", false),
                new TargetField("o_id", "bigint", false),
                new TargetField("o_region", "text", false)));
        Map<String, NodeColumns> inputs = Map.of("orders",
                known("o_total", "DECIMAL NULL", "o_id", "INT64 NOT NULL", "o_region", "STRING NULL"));

        Map<String, String> columns = NodeColumns.of(nest("orders", key), inputs, null).columns();

        assertThat(columns.keySet()).containsExactlyElementsOf(
                TargetModelResolver.keyedOn(model, key).fields().stream().map(TargetField::name).toList());
    }

    // ---- join ----

    @Test
    @DisplayName("a join publishes the row its compiled query produces, in that order")
    void aJoinPublishesItsCompiledQuerysRow() {
        Map<String, String> columns = NodeColumns.of(
                new TransformBody.Join(JoinEngine.BUILTIN, JOIN_SQL), Map.of(), joinPlan()).columns();

        assertThat(columns.keySet()).containsExactly("o_id", "o_total", "customer_name");
        // The fact table's own column keeps what the source declared for it; the outer-joined
        // dimension's cannot, because a fact row that matches nothing publishes no value there.
        assertThat(columns).containsEntry("o_id", "INT64 NOT NULL");
        assertThat(columns.get("customer_name")).endsWith(" NULL").doesNotContain("NOT NULL");
    }

    @Test
    @DisplayName("the join answer is the one the drift record is written from, column for column")
    void theJoinAnswerIsTheOneTheDriftRecordIsWrittenFrom() {
        // The whole of what makes moving this derivation a refactor rather than a change: the record a
        // start is held to has to be byte-identical to what this now computes, or the first start after
        // the move refuses over a difference nobody made.
        InMemoryDerivedSchemaStore records = new InMemoryDerivedSchemaStore();
        new JoinSchemaDrift(records).checkAndRecord(
                "flow", "widen", new TransformBody.Join(JoinEngine.BUILTIN, JOIN_SQL), joinPlan(),
                JOIN_TABLES);

        Map<String, String> columns = NodeColumns.of(
                new TransformBody.Join(JoinEngine.BUILTIN, JOIN_SQL), Map.of(), joinPlan()).columns();

        assertThat(columns).isEqualTo(records.latest("flow", "widen").orElseThrow().schema());
    }

    @Test
    @DisplayName("a join reached with nothing compiled crashes bare")
    void aJoinWithNothingCompiledCrashesBare() {
        assertThatThrownBy(() -> NodeColumns.of(
                new TransformBody.Join(JoinEngine.BUILTIN, JOIN_SQL), Map.of(), null))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("compiled");
    }

    /** Several streams reaching one step, in the order they are given. */
    private static Map<String, NodeColumns> inputs(NodeColumns... streams) {
        Map<String, NodeColumns> byName = new LinkedHashMap<>();
        for (int i = 0; i < streams.length; i++) {
            byName.put("in" + i, streams[i]);
        }
        return byName;
    }

    /** A known answer written out as name, declared type, name, declared type. */
    private static NodeColumns known(String... nameThenType) {
        Map<String, String> columns = new LinkedHashMap<>();
        for (int i = 0; i < nameThenType.length; i += 2) {
            columns.put(nameThenType[i], nameThenType[i + 1]);
        }
        return NodeColumns.known(columns);
    }

    private static TransformBody.Nest nest(String rootAlias, List<String> key) {
        return new TransformBody.Nest(null, null, new NestRoot(rootAlias, key, null, null, null));
    }

    private static final String JOIN_SQL =
            "SELECT o.o_id, o.o_total, c.c_name AS customer_name"
                    + " FROM orders o LEFT JOIN customers c ON o.o_cust_id = c.c_id";

    private static final List<SourceTable> JOIN_TABLES = List.of(
            new SourceTable("orders", List.of(
                    new SourceColumn("o_id", TapstateType.INT64, false),
                    new SourceColumn("o_cust_id", TapstateType.INT64, true),
                    new SourceColumn("o_total", TapstateType.DECIMAL, false))),
            new SourceTable("customers", List.of(
                    new SourceColumn("c_id", TapstateType.INT64, false),
                    new SourceColumn("c_name", TapstateType.STRING, false))));

    private static JoinPlan joinPlan() {
        return SqlFrontEnd.derive(JOIN_SQL, JOIN_TABLES);
    }

    /** One stream reaching a step, under whatever name its wiring calls it by. */
    private static Map<String, NodeColumns> one(NodeColumns upstream) {
        return Map.of("in", upstream);
    }

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
        return new PushElement(null, "orders_src", "topic", format);
    }

    private static ServeBlock.Inline serve(List<PushElement> push) {
        return new ServeBlock.Inline("s", FromRef.literal("orders"), null, null, push);
    }
}
