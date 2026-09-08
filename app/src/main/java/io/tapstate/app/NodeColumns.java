package io.tapstate.app;

import io.tapstate.core.common.TapstateType;
import io.tapstate.core.dsl.RowExpressions;
import io.tapstate.core.model.FieldRule;
import io.tapstate.core.model.PushElement;
import io.tapstate.core.model.PushFormat;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.model.ViewBlock;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * What one node of a pipeline works its own output columns out to be, or the reason nobody can say.
 *
 * <p><b>Three faces, three switches, and the count is why.</b> The nodes that change the shape of a row
 * do not all sit in one place: most are transform steps, but the one that publishes and the one that
 * stores are separate blocks of the pipeline beside the steps. Anything written as "walk the transforms"
 * therefore covers most of them and says nothing at all about the other two - which is how these were
 * last counted, and the miscount was invisible because a node nobody asked produces no error.
 *
 * <p><b>Every switch here is exhaustive and carries no catch-all, deliberately.</b> A node kind added
 * later with no answer here does not fail: its columns go unknown, unknown is allowed through, and what
 * the pipeline says it produces is quietly short of a column - which reads exactly like a source that
 * dropped one, and is chased on the source side. There is no test that catches it either, because the
 * kind that is missing is by definition the one nobody wrote a case for. The compiler refusing to build
 * is the only check that fires before somebody has to notice, so nothing here may be made to compile by
 * adding a catch-all; the arm is the work.
 *
 * <p><b>Why the answer has two cases.</b> One node - a script - decides its own columns while it runs,
 * so "this produces no columns" and "nobody can say what this produces" are different answers and must
 * not arrive in the same shape: folded together, an unknown reads as an empty schema and every column
 * below it reads as newly appeared. The unknown carries which node went dark and why, because by the
 * time anyone looks at a pipeline's model, the useful question is which step stopped being able to
 * answer rather than that some step did.
 *
 * <p><b>An unknown here and an unknown on the value side are the same judgement.</b> A row value that
 * met a source connector's own conversion travels in a carrier holding what that source's schema
 * called the column it came from, and that name is absent exactly where the schema said nothing about
 * the value. Where an arm reports a column unknown for that reason, it is reporting the same absence,
 * and the two are not allowed to disagree - a column the model calls known while the value arrives
 * with no declared name is a write the target rebuilds from a guess. <b>The absence has more than one
 * cause and they must not be folded into one</b>: a value the schema cannot name at all (an element
 * inside an array), a path the schema simply does not carry, and a lane that never consulted a schema
 * because nothing downstream of it rebuilds anything. Only the cause says whether the schema is what
 * needs fixing, and an arm that answers "unknown" without it sends every reader to the same wrong
 * place.
 *
 * <p><b>The vocabulary is the one already recorded</b> - column name to declared type, in output order,
 * rendered the way every other side that writes down a derived column renders it. A second rendering of
 * the same column would drift from that one eventually, and the shape that takes is a recorded schema
 * that no longer equals the one the next start computes, which reads as a difference nobody made.
 *
 * <p><b>Every answer here is worked out from one upstream model</b> - the one that reaches the node
 * being asked about. Which model that is, where several streams reach one step, is a question this
 * does not answer and deliberately does not take a shape for: merging several upstreams is its own
 * ruling, and a different one for a merge, for a nested document and for a joined table. A parameter
 * shaped for it before a single arm reads one would be a guess at three answers at once.
 *
 * @param columns       the node's output columns, name to declared type, in output order; empty when
 *                      nothing can be said
 * @param unknownBecause why the columns cannot be given, naming the node, or null when they are given
 */
record NodeColumns(Map<String, String> columns, String unknownBecause) {

    NodeColumns {
        // Order-preserving rather than Map.copyOf: the declared order is the output order, and a copy
        // that loses it turns every rebuild of the same node into a differently ordered record.
        columns = Collections.unmodifiableMap(new LinkedHashMap<>(columns));
    }

    /** The columns a node produces, worked out. */
    static NodeColumns known(Map<String, String> columns) {
        return new NodeColumns(columns, null);
    }

    /**
     * No columns, and why - which starts with the node this is about. Empty columns rather than none, so
     * that a caller walking whatever it was handed does not have to ask which of the two it got before
     * it can walk nothing.
     */
    static NodeColumns unknown(String because) {
        return new NodeColumns(Map.of(), because);
    }

    /** Whether the columns are the answer, as opposed to the reason there is none. */
    boolean known() {
        return unknownBecause == null;
    }

    /**
     * What a transform step produces, given what reaches it. Six kinds, and the switch is exhaustive
     * over all of them: a seventh cannot be added to the grammar without this stopping the build.
     *
     * <p><b>A script is the one kind that cannot be answered for, and the answer is final rather than
     * pending.</b> Its columns are whatever the script writes on the row while it runs, and its types
     * are not merely hard but undefined - one JS number type means the same script can put a whole
     * number on one row and a fractional one on the next. Analysing the source would answer
     * confidently and sometimes wrongly, which is the one failure this model must not have; asking the
     * author to declare the shape moves the guess rather than removing it, since nothing would hold a
     * row to the declaration. <b>A script also sees events that carry no row at all</b> - it is the
     * only step handed schema changes - so for part of what it sees the question does not even apply,
     * and nothing here may work a row's shape out from one of those.
     */
    static NodeColumns of(TransformBody body, NodeColumns upstream) {
        return switch (body) {
            case TransformBody.Js ignored ->
                    unknown("js: a script settles its own columns while it runs, not before");
            case TransformBody.MapProjection projection -> project(projection.fields(), upstream);
            // A predicate decides which rows travel on, never which columns they carry. Handing the
            // upstream answer back unchanged is the whole of it - including when that answer is an
            // unknown, which then keeps naming the step that actually went dark.
            case TransformBody.Filter ignored -> upstream;
            case TransformBody.Union ignored ->
                    unknown("union: the several upstreams it merges are not read here yet");
            case TransformBody.Nest ignored ->
                    unknown("nest: the tree of embedded streams is not read here yet");
            case TransformBody.Join ignored ->
                    unknown("join: the compiled query's output fields are not read here yet");
        };
    }

    /**
     * What one push element publishes. Its shape is chosen by the format it carries, and carrying none
     * is a choice of its own - the envelope - rather than a node that is not there, so it is a case here
     * and not a guard above.
     */
    static NodeColumns of(PushElement element, NodeColumns upstream) {
        return switch (element.format()) {
            // The envelope wraps the row in metadata rather than reshaping it, so the row travels on
            // with the columns it arrived with. The wrapper's own names are not among them: they
            // describe the change, and this records what the change is to.
            case null -> upstream;
            // One expression produces the entire body, so what it yields is a single value and not a
            // set of named columns - a map literal included, since CEL types a map by its value type
            // and never by its keys. The type it does yield is named because that is the whole of what
            // can be said, and a reader who wants the columns has to look at the expression.
            case PushFormat.Cel cel -> upstream.known()
                    ? unknown("serve.push cel: the body is one expression producing "
                            + RowExpressions.typedValueType(cel.expr(), typesOf(upstream))
                            + ", which names no columns of its own")
                    : upstream;
            case PushFormat.Fields fields -> project(fields.fields(), upstream);
        };
    }

    /**
     * What a view stores: the rows it is given, so the columns it is given. A view carries a schema
     * policy of its own - whether the shape is held to and how it may move - but a policy says what
     * happens to a shape rather than what the shape is, so nothing there is read here.
     */
    static NodeColumns of(ViewBlock view, NodeColumns upstream) {
        return switch (view) {
            case ViewBlock.Inline ignored -> upstream;
            case ViewBlock.Use use -> throw notExpanded("view", use.use());
        };
    }

    /**
     * The push nodes a serve block carries, and none when it carries no push at all. A serve block is
     * not itself a node - it holds them - so this is the enumeration the two switches above are reached
     * through, and it is a switch for the same reason they are: a serve block of a shape nobody listed
     * here would contribute no nodes and say nothing about it.
     */
    static List<PushElement> pushNodesOf(ServeBlock serve) {
        return switch (serve) {
            case ServeBlock.Inline inline -> inline.push() == null ? List.of() : inline.push();
            case ServeBlock.Use use -> throw notExpanded("serve", use.use());
        };
    }

    /**
     * The projection both the map step and the per-field push format are: the declared rules in
     * declared order, then every upstream column no rule already spoke for, in the order it arrived.
     * <b>This mirrors what the projection actually does to a row, rule for rule</b> - a rename takes
     * the source column's type and consumes it, a drop removes it, a literal and a computed value add
     * one, an output name wins over a same-named column arriving from upstream, and a rename whose
     * source is not there produces nothing rather than an empty column. The two have to agree: this is
     * the shape a target table is later built to, and a row that does not fit it fails at the write.
     *
     * <p>An unknown upstream comes straight back out. Re-wording it here would replace the name of the
     * step that went dark with the name of a step that merely could not see past it, and the first is
     * the one worth having by the time anybody reads a pipeline's model.
     */
    private static NodeColumns project(Map<String, FieldRule> rules, NodeColumns upstream) {
        if (!upstream.known()) {
            return upstream;
        }
        Map<String, TapstateType> upstreamTypes = typesOf(upstream);
        Map<String, String> out = new LinkedHashMap<>();
        Set<String> consumed = new LinkedHashSet<>();
        Set<String> dropped = new LinkedHashSet<>();
        rules.forEach((output, rule) -> {
            switch (rule) {
                case FieldRule.Rename rename -> {
                    consumed.add(rename.sourceField());
                    String renamed = upstream.columns().get(rename.sourceField());
                    if (renamed != null) {
                        out.put(output, renamed);
                    }
                }
                case FieldRule.Drop ignored -> dropped.add(output);
                // A literal is the one column here that is known not to be absent: it is the same
                // written-down value on every row.
                case FieldRule.Literal literal ->
                        out.put(output, JoinSchemaDrift.declaredType(literalType(literal.value()), false));
                // Nullable, always: an expression over a column that may be absent may itself yield
                // nothing, and no part of the expression language says otherwise.
                case FieldRule.Computed computed -> out.put(output, JoinSchemaDrift.declaredType(
                        RowExpressions.typedValueType(computed.celExpr(), upstreamTypes), true));
            }
        });
        upstream.columns().forEach((name, type) -> {
            if (!out.containsKey(name) && !consumed.contains(name) && !dropped.contains(name)) {
                out.put(name, type);
            }
        });
        return known(out);
    }

    /**
     * What a written-down value is. A literal is written in the pipeline's own text, so its type is
     * whatever the text parsed to and nothing else is consulted; a shape the parser produces that is
     * not one of these is unknown rather than the closest of them, for the same reason a computed
     * value outside the expression language's own types is.
     */
    private static TapstateType literalType(Object value) {
        return switch (value) {
            case Boolean ignored -> TapstateType.BOOLEAN;
            case String ignored -> TapstateType.STRING;
            case Integer ignored -> TapstateType.INT64;
            case Long ignored -> TapstateType.INT64;
            case Double ignored -> TapstateType.DOUBLE;
            default -> TapstateType.UNKNOWN;
        };
    }

    /** The upstream columns as the expression checker wants them: the type alone, without the null. */
    private static Map<String, TapstateType> typesOf(NodeColumns upstream) {
        Map<String, TapstateType> types = new LinkedHashMap<>();
        upstream.columns().forEach((name, declared) -> types.put(name, JoinSchemaDrift.typeOf(declared)));
        return types;
    }

    /**
     * Reaching a reference here means the caller skipped expanding it. Bare rather than coded: the
     * reference was resolved against the workspace when the pipeline was applied, so its survival to
     * this point is a defect on this side and nothing an author can act on.
     */
    private static IllegalStateException notExpanded(String block, String referenced) {
        return new IllegalStateException(block + " '" + referenced
                + "' is still a use-reference; it has to be expanded before a node is worked out");
    }
}
