package io.tapstate.app;

import io.tapstate.core.common.TapstateType;
import io.tapstate.core.dsl.RowExpressions;
import io.tapstate.core.model.FieldRule;
import io.tapstate.core.model.PushElement;
import io.tapstate.core.model.PushFormat;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.model.ViewBlock;
import io.tapstate.core.sql.JoinPlan;
import io.tapstate.core.sql.OutputField;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * What one node of a pipeline works its own output columns out to be, or the reason nobody can say.
 *
 * <p>The derivation rules cover transforms, views, and push formats. Runtime model recording walks
 * transforms and views. Push delivery is not assembled by the runtime, so its format rules and serve
 * enumeration are exercised only by rule tests; push elements have no recorded model or comparison
 * row. These exhaustive switches check coverage of the grammar, not runtime wiring. Adding push
 * execution will also require wiring its model derivation, recording, and comparison into assembly.
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
 * <p><b>A node is handed its inputs by name, and what each kind does with them differs.</b> Most read
 * whatever reaches them as one model - several streams merge into it, and a single stream is that
 * merge's degenerate case. A nest reads exactly one of them, the stream its own root names, which is
 * why the inputs arrive keyed by the name that node's wiring calls them by rather than in an order
 * somebody would have to know. A join reads none of them: its columns come from its compiled query,
 * which is the one other thing handed in here, and it is the only node whose answer does not follow
 * from what reaches it.
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
     * What a transform step produces, given what reaches it and, where it has one, what it compiled to.
     * Seven kinds, and the switch is exhaustive over all of them: an eighth cannot be added to the
     * grammar without this stopping the build.
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
    static NodeColumns of(TransformBody body, Map<String, NodeColumns> inputs, JoinPlan compiledJoin) {
        return switch (body) {
            case TransformBody.Js ignored ->
                    unknown("js: a script settles its own columns while it runs, not before");
            case TransformBody.MapProjection projection ->
                    project(projection.fields(), merged(inputs.values()));
            // A predicate decides which rows travel on, never which columns they carry. Handing what
            // reached it back unchanged is the whole of it - including when that answer is an unknown,
            // which then keeps naming the step that actually went dark.
            case TransformBody.Filter ignored -> merged(inputs.values());
            case TransformBody.Unwind unwind -> expanded(unwind, merged(inputs.values()));
            // The merge itself, and nothing besides. It computes what the arm above does, and is
            // written out separately all the same: the two coincide only because a predicate happens
            // not to touch a column, so folding them together would make a later change to one of
            // them silently a change to the other.
            case TransformBody.Union ignored -> merged(inputs.values());
            case TransformBody.Nest nest -> rootOf(nest, inputs);
            case TransformBody.Join ignored -> published(compiledJoin);
        };
    }

    /**
     * The one model that reaches a node where several streams do: every column any input carries, and
     * each column only as certain as the least certain input carrying it.
     *
     * <p><b>The union of the field sets, never the intersection.</b> A merge forwards each row as it
     * arrives - nothing strips a column off a row because a sibling stream has no such column - so the
     * intersection describes a row that is never produced, and it is short in the one direction this
     * model must not err in: a column missing here is a column the target table is never built with,
     * and every value in it is then dropped at the write with nothing reporting it.
     *
     * <p><b>Certainty only ever goes down.</b> A column absent from any one input is nullable whatever
     * the inputs carrying it said, because a row from an input without it carries nothing there. A
     * column the inputs disagree about the type of is UNKNOWN rather than the first answer or the
     * wider one: they disagree, and either choice states a type that is wrong for the other's rows.
     *
     * <p>An unknown input carries the whole merge, unreworded. The columns of a merge one of whose
     * inputs nobody can describe are not knowable either, and the name worth keeping is the node that
     * actually went dark rather than the one that merely could not see past it.
     */
    static NodeColumns merged(Collection<NodeColumns> inputs) {
        if (inputs.isEmpty()) {
            throw new IllegalStateException("a node was worked out with no input at all; every node "
                    + "reads at least one stream, so this is a caller that wired none");
        }
        for (NodeColumns input : inputs) {
            if (!input.known()) {
                return input;
            }
        }
        if (inputs.size() == 1) {
            return inputs.iterator().next();
        }
        Map<String, String> seen = new LinkedHashMap<>();
        Map<String, Integer> carriedBy = new LinkedHashMap<>();
        for (NodeColumns input : inputs) {
            input.columns().forEach((name, declared) -> {
                carriedBy.merge(name, 1, Integer::sum);
                String already = seen.get(name);
                seen.put(name, already == null ? declared : reconciled(already, declared));
            });
        }
        Map<String, String> out = new LinkedHashMap<>();
        seen.forEach((name, declared) -> out.put(name, carriedBy.get(name) == inputs.size()
                ? declared
                : JoinSchemaDrift.declaredType(JoinSchemaDrift.typeOf(declared), true)));
        return known(out);
    }

    /** Two inputs' answers for one column: the type they agree on, and null wherever either allows it. */
    private static String reconciled(String seen, String declared) {
        TapstateType type = JoinSchemaDrift.typeOf(seen) == JoinSchemaDrift.typeOf(declared)
                ? JoinSchemaDrift.typeOf(seen)
                : TapstateType.UNKNOWN;
        return JoinSchemaDrift.declaredType(type,
                JoinSchemaDrift.nullableOf(seen) || JoinSchemaDrift.nullableOf(declared));
    }

    /**
     * What an expansion produces: the row it was handed, with the expanded column re-declared as one
     * element instead of the list it was, and an ordinal column beside it where the author asked for
     * one. <b>The expanded column keeps its position</b> - the row is the parent's, one field
     * replaced, so a reader comparing this against the row that arrived sees one column change type
     * rather than a column vanish and another appear at the end.
     *
     * <p><b>A column the row does not carry is not conjured.</b> An expansion naming a field that is
     * not there produces nothing here, the same answer a rename whose source is missing produces:
     * inventing it would describe a target column no write ever fills. Whether that declaration
     * should have been refused in the first place is the validator's question, not this one's.
     *
     * <p><b>The element column is nullable whatever it is declared as.</b> A list may hold a null
     * among its elements, and a row kept for an empty list carries nothing there at all - neither is
     * visible in any declaration, so the only honest answer is the one that allows both. The ordinal
     * is the other way round: every expanded row has a position, so it is only nullable where empty
     * lists are kept and a row without an element can reach the target.
     *
     * <p><b>An element type nobody declared comes out unknown, and that is the answer rather than a
     * failure.</b> The type of what sits inside a list is not something a source's schema carries -
     * the connector framework's own array type has no field for it - so there is nothing here to
     * read and nothing to infer from. Unknown then travels to the write side as a column with no
     * declared type, which is the existing way of saying "the connector decides", not a new one.
     * <b>The declared name is taken as written and not judged here</b>: a name outside the shared
     * vocabulary lands as unknown, which is the same answer as declaring nothing - so whether a
     * misspelling is worth refusing is a question for the validator, where refusing it can carry a
     * reason.
     */
    private static NodeColumns expanded(TransformBody.Unwind unwind, NodeColumns upstream) {
        if (!upstream.known()) {
            return upstream;
        }
        Map<String, String> out = new LinkedHashMap<>(upstream.columns());
        if (out.containsKey(unwind.path())) {
            out.put(unwind.path(),
                    JoinSchemaDrift.declaredType(elementType(unwind.elementType()), true));
        }
        if (unwind.includeArrayIndex() != null) {
            out.put(unwind.includeArrayIndex(), JoinSchemaDrift.declaredType(TapstateType.INT64,
                    Boolean.TRUE.equals(unwind.preserveNullAndEmptyArrays())));
        }
        return known(out);
    }

    /** The declared element type as the shared vocabulary spells it, or unknown for anything else. */
    private static TapstateType elementType(String declared) {
        if (declared == null) {
            return TapstateType.UNKNOWN;
        }
        try {
            return TapstateType.valueOf(declared.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException outsideTheVocabulary) {
            return TapstateType.UNKNOWN;
        }
    }

    /**
     * What a nest publishes: the columns of the stream its root names, that root's upsert key leading.
     * <b>The embedded children contribute none of their own</b> - their rows sit inside the documents
     * rather than beside them, so a column of theirs here would be a target column no write ever
     * fills. That is what the assembled model already said; this is the same answer, moved to where
     * every other node's is worked out rather than a second one beside it.
     *
     * <p>A key naming a column the root does not carry is left out rather than invented. It travels on
     * one column short, which the sink reports against the table it is writing, where conjuring the
     * column would instead describe a table the connector cannot create.
     *
     * <p><b>Which columns those are survives only as their order.</b> This vocabulary is column name to
     * declared type and has nowhere to say "and this one is the key" - said out loud because an order
     * that happens to lead with a key reads like a model that carries one.
     *
     * <p>A root that is not among the inputs is a caller that has not resolved the nest's wiring rather
     * than anything an author wrote, so it crashes bare: an unknown would file it away as a stream
     * that merely could not be described, which is an ordinary state nobody investigates.
     */
    private static NodeColumns rootOf(TransformBody.Nest nest, Map<String, NodeColumns> inputs) {
        String alias = nest.root().from();
        NodeColumns root = inputs.get(alias);
        if (root == null) {
            throw new IllegalStateException("nest root '" + alias + "' is not among the inputs "
                    + inputs.keySet() + "; the root's own stream has to be resolved before a nest's"
                    + " columns are worked out");
        }
        if (!root.known()) {
            return root;
        }
        Map<String, String> out = new LinkedHashMap<>();
        for (String column : nest.root().key() == null ? List.<String>of() : nest.root().key()) {
            String declared = root.columns().get(column);
            if (declared != null) {
                out.put(column, declared);
            }
        }
        root.columns().forEach(out::putIfAbsent);
        return known(out);
    }

    /**
     * What a join publishes: the flat row its compiled query produces, in the order it publishes it,
     * each column rendered the way every other derived column is.
     *
     * <p><b>This is the one node whose answer does not follow from what reaches it.</b> A join's
     * columns come from its SELECT and from what its sources say they hold, which is what compiling
     * the query works out; the streams themselves say nothing about the shape of the widened row. So
     * the inputs go unread here, and arriving with nothing compiled is a caller that has not compiled
     * the step - a defect on this side, hence bare.
     */
    private static NodeColumns published(JoinPlan compiledJoin) {
        if (compiledJoin == null) {
            throw new IllegalStateException("a join was worked out with no compiled query; its columns"
                    + " come from its SELECT, so the step has to be compiled before it is asked");
        }
        Map<String, String> columns = new LinkedHashMap<>();
        for (OutputField field : compiledJoin.outputFields()) {
            columns.put(field.name(), JoinSchemaDrift.declaredType(field));
        }
        return known(columns);
    }

    /**
     * The column rule for a push format, currently exercised only by rule tests because push delivery
     * is not assembled. An omitted format selects the envelope rule.
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
     * Enumerates the push definitions for the rule tests. This has no runtime caller: push delivery
     * is not assembled and these definitions must not acquire placeholder model records. The switch
     * remains exhaustive so a new serve shape requires an explicit enumeration rule.
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
