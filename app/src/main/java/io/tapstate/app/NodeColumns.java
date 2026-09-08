package io.tapstate.app;

import io.tapstate.core.model.PushElement;
import io.tapstate.core.model.PushFormat;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.model.ViewBlock;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
 * <p><b>The vocabulary is the one already recorded</b> - column name to declared type, in output order,
 * rendered the way every other side that writes down a derived column renders it. A second rendering of
 * the same column would drift from that one eventually, and the shape that takes is a recorded schema
 * that no longer equals the one the next start computes, which reads as a difference nobody made.
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
     * What a transform step produces. Six kinds, and the switch is exhaustive over all of them: a
     * seventh cannot be added to the grammar without this stopping the build.
     */
    static NodeColumns of(TransformBody body) {
        return switch (body) {
            case TransformBody.Js ignored ->
                    unknown("js: a script settles its own columns while it runs, not before");
            case TransformBody.MapProjection ignored ->
                    unknown("map: the projection's field rules are not read here yet");
            case TransformBody.Filter ignored ->
                    unknown("filter: the upstream columns it passes through are not read here yet");
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
    static NodeColumns of(PushElement element) {
        return switch (element.format()) {
            case null ->
                    unknown("serve.push envelope: the envelope's own shape is not written down here yet");
            case PushFormat.Cel ignored ->
                    unknown("serve.push cel: the expression's result type is not read here yet");
            case PushFormat.Fields ignored ->
                    unknown("serve.push fields: the per-field rules are not read here yet");
        };
    }

    /** What a view stores. */
    static NodeColumns of(ViewBlock view) {
        return switch (view) {
            case ViewBlock.Inline ignored ->
                    unknown("view: the columns it holds are not read here yet");
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
     * Reaching a reference here means the caller skipped expanding it. Bare rather than coded: the
     * reference was resolved against the workspace when the pipeline was applied, so its survival to
     * this point is a defect on this side and nothing an author can act on.
     */
    private static IllegalStateException notExpanded(String block, String referenced) {
        return new IllegalStateException(block + " '" + referenced
                + "' is still a use-reference; it has to be expanded before a node is worked out");
    }
}
