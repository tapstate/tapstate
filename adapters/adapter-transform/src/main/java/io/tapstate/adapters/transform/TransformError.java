package io.tapstate.adapters.transform;

import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.core.common.Severity;

import java.util.Set;

/**
 * The {@code transform} domain's error codes: the first-party, diagnosable failures a stateless row
 * transform raises while it evaluates an author's expression or script. These are user-facing — the
 * author wrote a CEL expression that fails against the row it meets, or a js script that will not
 * compile, defines no entry point, throws at runtime, or produces output that is not a record.
 *
 * <p>Two-segment {@code transform.<symbol>} codes; they go through the full build-time gate set like
 * any other first-party domain. Programmer errors / invariant violations stay bare and are allowed to
 * crash — a checked AST that will not build into a program, for instance, is not laundered into a
 * {@code transform.*} code that would hide the defect. {@code placeholders()} is the named-argument
 * contract: every throw site supplies a value for each name, and the build-time placeholder gate
 * checks the catalog templates against it.
 */
public enum TransformError implements TapstateErrorCode {

    /**
     * A row expression (a filter predicate or a map computed value) failed to evaluate against an
     * event. {@code expr} is the expression text; {@code detail} is the evaluator diagnostic — a
     * missing field, a type clash on a dyn value, or a function that type-checks but is unbound at
     * runtime.
     */
    EXPRESSION_FAILED("transform.expression-failed", Set.of("expr", "detail")),

    /** A js transform script could not be compiled. {@code detail} is the script-engine diagnostic. */
    SCRIPT_COMPILE_FAILED("transform.script-compile-failed", Set.of("detail")),

    /** A js transform script defines no {@code process(record, ctx)} function — its required entry point. */
    SCRIPT_NO_PROCESS("transform.script-no-process", Set.of()),

    /**
     * A js transform script raised an error while processing an event. {@code detail} is the
     * guest-side failure the script engine reported.
     */
    SCRIPT_FAILED("transform.script-failed", Set.of("detail")),

    /**
     * A js transform script produced output that is not a valid record — a non-record return value, or
     * a {@code before} / {@code after} / {@code schema} that is not an object. {@code detail} names
     * what was wrong.
     */
    SCRIPT_OUTPUT_INVALID("transform.script-output-invalid", Set.of("detail")),

    /**
     * Running: an expansion met an update or a delete whose earlier row is not a whole row, so what
     * the row used to hold cannot be read and the rows it produced cannot be taken away.
     *
     * <p><b>Judged on what the row carries, never on whether it has the expanded column.</b> A whole
     * document that simply has no such column is a legitimate row - an optional field in a document
     * store is an ordinary thing, and a snapshot read of the same row is required to accept it and
     * produce nothing. What separates the two is that half a row carries the columns identifying it
     * and nothing else: a change stream with no pre-image sends exactly that, and so does a
     * relational source under its default replica identity. {@code detail} says which of the two
     * conditions failed, since the fix is a different setting in each case.
     *
     * <p>Refused rather than absorbed, and that is the whole of this code. Absorbed, the earlier row
     * reads as a row holding no elements, the delete expands into nothing, and every row the parent
     * once produced stays in the target with nothing left pointing at it - green run, no error, and
     * a delete case over a source that does send whole rows passes throughout.
     */
    UNWIND_NEEDS_A_COMPLETE_BEFORE_IMAGE(
            "transform.unwind-needs-a-complete-before-image", Set.of("path", "detail")),

    /**
     * Running: two elements of one row expand to rows carrying the same key, so the target keeps
     * whichever is written last and the other is gone with nothing recording it.
     *
     * <p>A warning rather than a refusal because the declaration is not wrong: whether two elements
     * of one row happen to share an identity is a property of the data, which no check made before
     * the run can see. Both rows are still sent - swallowing one would be this operator deciding
     * which of an author's elements counts - so what the target holds is unchanged by saying it, and
     * saying it is the only reason anyone finds out. {@code key} is the shared key value as the
     * comparison sees it, which is what makes the offending row findable at the source.
     */
    UNWIND_ROWS_SHARE_A_KEY(
            "transform.unwind-rows-share-a-key", Set.of("path", "key"), Severity.WARNING);

    private final String code;
    private final Set<String> placeholders;
    private final Severity severity;

    TransformError(String code, Set<String> placeholders) {
        this(code, placeholders, Severity.ERROR);
    }

    TransformError(String code, Set<String> placeholders, Severity severity) {
        this.code = code;
        this.placeholders = placeholders;
        this.severity = severity;
    }

    @Override
    public String code() {
        return code;
    }

    @Override
    public Severity severity() {
        return severity;
    }

    @Override
    public Set<String> placeholders() {
        return placeholders;
    }
}
