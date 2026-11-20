package io.tapstate.core.dsl;

import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.core.common.DslErrorDefinitions;
import io.tapstate.core.common.ProductErrorDefinition;
import io.tapstate.core.common.Severity;

import java.util.Set;

/**
 * Typed DSL errors backed by shared product definitions.
 * Each constant a well-formed artifact can raise on its own maps 1:1 to a corpus rule-vocabulary key
 * — the symbol is the vocabulary key, independent of its product diagnostic identity.
 *
 * <p>The exceptions are the codes no corpus artifact can witness, each proven by a direct test
 * instead. They are exempt for one of two reasons, and every exemption states which: a code is
 * <em>pre-semantic</em> when it fires before an artifact exists to be a witness ({@link #MALFORMED_YAML}
 * on unparseable text, the two interpolation codes on raw text), or <em>post-semantic</em> when it
 * needs knowledge no artifact carries — the row-expression type codes need a source model that only
 * exists once a connection has been discovered.
 *
 * <p>{@code placeholders()} is the named-argument contract: every throw site supplies a value for
 * each name, and (once the catalog lands in the presentation layer) the build-time placeholder
 * gate checks message templates against it. {@code path} is present on every semantic
 * code — the field path of the offending node, carried both as a typed accessor on {@link DslException}
 * and as a message argument — but absent on {@link #MALFORMED_YAML}, which is pre-semantic.
 */
public enum DslError implements TapstateErrorCode {

    /**
     * The document declares a grammar version this build cannot read, or declares none. One code for
     * both: to whoever wrote it they are the same situation -- the document does not say which grammar
     * it is in -- and the argument says which of the two happened.
     */
    UNSUPPORTED_VERSION("unsupported-version", DslErrorDefinitions.UNSUPPORTED_VERSION),
    /** A field outside the tapstate/v1 schema (§11.5, strict rejection). */
    UNKNOWN_FIELD("unknown-field", DslErrorDefinitions.UNKNOWN_FIELD),
    /** A field known to the schema but banned in this position (X18/X19). */
    FORBIDDEN_FIELD("forbidden-field", DslErrorDefinitions.FORBIDDEN_FIELD),
    /**
     * A field the model declares required and the parser cannot supply, left out of the document.
     * Narrower than the model's non-null contract: an id the parser generates and a {@code from:} it
     * derives from natural order are non-null in the model yet legitimately absent from the YAML, so
     * what a document must carry is its own list rather than a reading of the model's.
     */
    MISSING_FIELD("missing-field", DslErrorDefinitions.MISSING_FIELD),
    /** An id / table / step reference with no target in the batch (§1/§4/§8). */
    MISSING_REFERENCE("missing-reference", DslErrorDefinitions.MISSING_REFERENCE),
    /** A bare table name colliding across declared sources (§4). */
    AMBIGUOUS_REFERENCE("ambiguous-reference", DslErrorDefinitions.AMBIGUOUS_REFERENCE),
    /** An option / block illegal for the source mode or boundedness (§4/X7/X10). */
    MODE_MISMATCH("mode-mismatch", DslErrorDefinitions.MODE_MISMATCH),
    /**
     * A source a pipeline reads that declares no {@code mode} (X18). Its own code rather than
     * {@link #MISSING_FIELD} because the requirement is not a property of the source: the same
     * document is correct until something reads it, so {@code pipeline} names what made it required.
     */
    MODE_REQUIRED_FOR_READ("mode-required-for-read", DslErrorDefinitions.MODE_REQUIRED_FOR_READ),
    /** An enum or format constraint violation (§2/§8). */
    ILLEGAL_VALUE("illegal-value", DslErrorDefinitions.ILLEGAL_VALUE),
    /** A CEL expression field that fails to compile or type-check (§12); {@code detail} carries
     *  the compiler diagnostic. */
    ILLEGAL_EXPRESSION("illegal-expression", DslErrorDefinitions.ILLEGAL_EXPRESSION),
    /** A structural composition rule broken (X17); {@code detail} names the specific rule. */
    COMPOSITION("composition", DslErrorDefinitions.COMPOSITION),
    /** An id collision: top-level / pipeline-internal uniqueness, or step-id shadowing (§2/F8, §5). */
    DUPLICATE_ID("duplicate-id", DslErrorDefinitions.DUPLICATE_ID),
    /** A source mode outside the connector's declared capability matrix (§4 / C3); {@code allowed}
     *  lists the connector's legal modes. */
    UNSUPPORTED_MODE("unsupported-mode", DslErrorDefinitions.UNSUPPORTED_MODE),
    /** A connector config value whose type does not match the connector's declared field type
     *  (C3); {@code expected} names the declared type. */
    CONFIG_TYPE_MISMATCH("config-type-mismatch", DslErrorDefinitions.CONFIG_TYPE_MISMATCH),
    /** A required connector config field is absent after its visibility conditions are applied. */
    CONFIG_REQUIRED("config-required", DslErrorDefinitions.CONFIG_REQUIRED),
    /** A connector config value outside the connector's declared enum choices (C3); {@code allowed}
     *  lists the legal values. */
    INVALID_CONFIG_VALUE("invalid-config-value", DslErrorDefinitions.INVALID_CONFIG_VALUE),
    /** A document whose YAML does not parse at all. Pre-semantic, so unlike every other
     *  code it carries no field {@code path} (none is known) and has no corpus witness (a syntax error
     *  cannot be a well-formed corpus artifact) — {@code detail} carries the parser diagnostic, the
     *  typed line / column carry the location, and it is proven by a direct parser test. */
    MALFORMED_YAML("malformed-yaml", DslErrorDefinitions.MALFORMED_YAML),
    /** A {@code ${...}} reference naming a variable that is not set, where no default was given (§9).
     *  Pre-semantic like {@link #MALFORMED_YAML}: interpolation runs on raw text before the parse, so
     *  no field {@code path} exists yet and the typed line / column carry the location. Whether it
     *  fires depends on the environment rather than on the document, so it has no corpus witness. */
    UNDEFINED_VARIABLE("undefined-variable", DslErrorDefinitions.UNDEFINED_VARIABLE),
    /** A {@code ${...}} reference that is not one of the forms the grammar defines (§9) — an unclosed
     *  reference, an unknown prefix, or a name that is not a variable name. Pre-semantic and without a
     *  corpus witness, for the same reasons as {@link #UNDEFINED_VARIABLE}; {@code ref} echoes the
     *  offending reference. */
    MALFORMED_INTERPOLATION("malformed-interpolation", DslErrorDefinitions.MALFORMED_INTERPOLATION),
    /** A row expression reading a column of a source whose schema has never been discovered, so there
     *  is nothing to judge the expression against. {@code source} names the source to discover.
     *  Post-semantic: it turns on what has been discovered, not on the document, so it has no corpus
     *  witness and is proven by a direct test. */
    ROW_EXPRESSION_NEEDS_DISCOVERY("row-expression-needs-discovery", DslErrorDefinitions.ROW_EXPRESSION_NEEDS_DISCOVERY),
    /** A row expression computing on a column whose type cannot survive the operation — an exact
     *  fixed-point number, a temporal value, a shape only known to be JSON. {@code column} names the
     *  column, {@code type} what it resolved to, and {@code table} the discovered table it resolved to
     *  that there - an expression reading several is judged against each, so which one refused it is
     *  the difference between a fixable diagnostic and a puzzle. Post-semantic: it needs a discovered
     *  source model, which no offline artifact carries, so it has no corpus witness and is proven by a
     *  direct test. */
    ROW_EXPRESSION_TYPE_UNSUPPORTED("row-expression-type-unsupported", DslErrorDefinitions.ROW_EXPRESSION_TYPE_UNSUPPORTED),
    /** A row expression reading a column the source declared a type outside the tapstate namespace
     *  for, so nothing resolved what it holds. {@code table} names the discovered table the column was
     *  read from. Post-semantic and without a corpus witness, for the same reason as
     *  {@link #ROW_EXPRESSION_TYPE_UNSUPPORTED}. */
    ROW_EXPRESSION_TYPE_UNKNOWN("row-expression-type-unknown", DslErrorDefinitions.ROW_EXPRESSION_TYPE_UNKNOWN),
    /** An upsert writing a table whose source declares no key, so no write can be matched to a row.
     *  {@code table} names the discovered table and {@code source} the connection it was discovered
     *  through. Post-semantic and without a corpus witness, for the same reason as
     *  {@link #ROW_EXPRESSION_TYPE_UNSUPPORTED}: whether a table has a key is knowledge no artifact
     *  carries, only a discovered model does. */
    UPSERT_NEEDS_KEY("upsert-needs-key", DslErrorDefinitions.UPSERT_NEEDS_KEY),
    /**
     * An unwind naming nothing that varies from one element to the next, so every row it produces
     * carries the same key as the row it came from. {@code step} names the unwind. Unlike
     * {@link #UPSERT_NEEDS_KEY}, which asks what a table declares, this one is answered by the
     * document alone: whether the elements can be told apart is the author's to state, and an
     * expansion that does not state it collapses back into a single row at the target with nothing
     * reporting how many were overwritten.
     */
    UNWIND_NEEDS_AN_ELEMENT_KEY("unwind-needs-an-element-key", DslErrorDefinitions.UNWIND_NEEDS_AN_ELEMENT_KEY),
    /**
     * An unwind whose rows reach a sync writing in append mode, where a delete is written as another
     * row rather than removing one. {@code step} names the unwind and {@code sync} the element it
     * reaches. Refused rather than run: the expansion is correct there and still cannot converge, so
     * deleting a parent appends the elements it used to hold instead of taking their rows away.
     */
    UNWIND_NEEDS_AN_UPSERT_TARGET("unwind-needs-an-upsert-target", DslErrorDefinitions.UNWIND_NEEDS_AN_UPSERT_TARGET),
    /**
     * A sync element writing through a connector this release does not support as a write target.
     * {@code connector} names it, {@code source} the connection supplier the element referenced.
     *
     * <p>Judged only for a connector this release officially supports: one it does not is a
     * connector no shipped deployment can register in the first place, so refusing it here would
     * speak about a document only a deployment that widened its own accepted set can produce.
     */
    UNSUPPORTED_TARGET_CONNECTOR("unsupported-target-connector", DslErrorDefinitions.UNSUPPORTED_TARGET_CONNECTOR),
    /** A generated unwind column would overwrite a parent column or another generated column. */
    UNWIND_COLUMN_ALREADY_EXISTS("unwind-column-already-exists", DslErrorDefinitions.UNWIND_COLUMN_ALREADY_EXISTS),
    /** A step names an output field with the one top-level name the product reserves for its own
     *  data on a row. The author controls that name, so it is refused rather than renamed: whatever
     *  serves a row removes the reserved field, and the declared column would be removed with it.
     *  {@code step} is the declaring step or definition, {@code field} the name. */
    RESERVED_FIELD_DECLARED("reserved-field-declared", DslErrorDefinitions.RESERVED_FIELD_DECLARED),
    /** A join's {@code sql:} is not SQL at all. {@code detail} carries the parser's own diagnosis,
     *  which already names the line and column it stopped at. Kept apart from
     *  {@link #JOIN_SQL_UNSUPPORTED} because the two send a reader in opposite directions: one to
     *  their own typing, the other to what this release supports. */
    JOIN_SQL_NOT_PARSABLE("join-sql-not-parsable", DslErrorDefinitions.JOIN_SQL_NOT_PARSABLE),
    /** A join's {@code sql:} parses but uses a construct this release does not run. {@code shape}
     *  names it as SQL spells it ({@code FULL OUTER JOIN}, {@code GROUP BY}, {@code COUNT}), and
     *  {@code line} / {@code column} locate it within the SQL text itself, not within the YAML. */
    JOIN_SQL_UNSUPPORTED("join-sql-unsupported", DslErrorDefinitions.JOIN_SQL_UNSUPPORTED),
    /** A join's {@code from:} map names another step of the pipeline rather than a source table.
     *  {@code step} is the join, {@code alias} the alias whose value it is and {@code ref} the step
     *  it names. A join resolves its SQL against the discovered columns of each input and keys its
     *  state on the driving table's primary key; a step's output has neither, so the join could
     *  only fail at every start. */
    JOIN_INPUT_NOT_A_TABLE("join-input-not-a-table", DslErrorDefinitions.JOIN_INPUT_NOT_A_TABLE),
    /** A join's {@code from:} map binds an alias to a {@code /…/} pattern. {@code step} is the join,
     *  {@code alias} the alias and {@code pattern} the expression. A join reads one table per alias -
     *  its SQL names that table's columns and its state is keyed on that table's primary key - and a
     *  pattern names a set whose members can change, so it is refused even where it matches exactly
     *  one table today. */
    JOIN_INPUT_IS_A_PATTERN("join-input-is-a-pattern", DslErrorDefinitions.JOIN_INPUT_IS_A_PATTERN),
    /** A rollup field names no function, an unknown one, or more than one. {@code field} is the rollup
     *  field, {@code function} what it gave in the function's place and {@code known} the functions there
     *  are. One function per field, so which one applies never depends on key order. */
    ROLLUP_FUNCTION_UNKNOWN("rollup-function-unknown", DslErrorDefinitions.ROLLUP_FUNCTION_UNKNOWN),
    /** {@code rollup:} on an embed that holds at most one row ({@code as: object} or {@code as: flat}).
     *  {@code as} is the embed's shape. A value computed over one row is that row's own column. */
    ROLLUP_ON_SINGLE_ROW_EMBED("rollup-on-single-row-embed", DslErrorDefinitions.ROLLUP_ON_SINGLE_ROW_EMBED),
    /** {@code as: rollup} with no {@code rollup:} fields: the level would be held and never shown. */
    ROLLUP_LEVEL_WITHOUT_FIELDS("rollup-level-without-fields", DslErrorDefinitions.ROLLUP_LEVEL_WITHOUT_FIELDS);


    private final String code;
    private final String symbol;
    private final Set<String> placeholders;

    DslError(String symbol, ProductErrorDefinition definition) {
        this.symbol = symbol;
        this.code = definition.code();
        this.placeholders = definition.parameters();
    }

    @Override
    public String code() {
        return code;
    }

    @Override
    public Severity severity() {
        return Severity.ERROR;
    }

    @Override
    public Set<String> placeholders() {
        return placeholders;
    }

    /** The stable corpus rule-vocabulary key, independent of the diagnostic wire identity. */
    public String symbol() {
        return symbol;
    }

    /** Resolves a corpus-vocabulary symbol (e.g. {@code "unknown-field"}) to its code; throws if unknown. */
    public static DslError ofSymbol(String symbol) {
        for (DslError e : values()) {
            if (e.symbol().equals(symbol)) {
                return e;
            }
        }
        throw new IllegalArgumentException("no DslError for corpus rule '" + symbol + "'");
    }
}
