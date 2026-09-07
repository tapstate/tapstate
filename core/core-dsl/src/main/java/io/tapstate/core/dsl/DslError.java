package io.tapstate.core.dsl;

import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.core.common.Severity;

import java.util.Set;

/**
 * The {@code dsl} domain's error codes (ADR-0024 D1; the domain's first consumer, plan poc1 B3-7).
 * Each constant a well-formed artifact can raise on its own maps 1:1 to a corpus rule-vocabulary key
 * (corpus/README.md) — the symbol is the vocabulary key, the canonical code prefixes it with the
 * {@code dsl.} domain.
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
 * gate checks message templates against it (ADR-0024 D5-4). {@code path} is present on every semantic
 * code — the field path of the offending node, carried both as a typed accessor on {@link DslException}
 * and as a message argument — but absent on {@link #MALFORMED_YAML}, which is pre-semantic.
 */
public enum DslError implements TapstateErrorCode {

    /** A field outside the tapstate/v1 schema (§11.5, strict rejection). */
    UNKNOWN_FIELD("dsl.unknown-field", Set.of("field", "path")),
    /** A field known to the schema but banned in this position (X18/X19). */
    FORBIDDEN_FIELD("dsl.forbidden-field", Set.of("field", "path")),
    /** An id / table / step reference with no target in the batch (§1/§4/§8). */
    MISSING_REFERENCE("dsl.missing-reference", Set.of("ref", "path")),
    /** A bare table name colliding across declared sources (§4). */
    AMBIGUOUS_REFERENCE("dsl.ambiguous-reference", Set.of("ref", "path")),
    /** An option / block illegal for the source mode or boundedness (§4/X7/X10). */
    MODE_MISMATCH("dsl.mode-mismatch", Set.of("field", "mode", "path")),
    /** An enum or format constraint violation (§2/§8). */
    ILLEGAL_VALUE("dsl.illegal-value", Set.of("value", "expected", "path")),
    /** A CEL expression field that fails to compile or type-check (§12); {@code detail} carries
     *  the compiler diagnostic. */
    ILLEGAL_EXPRESSION("dsl.illegal-expression", Set.of("expr", "detail", "path")),
    /** A structural composition rule broken (X17); {@code detail} names the specific rule. */
    COMPOSITION("dsl.composition", Set.of("detail", "path")),
    /** An id collision: top-level / pipeline-internal uniqueness, or step-id shadowing (§2/F8, §5). */
    DUPLICATE_ID("dsl.duplicate-id", Set.of("id", "path")),
    /** A source mode outside the connector's declared capability matrix (§4 / C3); {@code allowed}
     *  lists the connector's legal modes. */
    UNSUPPORTED_MODE("dsl.unsupported-mode", Set.of("connector", "mode", "allowed", "path")),
    /** A connector config value whose type does not match the connector's declared field type
     *  (C3); {@code expected} names the declared type. */
    CONFIG_TYPE_MISMATCH("dsl.config-type-mismatch", Set.of("connector", "field", "expected", "path")),
    /** A required connector config field is absent after its visibility conditions are applied. */
    CONFIG_REQUIRED("dsl.config-required", Set.of("connector", "field", "path")),
    /** A connector config value outside the connector's declared enum choices (C3); {@code allowed}
     *  lists the legal values. */
    INVALID_CONFIG_VALUE("dsl.invalid-config-value", Set.of("connector", "field", "value", "allowed", "path")),
    /** A document whose YAML does not parse at all. Pre-semantic, so unlike every other
     *  code it carries no field {@code path} (none is known) and has no corpus witness (a syntax error
     *  cannot be a well-formed corpus artifact) — {@code detail} carries the parser diagnostic, the
     *  typed line / column carry the location, and it is proven by a direct parser test. */
    MALFORMED_YAML("dsl.malformed-yaml", Set.of("detail")),
    /** A {@code ${...}} reference naming a variable that is not set, where no default was given (§9).
     *  Pre-semantic like {@link #MALFORMED_YAML}: interpolation runs on raw text before the parse, so
     *  no field {@code path} exists yet and the typed line / column carry the location. Whether it
     *  fires depends on the environment rather than on the document, so it has no corpus witness. */
    UNDEFINED_VARIABLE("dsl.undefined-variable", Set.of("name")),
    /** A {@code ${...}} reference that is not one of the forms the grammar defines (§9) — an unclosed
     *  reference, an unknown prefix, or a name that is not a variable name. Pre-semantic and without a
     *  corpus witness, for the same reasons as {@link #UNDEFINED_VARIABLE}; {@code ref} echoes the
     *  offending reference. */
    MALFORMED_INTERPOLATION("dsl.malformed-interpolation", Set.of("ref")),
    /** A row expression reading a column of a source whose schema has never been discovered, so there
     *  is nothing to judge the expression against. {@code source} names the source to discover.
     *  Post-semantic: it turns on what has been discovered, not on the document, so it has no corpus
     *  witness and is proven by a direct test. */
    ROW_EXPRESSION_NEEDS_DISCOVERY(
            "dsl.row-expression-needs-discovery", Set.of("expr", "source", "path")),
    /** A row expression computing on a column whose type cannot survive the operation — an exact
     *  fixed-point number, a temporal value, a shape only known to be JSON. {@code column} names the
     *  column, {@code type} what it resolved to, and {@code table} the discovered table it resolved to
     *  that there - an expression reading several is judged against each, so which one refused it is
     *  the difference between a fixable diagnostic and a puzzle. Post-semantic: it needs a discovered
     *  source model, which no offline artifact carries, so it has no corpus witness and is proven by a
     *  direct test. */
    ROW_EXPRESSION_TYPE_UNSUPPORTED(
            "dsl.row-expression-type-unsupported", Set.of("expr", "column", "type", "table", "path")),
    /** A row expression reading a column the source declared a type outside the tapstate namespace
     *  for, so nothing resolved what it holds. {@code table} names the discovered table the column was
     *  read from. Post-semantic and without a corpus witness, for the same reason as
     *  {@link #ROW_EXPRESSION_TYPE_UNSUPPORTED}. */
    ROW_EXPRESSION_TYPE_UNKNOWN(
            "dsl.row-expression-type-unknown", Set.of("expr", "column", "table", "path")),
    /** An upsert writing a table whose source declares no key, so no write can be matched to a row.
     *  {@code table} names the discovered table and {@code source} the connection it was discovered
     *  through. Post-semantic and without a corpus witness, for the same reason as
     *  {@link #ROW_EXPRESSION_TYPE_UNSUPPORTED}: whether a table has a key is knowledge no artifact
     *  carries, only a discovered model does. */
    UPSERT_NEEDS_KEY(
            "dsl.upsert-needs-key", Set.of("table", "source", "path")),
    /** A join's {@code sql:} is not SQL at all. {@code detail} carries the parser's own diagnosis,
     *  which already names the line and column it stopped at. Kept apart from
     *  {@link #JOIN_SQL_UNSUPPORTED} because the two send a reader in opposite directions: one to
     *  their own typing, the other to what this release supports. */
    JOIN_SQL_NOT_PARSABLE("dsl.join-sql-not-parsable", Set.of("detail", "path")),
    /** A join's {@code sql:} parses but uses a construct this release does not run. {@code shape}
     *  names it as SQL spells it ({@code FULL OUTER JOIN}, {@code GROUP BY}, {@code COUNT}), and
     *  {@code line} / {@code column} locate it within the SQL text itself, not within the YAML. */
    JOIN_SQL_UNSUPPORTED(
            "dsl.join-sql-unsupported", Set.of("shape", "line", "column", "path"));

    private final String code;
    private final Set<String> placeholders;

    DslError(String code, Set<String> placeholders) {
        this.code = code;
        this.placeholders = placeholders;
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

    /** The corpus rule-vocabulary key (the canonical code without the {@code dsl.} domain prefix). */
    public String symbol() {
        return code.substring(code.indexOf('.') + 1);
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
