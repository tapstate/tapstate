package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/** Product definitions shared by the parser and the independent presentation layer. */
public final class DslErrorDefinitions {
    public static final ProductErrorDefinition UNSUPPORTED_VERSION = error(1, "got", "supported");
    public static final ProductErrorDefinition UNKNOWN_FIELD = error(2, "field", "path");
    public static final ProductErrorDefinition FORBIDDEN_FIELD = error(3, "field", "path");
    public static final ProductErrorDefinition MISSING_FIELD = error(4, "field", "path");
    public static final ProductErrorDefinition MISSING_REFERENCE = error(5, "ref", "path");
    public static final ProductErrorDefinition AMBIGUOUS_REFERENCE = error(6, "ref", "path");
    public static final ProductErrorDefinition MODE_MISMATCH = error(7, "field", "mode", "path");
    public static final ProductErrorDefinition MODE_REQUIRED_FOR_READ = error(8, "source", "pipeline", "path");
    public static final ProductErrorDefinition ILLEGAL_VALUE = error(9, "value", "expected", "path");
    public static final ProductErrorDefinition ILLEGAL_EXPRESSION = error(10, "expr", "detail", "path");
    public static final ProductErrorDefinition COMPOSITION = error(11, "detail", "path");
    public static final ProductErrorDefinition DUPLICATE_ID = error(12, "id", "path");
    public static final ProductErrorDefinition UNSUPPORTED_MODE = error(13, "connector", "mode", "allowed", "path");
    public static final ProductErrorDefinition CONFIG_TYPE_MISMATCH = error(14, "connector", "field", "expected", "path");
    public static final ProductErrorDefinition CONFIG_REQUIRED = error(15, "connector", "field", "path");
    public static final ProductErrorDefinition INVALID_CONFIG_VALUE = error(16, "connector", "field", "value", "allowed", "path");
    public static final ProductErrorDefinition MALFORMED_YAML = error(17, "detail");
    public static final ProductErrorDefinition UNDEFINED_VARIABLE = error(18, "name");
    public static final ProductErrorDefinition MALFORMED_INTERPOLATION = error(19, "ref");
    public static final ProductErrorDefinition ROW_EXPRESSION_NEEDS_DISCOVERY = error(20, "expr", "source", "path");
    public static final ProductErrorDefinition ROW_EXPRESSION_TYPE_UNSUPPORTED = error(21, "expr", "column", "type", "table", "path");
    public static final ProductErrorDefinition ROW_EXPRESSION_TYPE_UNKNOWN = error(22, "expr", "column", "table", "path");
    public static final ProductErrorDefinition UPSERT_NEEDS_KEY = error(23, "table", "source", "path");
    public static final ProductErrorDefinition UNWIND_NEEDS_AN_ELEMENT_KEY = error(24, "step", "path");
    public static final ProductErrorDefinition UNWIND_NEEDS_AN_UPSERT_TARGET = error(25, "step", "sync", "path");
    public static final ProductErrorDefinition UNSUPPORTED_TARGET_CONNECTOR = error(26, "connector", "source", "resource", "supported", "path");
    public static final ProductErrorDefinition UNWIND_COLUMN_ALREADY_EXISTS = error(27, "column", "option");
    public static final ProductErrorDefinition JOIN_SQL_NOT_PARSABLE = error(28, "detail", "path");
    public static final ProductErrorDefinition JOIN_SQL_UNSUPPORTED = error(29, "shape", "line", "column", "path");
    public static final ProductErrorDefinition JOIN_INPUT_NOT_A_TABLE = error(30, "step", "alias", "ref", "path");
    public static final ProductErrorDefinition JOIN_INPUT_IS_A_PATTERN = error(31, "step", "alias", "pattern", "path");
    public static final ProductErrorDefinition SETTING_HAS_NO_EFFECT =
            new ProductErrorDefinition(ProductErrorModule.DSL, 32, Severity.WARNING, Set.of("pipeline", "setting", "instead"));
    public static final ProductErrorDefinition SOURCE_READ_NOT_SPLIT =
            new ProductErrorDefinition(ProductErrorModule.DSL, 33, Severity.WARNING, Set.of("source", "requested"));

    private DslErrorDefinitions() {}

    private static ProductErrorDefinition error(int serial, String... parameters) {
        return new ProductErrorDefinition(ProductErrorModule.DSL, serial, Severity.ERROR, Set.of(parameters));
    }

    public static List<ProductErrorDefinition> registered() {
        return List.of(UNSUPPORTED_VERSION, UNKNOWN_FIELD, FORBIDDEN_FIELD, MISSING_FIELD,
                MISSING_REFERENCE, AMBIGUOUS_REFERENCE, MODE_MISMATCH, MODE_REQUIRED_FOR_READ,
                ILLEGAL_VALUE, ILLEGAL_EXPRESSION, COMPOSITION, DUPLICATE_ID, UNSUPPORTED_MODE,
                CONFIG_TYPE_MISMATCH, CONFIG_REQUIRED, INVALID_CONFIG_VALUE, MALFORMED_YAML,
                UNDEFINED_VARIABLE, MALFORMED_INTERPOLATION, ROW_EXPRESSION_NEEDS_DISCOVERY,
                ROW_EXPRESSION_TYPE_UNSUPPORTED, ROW_EXPRESSION_TYPE_UNKNOWN, UPSERT_NEEDS_KEY,
                UNWIND_NEEDS_AN_ELEMENT_KEY, UNWIND_NEEDS_AN_UPSERT_TARGET, UNSUPPORTED_TARGET_CONNECTOR,
                UNWIND_COLUMN_ALREADY_EXISTS, JOIN_SQL_NOT_PARSABLE, JOIN_SQL_UNSUPPORTED,
                JOIN_INPUT_NOT_A_TABLE, JOIN_INPUT_IS_A_PATTERN, SETTING_HAS_NO_EFFECT, SOURCE_READ_NOT_SPLIT);
    }
}
