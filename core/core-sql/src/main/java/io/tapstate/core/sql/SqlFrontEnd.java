package io.tapstate.core.sql;

import io.tapstate.core.common.TapstateType;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;

import java.util.ArrayList;
import java.util.List;

/**
 * Turns a join declaration's SQL into a carrier-agnostic plan: parse, validate against the source
 * columns, derive the output row's names and types. It never executes anything and never reaches a
 * database -- everything it answers, it answers from the text and the schema it was handed.
 *
 * <p>Rejecting a shape this product does not support is deliberately NOT done here. This module
 * reports what it derived; deciding that a shape is out of bounds, and saying so with a diagnostic
 * a person can act on, belongs to the validation layer that calls this one. Splitting it that way
 * is what keeps the dependency pointing one direction, and it is why the failure below carries no
 * error code: the layer that owns the user-facing vocabulary assigns one.
 */
public final class SqlFrontEnd {

    /**
     * PROVISIONAL: the lexical policy -- how identifiers are quoted and whether they match
     * case-sensitively -- is surface syntax a person writes, so it is a product decision and not
     * this module's to settle. What is here is the policy every measurement behind the engine
     * choice was taken under, so it is the continuation rather than a fresh guess.
     */
    private static final SqlParser.Config PARSER =
            SqlParser.config().withLex(Lex.JAVA).withCaseSensitive(false);

    /**
     * @param sql    the join declaration, exactly as written
     * @param tables the tables the SQL may name, with the columns it may select
     * @return the derived plan
     * @throws SqlFrontEndException if the text does not parse, or does not validate against these
     *                              tables
     */
    public static JoinPlan derive(String sql, List<SourceTable> tables) {
        CalciteSchema root = CalciteSchema.createRootSchema(false);
        for (SourceTable t : tables) {
            root.add(t.name(), asTable(t));
        }
        try (Planner planner = Frameworks.getPlanner(Frameworks.newConfigBuilder()
                .defaultSchema(root.plus())
                .parserConfig(PARSER)
                .build())) {
            SqlNode parsed = planner.parse(sql);
            RelDataType row = planner.validateAndGetType(parsed).right;
            List<OutputField> fields = new ArrayList<>(row.getFieldCount());
            for (RelDataTypeField f : row.getFieldList()) {
                fields.add(new OutputField(f.getName(),
                        toTapstate(f.getType().getSqlTypeName()),
                        f.getType().isNullable()));
            }
            return new JoinPlan(List.copyOf(fields));
        } catch (Exception e) {
            throw new SqlFrontEndException(e.getMessage(), e);
        }
    }

    private static AbstractTable asTable(SourceTable table) {
        return new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory factory) {
                RelDataTypeFactory.Builder builder = factory.builder();
                for (SourceColumn c : table.columns()) {
                    builder.add(c.name(), factory.createTypeWithNullability(
                            factory.createSqlType(fromTapstate(c.type())), c.nullable()));
                }
                return builder.build();
            }
        };
    }

    /**
     * The shared vocabulary is not a subset of SQL's: a year, a JSON document and the two
     * container types have no SQL type that means the same thing. They are handed to the validator
     * as the unconstrained type rather than as a near-miss, so a column of one is carried through
     * and projected but is not type-checked against the operators applied to it. Narrowing that is
     * a decision about which SQL a person may write over such a column, not a mapping detail.
     */
    private static SqlTypeName fromTapstate(TapstateType type) {
        return switch (type) {
            case STRING -> SqlTypeName.VARCHAR;
            case DECIMAL -> SqlTypeName.DECIMAL;
            case INT64 -> SqlTypeName.BIGINT;
            case DOUBLE -> SqlTypeName.DOUBLE;
            case BOOLEAN -> SqlTypeName.BOOLEAN;
            case DATE -> SqlTypeName.DATE;
            case TIME -> SqlTypeName.TIME;
            case DATETIME -> SqlTypeName.TIMESTAMP;
            case BINARY -> SqlTypeName.VARBINARY;
            case YEAR, JSON, ARRAY, MAP, UNKNOWN -> SqlTypeName.ANY;
        };
    }

    /**
     * Anything the shared vocabulary has no member for comes back as {@code UNKNOWN} rather than
     * as the nearest thing. That is a named outcome the caller has to rule on; a near-miss would
     * be carried onward as if it were the truth.
     */
    private static TapstateType toTapstate(SqlTypeName type) {
        return switch (type) {
            case BIGINT, INTEGER, SMALLINT, TINYINT -> TapstateType.INT64;
            case DECIMAL -> TapstateType.DECIMAL;
            case FLOAT, REAL, DOUBLE -> TapstateType.DOUBLE;
            case CHAR, VARCHAR -> TapstateType.STRING;
            case BOOLEAN -> TapstateType.BOOLEAN;
            case DATE -> TapstateType.DATE;
            case TIME, TIME_WITH_LOCAL_TIME_ZONE -> TapstateType.TIME;
            case TIMESTAMP, TIMESTAMP_WITH_LOCAL_TIME_ZONE -> TapstateType.DATETIME;
            case BINARY, VARBINARY -> TapstateType.BINARY;
            case ARRAY -> TapstateType.ARRAY;
            case MAP -> TapstateType.MAP;
            default -> TapstateType.UNKNOWN;
        };
    }

    private SqlFrontEnd() {
    }
}
