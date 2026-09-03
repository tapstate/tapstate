package io.tapstate.core.sql;

import io.tapstate.core.common.TapstateType;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Turns a join declaration's SQL into a carrier-agnostic plan: parse, validate against the source
 * columns, derive the output row's names and types and the shape the rows are read from. It never
 * executes anything and never reaches a database -- everything it answers, it answers from the
 * text and the schema it was handed.
 *
 * <p>Rejecting a shape this product does not support is deliberately NOT done here. This module
 * reports what it derived; deciding that a shape is out of bounds, and saying so with a diagnostic
 * a person can act on, belongs to the validation layer that calls this one. Splitting it that way
 * is what keeps the dependency pointing one direction, and it is why the failure below carries no
 * error code: the layer that owns the user-facing vocabulary assigns one.
 *
 * <p>{@link #unsupported} is the same split seen from the other end: it names the constructs the
 * derived plan has no way to state, and leaves refusing them to the caller. It deliberately does
 * not run inside {@link #derive} -- a full outer join and an uncaptured condition are things the
 * plan is built to <em>report</em>, and a derive that threw on them would destroy the only signal
 * the layer above has.
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
     * Every standard aggregate's name, read off the SQL library's own operator table rather than
     * listed here by hand. The parser cannot help: it reports {@code COUNT} and {@code SUM} as
     * unresolved functions of the same kind it reports {@code SUBSTRING} as, so a scan of the
     * statement's shape alone cannot tell an aggregate from an ordinary per-row call.
     */
    private static final Set<String> AGGREGATES = SqlStdOperatorTable.instance().getOperatorList()
            .stream()
            .filter(operator -> operator instanceof SqlAggFunction)
            .map(operator -> operator.getName().toUpperCase(Locale.ROOT))
            .collect(Collectors.toUnmodifiableSet());

    /** A join condition the key list cannot hold, however it is spelled. */
    private static final String NOT_AN_EQUALITY =
            "a join condition that is not an equality between two columns";

    /**
     * The first construct in this SQL that a join declaration may not be written with, if any.
     *
     * <p>Reads the text alone -- no schema, no table names, no connection -- so the offline gate
     * can run it long before anything knows what columns exist. The subset it admits is one
     * {@code SELECT} over tables combined by equality conditions: exactly one output row per
     * matched combination of input rows, and nothing that adds, removes, reorders or collapses
     * rows. Everything outside that is refused rather than ignored, because a plan cannot state
     * it and a carrier handed such a plan does not fail -- it publishes rows that read as
     * ordinary output.
     *
     * @param sql the join declaration, exactly as written
     * @return the construct, or empty when the whole statement is inside the subset
     * @throws SqlFrontEndException if the text is not SQL at all. Not the same answer as a shape
     *                              this release refuses, and not one to fold together: they send
     *                              a reader to opposite places
     */
    public static Optional<Unsupported> unsupported(String sql) {
        SqlNode statement;
        try {
            statement = SqlParser.create(sql, PARSER).parseStmt();
        } catch (SqlParseException e) {
            throw new SqlFrontEndException(e.getMessage(), e);
        }
        return Optional.ofNullable(outOfBounds(statement));
    }

    private static Unsupported outOfBounds(SqlNode statement) {
        if (statement instanceof SqlOrderBy ordered) {
            // all three arrive wrapped the same way, and all three mean the published row set is
            // not simply "the matches"
            if (!ordered.orderList.isEmpty()) {
                return at("ORDER BY", ordered);
            }
            return ordered.fetch != null ? at("LIMIT", ordered) : at("OFFSET", ordered);
        }
        if (!(statement instanceof SqlSelect select)) {
            return at(nameOf(statement), statement);
        }
        Unsupported nested = firstNestedQuery(select);
        if (nested != null) {
            return nested;
        }
        if (select.getFrom() == null) {
            return at("SELECT without FROM", select);
        }
        if (select.isDistinct()) {
            return at("DISTINCT", select);
        }
        if (select.getWhere() != null) {
            return at("WHERE", select.getWhere());
        }
        if (select.getGroup() != null) {
            return at("GROUP BY", select.getGroup());
        }
        if (select.getHaving() != null) {
            return at("HAVING", select.getHaving());
        }
        SqlNodeList windows = select.getWindowList();
        if (windows != null && !windows.isEmpty()) {
            return at("WINDOW", windows);
        }
        Unsupported inFrom = fromItem(select.getFrom());
        return inFrom != null ? inFrom : firstNonRowCall(select);
    }

    /** Each FROM item must be a table, an aliased table, or a join of those. */
    private static Unsupported fromItem(SqlNode node) {
        if (node instanceof SqlJoin join) {
            Unsupported refused = joinShape(join);
            if (refused != null) {
                return refused;
            }
            Unsupported left = fromItem(join.getLeft());
            return left != null ? left : fromItem(join.getRight());
        }
        if (node instanceof SqlIdentifier) {
            return null;
        }
        if (node instanceof SqlBasicCall call && call.getOperator().getKind() == SqlKind.AS
                && call.operand(0) instanceof SqlIdentifier) {
            return null;
        }
        return at(nameOf(node), node);
    }

    private static Unsupported joinShape(SqlJoin join) {
        // FULL is reported by kindOf rather than rejected there, so that this is the one place it
        // is refused and the refusal can say which words in the SQL it is about
        if (join.getJoinType() == JoinType.FULL) {
            return at("FULL OUTER JOIN", join);
        }
        if (join.getJoinType() == JoinType.LEFT_SEMI_JOIN) {
            return at("LEFT SEMI JOIN", join);
        }
        if (join.isNatural()) {
            return at("NATURAL JOIN", join);
        }
        return switch (join.getConditionType()) {
            case USING -> at("USING", join);
            // a cross join and the comma form are the same statement, and the one shape that
            // multiplies every row by every other row
            case NONE -> at("CROSS JOIN", join);
            case ON -> equalitiesOnly(join.getCondition());
        };
    }

    /** Null when the whole condition is a conjunction of equalities between two columns. */
    private static Unsupported equalitiesOnly(SqlNode condition) {
        if (condition instanceof SqlBasicCall call) {
            SqlKind kind = call.getOperator().getKind();
            if (kind == SqlKind.AND) {
                for (SqlNode operand : call.getOperandList()) {
                    Unsupported refused = equalitiesOnly(operand);
                    if (refused != null) {
                        return refused;
                    }
                }
                return null;
            }
            if (kind == SqlKind.EQUALS && qualified(call.operand(0)) && qualified(call.operand(1))) {
                return null;
            }
        }
        return at(NOT_AN_EQUALITY, condition);
    }

    private static boolean qualified(SqlNode node) {
        if (!(node instanceof SqlIdentifier identifier)) {
            return false;
        }
        // read through the plain interface, as columnIn does: the field's own type belongs to a
        // collections library this module is not granted
        List<String> names = identifier.names;
        return names.size() >= 2;
    }

    /** A query nested anywhere below the statement: a derived table, an IN list, a scalar read. */
    private static Unsupported firstNestedQuery(SqlSelect select) {
        SqlNode[] found = new SqlNode[1];
        select.accept(new SqlBasicVisitor<Void>() {
            @Override
            public Void visit(SqlCall call) {
                if (found[0] == null && call != select && call instanceof SqlSelect) {
                    found[0] = call;
                }
                return super.visit(call);
            }
        });
        return found[0] == null ? null : at("a subquery", found[0]);
    }

    /** A call that does not answer per row: a window, or an aggregate with nothing to give it away. */
    private static Unsupported firstNonRowCall(SqlSelect select) {
        Unsupported[] found = new Unsupported[1];
        select.accept(new SqlBasicVisitor<Void>() {
            @Override
            public Void visit(SqlCall call) {
                if (found[0] == null) {
                    String name = call.getOperator().getName().toUpperCase(Locale.ROOT);
                    if (call.getOperator().getKind() == SqlKind.OVER) {
                        found[0] = at("OVER", call);
                    } else if (AGGREGATES.contains(name)) {
                        found[0] = at(name, call);
                    }
                }
                return super.visit(call);
            }
        });
        return found[0];
    }

    private static String nameOf(SqlNode node) {
        return node instanceof SqlCall call
                ? call.getOperator().getName() : node.getKind().toString();
    }

    private static Unsupported at(String shape, SqlNode node) {
        SqlParserPos pos = node.getParserPosition();
        return new Unsupported(shape, pos.getLineNum(), pos.getColumnNum());
    }

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
            var validated = planner.validateAndGetType(parsed);
            RelDataType row = validated.right;
            List<OutputField> fields = new ArrayList<>(row.getFieldCount());
            for (RelDataTypeField f : row.getFieldList()) {
                fields.add(new OutputField(f.getName(),
                        toTapstate(f.getType().getSqlTypeName()),
                        f.getType().isNullable()));
            }
            SqlSelect select = selectOf(validated.left);
            JoinTree from = tree(select.getFrom());
            return new JoinPlan(List.copyOf(fields), from, readColumns(select, from));
        } catch (SqlFrontEndException e) {
            throw e;
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

    private static SqlSelect selectOf(SqlNode validated) {
        SqlNode statement = validated instanceof SqlOrderBy ordered ? ordered.query : validated;
        if (statement instanceof SqlSelect select && select.getFrom() != null) {
            return select;
        }
        throw new SqlFrontEndException("the statement does not read from a from clause", null);
    }

    /**
     * Every column each source is read for, wherever the SQL names it -- projected, joined on,
     * filtered, grouped or ordered by.
     *
     * <p>Collecting only the projected columns is the tempting shortcut and it is wrong in a way
     * that does not announce itself: a join key is routinely never selected, so a carrier that
     * mirrored the output row would hold rows it cannot match on, and would emit rows that are
     * merely missing their matches. Sorted, so the same SQL always states the same plan.
     */
    private static Map<String, List<String>> readColumns(SqlSelect select, JoinTree from) {
        Map<String, String> spellings = spellings(from);
        Map<String, Set<String>> found = new LinkedHashMap<>();
        for (JoinTree.Source source : from.sources()) {
            found.put(source.name(), new TreeSet<>());
        }
        select.accept(new SqlBasicVisitor<Void>() {
            @Override
            public Void visit(SqlIdentifier identifier) {
                JoinTree.ColumnRef ref = columnIn(identifier, spellings);
                if (ref != null) {
                    found.get(ref.source()).add(ref.column());
                }
                return null;
            }
        });
        Map<String, List<String>> columns = new LinkedHashMap<>();
        found.forEach((source, names) -> columns.put(source, List.copyOf(names)));
        return Collections.unmodifiableMap(columns);
    }

    /**
     * The tree the rows are read from.
     *
     * <p>A right outer join is turned into a left outer join over swapped sides here, and nowhere
     * else. There is no right-outer execution operator, so leaving the rewrite to each carrier
     * would mean each one gets a chance to do it slightly differently -- and getting it wrong
     * produces rows that look exactly like correct output. Only the plan changes: the output row a
     * person asked for keeps its field order and its nullability, both of which come from the
     * validated type of the SQL as written.
     */
    private static JoinTree tree(SqlNode node) {
        if (node instanceof SqlJoin join) {
            JoinTree left = tree(join.getLeft());
            JoinTree right = tree(join.getRight());
            List<JoinTree.KeyPair> keys = new ArrayList<>();
            boolean captured = switch (join.getConditionType()) {
                // a natural join matches on columns nobody wrote down, so an empty key list is not
                // the same statement here as it is for a cross join, and cannot say so itself
                case NONE -> !join.isNatural();
                case ON -> equalities(join.getCondition(), spellings(left), spellings(right), keys);
                case USING -> false;
            };
            boolean swap = join.getJoinType() == JoinType.RIGHT;
            if (swap) {
                keys.replaceAll(pair -> new JoinTree.KeyPair(pair.right(), pair.left()));
            }
            return new JoinTree.Join(swap ? right : left, swap ? left : right,
                    kindOf(join.getJoinType()), List.copyOf(keys), !captured);
        }
        if (node instanceof SqlBasicCall call && call.getOperator().getKind() == SqlKind.AS) {
            return new JoinTree.Source(lastName(call.operand(1)), lastName(call.operand(0)));
        }
        if (node instanceof SqlIdentifier identifier) {
            String name = lastName(identifier);
            return new JoinTree.Source(name, name);
        }
        throw new SqlFrontEndException(
                "the from clause holds a form this front end does not read: " + node.getKind(), null);
    }

    /**
     * Splits a join condition into the equalities between the two sides.
     *
     * @return whether the whole condition was captured. False means something in it is not
     *         represented by the key pairs -- and executing the plan without it emits rows that
     *         should not exist, which is why it is reported rather than quietly left behind.
     */
    private static boolean equalities(SqlNode condition, Map<String, String> left,
                                      Map<String, String> right, List<JoinTree.KeyPair> into) {
        if (condition instanceof SqlBasicCall call) {
            SqlKind kind = call.getOperator().getKind();
            if (kind == SqlKind.AND) {
                boolean captured = true;
                for (SqlNode operand : call.getOperandList()) {
                    captured &= equalities(operand, left, right, into);
                }
                return captured;
            }
            if (kind == SqlKind.EQUALS) {
                JoinTree.KeyPair pair = keyPair(call.operand(0), call.operand(1), left, right);
                if (pair != null) {
                    into.add(pair);
                    return true;
                }
            }
        }
        return false;
    }

    /** One equality, oriented so that its first column belongs to the left side. */
    private static JoinTree.KeyPair keyPair(SqlNode first, SqlNode second,
                                            Map<String, String> left, Map<String, String> right) {
        JoinTree.ColumnRef asWritten = columnIn(first, left);
        JoinTree.ColumnRef otherSide = columnIn(second, right);
        if (asWritten != null && otherSide != null) {
            return new JoinTree.KeyPair(asWritten, otherSide);
        }
        JoinTree.ColumnRef reversed = columnIn(second, left);
        JoinTree.ColumnRef reversedOther = columnIn(first, right);
        return reversed != null && reversedOther != null
                ? new JoinTree.KeyPair(reversed, reversedOther) : null;
    }

    /** The column this node names, if it names one of these sources; null otherwise. */
    private static JoinTree.ColumnRef columnIn(SqlNode node, Map<String, String> spellings) {
        if (!(node instanceof SqlIdentifier identifier)) {
            return null;
        }
        // read through the plain interface: the field's own type is the SQL library's collection
        // library, and calling a method on it directly would widen this module's dependency grant
        // to a collections framework nothing here needs
        List<String> names = identifier.names;
        if (names.size() < 2) {
            return null;
        }
        String source = spellings.get(names.get(names.size() - 2));
        return source == null ? null : new JoinTree.ColumnRef(source, names.get(names.size() - 1));
    }

    /**
     * Every spelling a validated column reference may carry for a source, mapped onto the one name
     * the plan calls that source by. Validation qualifies a column with the name in scope, which is
     * the alias where a source has one, so both spellings are accepted and the alias wins.
     */
    private static Map<String, String> spellings(JoinTree tree) {
        Map<String, String> map = new HashMap<>();
        for (JoinTree.Source source : tree.sources()) {
            map.put(source.table(), source.name());
            map.put(source.name(), source.name());
        }
        return map;
    }

    private static String lastName(SqlNode node) {
        List<String> names = ((SqlIdentifier) node).names;
        return names.get(names.size() - 1);
    }

    private static JoinKind kindOf(JoinType type) {
        return switch (type) {
            // right is reported as left because the caller has already swapped the two sides, which
            // is what makes the two the same rows
            case LEFT, RIGHT -> JoinKind.LEFT;
            case FULL -> JoinKind.FULL;
            case INNER, COMMA, CROSS -> JoinKind.INNER;
            // a semi join keeps one row per matching left row and none of the right side's columns.
            // No kind a plan can state means that, and the nearest one -- inner -- emits a row per
            // match instead of per left row, so mapping it there would silently multiply rows.
            case LEFT_SEMI_JOIN -> throw new SqlFrontEndException(
                    "the from clause holds a join kind this front end does not read: " + type, null);
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
