package io.tapstate.core.sql;

import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlShuttle;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * A left outer join whose condition is an {@code OR} of equality groups, said as something the
 * equality-keyed plan can state.
 *
 * <p><b>What it means.</b> {@code LEFT JOIN location l ON t.code = l.code OR t.hub = l.hub} is read
 * as an ordered list of alternatives: each fact row takes its columns of {@code l} from the first
 * alternative that has a matching row, and is published with nulls there when none has. It never
 * publishes one row per matching alternative. That is what the statement is written for - a fact
 * row naming its dimension row by one column or by another - and it keeps the join many-to-one, so
 * the fact row stays the unit a sink writes.
 *
 * <p><b>How it is stated.</b> Each alternative becomes a left outer join of its own onto the same
 * table, under a name of its own ({@code l}, {@code l$or2}, ...), matched on that alternative's
 * equalities alone. Every reference to {@code l} in the select list becomes a {@code CASE} that
 * reads the first of those joins whose matched row is present - present being told by the first
 * column it was matched on, which an equality never matches when null. Each added join is a plain
 * equality join, so the carriers keep it under change exactly as they keep any other: a row of
 * either table moving under one alternative's key moves only that alternative's match, and the
 * {@code CASE} is worked out again over whichever matches remain.
 *
 * <p><b>What it admits.</b> A {@code LEFT JOIN} onto a single table, each alternative a conjunction
 * of equalities between that table and the tables to its left. The OR-joined table may only be read
 * through qualified columns of the select list: a star would publish every alternative's copy, and a
 * later join matching on it would have to say which copy it means. Both are refused while the SQL is
 * still text, by {@link #refused}.
 */
final class OrJoins {

    /** What separates a source's added alternatives from the name the SQL gave it. */
    static final String SEPARATOR = "$or";

    private OrJoins() {
    }

    /** Whether this join condition holds an {@code OR} at its top. */
    static boolean isDisjunction(SqlNode condition) {
        return condition instanceof SqlBasicCall call && call.getOperator().getKind() == SqlKind.OR;
    }

    /** The condition's alternatives, nested {@code OR}s flattened, in the order they are written. */
    static List<SqlNode> alternatives(SqlNode condition) {
        List<SqlNode> into = new ArrayList<>();
        collect(condition, into);
        return into;
    }

    private static void collect(SqlNode condition, List<SqlNode> into) {
        if (isDisjunction(condition)) {
            for (SqlNode operand : ((SqlBasicCall) condition).getOperandList()) {
                collect(operand, into);
            }
        } else {
            into.add(condition);
        }
    }

    /** The name the right side of this join goes by, or null when it is not a single table. */
    static String tableName(SqlNode right) {
        if (right instanceof SqlIdentifier identifier) {
            return last(identifier);
        }
        if (right instanceof SqlBasicCall call && call.getOperator().getKind() == SqlKind.AS
                && call.operand(0) instanceof SqlIdentifier) {
            return last((SqlIdentifier) call.operand(1));
        }
        return null;
    }

    /**
     * The shape in a join with an {@code OR} condition that cannot be stated, or null where the join
     * is admitted. {@code equalities} is the caller's own check that a condition is a conjunction of
     * column equalities, so the two admit the same equalities.
     */
    static Unsupported joinShape(SqlJoin join, java.util.function.Function<SqlNode, Unsupported> equalities) {
        SqlNode condition = join.getCondition();
        if (join.getJoinType() != JoinType.LEFT) {
            return at("OR in a join condition other than LEFT JOIN", condition);
        }
        String table = tableName(join.getRight());
        if (table == null) {
            return at("OR in a join condition whose right side is not a table", condition);
        }
        for (SqlNode alternative : alternatives(condition)) {
            Unsupported refused = equalities.apply(alternative);
            if (refused != null) {
                return refused;
            }
            if (firstKey(alternative, table) == null) {
                return at("an OR alternative that does not match on the joined table", alternative);
            }
        }
        return null;
    }

    /**
     * The first reference to a table joined on {@code OR} that cannot be read as one of its
     * alternatives: a star, or another join's condition naming it. Null when there is none.
     */
    static Unsupported refused(SqlSelect select) {
        Set<String> joinedOnOr = new HashSet<>();
        List<SqlJoin> joins = new ArrayList<>();
        joinsIn(select.getFrom(), joins);
        for (SqlJoin join : joins) {
            if (join.getCondition() != null && isDisjunction(join.getCondition())) {
                String table = tableName(join.getRight());
                if (table != null) {
                    joinedOnOr.add(table.toLowerCase(Locale.ROOT));
                }
            }
        }
        if (joinedOnOr.isEmpty()) {
            return null;
        }
        for (SqlNode item : select.getSelectList()) {
            SqlNode value = item instanceof SqlBasicCall call && call.getOperator().getKind() == SqlKind.AS
                    ? call.operand(0) : item;
            if (value instanceof SqlIdentifier identifier && identifier.isStar()) {
                List<String> names = identifier.names;
                if (names.size() == 1 || joinedOnOr.contains(names.get(names.size() - 2).toLowerCase(Locale.ROOT))) {
                    return at("a star over a table joined on OR", identifier);
                }
            }
        }
        for (SqlJoin join : joins) {
            SqlNode condition = join.getCondition();
            if (condition == null || isDisjunction(condition)) {
                continue;
            }
            SqlNode[] found = new SqlNode[1];
            condition.accept(new SqlBasicVisitor<Void>() {
                @Override
                public Void visit(SqlIdentifier identifier) {
                    List<String> names = identifier.names;
                    if (found[0] == null && names.size() >= 2
                            && joinedOnOr.contains(names.get(names.size() - 2).toLowerCase(Locale.ROOT))) {
                        found[0] = identifier;
                    }
                    return null;
                }
            });
            if (found[0] != null) {
                return at("a join condition on a table joined on OR", found[0]);
            }
        }
        return null;
    }

    /**
     * Rewrites every admitted {@code OR} join in this statement into one join per alternative, and
     * every select-list reference to its table into the reading of the first alternative that matched.
     * A join of any other shape is left as written, for the plan to report.
     *
     * @return the added sources, each mapped to the name the SQL gave the table it stands in for
     */
    static Map<String, String> rewrite(SqlSelect select) {
        Set<String> taken = new HashSet<>();
        namesIn(select.getFrom(), taken);
        Map<String, List<Alternative>> byTable = new LinkedHashMap<>();
        select.setFrom(rewriteFrom(select.getFrom(), taken, byTable));
        if (byTable.isEmpty()) {
            return Map.of();
        }
        Map<String, String> added = new LinkedHashMap<>();
        byTable.forEach((table, alternatives) -> alternatives.stream().skip(1)
                .forEach(alternative -> added.put(alternative.name(), table)));

        List<SqlNode> items = new ArrayList<>();
        for (SqlNode item : select.getSelectList()) {
            if (item instanceof SqlBasicCall call && call.getOperator().getKind() == SqlKind.AS) {
                items.add(SqlStdOperatorTable.AS.createCall(call.getParserPosition(),
                        readFirstMatch(call.operand(0), byTable), call.operand(1)));
            } else if (item instanceof SqlIdentifier identifier && !identifier.isStar()
                    && byTable.containsKey(qualifier(identifier))) {
                // the column keeps the name it would have had, which a CASE alone would lose
                items.add(SqlStdOperatorTable.AS.createCall(identifier.getParserPosition(),
                        readFirstMatch(identifier, byTable),
                        new SqlIdentifier(last(identifier), identifier.getParserPosition())));
            } else {
                items.add(readFirstMatch(item, byTable));
            }
        }
        select.setSelectList(new SqlNodeList(items, select.getSelectList().getParserPosition()));
        return added;
    }

    /** One alternative's join: the name its rows go by, and the column whose presence says it matched. */
    private record Alternative(String name, String presentWhen) {
    }

    private static SqlNode rewriteFrom(SqlNode node, Set<String> taken, Map<String, List<Alternative>> byTable) {
        if (!(node instanceof SqlJoin join)) {
            return node;
        }
        join.setLeft(rewriteFrom(join.getLeft(), taken, byTable));
        SqlNode condition = join.getCondition();
        String table = tableName(join.getRight());
        if (condition == null || !isDisjunction(condition) || join.getJoinType() != JoinType.LEFT
                || table == null) {
            return join;
        }
        List<SqlNode> written = alternatives(condition);
        List<Alternative> alternatives = new ArrayList<>();
        SqlNode joined = join.getLeft();
        for (int i = 0; i < written.size(); i++) {
            SqlNode alternative = written.get(i);
            String presentWhen = firstKey(alternative, table);
            if (presentWhen == null) {
                // not an admitted shape: leave the statement as written, so the plan reports it
                return join;
            }
            String name = i == 0 ? table : table + SEPARATOR + (i + 1);
            if (i > 0 && !taken.add(name.toLowerCase(Locale.ROOT))) {
                throw new SqlFrontEndException("the from clause already names a source " + name
                        + ", which is the name an OR alternative of " + table + " is joined under", null);
            }
            alternatives.add(new Alternative(name, presentWhen));
            SqlParserPos pos = alternative.getParserPosition();
            SqlNode right = i == 0 ? join.getRight() : SqlStdOperatorTable.AS.createCall(pos,
                    tableIdentifier(join.getRight()), new SqlIdentifier(name, pos));
            joined = new SqlJoin(pos, joined, SqlLiteral.createBoolean(false, pos),
                    JoinType.LEFT.symbol(pos), right, JoinConditionType.ON.symbol(pos),
                    i == 0 ? alternative : renamed(alternative, table, name));
        }
        byTable.put(table.toLowerCase(Locale.ROOT), alternatives);
        return joined;
    }

    /**
     * This expression with each column of an OR-joined table read from the first alternative that
     * matched: {@code CASE WHEN l.code IS NOT NULL THEN l.name WHEN l$or2.hub IS NOT NULL THEN
     * l$or2.name END}. The last alternative is the {@code ELSE}, which reads null where it did not match
     * either, so the {@code CASE} needs no test for it.
     */
    private static SqlNode readFirstMatch(SqlNode node, Map<String, List<Alternative>> byTable) {
        return node.accept(new SqlShuttle() {
            @Override
            public SqlNode visit(SqlIdentifier identifier) {
                List<Alternative> alternatives = byTable.get(qualifier(identifier));
                if (alternatives == null || identifier.isStar()) {
                    return identifier;
                }
                SqlParserPos pos = identifier.getParserPosition();
                String column = last(identifier);
                List<SqlNode> whens = new ArrayList<>();
                List<SqlNode> thens = new ArrayList<>();
                for (Alternative alternative : alternatives.subList(0, alternatives.size() - 1)) {
                    whens.add(SqlStdOperatorTable.IS_NOT_NULL.createCall(pos,
                            new SqlIdentifier(List.of(alternative.name(), alternative.presentWhen()), pos)));
                    thens.add(new SqlIdentifier(List.of(alternative.name(), column), pos));
                }
                Alternative last = alternatives.get(alternatives.size() - 1);
                return new SqlCase(pos, null, new SqlNodeList(whens, pos), new SqlNodeList(thens, pos),
                        new SqlIdentifier(List.of(last.name(), column), pos));
            }
        });
    }

    /** The condition with every column of {@code table} read from {@code name} instead. */
    private static SqlNode renamed(SqlNode condition, String table, String name) {
        return condition.accept(new SqlShuttle() {
            @Override
            public SqlNode visit(SqlIdentifier identifier) {
                if (table.equalsIgnoreCase(qualifierAsWritten(identifier))) {
                    return new SqlIdentifier(List.of(name, last(identifier)), identifier.getParserPosition());
                }
                return identifier;
            }
        });
    }

    /**
     * The column of {@code table} that the first equality of this alternative matches on, or null
     * where none of them names {@code table} - an alternative that does not, does not join it at all.
     */
    private static String firstKey(SqlNode alternative, String table) {
        List<SqlNode> equalities = new ArrayList<>();
        conjuncts(alternative, equalities);
        for (SqlNode equality : equalities) {
            if (equality instanceof SqlBasicCall call && call.getOperator().getKind() == SqlKind.EQUALS) {
                String first = columnOf(call.operand(0), table);
                String second = columnOf(call.operand(1), table);
                if ((first == null) != (second == null)) {
                    return first != null ? first : second;
                }
            }
        }
        return null;
    }

    private static void conjuncts(SqlNode condition, List<SqlNode> into) {
        if (condition instanceof SqlBasicCall call && call.getOperator().getKind() == SqlKind.AND) {
            for (SqlNode operand : call.getOperandList()) {
                conjuncts(operand, into);
            }
        } else {
            into.add(condition);
        }
    }

    private static String columnOf(SqlNode node, String table) {
        return node instanceof SqlIdentifier identifier && table.equalsIgnoreCase(qualifierAsWritten(identifier))
                ? last(identifier) : null;
    }

    private static SqlNode tableIdentifier(SqlNode right) {
        return right instanceof SqlCall call && call.getOperator().getKind() == SqlKind.AS ? call.operand(0) : right;
    }

    private static void joinsIn(SqlNode node, List<SqlJoin> into) {
        if (node instanceof SqlJoin join) {
            joinsIn(join.getLeft(), into);
            joinsIn(join.getRight(), into);
            into.add(join);
        }
    }

    private static void namesIn(SqlNode node, Set<String> into) {
        if (node instanceof SqlJoin join) {
            namesIn(join.getLeft(), into);
            namesIn(join.getRight(), into);
            return;
        }
        String name = tableName(node);
        if (name != null) {
            into.add(name.toLowerCase(Locale.ROOT));
        }
    }

    // names are read through the plain List interface, as the front end reads them: the field's own type
    // belongs to a collections library this module is not granted

    private static String qualifierAsWritten(SqlIdentifier identifier) {
        List<String> names = identifier.names;
        return names.size() >= 2 ? names.get(names.size() - 2) : null;
    }

    private static String qualifier(SqlIdentifier identifier) {
        String qualifier = qualifierAsWritten(identifier);
        return qualifier == null ? null : qualifier.toLowerCase(Locale.ROOT);
    }

    private static String last(SqlIdentifier identifier) {
        List<String> names = identifier.names;
        return names.get(names.size() - 1);
    }

    private static Unsupported at(String shape, SqlNode node) {
        SqlParserPos pos = node.getParserPosition();
        return new Unsupported(shape, pos.getLineNum(), pos.getColumnNum());
    }
}
