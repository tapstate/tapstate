package io.tapstate.core.dsl;

import dev.cel.common.CelAbstractSyntaxTree;
import dev.cel.common.ast.CelExpr;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationPredicate;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import static io.tapstate.core.validation.ValidationPredicate.*;

/**
 * Finite scalar classifier over the shared parser's checked AST. No CEL runtime or production row
 * program is created. Checked syntax alone is not admission: every expression kind, root, function
 * and arity is classified here. Collection predicates require their separate explicit admission.
 */
public final class ValidationPredicateCompiler {
    private final String node;
    private final CelAbstractSyntaxTree ast;
    private int nodes;

    private ValidationPredicateCompiler(String node, CelAbstractSyntaxTree ast) { this.node = node; this.ast = ast; }

    public static ValidationPredicate compile(String node, String expression, ValidationPredicate.Limits limits) {
        if (node == null || node.isBlank() || node.length() > 512 || expression == null || expression.isBlank())
            throw new TapstateException(ValidationError.INVALID_CONTRACT, Map.of("field", "filter.expression"), null);
        if (expression.length() > 16384) throw new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED,
                Map.of("limit", "filter.sourceLength"), null);
        CelAbstractSyntaxTree ast;
        try { ast = RowExpressions.predicateAst(expression); }
        catch (IllegalArgumentException invalid) {
            throw new TapstateException(ValidationError.RULE_UNSUPPORTED,
                    Map.of("node", node, "field", "expr", "rule", "filter.syntax"), invalid);
        }
        return ValidationPredicate.compile(node, new ValidationPredicateCompiler(node, ast).expression(ast.getExpr(), 1), limits);
    }

    private Expr expression(CelExpr expression, int depth) {
        visit(depth);
        return switch (expression.getKind()) {
            case CONSTANT -> new Constant(switch (expression.constant().getKind()) {
                case NULL_VALUE -> null;
                case BOOLEAN_VALUE -> expression.constant().booleanValue();
                case INT64_VALUE -> expression.constant().int64Value();
                case UINT64_VALUE -> new BigInteger(expression.constant().uint64Value().toString());
                case DOUBLE_VALUE -> expression.constant().doubleValue();
                case STRING_VALUE -> expression.constant().stringValue();
                default -> throw unsupported(expression, "filter.literal");
            });
            case SELECT -> expression.select().testOnly()
                    ? new Present(fieldPath(expression, depth)) : new Field(fieldPath(expression, depth));
            case CALL -> call(expression, depth);
            default -> throw unsupported(expression, "filter." + expression.getKind().name().toLowerCase(java.util.Locale.ROOT));
        };
    }

    private Expr call(CelExpr expression, int depth) {
        var call = expression.call();
        if (call.target().isPresent()) throw unsupported(expression, "filter.receiver");
        var args = call.args();
        String function = call.function();
        if (function.equals("!_") && args.size() == 1) return new Not(expression(args.getFirst(), depth + 1));
        if ((function.equals("_&&_") || function.equals("_||_")) && args.size() >= 2) {
            if (args.size() > 256) throw unsupported(expression, "filter.arity");
            var operands = new ArrayList<Expr>();
            for (var arg : args) operands.add(expression(arg, depth + 1));
            return logical(operands, 0, operands.size(), function.equals("_&&_"));
        }
        Operator operator = switch (function) {
            case "_==_" -> Operator.EQ; case "_!=_" -> Operator.NE;
            case "_<_" -> Operator.LT; case "_<=_" -> Operator.LE;
            case "_>_" -> Operator.GT; case "_>=_" -> Operator.GE;
            default -> null;
        };
        if (operator != null && args.size() == 2)
            return new Compare(operator, expression(args.get(0), depth + 1), expression(args.get(1), depth + 1));
        throw unsupported(expression, "filter.function." + function.substring(0, Math.min(function.length(), 96)));
    }

    private List<String> fieldPath(CelExpr expression, int depth) {
        var segments = new ArrayList<String>();
        CelExpr current = expression;
        boolean first = true;
        while (current.getKind() == CelExpr.ExprKind.Kind.SELECT) {
            if (!first) {
                visit(++depth);
                if (current.select().testOnly()) throw unsupported(expression, "filter.fieldPath");
            }
            String field = current.select().field();
            if (field.isEmpty() || field.length() > 512 || segments.size() == 32)
                throw unsupported(expression, "filter.fieldPath");
            segments.add(field);
            current = current.select().operand(); first = false;
        }
        visit(depth + 1);
        if (current.getKind() != CelExpr.ExprKind.Kind.IDENT || !current.ident().name().equals("after"))
            throw unsupported(expression, "filter.currentRowRequired");
        Collections.reverse(segments);
        return segments;
    }

    private static Expr logical(List<Expr> operands, int first, int end, boolean and) {
        if (end - first == 1) return operands.get(first);
        int middle = first + (end - first) / 2;
        Expr left = logical(operands, first, middle, and), right = logical(operands, middle, end, and);
        return and ? new And(left, right) : new Or(left, right);
    }

    private void visit(int depth) {
        if (++nodes > 256 || depth > 32) throw new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED,
                Map.of("limit", "filter.ast"), null);
    }
    private TapstateException unsupported(CelExpr expression, String rule) {
        int offset = ast.getSource().getPositionsMap().getOrDefault(expression.id(), -1);
        return new TapstateException(ValidationError.RULE_UNSUPPORTED,
                Map.of("node", node, "field", "expr@" + offset, "rule", rule), null);
    }
}
