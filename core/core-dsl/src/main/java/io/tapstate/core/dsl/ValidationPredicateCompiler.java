package io.tapstate.core.dsl;

import dev.cel.common.CelAbstractSyntaxTree;
import dev.cel.common.ast.CelExpr;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationPredicate;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import static io.tapstate.core.validation.ValidationPredicate.*;

/**
 * Finite scalar classifier over the shared parser's checked AST. No CEL runtime or production row
 * program is created. Checked syntax alone is not admission: every expression kind, root, function
 * and arity is classified here. Array exists admits only the registered comprehension structure;
 * its member aliases are lexically bound, never mistaken for event-envelope variables.
 */
public final class ValidationPredicateCompiler {
    private final String node;
    private final CelAbstractSyntaxTree ast;
    private record Alias(String name, int slot) {}
    private record Access(int slot, List<String> path) {}
    private record AstFrame(CelExpr expression, int depth) {}
    private final List<Alias> aliases = new ArrayList<>();
    private int nextSlot;
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
        var compiler = new ValidationPredicateCompiler(node, ast);
        compiler.checkTree();
        return ValidationPredicate.compile(node, compiler.expression(ast.getExpr(), 1), limits);
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
            case SELECT -> {
                Access access = fieldPath(expression, depth);
                yield expression.select().testOnly() ? new Present(access.slot(), access.path()) : new Field(access.slot(), access.path());
            }
            case IDENT -> {
                Integer slot = slot(expression.ident().name());
                if (slot == null) throw unsupported(expression, "filter.boundMemberRequired");
                yield new Field(slot, List.of());
            }
            case COMPREHENSION -> exists(expression, depth);
            case CALL -> call(expression, depth);
            default -> throw unsupported(expression, "filter." + expression.getKind().name().toLowerCase(java.util.Locale.ROOT));
        };
    }

    private Expr call(CelExpr expression, int depth) {
        var call = expression.call();
        if (call.target().isPresent()) throw unsupported(expression, "filter.receiver");
        var args = call.args();
        String function = call.function();
        if (function.equals("size") && args.size() == 1) return new ArraySize(expression(args.getFirst(), depth + 1));
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

    private Access fieldPath(CelExpr expression, int depth) {
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
        if (current.getKind() != CelExpr.ExprKind.Kind.IDENT) throw unsupported(expression, "filter.currentRowRequired");
        Integer slot = slot(current.ident().name());
        if (slot == null && !current.ident().name().equals("after")) throw unsupported(expression, "filter.currentRowRequired");
        Collections.reverse(segments);
        return new Access(slot == null ? -1 : slot, segments);
    }

    private Expr exists(CelExpr expression, int depth) {
        var loop = expression.comprehension();
        String accumulator = loop.accuVar();
        if (accumulator.isBlank() || accumulator.equals(loop.iterVar()) || loop.iterVar().isBlank() || loop.iterVar().length() > 512
                || loop.accuInit().getKind() != CelExpr.ExprKind.Kind.CONSTANT
                || loop.accuInit().constant().getKind() != dev.cel.common.ast.CelConstant.Kind.BOOLEAN_VALUE
                || loop.accuInit().constant().booleanValue()
                || !identifier(loop.result(), accumulator)
                || !global(loop.loopCondition(), "@not_strictly_false", 1)
                || !global(loop.loopCondition().call().args().getFirst(), "!_", 1)
                || !identifier(loop.loopCondition().call().args().getFirst().call().args().getFirst(), accumulator)
                || !global(loop.loopStep(), "_||_", 2)
                || !identifier(loop.loopStep().call().args().getFirst(), accumulator))
            throw unsupported(expression, "filter.comprehension");
        // The range is outside the new member's lexical scope. A reused alias shadows only the body.
        Expr range = expression(loop.iterRange(), depth + 1);
        int slot = nextSlot++;
        aliases.add(new Alias(loop.iterVar(), slot));
        try { return new Exists(slot, range, expression(loop.loopStep().call().args().get(1), depth + 1)); }
        finally { aliases.removeLast(); }
    }

    private Integer slot(String name) {
        for (int i = aliases.size() - 1; i >= 0; i--) if (aliases.get(i).name().equals(name)) return aliases.get(i).slot();
        return null;
    }
    private static boolean identifier(CelExpr expression, String name) {
        return expression.getKind() == CelExpr.ExprKind.Kind.IDENT && expression.ident().name().equals(name);
    }
    private static boolean global(CelExpr expression, String name, int arity) {
        return expression.getKind() == CelExpr.ExprKind.Kind.CALL && expression.call().target().isEmpty()
                && expression.call().function().equals(name) && expression.call().args().size() == arity;
    }

    /** Count the complete admitted AST, including the fixed internal nodes of a macro expansion. */
    private void checkTree() {
        var pending = new ArrayDeque<AstFrame>(); pending.push(new AstFrame(ast.getExpr(), 1));
        int count = 0;
        while (!pending.isEmpty()) {
            var current = pending.pop();
            if (++count > 256 || current.depth() > 32) throw new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED,
                    Map.of("limit", "filter.ast"), null);
            int depth = current.depth() + 1;
            switch (current.expression().getKind()) {
                case SELECT -> pending.push(new AstFrame(current.expression().select().operand(), depth));
                case CALL -> {
                    var call = current.expression().call();
                    if (call.args().size() > 256) throw unsupported(current.expression(), "filter.arity");
                    call.target().ifPresent(target -> pending.push(new AstFrame(target, depth)));
                    for (var arg : call.args()) pending.push(new AstFrame(arg, depth));
                }
                case COMPREHENSION -> {
                    var loop = current.expression().comprehension();
                    for (var part : List.of(loop.iterRange(), loop.accuInit(), loop.loopCondition(), loop.loopStep(), loop.result()))
                        pending.push(new AstFrame(part, depth));
                }
                // Other aggregate kinds are refused by classification before any descendant is admitted.
                default -> { }
            }
        }
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
