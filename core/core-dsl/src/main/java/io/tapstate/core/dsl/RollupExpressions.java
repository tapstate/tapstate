package io.tapstate.core.dsl;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import dev.cel.common.CelAbstractSyntaxTree;
import dev.cel.common.CelValidationException;
import dev.cel.common.CelValidationResult;
import dev.cel.common.ast.CelExpr;
import dev.cel.common.navigation.CelNavigableAst;
import dev.cel.common.types.CelType;
import dev.cel.common.types.CelTypeProvider;
import dev.cel.common.types.ListType;
import dev.cel.common.types.MapType;
import dev.cel.common.types.SimpleType;
import dev.cel.common.types.StructType;
import dev.cel.common.types.StructTypeReference;
import dev.cel.compiler.CelCompiler;
import dev.cel.compiler.CelCompilerBuilder;
import dev.cel.compiler.CelCompilerFactory;
import dev.cel.parser.CelStandardMacro;
import io.tapstate.core.common.TapstateType;
import io.tapstate.core.dsl.aggregate.RollupScope;
import io.tapstate.core.dsl.aggregate.ValueType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Row expressions as a rollup field reads them. The row is the element as it will be rendered and is reached
 * as {@code after}, the same name every row expression uses: {@code after.amount},
 * {@code after.prize_definition.prize_value}, {@code has(after.prize_definition)}. A {@code $column} is the
 * shorthand for a bare read of one of the element's own fields. There is no {@code before}, no envelope and
 * no clock: a rollup is a function of the elements alone.
 *
 * <p>Checking is typed against the element's columns and the models of the rows its references resolve to.
 * A reference whose model was discovered is a closed type, so a misspelt reference column is refused; the
 * element's own columns stay open, as they do for every row expression, since a sampled model may simply
 * not have seen a column yet.
 */
public final class RollupExpressions {

    private static final String ROW = "after";
    private static final String ROOT_TYPE = "tapstate.Rollup";
    private static final Pattern COLUMN = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    /** The element as an untyped map: what the expression is evaluated against, and checked against offline. */
    private static final CelType UNTYPED_ROW = MapType.create(SimpleType.STRING, SimpleType.DYN);
    private static final CelCompiler UNTYPED_VALUE = untyped().build();
    private static final CelCompiler UNTYPED_PREDICATE = untyped().setResultType(SimpleType.BOOL).build();

    private RollupExpressions() {
    }

    /** Whether {@code name} can follow a {@code $}: one field of the element, named as an identifier. */
    public static boolean isColumnName(String name) {
        return COLUMN.matcher(name).matches();
    }

    /** {@code written} as the expression text it stands for: {@code $amount} is {@code after.amount}. */
    public static String expressionOf(String written) {
        if (written.startsWith("$")) {
            return ROW + "." + written.substring(1);
        }
        if (written.startsWith("=")) {
            return written.substring(1);
        }
        throw new IllegalArgumentException("not a rollup expression: " + written);
    }

    /** The untyped check of a value expression, or null when it compiles. */
    static String untypedValueError(String expr) {
        return error(UNTYPED_VALUE, expr);
    }

    /** The untyped check of a predicate, or null when it compiles to {@code bool}. */
    static String untypedPredicateError(String expr) {
        return error(UNTYPED_PREDICATE, expr);
    }

    /** The checked, untyped AST evaluation runs: the typed check has already been passed by then. */
    static CelAbstractSyntaxTree evaluableAst(String expr, boolean predicate) {
        CelValidationResult result = (predicate ? UNTYPED_PREDICATE : UNTYPED_VALUE).compile(expr);
        if (result.hasError()) {
            throw new IllegalArgumentException(result.getErrorString());
        }
        try {
            return result.getAst();
        } catch (CelValidationException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    /**
     * What a typed check of {@code expr} over {@code scope} found: the type it produces, or why it does not
     * compile. A predicate must produce {@code bool}.
     */
    record Typed(ValueType type, String error) {
    }

    static Typed typed(String expr, RollupScope scope, boolean predicate) {
        CelCompilerBuilder builder = typedEnvironment(scope, expr);
        if (predicate) {
            builder.setResultType(SimpleType.BOOL);
        }
        CelValidationResult result = builder.build().compile(expr);
        if (result.hasError()) {
            return new Typed(ValueType.UNKNOWN, result.getErrorString());
        }
        CelAbstractSyntaxTree ast;
        try {
            ast = result.getAst();
        } catch (CelValidationException e) {
            return new Typed(ValueType.UNKNOWN, e.getMessage());
        }
        String indexed = RowExpressions.indexedRow(ast);
        if (indexed != null) {
            return new Typed(ValueType.UNKNOWN, indexed);
        }
        CelType celType = ast.getResultType();
        TapstateType type = celType.equals(SimpleType.DYN) ? TapstateType.UNKNOWN
                : RowExpressions.tapstateTypeOf(celType);
        Integer scale = null;
        if (type == TapstateType.DECIMAL) {
            scale = scaleOfRead(readPath(ast.getExpr()), scope);
        }
        return new Typed(type == TapstateType.DECIMAL ? ValueType.decimal(scale) : ValueType.of(type), null);
    }

    /**
     * The element fields {@code expr} reads that are where a reference renders: the references the field
     * cannot be computed without. A presence test counts, since it reads the reference as much as a value
     * access does.
     */
    static Set<String> referencesRead(String expr, RollupScope scope) {
        Set<String> read = new LinkedHashSet<>();
        CelNavigableAst.fromAst(evaluableAst(expr, false)).getRoot().allNodes()
                .filter(node -> node.getKind() == CelExpr.ExprKind.Kind.SELECT)
                .forEach(node -> {
                    CelExpr.CelSelect select = node.expr().select();
                    CelExpr operand = select.operand();
                    if (operand.getKind() == CelExpr.ExprKind.Kind.IDENT
                            && operand.ident().name().equals(ROW) && scope.isReference(select.field())) {
                        read.add(select.field());
                    }
                });
        return read;
    }

    // ---- environments ---------------------------------------------------------------------------

    private static CelCompilerBuilder untyped() {
        return CelCompilerFactory.standardCelCompilerBuilder()
                .setStandardMacros(CelStandardMacro.STANDARD_MACROS)
                .addVar(ROW, UNTYPED_ROW);
    }

    /**
     * The typed environment over one element. The element is a struct whose fields are its columns (plus
     * any column the expression names that no model lists, as {@code dyn}), its references as structs of
     * their own, and its gathered child embeds as lists or maps of untyped values.
     */
    private static CelCompilerBuilder typedEnvironment(RollupScope scope, String expr) {
        Map<String, StructType> types = new LinkedHashMap<>();
        Set<String> named = namedFields(expr);
        StructType row = struct(ROOT_TYPE, scope, named, false, types);
        types.put(ROOT_TYPE, row);
        CelTypeProvider provider = new CelTypeProvider() {
            @Override
            public ImmutableCollection<CelType> types() {
                return ImmutableList.copyOf(types.values());
            }

            @Override
            public Optional<CelType> findType(String typeName) {
                return Optional.ofNullable(types.get(typeName));
            }
        };
        return CelCompilerFactory.standardCelCompilerBuilder()
                .setStandardMacros(CelStandardMacro.STANDARD_MACROS)
                .setTypeProvider(provider)
                .addVar(ROW, StructTypeReference.create(ROOT_TYPE));
    }

    private static StructType struct(String name, RollupScope scope, Set<String> named, boolean closed,
            Map<String, StructType> types) {
        Map<String, CelType> fields = new LinkedHashMap<>();
        scope.columns().forEach((column, type) -> fields.put(column, celTypeOf(type)));
        scope.gathered().forEach((path, array) -> fields.put(path,
                array ? ListType.create(SimpleType.DYN) : MapType.create(SimpleType.STRING, SimpleType.DYN)));
        scope.referenceObjects().forEach((path, referenced) -> fields.put(path,
                reference(name + "." + path, referenced, types)));
        scope.referenceArrays().forEach((path, referenced) -> fields.put(path,
                ListType.create(reference(name + "." + path, referenced, types))));
        if (!closed) {
            for (String column : named) {
                fields.putIfAbsent(column, SimpleType.DYN);
            }
        }
        return StructType.create(name, ImmutableSet.copyOf(fields.keySet()),
                field -> Optional.ofNullable(fields.get(field)));
    }

    /** A referenced row: a closed struct when its model is known, an untyped map when nothing discovered it. */
    private static CelType reference(String name, RollupScope referenced, Map<String, StructType> types) {
        if (!referenced.columnsKnown()) {
            return MapType.create(SimpleType.STRING, SimpleType.DYN);
        }
        StructType type = struct(name, referenced, Set.of(), true, types);
        types.put(name, type);
        return StructTypeReference.create(name);
    }

    private static CelType celTypeOf(ValueType type) {
        return type.type() == TapstateType.UNKNOWN ? SimpleType.DYN : RowExpressions.celTypeOf(type.type());
    }

    /** The element fields an expression names directly ({@code after.<field>}). */
    private static Set<String> namedFields(String expr) {
        Set<String> named = new LinkedHashSet<>();
        CelValidationResult result = UNTYPED_VALUE.compile(expr);
        if (result.hasError()) {
            result = UNTYPED_PREDICATE.compile(expr);
        }
        if (result.hasError()) {
            return named;
        }
        try {
            CelNavigableAst.fromAst(result.getAst()).getRoot().allNodes()
                    .filter(node -> node.getKind() == CelExpr.ExprKind.Kind.SELECT)
                    .forEach(node -> {
                        CelExpr operand = node.expr().select().operand();
                        if (operand.getKind() == CelExpr.ExprKind.Kind.IDENT && operand.ident().name().equals(ROW)) {
                            named.add(node.expr().select().field());
                        }
                    });
        } catch (CelValidationException e) {
            return named;
        }
        return named;
    }

    /** The fields {@code expr} does nothing but read, outermost first, or empty when it does anything more. */
    private static List<String> readPath(CelExpr expr) {
        List<String> path = new ArrayList<>();
        CelExpr at = expr;
        while (at.getKind() == CelExpr.ExprKind.Kind.SELECT && !at.select().testOnly()) {
            path.addFirst(at.select().field());
            at = at.select().operand();
        }
        if (at.getKind() != CelExpr.ExprKind.Kind.IDENT || !at.ident().name().equals(ROW)) {
            return List.of();
        }
        return path;
    }

    /** The declared scale of the decimal a bare read reaches, or null when nothing declared one. */
    private static Integer scaleOfRead(List<String> path, RollupScope scope) {
        if (path.size() == 1) {
            ValueType type = scope.columns().get(path.getFirst());
            return type == null ? null : type.scale();
        }
        if (path.size() == 2 && scope.referenceObjects().containsKey(path.getFirst())) {
            ValueType type = scope.referenceObjects().get(path.getFirst()).columns().get(path.get(1));
            return type == null ? null : type.scale();
        }
        return null;
    }

    private static String error(CelCompiler compiler, String expr) {
        CelValidationResult result = compiler.compile(expr);
        if (result.hasError()) {
            return result.getErrorString();
        }
        try {
            return RowExpressions.indexedRow(result.getAst());
        } catch (CelValidationException e) {
            return e.getMessage();
        }
    }
}
