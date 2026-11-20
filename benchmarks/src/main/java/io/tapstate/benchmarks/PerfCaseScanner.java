package io.tapstate.benchmarks;

import com.sun.source.tree.AnnotationTree;
import com.sun.source.tree.AssignmentTree;
import com.sun.source.tree.ClassTree;
import com.sun.source.tree.CompilationUnitTree;
import com.sun.source.tree.ExpressionTree;
import com.sun.source.tree.LiteralTree;
import com.sun.source.tree.MethodTree;
import com.sun.source.tree.NewArrayTree;
import com.sun.source.util.JavacTask;
import com.sun.source.util.SourcePositions;
import com.sun.source.util.TreeScanner;
import com.sun.source.util.Trees;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaFileObject;
import javax.tools.ToolProvider;

/** Emits the case inventory using only the Java parse phase. */
public final class PerfCaseScanner {
    private static final String ANNOTATION = "io.tapstate.benchmarks.PerfCase";
    private static final Pattern CASE_ID = Pattern.compile("[a-z0-9]+(?:[.-][a-z0-9]+)+");

    private PerfCaseScanner() {
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: java PerfCaseScanner.java <case-source-root>");
            System.exit(2);
        }
        try {
            System.out.println(scan(Path.of(args[0])));
        } catch (IOException | IllegalArgumentException e) {
            System.err.println("perf-case-scan: " + e.getMessage());
            System.exit(1);
        }
    }

    static String scan(Path directory) throws IOException {
        Path root = directory.toRealPath();
        if (!Files.isDirectory(root)) {
            throw new IllegalArgumentException("Expected a case source directory: " + root);
        }
        List<Path> sources;
        try (var entries = Files.walk(root)) {
            sources = entries.filter(path -> Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS))
                    .filter(path -> path.getFileName().toString().endsWith(".java"))
                    .sorted()
                    .toList();
        }
        Map<String, Map<String, Object>> cases = new TreeMap<>();
        if (!sources.isEmpty()) {
            parseCases(root, sources, cases);
        }
        Map<String, Object> catalog = new LinkedHashMap<>();
        catalog.put("schema_version", 1);
        catalog.put("cases", List.copyOf(cases.values()));
        return json(catalog);
    }

    private static void parseCases(Path root, List<Path> sources, Map<String, Map<String, Object>> cases)
            throws IOException {
        var compiler = ToolProvider.getSystemJavaCompiler();
        var diagnostics = new DiagnosticCollector<JavaFileObject>();
        try (var files = compiler.getStandardFileManager(diagnostics, Locale.ROOT, StandardCharsets.UTF_8)) {
            JavacTask task = (JavacTask) compiler.getTask(null, files, diagnostics, List.of("-proc:none"),
                    null, files.getJavaFileObjectsFromPaths(sources));
            var units = task.parse();
            for (var diagnostic : diagnostics.getDiagnostics()) {
                if (diagnostic.getKind() == Diagnostic.Kind.ERROR) {
                    throw new IllegalArgumentException(diagnostic.toString());
                }
            }
            var positions = Trees.instance(task).getSourcePositions();
            for (CompilationUnitTree unit : units) {
                scanUnit(root, unit, positions, cases);
            }
        }
    }

    private static void scanUnit(Path root, CompilationUnitTree unit,
            SourcePositions positions, Map<String, Map<String, Object>> cases) {
        Path source = Path.of(unit.getSourceFile().toUri());
        String relativeSource = relativePath(root, source);
        List<String> imports = unit.getImports().stream()
                .map(item -> item.getQualifiedIdentifier().toString()).toList();
        boolean shortName = "io.tapstate.benchmarks".equals(String.valueOf(unit.getPackageName()))
                || imports.contains(ANNOTATION) || imports.contains("io.tapstate.benchmarks.*");
        boolean shortContainer = imports.contains(ANNOTATION + ".Cases") || imports.contains(ANNOTATION + ".*");
        new TreeScanner<Void, Void>() {
            @Override
            public Void visitClass(ClassTree node, Void unused) {
                node.getModifiers().getAnnotations().forEach(this::read);
                return super.visitClass(node, unused);
            }

            @Override
            public Void visitMethod(MethodTree node, Void unused) {
                node.getModifiers().getAnnotations().forEach(this::read);
                return super.visitMethod(node, unused);
            }

            private void read(AnnotationTree annotation) {
                String name = annotation.getAnnotationType().toString();
                if (isCaseAnnotation(annotation)) {
                    add(annotation);
                } else if (name.equals(ANNOTATION + ".Cases") || shortName && name.equals("PerfCase.Cases")
                        || shortContainer && name.equals("Cases")) {
                    readContainer(annotation);
                }
            }

            private boolean isCaseAnnotation(AnnotationTree annotation) {
                String type = annotation.getAnnotationType().toString();
                return type.equals(ANNOTATION) || shortName && type.equals("PerfCase");
            }

            private void readContainer(AnnotationTree annotation) {
                List<? extends ExpressionTree> args = annotation.getArguments();
                if (args.size() != 1) {
                    throw invalid(location(annotation), "Expected one value in @PerfCase.Cases");
                }
                ExpressionTree value = args.getFirst();
                if (value instanceof AssignmentTree named) {
                    if (!named.getVariable().toString().equals("value")) {
                        throw invalid(location(annotation), "Expected the value attribute of @PerfCase.Cases");
                    }
                    value = named.getExpression();
                }
                for (ExpressionTree element : elements(value)) {
                    if (!(element instanceof AnnotationTree nested) || !isCaseAnnotation(nested)) {
                        throw invalid(location(annotation), "Expected @PerfCase inside @PerfCase.Cases");
                    }
                    add(nested);
                }
            }

            private void add(AnnotationTree annotation) {
                String location = location(annotation);
                Map<String, Object> definition = readCase(root, source, annotation, location);
                String id = (String) definition.get("id");
                definition.put("definition", relativeSource);
                definition.put("line", line(annotation));
                Map<String, Object> previous = cases.putIfAbsent(id, definition);
                if (previous != null) {
                    throw invalid(location, "Duplicate case ID " + id + " (first declared at "
                            + previous.get("definition") + ":" + previous.get("line") + ")");
                }
            }

            private long line(AnnotationTree annotation) {
                return unit.getLineMap().getLineNumber(positions.getStartPosition(unit, annotation));
            }

            private String location(AnnotationTree annotation) {
                return relativeSource + ":" + line(annotation);
            }
        }.scan(unit, null);
    }

    private static Map<String, Object> readCase(Path root, Path source, AnnotationTree annotation, String location) {
        Map<String, ExpressionTree> arguments = new TreeMap<>();
        for (ExpressionTree expression : annotation.getArguments()) {
            if (!(expression instanceof AssignmentTree entry)) {
                throw invalid(location, "Use named @PerfCase attributes");
            }
            String name = entry.getVariable().toString();
            if (arguments.putIfAbsent(name, entry.getExpression()) != null) {
                throw invalid(location, "Duplicate attribute " + name);
            }
        }

        Map<String, Object> result = new LinkedHashMap<>();
        for (String name : List.of("id", "description", "group", "phase", "mode", "profile",
                "source", "target", "workload")) {
            result.put(name, string(arguments.remove(name), name, location));
        }
        String id = (String) result.get("id");
        if (!CASE_ID.matcher(id).matches()) {
            throw invalid(location, "Invalid case ID " + id);
        }
        result.put("case_version", positiveInteger(arguments.remove("version"), "version", location));
        result.put("profile_version", positiveInteger(arguments.remove("profileVersion"), "profileVersion", location));
        List<String> operators = strings(arguments.remove("operators"), "operators", location, true);
        String group = (String) result.get("group");
        if (group.equals("copy") ? !operators.isEmpty()
                : operators.isEmpty() || !group.equals("mixed") && !operators.contains(group)) {
            throw invalid(location, "Operators do not match group " + group);
        }
        result.put("operators", operators);
        List<String> environments = strings(arguments.remove("environments"), "environments", location, true);
        if (environments.isEmpty()) {
            throw invalid(location, "Declare at least one environment");
        }
        result.put("environments", environments);
        result.put("purposes", strings(arguments.remove("purposes"), "purposes", location, false));
        result.put("data_features", strings(arguments.remove("dataFeatures"), "dataFeatures", location, false));
        result.put("tags", strings(arguments.remove("tags"), "tags", location, false));
        ExpressionTree execution = arguments.remove("execution");
        result.put("execution", execution == null ? "single-run" : string(execution, "execution", location));
        ExpressionTree spec = arguments.remove("spec");
        result.put("spec", specPath(root, source, spec, location));
        if (!arguments.isEmpty()) {
            throw invalid(location, "Unknown @PerfCase attributes " + arguments.keySet());
        }
        return result;
    }

    private static String specPath(Path root, Path source, ExpressionTree value, String location) {
        if (value == null || value instanceof LiteralTree literal && "".equals(literal.getValue())) {
            return null;
        }
        String declared = string(value, "spec", location);
        try {
            Path relative = Path.of(declared);
            Path resolved = source.getParent().resolve(relative).toRealPath();
            if (!relative.isAbsolute() && resolved.startsWith(root) && Files.isRegularFile(resolved)
                    && Files.isReadable(resolved)) {
                return relativePath(root, resolved);
            }
        } catch (IOException | InvalidPathException e) {
            throw invalid(location, "Invalid spec path " + declared);
        }
        throw invalid(location, "Spec must be a readable file within the case source root: " + declared);
    }

    private static String relativePath(Path root, Path path) {
        return root.relativize(path).toString().replace(path.getFileSystem().getSeparator(), "/");
    }

    private static String string(ExpressionTree value, String name, String location) {
        if (value instanceof LiteralTree literal && literal.getValue() instanceof String text && !text.isBlank()) {
            return text;
        }
        throw invalid(location, name + " must be a non-blank string literal");
    }

    private static int positiveInteger(ExpressionTree value, String name, String location) {
        if (value instanceof LiteralTree literal && literal.getValue() instanceof Integer number && number > 0) {
            return number;
        }
        throw invalid(location, name + " must be a positive integer literal");
    }

    private static List<? extends ExpressionTree> elements(ExpressionTree value) {
        return value instanceof NewArrayTree array ? array.getInitializers() : List.of(value);
    }

    private static List<String> strings(ExpressionTree value, String name, String location, boolean required) {
        if (value == null && !required) {
            return List.of();
        }
        if (value == null) {
            throw invalid(location, "Missing " + name);
        }
        List<String> items = elements(value).stream().map(element -> string(element, name, location)).toList();
        if (items.stream().distinct().count() != items.size()) {
            throw invalid(location, "Duplicate value in " + name);
        }
        return items;
    }

    private static IllegalArgumentException invalid(String location, String message) {
        return new IllegalArgumentException(location + ": " + message);
    }

    private static String json(Object value) {
        return switch (value) {
            case null -> "null";
            case String string -> quote(string);
            case Number number -> number.toString();
            case List<?> list -> list.stream().map(PerfCaseScanner::json).collect(Collectors.joining(",", "[", "]"));
            case Map<?, ?> map -> map.entrySet().stream()
                    .map(entry -> quote((String) entry.getKey()) + ":" + json(entry.getValue()))
                    .collect(Collectors.joining(",", "{", "}"));
            default -> throw new IllegalStateException("Unsupported catalog value: " + value);
        };
    }

    private static String quote(String value) {
        StringBuilder output = new StringBuilder("\"");
        for (int i = 0; i < value.length(); i++) {
            char character = value.charAt(i);
            if (character == '"' || character == '\\') {
                output.append('\\').append(character);
            } else if (character < 0x20 || Character.isSurrogate(character)) {
                output.append(String.format(Locale.ROOT, "\\u%04x", (int) character));
            } else {
                output.append(character);
            }
        }
        return output.append('"').toString();
    }
}
