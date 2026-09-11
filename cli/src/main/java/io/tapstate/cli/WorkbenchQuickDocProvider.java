package io.tapstate.cli;

import io.tapstate.core.dsl.DslException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.schema.SchemaNavigator;
import io.tapstate.core.schema.SchemaNode;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Builds an immutable current-line documentation projection from the bundled grammar schema. */
final class WorkbenchQuickDocProvider {

    static final WorkbenchQuickDocProvider BUNDLED = new WorkbenchQuickDocProvider(
            SchemaNavigator.bundled(), new DslParser());

    private final SchemaNavigator schema;
    private final DslParser parser;

    WorkbenchQuickDocProvider(SchemaNavigator schema, DslParser parser) {
        this.schema = schema;
        this.parser = parser;
    }

    Optional<QuickDoc> document(String content, int line) {
        Optional<PathAtLine> current = YamlPath.at(content, line);
        if (current.isEmpty()) {
            return Optional.empty();
        }
        PathAtLine path = current.orElseThrow();
        Optional<String> validation = validationAt(content, line, path.localPath());
        if (validation.isPresent()) {
            return Optional.of(QuickDoc.error(path.schemaPath(), validation.orElseThrow()));
        }
        return schema.navigate(path.schemaPath()).map(QuickDoc::from);
    }

    private Optional<String> validationAt(String content, int line, String localPath) {
        try {
            parser.parse(content);
            return Optional.empty();
        } catch (DslException failure) {
            boolean sameLine = failure.line() > 0 && failure.line() == line + 1;
            boolean samePath = failure.path() != null && failure.path().equals(localPath);
            return sameLine || samePath ? Optional.of(failure.code().code()) : Optional.empty();
        }
    }

    record QuickDoc(String title, List<String> entries, Optional<String> validationError) {
        QuickDoc {
            entries = List.copyOf(entries);
            validationError = Objects.requireNonNull(validationError, "validationError");
        }

        static QuickDoc from(SchemaNode node) {
            String title = node.path() + "  " + node.type() + (node.isRequired() ? "  required" : "  optional");
            String description = node.description() == null || node.description().isBlank()
                    ? ""
                    : node.description();
            String constraint = node.enumValues().isEmpty()
                    ? node.children().isEmpty() ? "" : "Fields: " + String.join(", ", node.children())
                    : "Values: " + node.enumValues().stream()
                            .map(SchemaNode.EnumValue::value)
                            .reduce((left, right) -> left + ", " + right)
                            .orElse("");
            return new QuickDoc(title, List.of(description, constraint).stream()
                    .filter(value -> !value.isBlank())
                    .toList(), Optional.empty());
        }

        static QuickDoc error(String path, String error) {
            return new QuickDoc(path, List.of("Validation error: " + error), Optional.of(error));
        }
    }

    private record PathAtLine(String schemaPath, String localPath) {
    }

    private static final class YamlPath {

        private YamlPath() {
        }

        static Optional<PathAtLine> at(String content, int targetLine) {
            String[] lines = content.split("\\n", -1);
            if (targetLine < 0 || targetLine >= lines.length) {
                return Optional.empty();
            }
            String kind = resourceKind(lines);
            if (kind == null) {
                return Optional.empty();
            }
            ArrayDeque<Scope> scopes = new ArrayDeque<>();
            for (int index = 0; index <= targetLine; index++) {
                Field field = Field.parse(lines[index]);
                if (field == null) {
                    continue;
                }
                while (!scopes.isEmpty() && scopes.peekLast().indent() >= field.indent()) {
                    scopes.removeLast();
                }
                String local = join(scopes, field.name());
                if (index == targetLine) {
                    String schemaPath = field.name().equals("kind") && scopes.isEmpty()
                            ? "kind" : kind + "." + local;
                    return Optional.of(new PathAtLine(schemaPath, local));
                }
                if (field.container()) {
                    scopes.addLast(new Scope(field.indent(), field.name()));
                }
            }
            return Optional.empty();
        }

        private static String resourceKind(String[] lines) {
            for (String line : lines) {
                Field field = Field.parse(line);
                if (field != null && field.indent() == 0 && field.name().equals("kind")) {
                    String value = field.value();
                    return List.of("source", "pipeline", "transform", "view", "serve").contains(value)
                            ? value : null;
                }
            }
            return null;
        }

        private static String join(ArrayDeque<Scope> scopes, String leaf) {
            StringBuilder path = new StringBuilder();
            for (Scope scope : scopes) {
                if (!path.isEmpty()) {
                    path.append('.');
                }
                path.append(scope.name());
            }
            if (!path.isEmpty()) {
                path.append('.');
            }
            return path.append(leaf).toString();
        }

        private record Scope(int indent, String name) {
        }

        private record Field(int indent, String name, String value) {
            static Field parse(String line) {
                String withoutComment = line.substring(0, line.indexOf('#') >= 0 ? line.indexOf('#') : line.length());
                int indent = 0;
                while (indent < withoutComment.length() && withoutComment.charAt(indent) == ' ') {
                    indent++;
                }
                String text = withoutComment.substring(indent).stripLeading();
                if (text.startsWith("- ")) {
                    text = text.substring(2);
                }
                int colon = text.indexOf(':');
                if (colon <= 0) {
                    return null;
                }
                String name = text.substring(0, colon).trim();
                if (!name.matches("[A-Za-z][A-Za-z0-9_-]*")) {
                    return null;
                }
                return new Field(indent, name, text.substring(colon + 1).trim());
            }

            boolean container() {
                return value.isEmpty();
            }
        }
    }
}
