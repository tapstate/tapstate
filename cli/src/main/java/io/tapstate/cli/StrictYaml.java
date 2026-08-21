package io.tapstate.cli;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** A strict parser for the small YAML subset used by the context configuration. */
final class StrictYaml {

    private StrictYaml() {
    }

    static Map<String, Object> parse(String source) {
        List<Line> lines = tokenize(source);
        if (lines.isEmpty()) {
            throw new ParseFailure("document is empty");
        }
        Parser parser = new Parser(lines);
        Object value = parser.block(0);
        if (!(value instanceof Map<?, ?> map) || parser.hasMore()) {
            throw new ParseFailure("document root must be a map");
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) map;
        return result;
    }

    private static List<Line> tokenize(String source) {
        List<Line> result = new ArrayList<>();
        String[] rawLines = source.replace("\r\n", "\n").replace('\r', '\n').split("\n", -1);
        for (int index = 0; index < rawLines.length; index++) {
            String raw = rawLines[index];
            if (raw.indexOf('\t') >= 0) {
                throw new ParseFailure("tabs are not allowed at line " + (index + 1));
            }
            int indent = 0;
            while (indent < raw.length() && raw.charAt(indent) == ' ') {
                indent++;
            }
            if ((indent & 1) != 0) {
                throw new ParseFailure("indentation must use two-space steps at line " + (index + 1));
            }
            String content = stripComment(raw.substring(indent)).stripTrailing();
            if (content.isBlank()) {
                continue;
            }
            if (content.equals("---") || content.equals("...") || content.startsWith("%")) {
                throw new ParseFailure("YAML directives and multiple documents are not allowed");
            }
            result.add(new Line(indent, content, index + 1));
        }
        return result;
    }

    private static String stripComment(String value) {
        boolean single = false;
        boolean dual = false;
        boolean escaped = false;
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (dual && c == '\\' && !escaped) {
                escaped = true;
                continue;
            }
            if (c == '"' && !single && !escaped) {
                dual = !dual;
            } else if (c == '\'' && !dual) {
                single = !single;
            } else if (c == '#' && !single && !dual && (i == 0 || Character.isWhitespace(value.charAt(i - 1)))) {
                return value.substring(0, i);
            }
            escaped = false;
        }
        if (single || dual) {
            throw new ParseFailure("unterminated quoted scalar");
        }
        return value;
    }

    static final class ParseFailure extends RuntimeException {
        ParseFailure(String message) {
            super(message);
        }
    }

    private record Line(int indent, String content, int number) {
    }

    private static final class Parser {
        private final List<Line> lines;
        private int index;

        private Parser(List<Line> lines) {
            this.lines = lines;
        }

        private boolean hasMore() {
            return index < lines.size();
        }

        private Object block(int indent) {
            if (!hasMore() || lines.get(index).indent() != indent) {
                throw failure("expected indentation " + indent);
            }
            return lines.get(index).content().startsWith("- ") ? sequence(indent) : mapping(indent);
        }

        private Map<String, Object> mapping(int indent) {
            Map<String, Object> result = new LinkedHashMap<>();
            while (hasMore() && lines.get(index).indent() == indent
                    && !lines.get(index).content().startsWith("- ")) {
                Line line = lines.get(index++);
                int split = colon(line.content());
                if (split < 1) {
                    throw new ParseFailure("expected key and colon at line " + line.number());
                }
                String key = string(line.content().substring(0, split).strip(), line.number());
                if (key.isEmpty() || result.containsKey(key)) {
                    throw new ParseFailure("duplicate or empty key at line " + line.number());
                }
                String tail = line.content().substring(split + 1).strip();
                Object value;
                if (!tail.isEmpty()) {
                    value = scalar(tail, line.number());
                } else {
                    if (!hasMore() || lines.get(index).indent() <= indent) {
                        throw new ParseFailure("missing value for key at line " + line.number());
                    }
                    if (lines.get(index).indent() != indent + 2) {
                        throw new ParseFailure("invalid nested indentation at line " + lines.get(index).number());
                    }
                    value = block(indent + 2);
                }
                result.put(key, value);
            }
            if (hasMore() && lines.get(index).indent() > indent) {
                throw failure("unexpected indentation");
            }
            return result;
        }

        private List<Object> sequence(int indent) {
            List<Object> result = new ArrayList<>();
            while (hasMore() && lines.get(index).indent() == indent
                    && lines.get(index).content().startsWith("- ")) {
                Line line = lines.get(index++);
                String tail = line.content().substring(2).strip();
                if (tail.isEmpty()) {
                    throw new ParseFailure("empty sequence item at line " + line.number());
                }
                result.add(scalar(tail, line.number()));
            }
            if (hasMore() && lines.get(index).indent() > indent) {
                throw failure("nested sequence values are not supported");
            }
            return result;
        }

        private ParseFailure failure(String message) {
            int line = hasMore() ? lines.get(index).number() : lines.get(lines.size() - 1).number();
            return new ParseFailure(message + " at line " + line);
        }

        private static int colon(String value) {
            boolean single = false;
            boolean dual = false;
            boolean escaped = false;
            for (int i = 0; i < value.length(); i++) {
                char c = value.charAt(i);
                if (dual && c == '\\' && !escaped) {
                    escaped = true;
                    continue;
                }
                if (c == '"' && !single && !escaped) {
                    dual = !dual;
                } else if (c == '\'' && !dual) {
                    single = !single;
                } else if (c == ':' && !single && !dual) {
                    return i;
                }
                escaped = false;
            }
            return -1;
        }

        private static Object scalar(String value, int line) {
            if (value.equals("{}")) {
                return Map.of();
            }
            if (value.equals("[]")) {
                return List.of();
            }
            if (value.equals("true")) {
                return Boolean.TRUE;
            }
            if (value.equals("false")) {
                return Boolean.FALSE;
            }
            if (value.equals("null") || value.equals("~")) {
                return null;
            }
            if (value.matches("-?[0-9]+")) {
                try {
                    return Integer.valueOf(value);
                } catch (NumberFormatException tooLarge) {
                    throw new ParseFailure("integer is out of range at line " + line);
                }
            }
            if (value.startsWith("[") || value.startsWith("{") || value.startsWith("&")
                    || value.startsWith("*") || value.startsWith("!")) {
                throw new ParseFailure("unsupported YAML construct at line " + line);
            }
            return string(value, line);
        }

        private static String string(String value, int line) {
            if (value.length() >= 2 && value.charAt(0) == '"' && value.charAt(value.length() - 1) == '"') {
                StringBuilder result = new StringBuilder();
                for (int i = 1; i < value.length() - 1; i++) {
                    char c = value.charAt(i);
                    if (c != '\\') {
                        result.append(c);
                        continue;
                    }
                    if (++i >= value.length() - 1) {
                        throw new ParseFailure("invalid escape at line " + line);
                    }
                    char escaped = value.charAt(i);
                    result.append(switch (escaped) {
                        case '"' -> '"';
                        case '\\' -> '\\';
                        case 'n' -> '\n';
                        case 'r' -> '\r';
                        case 't' -> '\t';
                        default -> throw new ParseFailure("unsupported escape at line " + line);
                    });
                }
                return result.toString();
            }
            if (value.length() >= 2 && value.charAt(0) == '\'' && value.charAt(value.length() - 1) == '\'') {
                return value.substring(1, value.length() - 1).replace("''", "'");
            }
            if (value.startsWith("\"") || value.startsWith("'") || value.contains("\n")) {
                throw new ParseFailure("invalid scalar at line " + line);
            }
            return value;
        }
    }
}
