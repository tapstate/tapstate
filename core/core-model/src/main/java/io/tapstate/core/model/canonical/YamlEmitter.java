package io.tapstate.core.model.canonical;

import java.util.regex.Pattern;

/**
 * Renders a {@link Node} tree as canonical YAML text (canonical-form.md §2/§6): 2-space
 * indent, block mappings everywhere, flow style for scalar-only sequences, plain scalars
 * wherever the YAML 1.2 core schema keeps them strings, double quotes otherwise.
 *
 * <p>Hand-rolled on purpose: byte-level determinism is the contract (golden files), so the
 * core ring does not delegate emission details to a third-party emitter.
 */
final class YamlEmitter {

    private static final Pattern NON_STRING_PLAIN = Pattern.compile(
            "true|True|TRUE|false|False|FALSE"
                    + "|null|Null|NULL|~"
                    + "|[-+]?[0-9]+|0o[0-7]+|0x[0-9a-fA-F]+"
                    + "|[-+]?(\\.[0-9]+|[0-9]+(\\.[0-9]*)?)([eE][-+]?[0-9]+)?"
                    + "|[-+]?(\\.inf|\\.Inf|\\.INF)|\\.nan|\\.NaN|\\.NAN");

    private static final String LEADING_INDICATORS = "-?:,[]{}#&*!|>'\"%@` \t";

    String emit(Node.MapN root) {
        StringBuilder sb = new StringBuilder();
        writeMap(sb, root, 0, null);
        return sb.toString();
    }

    private void writeMap(StringBuilder sb, Node.MapN map, int indent, String firstLead) {
        boolean first = true;
        for (Node.Entry e : map.entries()) {
            String lead = (first && firstLead != null) ? firstLead : " ".repeat(indent);
            writeEntry(sb, lead, e.key(), e.value(), indent);
            first = false;
        }
    }

    private void writeEntry(StringBuilder sb, String lead, String key, Node value, int indent) {
        sb.append(lead).append(renderKey(key)).append(':');
        switch (value) {
            case Node.ScalarN s when s.style() == Node.Style.LITERAL -> {
                String text = (String) s.value();
                sb.append(" |").append(chompIndicator(text)).append('\n');
                appendLiteralLines(sb, text, indent + 2);
            }
            case Node.ScalarN s -> sb.append(' ').append(renderScalar(s, false)).append('\n');
            case Node.SeqN q when isFlow(q) -> sb.append(' ').append(renderFlow(q)).append('\n');
            case Node.SeqN q -> {
                sb.append('\n');
                writeSeq(sb, q, indent + 2);
            }
            case Node.MapN m when m.entries().isEmpty() -> sb.append(" {}\n");
            case Node.MapN m -> {
                sb.append('\n');
                writeMap(sb, m, indent + 2, null);
            }
        }
    }

    private void writeSeq(StringBuilder sb, Node.SeqN seq, int indent) {
        for (Node item : seq.items()) {
            switch (item) {
                case Node.ScalarN s -> sb.append(" ".repeat(indent)).append("- ")
                        .append(renderScalar(s, false)).append('\n');
                case Node.MapN m when m.entries().isEmpty() ->
                        sb.append(" ".repeat(indent)).append("- {}\n");
                case Node.MapN m -> writeMap(sb, m, indent + 2, " ".repeat(indent) + "- ");
                case Node.SeqN nested when isFlow(nested) -> sb.append(" ".repeat(indent)).append("- ")
                        .append(renderFlow(nested)).append('\n');
                case Node.SeqN nested -> {
                    sb.append(" ".repeat(indent)).append("-\n");
                    writeSeq(sb, nested, indent + 2);
                }
            }
        }
    }

    private boolean isFlow(Node.SeqN seq) {
        return seq.items().stream().allMatch(
                n -> n instanceof Node.ScalarN s && s.style() != Node.Style.LITERAL);
    }

    private String renderFlow(Node.SeqN seq) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < seq.items().size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(renderScalar((Node.ScalarN) seq.items().get(i), true));
        }
        return sb.append(']').toString();
    }

    private String renderKey(String key) {
        return plainSafe(key, false) ? key : quote(key);
    }

    private String renderScalar(Node.ScalarN s, boolean flow) {
        Object v = s.value();
        if (v instanceof Boolean || v instanceof Number) {
            return String.valueOf(v);
        }
        String text = (String) v;
        if (s.style() == Node.Style.EXPRESSION) {
            return quote(text);
        }
        return plainSafe(text, flow) ? text : quote(text);
    }

    private boolean plainSafe(String s, boolean flow) {
        if (s.isEmpty()) {
            return false;
        }
        if (LEADING_INDICATORS.indexOf(s.charAt(0)) >= 0) {
            return false;
        }
        char last = s.charAt(s.length() - 1);
        if (last == ' ' || last == '\t' || last == ':') {
            return false;
        }
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c < 0x20 || c == 0x7F) {
                return false;
            }
            // Flow plain scalars cannot carry mapping indicators such as the question mark in a
            // regex lookaround; quote them so the canonical text remains parseable.
            if (flow && ",[]{}?:".indexOf(c) >= 0) {
                return false;
            }
        }
        if (s.contains(": ") || s.contains(" #")) {
            return false;
        }
        return !NON_STRING_PLAIN.matcher(s).matches();
    }

    private String quote(String s) {
        StringBuilder sb = new StringBuilder("\"");
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20 || c == 0x7F) {
                        sb.append(String.format("\\u%04X", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        return sb.append('"').toString();
    }

    private String chompIndicator(String text) {
        if (!text.endsWith("\n")) {
            return "-";
        }
        return text.endsWith("\n\n") ? "+" : "";
    }

    private void appendLiteralLines(StringBuilder sb, String text, int indent) {
        String content = text.endsWith("\n") ? text.substring(0, text.length() - 1) : text;
        for (String line : content.split("\n", -1)) {
            if (line.isEmpty()) {
                sb.append('\n');
            } else {
                sb.append(" ".repeat(indent)).append(line).append('\n');
            }
        }
    }
}
