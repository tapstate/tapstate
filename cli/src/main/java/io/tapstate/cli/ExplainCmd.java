package io.tapstate.cli;

import io.tapstate.core.schema.SchemaNavigator;
import io.tapstate.core.schema.SchemaNode;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;

/**
 * {@code explain} without a server target — the grammar field manual, kubectl-explain style. Backed by the same-source
 * {@code tapstate/v1} JSON Schema through {@link SchemaNavigator}: with no path it lists the resource
 * kinds; a dotted path resolves to a field and shows its type, description, allowed enum values and
 * child fields. {@code text} is the human surface (colour-aware); {@code json} / {@code yaml} write a
 * stable node envelope for scripts and AI. A path that does not exist in the grammar is a CLI usage
 * affordance — a plain stderr message and exit 2, not a coded domain diagnostic, in every format.
 */
@Command(name = "explain", mixinStandardHelpOptions = true,
        description = "Explain a grammar field path offline; with a server target, explain one pipeline.")
final class ExplainCmd implements Callable<Integer> {

    /** Exit code when the requested field path does not exist in the grammar. */
    static final int EXIT_USAGE = 2;

    /** The grammar version this build's bundled schema targets. */
    private static final String SCHEMA_VERSION = "tapstate/v1";

    @Spec
    CommandSpec spec;

    @Parameters(index = "0", arity = "0..1",
            description = "Dotted field path to explain (e.g. source.mode); omit to list the resource kinds.")
    String path;

    @Option(names = {"-o", "--output"}, paramLabel = "FORMAT",
            description = "Output format: text, json or yaml (default: text).",
            defaultValue = "text", completionCandidates = OutputFormat.Candidates.class)
    OutputFormat output;

    @Override
    public Integer call() {
        SchemaNavigator nav = SchemaNavigator.bundled();
        String target = path == null ? "" : path;
        Optional<SchemaNode> node = nav.navigate(target);
        if (node.isEmpty()) {
            PrintWriter err = CliIo.err(spec);
            err.println("no such grammar field path: " + target);
            err.println("(run 'explain' with no path to list the resource kinds)");
            err.flush();
            return EXIT_USAGE;
        }
        SchemaNode n = node.get();
        switch (output) {
            case JSON -> emit(JsonOut.write(envelope(nav, n)));
            case YAML -> emit(YamlOut.write(envelope(nav, n)));
            default -> emitText(nav, n);
        }
        return 0;
    }

    private void emit(String body) {
        PrintWriter out = CliIo.out(spec);
        out.println(body);
        out.flush();
    }

    // --- structured (json / yaml) envelope ---------------------------------

    private Map<String, Object> envelope(SchemaNavigator nav, SchemaNode n) {
        Map<String, Object> env = new LinkedHashMap<>();
        env.put("path", n.path());
        env.put("type", n.type());
        env.put("description", n.description());
        env.put("required", n.isRequired());
        if (!n.enumValues().isEmpty()) {
            List<Object> values = new ArrayList<>();
            for (SchemaNode.EnumValue v : n.enumValues()) {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("value", v.value());
                m.put("description", v.description());
                values.add(m);
            }
            env.put("values", values);
        }
        if (!n.children().isEmpty()) {
            List<Object> fields = new ArrayList<>();
            for (String child : n.children()) {
                fields.add(fieldSummary(nav, n.path(), child));
            }
            env.put("fields", fields);
        }
        return env;
    }

    private Map<String, Object> fieldSummary(SchemaNavigator nav, String parentPath, String name) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("name", name);
        nav.navigate(childPath(parentPath, name)).ifPresent(c -> {
            m.put("type", c.type());
            m.put("required", c.isRequired());
            m.put("description", c.description());
        });
        return m;
    }

    // --- human (text) rendering --------------------------------------------

    private void emitText(SchemaNavigator nav, SchemaNode n) {
        PrintWriter out = CliIo.out(spec);
        String header = n.path().isEmpty() ? SCHEMA_VERSION : n.path();
        out.println(Ansi.AUTO.string("@|bold " + header + "|@"));
        if (n.description() != null) {
            out.println(n.description());
        }
        out.println();
        out.println("TYPE: " + n.type() + (n.isRequired() ? " (required)" : ""));
        if (!n.enumValues().isEmpty()) {
            out.println();
            out.println(Ansi.AUTO.string("@|bold VALUES:|@"));
            int width = n.enumValues().stream().mapToInt(v -> v.value().length()).max().orElse(0);
            for (SchemaNode.EnumValue v : n.enumValues()) {
                out.println("  " + pad(v.value(), width) + "  " + orEmpty(v.description()));
            }
        }
        if (!n.children().isEmpty()) {
            out.println();
            out.println(Ansi.AUTO.string("@|bold " + (n.path().isEmpty() ? "KINDS:" : "FIELDS:") + "|@"));
            int width = n.children().stream().mapToInt(String::length).max().orElse(0);
            for (String child : n.children()) {
                SchemaNode c = nav.navigate(childPath(n.path(), child)).orElse(null);
                String type = c != null ? c.type() : "";
                boolean required = c != null && c.isRequired();
                out.println("  " + pad(child, width) + "  " + type + (required ? "  (required)" : ""));
            }
        }
        out.flush();
    }

    private static String childPath(String parentPath, String name) {
        return parentPath.isEmpty() ? name : parentPath + "." + name;
    }

    private static String pad(String s, int width) {
        return s + " ".repeat(Math.max(0, width - s.length()));
    }

    private static String orEmpty(String s) {
        return s == null ? "" : s;
    }
}
