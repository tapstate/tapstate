package io.tapstate.cli;

import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.core.dsl.DslException;
import io.tapstate.core.dsl.ReferenceGraph;
import io.tapstate.core.dsl.WorkspaceLoader;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.ServeResource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.TableRef;
import io.tapstate.core.model.TransformResource;
import io.tapstate.core.model.ViewResource;
import io.tapstate.messages.MessageCatalog;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;

/**
 * {@code desc} — the rich single-resource view. Where {@code ls} lists, {@code desc} resolves one id
 * structurally within the workspace and reports four things: a header (its structural kind, id and
 * file path), a per-kind field summary, the workspace validation status (the same gate {@code
 * validate} runs), and the reference relationships both ways — what this resource references and who
 * references it ({@link ReferenceGraph}). It is a read-only describe, not a gate: it exits 0 even when
 * the workspace fails validation, reporting that status as part of the description.
 *
 * <p>The outcome renders three ways via {@code -o}: {@code text} is the human surface; {@code json} /
 * {@code yaml} write a stable structured describe object to stdout. An id that resolves to no resource
 * is the one failure: a {@code cli.resource-not-found} coded diagnostic. Honesty mirrors {@code ls} —
 * an unreadable file is described as such rather than dropped, and a file whose declared kind does not
 * match its directory is flagged misplaced instead of being rendered with the wrong shape.
 */
@Command(name = "desc", mixinStandardHelpOptions = true,
        description = "Describe one workspace resource: summary, validation status and references.")
final class DescCmd implements Callable<Integer> {

    /** Exit code when the id resolves to no resource (a coded {@code cli.resource-not-found}). */
    static final int EXIT_DIAGNOSTIC = 1;

    @Spec
    CommandSpec spec;

    @Mixin
    WorkspaceOption workspace;

    @Parameters(index = "0", arity = "1", paramLabel = "ID",
            description = "Id of the resource to describe.")
    String id;

    @Option(names = {"-o", "--output"}, paramLabel = "FORMAT",
            description = "Output format: text, json or yaml (default: text).",
            defaultValue = "text", completionCandidates = OutputFormat.Candidates.class)
    OutputFormat output;

    @Override
    public Integer call() {
        Path root = workspace.root();
        List<WorkspaceScan.Artifact> scanned = WorkspaceScan.of(root);
        Optional<WorkspaceScan.Artifact> match = scanned.stream().filter(e -> e.id().equals(id)).findFirst();
        if (match.isEmpty()) {
            return emitNotFound();
        }
        List<Resource> batch = scanned.stream()
                .map(WorkspaceScan.Artifact::resource).filter(r -> r != null).toList();
        emit(root, match.get(), ReferenceGraph.of(batch), validate(root));
        return 0;
    }

    // ---- validation status (mirrors `validate` over the workspace root) ------------------

    /** valid | invalid (a coded diagnostic) | error (an IO fault reading the tree). */
    private record Validation(String status, TapstateErrorCode code, Map<String, Object> args,
                              String source, int line, int column, String detail) {
        static Validation valid() {
            return new Validation("valid", null, null, null, 0, 0, null);
        }

        static Validation invalid(TapstateErrorCode code, Map<String, Object> args, String source, int line, int column) {
            return new Validation("invalid", code, args, source, line, column, null);
        }

        static Validation error(String detail) {
            return new Validation("error", null, null, null, 0, 0, detail);
        }
    }

    private static Validation validate(Path root) {
        // structure is truth first, then the full DSL stack — the same order `validate` uses on the root
        Optional<WorkspaceLayout.Misplacement> misplaced = WorkspaceLayout.firstMisplacement(root);
        if (misplaced.isPresent()) {
            WorkspaceLayout.Misplacement m = misplaced.get();
            String file = m.file().getFileName().toString();
            return Validation.invalid(CliError.KIND_DIR_MISMATCH,
                    Map.of("path", file, "kind", m.declaredKind(), "dir", m.parentDir()), file, 0, 0);
        }
        try {
            WorkspaceLoader.load(root);
            return Validation.valid();
        } catch (DslException e) {
            return Validation.invalid(e.code(), e.args(), e.source(), e.line(), e.column());
        } catch (UncheckedIOException e) {
            Throwable cause = e.getCause();
            return Validation.error(cause != null ? cause.getMessage() : e.getMessage());
        }
    }

    // ---- rendering ----------------------------------------------------------------------

    private void emit(Path root, WorkspaceScan.Artifact entry, ReferenceGraph graph, Validation validation) {
        switch (output) {
            case JSON -> {
                PrintWriter out = CliIo.out(spec);
                out.println(JsonOut.write(describe(root, entry, graph, validation)));
                out.flush();
            }
            case YAML -> {
                PrintWriter out = CliIo.out(spec);
                out.println(YamlOut.write(describe(root, entry, graph, validation)));
                out.flush();
            }
            default -> emitText(root, entry, graph, validation);
        }
    }

    /** The structured describe object — the machine surface shared by json / yaml. */
    private static Map<String, Object> describe(Path root, WorkspaceScan.Artifact entry,
            ReferenceGraph graph, Validation validation) {
        String resolvedId = entry.id();
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", resolvedId);
        m.put("kind", entry.kind());
        m.put("path", relativize(root, entry.file()));
        Resource r = entry.resource();
        if (r == null) {
            m.put("readable", false);
        } else if (entry.misplaced()) {
            // structure is truth: never emit the wrong kind's summary; flag the conflict instead
            m.put("misplaced", true);
            m.put("declaredKind", r.kind());
        } else {
            m.put("summary", summary(r));
        }
        m.put("validation", validationMap(validation));
        m.put("references", edges(graph.references(resolvedId)));
        m.put("referencedBy", edges(graph.referencedBy(resolvedId)));
        return m;
    }

    private void emitText(Path root, WorkspaceScan.Artifact entry, ReferenceGraph graph, Validation validation) {
        String resolvedId = entry.id();
        PrintWriter out = CliIo.out(spec);
        out.println(Ansi.AUTO.string("@|bold " + entry.kind() + "|@") + "  " + resolvedId);
        line(out, "path", relativize(root, entry.file()));
        Resource r = entry.resource();
        if (r == null) {
            out.println("  (unreadable)");
        } else if (entry.misplaced()) {
            line(out, "misplaced", "declares '" + r.kind() + "'");
        } else {
            for (Map.Entry<String, Object> field : summary(r).entrySet()) {
                line(out, field.getKey(), textValue(field.getValue()));
            }
        }
        line(out, "validation", validationText(validation));
        if ("invalid".equals(validation.status())) {
            MessageCatalog.Rendered rendered = MessageCatalog.bundled().render(validation.code(), validation.args());
            out.println("    " + rendered.message());
        }
        line(out, "references", edgeText(graph.references(resolvedId)));
        line(out, "referenced by", edgeText(graph.referencedBy(resolvedId)));
        out.flush();
    }

    // ---- per-kind summary ---------------------------------------------------------------

    /** The per-kind field summary; a richer view than {@code ls}'s one-liner, still not the full document. */
    private static Map<String, Object> summary(Resource r) {
        Map<String, Object> m = new LinkedHashMap<>();
        if (r instanceof SourceResource s) {
            m.put("connector", s.connector());
            if (s.mode() != null) {
                m.put("mode", s.mode().yaml());
            }
            if (s.tables() != null && !s.tables().isEmpty()) {
                m.put("tables", tableNames(s.tables()));
            }
        } else if (r instanceof PipelineResource p) {
            m.put("sources", List.copyOf(p.sourceIds()));
            m.put("transforms", p.transforms() == null ? 0 : p.transforms().size());
            m.put("view", p.view() != null);
            m.put("serve", p.serve() != null);
        } else if (r instanceof TransformResource t) {
            m.put("type", t.body().type());
        } else if (r instanceof ViewResource v) {
            if (v.primaryKey() != null) {
                m.put("primaryKey", v.primaryKey());
            }
        } else if (r instanceof ServeResource serve) {
            m.put("sync", serve.sync() == null ? 0 : serve.sync().size());
            m.put("query", serve.query() == null ? 0 : serve.query().size());
            m.put("push", serve.push() == null ? 0 : serve.push().size());
        }
        return m;
    }

    /** Table selectors as display strings: a bare name, a {@code /regex/}, or an object form's name. */
    private static List<String> tableNames(List<TableRef> tables) {
        List<String> names = new ArrayList<>();
        for (TableRef t : tables) {
            switch (t) {
                case TableRef.Literal l -> names.add(l.name());
                case TableRef.Spec sp -> names.add(sp.name());
                case TableRef.Regex re -> names.add("/" + re.pattern() + "/");
            }
        }
        return names;
    }

    // ---- references ---------------------------------------------------------------------

    private static List<Map<String, Object>> edges(List<ReferenceGraph.Edge> edges) {
        List<Map<String, Object>> out = new ArrayList<>();
        for (ReferenceGraph.Edge e : edges) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("id", e.id());
            m.put("kind", e.kind());
            out.add(m);
        }
        return out;
    }

    private static String edgeText(List<ReferenceGraph.Edge> edges) {
        if (edges.isEmpty()) {
            return "(none)";
        }
        List<String> parts = new ArrayList<>();
        for (ReferenceGraph.Edge e : edges) {
            parts.add(e.id() + " (" + e.kind() + ")");
        }
        return String.join(", ", parts);
    }

    // ---- validation rendering -----------------------------------------------------------

    private static Map<String, Object> validationMap(Validation v) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("status", v.status());
        if ("invalid".equals(v.status())) {
            m.put("diagnostic", Diagnostics.map(v.code(), v.args(), v.source(), v.line(), v.column()));
        } else if ("error".equals(v.status())) {
            m.put("detail", v.detail());
        }
        return m;
    }

    private static String validationText(Validation v) {
        return switch (v.status()) {
            case "invalid" -> "invalid (" + v.code().code() + ")";
            case "error" -> "error: " + v.detail();
            default -> "valid";
        };
    }

    // ---- not found ----------------------------------------------------------------------

    private int emitNotFound() {
        TapstateErrorCode code = CliError.RESOURCE_NOT_FOUND;
        Map<String, Object> args = Map.of("id", id);
        switch (output) {
            case JSON -> {
                PrintWriter out = CliIo.out(spec);
                out.println(JsonOut.write(notFoundEnvelope(code, args)));
                out.flush();
            }
            case YAML -> {
                PrintWriter out = CliIo.out(spec);
                out.println(YamlOut.write(notFoundEnvelope(code, args)));
                out.flush();
            }
            default -> Diagnostics.printText(CliIo.err(spec), code, args);
        }
        return EXIT_DIAGNOSTIC;
    }

    /** The not-found machine envelope: a top-level {@code status} discriminator like the other coded verbs. */
    private static Map<String, Object> notFoundEnvelope(TapstateErrorCode code, Map<String, Object> args) {
        Map<String, Object> env = new LinkedHashMap<>();
        env.put("status", "error");
        env.put("diagnostics", List.of(Diagnostics.map(code, args, null, 0, 0)));
        return env;
    }

    // ---- text helpers -------------------------------------------------------------------

    private static String relativize(Path root, Path file) {
        try {
            return root.relativize(file).toString();
        } catch (IllegalArgumentException notRelative) {
            return file.toString();
        }
    }

    private static void line(PrintWriter out, String label, String value) {
        out.println("  " + pad(label) + value);
    }

    private static String pad(String label) {
        String padded = label + "             ";
        return padded.substring(0, Math.max(label.length(), 14));
    }

    private static String textValue(Object value) {
        if (value instanceof List<?> list) {
            List<String> parts = new ArrayList<>();
            for (Object o : list) {
                parts.add(String.valueOf(o));
            }
            return String.join(", ", parts);
        }
        if (value instanceof Boolean b) {
            return b ? "yes" : "no";
        }
        return String.valueOf(value);
    }
}
