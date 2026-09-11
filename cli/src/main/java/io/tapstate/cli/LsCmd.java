package io.tapstate.cli;

import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceResource;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * {@code ls} — the offline workspace browser. Structure is truth: each {@code <root>/<kind>/}
 * directory holds that kind's artifacts, so a bare {@code ls} reads every kind directory and groups
 * its listing by kind, while {@code ls <kind>} reads just one. Each artifact shows its {@code id} and
 * a per-kind summary (a source's connector and read mode; a pipeline's source count and which output
 * surfaces it carries). The outcome renders three ways via {@code -o}: {@code text} is the human
 * surface; {@code json} / {@code yaml} write a stable structured envelope to stdout for scripts and AI.
 *
 * <p>An empty or not-yet-created workspace is a normal browse outcome (it lists nothing), not an
 * error. A file that cannot be parsed is still listed — marked unreadable rather than silently
 * dropped. A well-formed file whose declared kind does not match its directory is listed in its
 * structural slot but flagged misplaced, so the listing never claims the wrong shape. Both are left
 * to {@code validate} to diagnose.
 */
@Command(name = "ls", mixinStandardHelpOptions = true,
        description = {
                "List workspace resources by kind, or limit to a single kind.",
                "This is the local listing. In a connected session `ls` lists what the server holds"
                        + " instead, and takes no options there yet."})
final class LsCmd implements Callable<Integer> {

    /** Exit code when the kind argument is not one of the known resource kinds. */
    static final int EXIT_USAGE = 2;

    @Spec
    CommandSpec spec;

    @Mixin
    WorkspaceOption workspace;

    @Parameters(index = "0", arity = "0..1", paramLabel = "KIND",
            description = "Limit to one kind: source, pipeline, transform, view or serve.")
    String kind;

    @Option(names = {"-o", "--output"}, paramLabel = "FORMAT",
            description = "Output format: text, json or yaml (default: text).",
            defaultValue = "text", completionCandidates = OutputFormat.Candidates.class)
    OutputFormat output;

    @Override
    public Integer call() {
        if (kind != null && !WorkspaceScan.KINDS.contains(kind)) {
            PrintWriter err = CliIo.err(spec);
            err.println("ls: unknown kind '" + kind + "' (expected one of: "
                    + String.join(", ", WorkspaceScan.KINDS) + ")");
            err.flush();
            return EXIT_USAGE;
        }
        Path root = workspace.root();
        List<WorkspaceScan.Artifact> entries = new ArrayList<>(WorkspaceScan.of(root).stream()
                .filter(a -> kind == null || a.kind().equals(kind))
                .toList());
        // within each kind directory, list by id (the scan delivers kind-grouped, filename order)
        entries.sort(Comparator.comparingInt((WorkspaceScan.Artifact a) -> WorkspaceScan.KINDS.indexOf(a.kind()))
                .thenComparing(WorkspaceScan.Artifact::id));
        emit(root, entries);
        return 0;
    }

    private void emit(Path root, List<WorkspaceScan.Artifact> entries) {
        switch (output) {
            case JSON -> {
                PrintWriter out = CliIo.out(spec);
                out.println(JsonOut.write(envelope(root, entries)));
                out.flush();
            }
            case YAML -> {
                PrintWriter out = CliIo.out(spec);
                out.println(YamlOut.write(envelope(root, entries)));
                out.flush();
            }
            default -> emitText(root, entries);
        }
    }

    private void emitText(Path root, List<WorkspaceScan.Artifact> entries) {
        PrintWriter out = CliIo.out(spec);
        if (entries.isEmpty()) {
            out.println("no resources in " + root);
            out.flush();
            return;
        }
        Map<String, List<WorkspaceScan.Artifact>> byKind = new LinkedHashMap<>();
        for (WorkspaceScan.Artifact e : entries) {
            byKind.computeIfAbsent(e.kind(), k -> new ArrayList<>()).add(e);
        }
        for (Map.Entry<String, List<WorkspaceScan.Artifact>> group : byKind.entrySet()) {
            List<WorkspaceScan.Artifact> es = group.getValue();
            out.println(Ansi.AUTO.string("@|bold " + group.getKey() + "|@") + " (" + es.size() + ")");
            int width = es.stream().mapToInt(e -> e.id().length()).max().orElse(0);
            for (WorkspaceScan.Artifact e : es) {
                String summary = summary(e);
                if (summary.isEmpty()) {
                    out.println("  " + e.id());
                } else {
                    out.println("  " + pad(e.id(), width) + "  " + summary);
                }
            }
        }
        out.flush();
    }

    /**
     * The human one-line summary for an entry's kind; empty when the kind carries no summary fields.
     * Shared with the guided first run, which describes each file it wrote in these same words: one
     * place holds the wording, so what {@code new} says a file is for is what {@code ls} then shows.
     */
    static String summary(WorkspaceScan.Artifact e) {
        if (e.resource() == null) {
            return "(unreadable)";
        }
        if (e.misplaced()) {
            // structure is truth: a file declaring a different kind than its directory is misplaced —
            // surfaced honestly rather than rendered with the wrong kind's summary; validate diagnoses it
            return "(misplaced: declares '" + e.resource().kind() + "')";
        }
        if (e.resource() instanceof SourceResource s) {
            return s.mode() != null ? s.connector() + ", " + s.mode().yaml() : s.connector();
        }
        if (e.resource() instanceof PipelineResource p) {
            int n = p.sources().size();
            StringBuilder sb = new StringBuilder().append(n).append(n == 1 ? " source" : " sources");
            if (p.view() != null) {
                sb.append(", view");
            }
            if (p.serve() != null) {
                sb.append(", serve");
            }
            return sb.toString();
        }
        // transform / view / serve carry no list-level summary in this build; `desc` is the rich view
        return "";
    }

    private static String pad(String s, int width) {
        return s.length() >= width ? s : s + " ".repeat(width - s.length());
    }

    private static Map<String, Object> envelope(Path root, List<WorkspaceScan.Artifact> entries) {
        Map<String, Object> env = new LinkedHashMap<>();
        env.put("workspace", root.toString());
        List<Map<String, Object>> resources = new ArrayList<>();
        for (WorkspaceScan.Artifact e : entries) {
            resources.add(entryMap(e));
        }
        env.put("resources", resources);
        return env;
    }

    private static Map<String, Object> entryMap(WorkspaceScan.Artifact e) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("kind", e.kind());
        m.put("id", e.id());
        if (e.resource() == null) {
            m.put("readable", false);
            return m;
        }
        if (e.misplaced()) {
            // the kind discriminator names the structural slot; declaredKind tells the truth, so the
            // envelope never claims a payload shape it does not carry (no source fields under kind:pipeline)
            m.put("misplaced", true);
            m.put("declaredKind", e.resource().kind());
            return m;
        }
        if (e.resource() instanceof SourceResource s) {
            m.put("connector", s.connector());
            if (s.mode() != null) {
                m.put("mode", s.mode().yaml());
            }
        } else if (e.resource() instanceof PipelineResource p) {
            m.put("sources", p.sources().size());
            m.put("view", p.view() != null);
            m.put("serve", p.serve() != null);
        }
        return m;
    }
}
