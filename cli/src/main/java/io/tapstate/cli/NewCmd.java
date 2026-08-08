package io.tapstate.cli;

import io.tapstate.core.catalog.ConnectorCatalogEntry;
import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslError;
import io.tapstate.core.model.FieldRule;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.ServeResource;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.model.TransformResource;
import io.tapstate.core.model.ViewResource;
import io.tapstate.core.model.canonical.CanonicalWriter;
import io.tapstate.messages.MessageCatalog;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * {@code new} — the catalog-driven scaffolding wizard. One entry, two paths that produce the same
 * canonical artifact: an interactive prompt flow (bare {@code new} at a terminal) and a
 * non-interactive flag-supplied flow (scripting / AI). Both feed a shared output contract: write
 * {@code <id>.tap.yml}, refuse to clobber unless {@code --force}, {@code --dry-run} previews on
 * stdout, and {@code -o json|yaml} reports a structured result envelope.
 */
@Command(name = "new", mixinStandardHelpOptions = true,
        description = "Scaffold a new artifact (source, pipeline, transform, view or serve) as a canonical *.tap.yml.")
final class NewCmd implements Callable<Integer> {

    /** Exit code when a coded domain diagnostic is reported (e.g. the target already exists). */
    static final int EXIT_DIAGNOSTIC = 1;
    /** Exit code when the request cannot be carried out as given (missing answer, unusable target). */
    static final int EXIT_USAGE = 2;

    @Spec
    CommandSpec spec;

    @Mixin
    WorkspaceOption workspace;

    @Option(names = {"-y", "--non-interactive"},
            description = "Never prompt; take every answer from flags (scripting / AI).")
    boolean nonInteractive;

    @Option(names = "--kind", paramLabel = "KIND",
            description = "Resource kind to scaffold: source, pipeline, transform, view or serve.")
    String kind;

    @Option(names = "--type", paramLabel = "TYPE",
            description = "Transform type (transform kind): filter, map, js, union, nest or join.")
    String type;

    @Option(names = {"-c", "--connector"}, paramLabel = "ID",
            description = "Connector id from the catalog (source kind).")
    String connector;

    @Option(names = "--id", paramLabel = "ID",
            description = "Top-level id of the scaffolded resource.")
    String id;

    @Option(names = {"-m", "--mode"}, paramLabel = "MODE",
            description = "Source read mode (cdc, snapshot, stream, file, api) — must suit the connector.")
    SourceMode mode;

    @Option(names = "--set", paramLabel = "KEY=VALUE",
            description = "A connector config entry (repeatable).")
    Map<String, String> config = new LinkedHashMap<>();

    @Option(names = "--source", paramLabel = "ID",
            description = "Source id the pipeline reads from (pipeline kind; repeatable).")
    List<String> sources = new ArrayList<>();

    @Option(names = "--sync-to", paramLabel = "ID",
            description = "Target source id to sync the pipeline output to (pipeline kind; repeatable).")
    List<String> syncTo = new ArrayList<>();

    @Option(names = "--out", paramLabel = "DIR",
            description = "Write the artifact flat into this exact directory, bypassing the workspace layout.")
    String out;

    @Option(names = "--force",
            description = "Overwrite an existing artifact at the target path.")
    boolean force;

    @Option(names = "--dry-run",
            description = "Preview the canonical artifact on stdout without writing any file.")
    boolean dryRun;

    @Option(names = {"-o", "--output"}, paramLabel = "FORMAT",
            description = "Output format for the result report: text, json or yaml (default: text).",
            defaultValue = "text", completionCandidates = OutputFormat.Candidates.class)
    OutputFormat output;

    /** Test seam: an injected prompter forces the interactive path; production opens a JLine one. */
    Prompter prompter;

    @Override
    public Integer call() {
        PrintWriter err = CliIo.err(spec);
        String resolved = kind == null ? "source" : kind;
        if (type != null && !"transform".equals(resolved)) {
            err.println("new: --type is only valid for --kind transform");
            err.flush();
            return EXIT_USAGE;
        }
        return switch (resolved) {
            case "source" -> callSource(err);
            case "pipeline" -> callPipeline(err);
            case "transform" -> callTransform(err);
            case "view" -> callView(err);
            case "serve" -> callServe(err);
            default -> {
                err.println("new: --kind must be 'source', 'pipeline', 'transform', 'view' or 'serve'");
                err.flush();
                yield EXIT_USAGE;
            }
        };
    }

    private int callSource(PrintWriter err) {
        if (!sources.isEmpty() || !syncTo.isEmpty()) {
            err.println("new: --source/--sync-to are not valid for --kind source");
            err.flush();
            return EXIT_USAGE;
        }
        boolean interactive = prompter != null
                || (!nonInteractive && connector == null && System.console() != null);
        if (!interactive && (id == null || connector == null)) {
            err.println("new: provide --id and --connector, or run interactively at a terminal");
            err.flush();
            return EXIT_USAGE;
        }
        try {
            TapstateCatalog catalog = TapstateCatalog.load();
            Resource resource = interactive ? runWizard(catalog) : buildFromFlags(catalog);
            return emit(resource);
        } catch (TapstateException e) {
            return emitDiagnostic(e);
        } catch (IOException e) {
            err.println("new: cannot write artifact: " + e.getMessage());
            err.flush();
            return EXIT_USAGE;
        }
    }

    private int callPipeline(PrintWriter err) {
        if (connector != null || mode != null || !config.isEmpty()) {
            err.println("new: --connector/--mode/--set are not valid for --kind pipeline");
            err.flush();
            return EXIT_USAGE;
        }
        boolean complete = id != null && !sources.isEmpty() && !syncTo.isEmpty();
        boolean interactive = prompter != null
                || (!nonInteractive && !complete && System.console() != null);
        if (!interactive && !complete) {
            err.println("new: provide --id, --source and --sync-to, or run interactively at a terminal");
            err.flush();
            return EXIT_USAGE;
        }
        try {
            Resource resource = interactive ? runPipelineWizard() : buildPipelineFromFlags();
            return emit(resource);
        } catch (TapstateException e) {
            return emitDiagnostic(e);
        } catch (IOException e) {
            err.println("new: cannot write artifact: " + e.getMessage());
            err.flush();
            return EXIT_USAGE;
        }
    }

    private Resource runPipelineWizard() throws IOException {
        Path dir = discoveryRoot();
        List<String> sourceIds = WorkspaceSources.idsIn(dir);
        List<String> transformIds = WorkspaceSources.idsOfKind(dir, "transform");
        List<String> viewIds = WorkspaceSources.idsOfKind(dir, "view");
        List<String> serveIds = WorkspaceSources.idsOfKind(dir, "serve");
        if (prompter != null) {
            return new PipelineWizard(prompter, sourceIds, transformIds, viewIds, serveIds).run();
        }
        try (JLinePrompter jline = JLinePrompter.system()) {
            return new PipelineWizard(jline, sourceIds, transformIds, viewIds, serveIds).run();
        }
    }

    private Resource buildPipelineFromFlags() {
        List<SyncElement> legs = new ArrayList<>();
        for (int i = 0; i < syncTo.size(); i++) {
            legs.add(new SyncElement("sync_" + (i + 1), syncTo.get(i), null, null, null, null));
        }
        ServeBlock serve = new ServeBlock.Inline("serve", FromRef.regex(".*"), legs, null, null);
        return new PipelineResource(id, null, List.copyOf(sources), null, null, serve, null, null);
    }

    private int callTransform(PrintWriter err) {
        // a transform is pure logic (X19): no connector, mode, config or pipeline-wiring flags
        if (connector != null || mode != null || !config.isEmpty() || !sources.isEmpty() || !syncTo.isEmpty()) {
            err.println("new: --connector/--mode/--set/--source/--sync-to are not valid for --kind transform");
            err.flush();
            return EXIT_USAGE;
        }
        if (type != null && !TransformBodyPrompter.TYPES.contains(type)) {
            err.println("new: --type must be one of " + String.join(", ", TransformBodyPrompter.TYPES));
            err.flush();
            return EXIT_USAGE;
        }
        // partial flags at a terminal launch the wizard (as for pipeline); -y demands the full set
        boolean complete = id != null && type != null;
        boolean interactive = prompter != null || (!nonInteractive && !complete && System.console() != null);
        if (!interactive && !complete) {
            err.println("new: provide --id and --type, or run interactively at a terminal");
            err.flush();
            return EXIT_USAGE;
        }
        try {
            Resource resource = interactive ? runTransformWizard() : buildTransformFromFlags();
            return emit(resource);
        } catch (TapstateException e) {
            return emitDiagnostic(e);
        } catch (IOException e) {
            err.println("new: cannot write artifact: " + e.getMessage());
            err.flush();
            return EXIT_USAGE;
        }
    }

    private Resource runTransformWizard() throws IOException {
        if (prompter != null) {
            return new TransformWizard(prompter).run();
        }
        try (JLinePrompter jline = JLinePrompter.system()) {
            return new TransformWizard(jline).run();
        }
    }

    private Resource buildTransformFromFlags() {
        return new TransformResource(id, null, scaffoldTransformBody(type), null, null);
    }

    /**
     * The minimal valid body for a non-interactive scaffold of {@code type}: union is complete, every
     * content-bearing type carries an editable placeholder so the artifact still parses and validates.
     */
    private static TransformBody scaffoldTransformBody(String type) {
        return switch (type) {
            case "union" -> new TransformBody.Union();
            case "filter" -> new TransformBody.Filter("op != 'd'");
            case "js" -> new TransformBody.Js("emit(after)\n");
            case "map" -> new TransformBody.MapProjection(Map.of("id", FieldRule.rename("id")));
            case "nest" -> new TransformBody.Nest(null, null, new NestRoot("main", null, null, null, null));
            case "join" -> new TransformBody.Join("duckdb", "SELECT * FROM a\n");
            default -> throw new IllegalStateException("unhandled transform type: " + type);
        };
    }

    private int callView(PrintWriter err) {
        if (rejectsDefinitionFlags(err, "view")) {
            return EXIT_USAGE;
        }
        boolean interactive = prompter != null || (!nonInteractive && id == null && System.console() != null);
        if (!interactive && id == null) {
            err.println("new: provide --id, or run interactively at a terminal");
            err.flush();
            return EXIT_USAGE;
        }
        try {
            Resource resource = interactive ? runViewWizard() : new ViewResource(id, null, null, null, null, null);
            return emit(resource);
        } catch (TapstateException e) {
            return emitDiagnostic(e);
        } catch (IOException e) {
            err.println("new: cannot write artifact: " + e.getMessage());
            err.flush();
            return EXIT_USAGE;
        }
    }

    private int callServe(PrintWriter err) {
        if (rejectsDefinitionFlags(err, "serve")) {
            return EXIT_USAGE;
        }
        boolean interactive = prompter != null || (!nonInteractive && id == null && System.console() != null);
        if (!interactive && id == null) {
            err.println("new: provide --id, or run interactively at a terminal");
            err.flush();
            return EXIT_USAGE;
        }
        try {
            Resource resource = interactive ? runServeWizard() : new ServeResource(id, null, null, null, null, null);
            return emit(resource);
        } catch (TapstateException e) {
            return emitDiagnostic(e);
        } catch (IOException e) {
            err.println("new: cannot write artifact: " + e.getMessage());
            err.flush();
            return EXIT_USAGE;
        }
    }

    /**
     * A standalone definition body (view / serve) is pure structure — it takes no connector, mode,
     * config or pipeline-wiring flags. Reports the offending flags and returns true when any are present.
     */
    private boolean rejectsDefinitionFlags(PrintWriter err, String kindLabel) {
        if (connector != null || mode != null || !config.isEmpty() || !sources.isEmpty() || !syncTo.isEmpty()) {
            err.println("new: --connector/--mode/--set/--source/--sync-to are not valid for --kind " + kindLabel);
            err.flush();
            return true;
        }
        return false;
    }

    private Resource runViewWizard() throws IOException {
        if (prompter != null) {
            return new ViewWizard(prompter).run();
        }
        try (JLinePrompter jline = JLinePrompter.system()) {
            return new ViewWizard(jline).run();
        }
    }

    private Resource runServeWizard() throws IOException {
        List<String> sourceIds = WorkspaceSources.idsIn(discoveryRoot());
        if (prompter != null) {
            return new ServeWizard(prompter, sourceIds).run();
        }
        try (JLinePrompter jline = JLinePrompter.system()) {
            return new ServeWizard(jline, sourceIds).run();
        }
    }

    private Resource runWizard(TapstateCatalog catalog) throws IOException {
        if (prompter != null) {
            return new SourceWizard(prompter, catalog).run();
        }
        try (JLinePrompter jline = JLinePrompter.system()) {
            return new SourceWizard(jline, catalog).run();
        }
    }

    private Resource buildFromFlags(TapstateCatalog catalog) {
        if (!catalog.ids().contains(connector)) {
            throw new TapstateException(CliError.UNKNOWN_CONNECTOR, Map.of("connector", connector), null);
        }
        ConnectorCatalogEntry entry = catalog.byId(connector);
        if (mode != null && !CapabilityHints.isModeAllowed(entry, mode)) {
            throw new TapstateException(DslError.UNSUPPORTED_MODE, Map.of(
                    "connector", connector,
                    "mode", mode.yaml(),
                    "allowed", String.join(", ", entry.modes().stream().map(SourceMode::yaml).toList()),
                    "path", "mode"), null);
        }
        Map<String, Object> cfg = new LinkedHashMap<>(config);
        return new SourceResource(id, null, connector, cfg, mode, null, null, null, null);
    }

    private int emit(Resource resource) throws IOException {
        String yaml = new CanonicalWriter().write(resource);
        if (dryRun) {
            // a preview writes nothing: emit just the canonical artifact so it can be redirected
            PrintWriter o = CliIo.out(spec);
            o.print(yaml);
            o.flush();
            return 0;
        }
        Path target = resolveTarget(resource);
        if (Files.exists(target) && !force) {
            throw new TapstateException(CliError.ARTIFACT_EXISTS, Map.of("path", target.toString()), null);
        }
        Files.createDirectories(target.getParent());
        Files.writeString(target, yaml);
        emitCreated(resource, target);
        return 0;
    }

    /**
     * The directory scanned for reusable resources offered by the pipeline wizard: an explicit
     * {@code --out} dir, otherwise the workspace root (walked recursively, so kind subdirs are found).
     */
    private Path discoveryRoot() {
        return out != null ? Path.of(out) : workspace.root();
    }

    /**
     * The file to write: an explicit {@code --out} writes flat into that directory; otherwise the
     * artifact lands under the workspace's kind subdirectory ({@code <root>/<kind>/<id>.tap.yml}).
     */
    private Path resolveTarget(Resource resource) {
        String file = resource.id() + ".tap.yml";
        return out != null
                ? Path.of(out).resolve(file)
                : workspace.root().resolve(resource.kind()).resolve(file);
    }

    private void emitCreated(Resource resource, Path target) {
        PrintWriter o = CliIo.out(spec);
        switch (output) {
            case JSON -> o.println(JsonOut.write(createdEnvelope(resource, target)));
            case YAML -> o.println(YamlOut.write(createdEnvelope(resource, target)));
            default -> o.println("created " + target);
        }
        o.flush();
    }

    private Map<String, Object> createdEnvelope(Resource resource, Path target) {
        Map<String, Object> env = new LinkedHashMap<>();
        env.put("status", "created");
        env.put("kind", resource.kind());
        env.put("id", resource.id());
        if (resource instanceof SourceResource source) {
            env.put("connector", source.connector());
        } else if (resource instanceof PipelineResource pipe) {
            env.put("sources", pipe.sources());
        } else if (resource instanceof TransformResource transform) {
            env.put("type", transform.body().type());
        } else if (resource instanceof ViewResource view && view.primaryKey() != null) {
            env.put("primary_key", view.primaryKey());
        } else if (resource instanceof ServeResource serve) {
            // a kind-specific field appears only when it has content (same convention as view's primary_key)
            List<String> surfaces = surfaceKinds(serve);
            if (!surfaces.isEmpty()) {
                env.put("surfaces", surfaces);
            }
        }
        env.put("path", target.toString());
        return env;
    }

    /** The publish surfaces a serve carries, in canonical order (sync / query / push); empty when none. */
    private static List<String> surfaceKinds(ServeResource serve) {
        List<String> kinds = new ArrayList<>();
        if (serve.sync() != null) {
            kinds.add("sync");
        }
        if (serve.query() != null) {
            kinds.add("query");
        }
        if (serve.push() != null) {
            kinds.add("push");
        }
        return kinds;
    }

    private int emitDiagnostic(TapstateException e) {
        switch (output) {
            case JSON -> {
                PrintWriter o = CliIo.out(spec);
                o.println(JsonOut.write(diagnosticEnvelope(e)));
                o.flush();
            }
            case YAML -> {
                PrintWriter o = CliIo.out(spec);
                o.println(YamlOut.write(diagnosticEnvelope(e)));
                o.flush();
            }
            default -> {
                MessageCatalog.Rendered rendered = MessageCatalog.bundled().render(e.code(), e.args());
                PrintWriter err = CliIo.err(spec);
                err.println(Ansi.AUTO.string("@|bold,red error:|@") + " " + e.code().code());
                err.println("  " + rendered.message());
                if (rendered.solution() != null) {
                    err.println("  " + rendered.solution());
                }
                err.flush();
            }
        }
        return EXIT_DIAGNOSTIC;
    }

    private static Map<String, Object> diagnosticEnvelope(TapstateException e) {
        Map<String, Object> env = new LinkedHashMap<>();
        env.put("status", "error");
        // the diagnostic body comes from the shared renderer — the single source for the coded-error shape
        env.put("diagnostics", List.of(Diagnostics.map(e.code(), e.args(), null, 0, 0)));
        return env;
    }
}
