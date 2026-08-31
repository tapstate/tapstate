package io.tapstate.cli;

import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.core.dsl.DslException;
import io.tapstate.core.dsl.Workspace;
import io.tapstate.core.dsl.WorkspaceLoader;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;

/**
 * {@code validate} — the offline DSL gate. Loads a workspace path (a directory of {@code *.tap.yml},
 * or a single artifact) through the workspace loader, which runs the full offline stack: parse,
 * structural strictness, batch reference closure and the connector capability matrix.
 *
 * <p>The validation outcome renders three ways via {@code -o}: {@code text} is the human surface
 * (colour-aware, the diagnostic to stderr); {@code json} / {@code yaml} write a stable structured
 * envelope to stdout for scripts and AI. A located validation error carries its canonical code, the
 * rendered message and its origin. A path with no artifacts, or one that cannot be read, is a CLI
 * usage error reported on stderr regardless of format — not a silent success and not a coded domain
 * diagnostic.
 */
@Command(name = "validate", mixinStandardHelpOptions = true,
        description = "Validate a workspace path (directory of *.tap.yml, or a single artifact).")
final class ValidateCmd implements Callable<Integer> {

    /** Exit code when the workspace is structurally / semantically invalid. */
    static final int EXIT_INVALID = 1;
    /** Exit code when the path cannot be used as a workspace (missing / empty / unreadable). */
    static final int EXIT_USAGE = 2;

    @Spec
    CommandSpec spec;

    @Mixin
    WorkspaceOption workspace;

    @Parameters(index = "0", arity = "0..1",
            description = "Workspace directory or artifact file (default: the workspace root).")
    String path;

    @Option(names = {"-o", "--output"}, paramLabel = "FORMAT",
            description = "Output format: text, json or yaml (default: text).",
            defaultValue = "text", completionCandidates = OutputFormat.Candidates.class)
    OutputFormat output;

    @Override
    public Integer call() {
        // an explicit positional path wins; with none, validate falls back to the workspace root
        Path target = path != null ? Path.of(path) : workspace.root();
        String display = displayPath();
        PrintWriter err = CliIo.err(spec);
        if (!Files.exists(target)) {
            err.println("path not found: " + display);
            err.flush();
            return EXIT_USAGE;
        }
        if (path == null && Files.isDirectory(target)) {
            // the managed workspace root only: structure is truth — each artifact must sit in its
            // kind's directory. An explicit path (file or directory) is ad-hoc validation and carries
            // no workspace-layout claim, so it skips this gate.
            Optional<WorkspaceLayout.Misplacement> misplaced = WorkspaceLayout.firstMisplacement(target);
            if (misplaced.isPresent()) {
                WorkspaceLayout.Misplacement m = misplaced.get();
                String file = m.file().getFileName().toString();
                emitInvalid(CliError.KIND_DIR_MISMATCH,
                        Map.of("path", file, "kind", m.declaredKind(), "dir", m.parentDir()),
                        file, 0, 0);
                return EXIT_INVALID;
            }
        }
        try {
            Workspace loaded = WorkspaceLoader.load(target);
            int n = loaded.resources().size();
            if (n == 0) {
                // an existing path with no *.tap.yml is almost always a wrong path / wrong
                // extension, not a deliberately empty workspace — surface it, do not call it valid
                err.println("no *.tap.yml artifacts found in " + display);
                err.flush();
                return EXIT_USAGE;
            }
            emitValid(n);
            return 0;
        } catch (DslException e) {
            emitInvalid(e);
            return EXIT_INVALID;
        } catch (UncheckedIOException e) {
            // a filesystem fault on user-supplied input (e.g. an unreadable artifact): render a
            // clean diagnostic rather than letting a raw stack trace reach the user boundary. A
            // coded io-domain exception at the loader boundary is the proper long-term home.
            Throwable cause = e.getCause();
            err.println("cannot read workspace " + display + ": "
                    + (cause != null ? cause.getMessage() : e.getMessage()));
            err.flush();
            return EXIT_USAGE;
        }
    }

    /** The path to echo in diagnostics: the user's exact positional, else the resolved workspace root. */
    private String displayPath() {
        return path != null ? path : workspace.root().toString();
    }

    private void emitValid(int n) {
        PrintWriter out = CliIo.out(spec);
        switch (output) {
            case JSON -> out.println(JsonOut.write(validEnvelope(n)));
            case YAML -> out.println(YamlOut.write(validEnvelope(n)));
            default -> out.println(Ansi.AUTO.string("@|bold,green valid:|@") + " "
                    + n + " resource" + (n == 1 ? "" : "s") + " in " + displayPath());
        }
        out.flush();
    }

    private void emitInvalid(DslException e) {
        // a located DSL-domain violation: its source filename and (when known) line / column
        emitInvalid(e.code(), e.args(), e.source(), e.line(), e.column());
    }

    /**
     * Renders one coded validation failure as the {@code invalid} outcome, in whichever format
     * {@code -o} selected. Works for any {@link TapstateErrorCode}: a DSL-domain violation carries its
     * source line / column; a cli-domain one (a workspace-layout fault) locates at its file alone.
     */
    private void emitInvalid(TapstateErrorCode code, Map<String, Object> args, String source, int line, int column) {
        switch (output) {
            case JSON -> {
                PrintWriter out = CliIo.out(spec);
                out.println(JsonOut.write(invalidEnvelope(code, args, source, line, column)));
                out.flush();
            }
            case YAML -> {
                PrintWriter out = CliIo.out(spec);
                out.println(YamlOut.write(invalidEnvelope(code, args, source, line, column)));
                out.flush();
            }
            default -> {
                MessageCatalog.Rendered rendered = MessageCatalog.bundled().render(code, args);
                PrintWriter err = CliIo.err(spec);
                err.println(Ansi.AUTO.string("@|bold,red invalid:|@") + " " + origin(source, line, column)
                        + "  " + code.code());
                err.println("  " + rendered.message());
                if (rendered.solution() != null) {
                    err.println("  " + rendered.solution());
                }
                // Stamped like every other reported problem. This surface is its own shape because it
                // carries a source position, but what a reader pastes has to say which build produced
                // it whichever surface it came from -- and this is the one they paste most.
                err.println("  (" + Diagnostics.OFFLINE_VERSIONS + ")");
                err.flush();
            }
        }
    }

    private static Map<String, Object> validEnvelope(int n) {
        Map<String, Object> env = new LinkedHashMap<>();
        env.put("status", "valid");
        env.put("resourceCount", n);
        env.put("diagnostics", List.of());
        return env;
    }

    private static Map<String, Object> invalidEnvelope(TapstateErrorCode code, Map<String, Object> args,
            String source, int line, int column) {
        Map<String, Object> env = new LinkedHashMap<>();
        env.put("status", "invalid");
        env.put("diagnostics", List.of(Diagnostics.map(code, args, source, line, column)));
        return env;
    }

    private static String origin(String source, int line, int column) {
        String origin = source != null ? source : "(batch)";
        if (line > 0) {
            origin += ":" + line + (column > 0 ? ":" + column : "");
        }
        return origin;
    }
}
