package io.tapstate.cli;

import io.tapstate.core.common.TapstateException;
import io.tapstate.messages.MessageCatalog;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * {@code demo} — writes the demo workspace, in one command.
 *
 * <p>The walkthrough teaches this workspace by having you write it: three resources, typed out, each
 * explained. That is the right way to meet it once and the wrong way to meet it a second time - someone
 * who has just watched the demo and wants to run it themselves is not learning the DSL yet, they are
 * deciding whether this is worth their afternoon, and asking them to transcribe three files first is
 * where that decision gets made against us.
 *
 * <p><b>The same three files, not a fourth copy of them.</b> They are carried as resources beside this
 * class and held byte-for-byte to the ones the quickstart writes, by a test that says why. A demo command
 * that generated its own variant would have re-created, in the same release, the drift that the
 * quickstart and the end-to-end case were just wired together to prevent.
 *
 * <p><b>It writes a workspace and nothing else.</b> It starts no containers, opens no ports and reaches
 * no network: bringing the stack up belongs to the quickstart script, which already does it, and doing
 * it here too would mean two things that must agree about what the demo stack is. So the failure this
 * command can actually have is a workspace already holding these files - refused with a code rather
 * than overwritten, because those files are the user's the moment they have edited one.
 *
 * <p>{@code --print-steps} prints the walkthrough instead of writing anything. The seven steps are what
 * the recording follows and what teaches the shape; this command is the shortcut for people who want
 * the result first, never a replacement for them.
 */
@Command(name = "demo", mixinStandardHelpOptions = true,
        description = {
                "Write the demo workspace: two sources on different engines, and the pipeline that"
                        + " assembles them into one object.",
                "Writes files only - bring the stack up with the quickstart script."})
final class DemoCmd implements Callable<Integer> {

    /** Exit code when a coded diagnostic is reported, as the other offline verbs spell it. */
    static final int EXIT_DIAGNOSTIC = 1;

    /** The resources this writes, as {@code <kind directory>/<file>}, in the order a reader meets them. */
    static final List<String> RESOURCES =
            List.of("source/orders_db.tap.yml", "source/fulfillment_db.tap.yml",
                    "pipeline/order_pipeline.tap.yml");

    /**
     * The read-shell calls step 4 shows, each with the note printed beside it.
     *
     * <p>Held apart from the prose rather than written into it, so that a test can parse the command
     * exactly as printed. This is not tidiness: a wrong command here is indistinguishable from a right
     * one to anybody reading the file, and it reaches a stranger as an error on the first thing they
     * were told to type. One shipped that way - the watched row was written as a {@code find} call on
     * the collection, which the shell reads as a collection whose name contains the call.
     */
    static final List<String[]> READS = List.of(
            new String[] {"show collections", "three: the two sources, and views.order_state"},
            new String[] {"views.order_state.find({id:1})", "one order, with its shipments inside it"},
            new String[] {"watch views.order_state {id:1}", "the same object, redrawn as it changes"});

    /** Column the notes beside step 4's calls start at, so they line up under each other. */
    private static final int NOTE_COLUMN = 38;

    /**
     * The walkthrough, in the order the recording follows it. Kept here rather than in a document
     * because this is the copy a user can ask for at any moment, on the machine they are on.
     */
    private static final List<String> STEPS = List.of(
            "1. Install and bring up the stack (databases, server and store, seeded):",
            "     curl -sSL https://install.tapstate.dev | sh",
            "2. Write the demo workspace - orders in MySQL, shipments in PostgreSQL:",
            "     tapstate demo -w work",
            "3. Go online, register the connectors this demo reads, and apply it:",
            "     tapstate -w work   then: connect http://127.0.0.1:8080 ; login admin",
            "     register ../mysql-connector.jar ; register ../postgres-connector.jar",
            "     register ../mongodb-connector.jar",
            "     apply source/orders_db.tap.yml ; apply source/fulfillment_db.tap.yml",
            "     discover-schema orders_db ; discover-schema fulfillment_db ; apply",
            "     start order_pipeline",
            "4. Look at what was assembled:",
            "5. Change either database and watch that object follow:",
            "     docker compose exec postgres psql -U postgres -d appdb \\",
            "       -c \"INSERT INTO shipments VALUES (7,1,'ups','pending');\"",
            "     docker compose exec mysql mysql -uroot -psecret appdb \\",
            "       -e \"UPDATE orders SET customer='alicia' WHERE id=1;\"",
            "6. What you are looking at is a materialized view assembled from two engines that cannot",
            "   see each other - no SQL view and no join can produce it.",
            "7. And an agent can read the same object over MCP:",
            "     data_browser_collections / data_browser_find");

    /**
     * Whether a {@code docker} command is on the PATH, as a seam a test can drive.
     *
     * <p>Probed rather than run: this command starts nothing, so the question is only whether the next
     * step the reader takes will work, and the answer is worth one line now instead of an error three
     * commands from here.
     */
    java.util.function.BooleanSupplier dockerIsOnThePath = DemoCmd::dockerIsInstalled;

    @Spec
    CommandSpec spec;

    @Mixin
    WorkspaceOption workspace;

    @Option(names = "--print-steps",
            description = "Print the walkthrough these files belong to, and write nothing.")
    boolean printSteps;

    @Option(names = "--force", description = "Overwrite the demo files if the workspace already holds them.")
    boolean force;

    @Option(names = {"-o", "--output"}, paramLabel = "FORMAT",
            description = "Output format: text, json or yaml (default: text).",
            defaultValue = "text", completionCandidates = OutputFormat.Candidates.class)
    OutputFormat output;

    @Override
    public Integer call() {
        if (printSteps) {
            emitSteps();
            return 0;
        }
        Path root = workspace.root();
        try {
            List<Path> written = write(root);
            emitWritten(root, written);
            return 0;
        } catch (TapstateException coded) {
            return emitDiagnostic(coded);
        }
    }

    /**
     * Writes every resource, or none of them - the all-or-none rule lives in {@link WorkspaceWrite},
     * shared with the guided first run's {@code sample} recipe, which writes these same files.
     */
    private List<Path> write(Path root) {
        return WorkspaceWrite.write(root, bundledFiles(), force, CliError.DEMO_WORKSPACE_EXISTS).stream()
                .map(WorkspaceWrite.Written::path)
                .toList();
    }

    /** The three files as they are written: bundled bytes, verbatim, in the order a reader meets them. */
    static List<WorkspaceWrite.File> bundledFiles() {
        return RESOURCES.stream().map(resource -> WorkspaceWrite.File.owned(resource, bundled(resource))).toList();
    }

    /**
     * Whether {@code docker} resolves on the PATH. Nothing is executed - a version probe would start a
     * process, and on a machine where the daemon is down it would hang the one command that has no
     * reason to touch it at all.
     */
    private static boolean dockerIsInstalled() {
        String path = System.getenv("PATH");
        if (path == null || path.isBlank()) {
            return false;
        }
        for (String entry : path.split(java.io.File.pathSeparator)) {
            if (!entry.isBlank() && Files.isExecutable(Path.of(entry, "docker"))) {
                return true;
            }
        }
        return false;
    }

    /** One bundled resource. Absent means a broken build, not a user error, so it crashes bare. */
    static String bundled(String resource) {
        String name = "/demo/" + resource.substring(resource.indexOf('/') + 1);
        try (InputStream in = DemoCmd.class.getResourceAsStream(name)) {
            if (in == null) {
                throw new IllegalStateException("the demo resource " + name + " is not on the classpath");
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException unreadable) {
            throw new UncheckedIOException("cannot read the bundled demo resource " + name, unreadable);
        }
    }

    /** The walkthrough as printed: the prose, with step 4's calls rendered under their heading. */
    static List<String> walkthrough() {
        List<String> lines = new ArrayList<>();
        for (String step : STEPS) {
            lines.add(step);
            if (step.startsWith("4. ")) {
                READS.forEach(read -> lines.add("     " + pad(read[0]) + read[1]));
            }
        }
        return lines;
    }

    private static String pad(String call) {
        return call.length() >= NOTE_COLUMN ? call + "  " : call + " ".repeat(NOTE_COLUMN - call.length());
    }

    private void emitSteps() {
        PrintWriter out = CliIo.out(spec);
        List<String> lines = walkthrough();
        switch (output) {
            case JSON -> out.println(JsonOut.write(Map.of("status", "ok", "steps", lines)));
            case YAML -> out.println(YamlOut.write(Map.of("status", "ok", "steps", lines)));
            default -> {
                out.println("The demo, end to end:");
                out.println();
                lines.forEach(out::println);
            }
        }
        out.flush();
    }

    private void emitWritten(Path root, List<Path> written) {
        PrintWriter out = CliIo.out(spec);
        switch (output) {
            case JSON -> out.println(JsonOut.write(envelope(root, written)));
            case YAML -> out.println(YamlOut.write(envelope(root, written)));
            default -> {
                out.println("Wrote the demo workspace to " + root + ":");
                written.forEach(path -> out.println("  " + root.relativize(path)));
                out.println();
                if (dockerIsOnThePath.getAsBoolean()) {
                    out.println("Next: bring the stack up, then apply it."
                            + " `tapstate demo --print-steps` prints the whole walkthrough.");
                } else {
                    // Said here rather than left for the stack to say later. This command needs no
                    // Docker and refuses nothing without it - the files are correct either way - but the
                    // next thing these files are for does, and finding that out three commands later is
                    // a worse place to find it out. A note, not an error: writing files is not the step
                    // that needs a container, and refusing here would be refusing work that succeeded.
                    out.println("These resources need a running stack next, and Docker is not on your"
                            + " PATH. Install Docker with the Compose v2 plugin first;"
                            + " `tapstate demo --print-steps` prints the whole walkthrough.");
                }
            }
        }
        out.flush();
    }

    private static Map<String, Object> envelope(Path root, List<Path> written) {
        Map<String, Object> env = new LinkedHashMap<>();
        env.put("status", "ok");
        env.put("workdir", root.toString());
        env.put("created", written.stream().map(path -> root.relativize(path).toString()).toList());
        return env;
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
                // printText flushes; the writer it was flushing is the one it now owns.
                Diagnostics.printText(CliIo.err(spec), e.code(), e.args());
            }
        }
        return EXIT_DIAGNOSTIC;
    }

    private static Map<String, Object> diagnosticEnvelope(TapstateException e) {
        Map<String, Object> env = new LinkedHashMap<>();
        env.put("status", "error");
        env.put("diagnostics", List.of(Diagnostics.map(e.code(), e.args(), null, 0, 0)));
        return env;
    }
}
