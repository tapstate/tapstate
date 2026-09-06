package io.tapstate.cli;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The first three recipes that write a workspace: {@code sample}, {@code mirrored-table} and
 * {@code blank}; the shaped ones are in {@code NewRecipeShapesTest}. Each is held to what
 * {@code docs/first-run/README.md} says it writes - the demo files byte for byte, a source and a
 * pipeline byte for byte against inline goldens, or nothing at all - and to the rules shared by all
 * of them: never overwrite without {@code --force}, secrets go to {@code .env} and not
 * into the artifact, the interactive and the flag-supplied forms produce the same bytes.
 *
 * <p>The server binding is rooted in a temporary home and probed through a fake, so no socket opens.
 */
class NewRecipeTest {

    private static final String ORDERS_SRC =
            """
            version: tapstate/v1
            kind: source
            id: orders_src
            connector: mysql
            config:
              host: db
              password: ${ORDERS_SRC_PASSWORD}
              port: "3306"
              username: u
            mode: cdc
            tables: [orders]
            """;

    private static final String ORDERS_SYNC =
            """
            version: tapstate/v1
            kind: pipeline
            id: orders_sync
            source: orders_src
            view:
              id: orders_view
              from: orders
              primary_key: id
            """;

    /** Captured outcome of one one-shot CLI invocation; shared with the other recipe tests. */
    record Run(int code, String out, String err) {
        String all() {
            return out + err;
        }
    }

    static Run run(Path home, Prompter prompter, String... args) {
        CommandLine cl = Cli.newCommandLine();
        NewCmd cmd = cl.getSubcommands().get("new").getCommand();
        cmd.prompter = prompter;
        cmd.home = home;
        StringWriter out = new StringWriter();
        StringWriter err = new StringWriter();
        cl.setOut(new PrintWriter(out));
        cl.setErr(new PrintWriter(err));
        int code = cl.execute(args);
        return new Run(code, out.toString(), err.toString());
    }

    private static Run demo(Path dir) {
        CommandLine cl = Cli.newCommandLine();
        StringWriter out = new StringWriter();
        cl.setOut(new PrintWriter(out));
        cl.setErr(new PrintWriter(new StringWriter()));
        return new Run(cl.execute("demo", "-w", dir.toString()), out.toString(), "");
    }

    private static Run mirrored(Path home, Path ws, String... extra) {
        List<String> args = new java.util.ArrayList<>(List.of(
                "new", "mirrored-table", "--yes", "--connector", "mysql",
                "--set", "host=db", "--set", "username=u", "--set", "password=s",
                "--table", "orders", "--view", "orders_view", "-w", ws.toString()));
        args.addAll(List.of(extra));
        return run(home, new ScriptedPrompter(), args.toArray(String[]::new));
    }

    // ---- sample ---------------------------------------------------------------------------------

    @Test
    void sampleWritesTheDemoFilesByteForByte(@TempDir Path home, @TempDir Path ws, @TempDir Path demoDir)
            throws IOException {
        Run r = run(home, new ScriptedPrompter(), "new", "sample", "--yes", "-w", ws.toString());

        assertThat(r.code()).isZero();
        assertThat(r.err()).isEmpty();
        assertThat(demo(demoDir).code()).isZero();
        for (String resource : DemoCmd.RESOURCES) {
            assertThat(Files.readString(ws.resolve(resource)))
                    .as("%s is the bundled resource, not a rendering of it", resource)
                    .isEqualTo(DemoCmd.bundled(resource))
                    .isEqualTo(Files.readString(demoDir.resolve(resource)));
        }
        assertThat(r.out()).startsWith("Workspace: " + ws + "\n"
                + "  source/orders_db.tap.yml  source orders_db: mysql, cdc\n"
                + "  source/fulfillment_db.tap.yml  source fulfillment_db: postgres, cdc\n"
                + "  pipeline/order_pipeline.tap.yml  pipeline order_pipeline: 2 sources, view\n");
    }

    @Test
    void sampleRefusesASecondRunWithoutForceAndChangesNothing(@TempDir Path home, @TempDir Path ws)
            throws IOException {
        run(home, new ScriptedPrompter(), "new", "sample", "--yes", "-w", ws.toString());
        Path edited = ws.resolve("pipeline/order_pipeline.tap.yml");
        Files.writeString(edited, "# mine now\n");

        Run again = run(home, new ScriptedPrompter(), "new", "sample", "--yes", "-w", ws.toString());

        assertThat(again.code()).isEqualTo(NewCmd.EXIT_DIAGNOSTIC);
        assertThat(again.err()).contains("cli.artifact-exists");
        assertThat(Files.readString(edited)).isEqualTo("# mine now\n");
    }

    @Test
    void sampleRewritesUnderForceAndSaysWhichFilesWereReplaced(@TempDir Path home, @TempDir Path ws)
            throws IOException {
        run(home, new ScriptedPrompter(), "new", "sample", "--yes", "-w", ws.toString());
        Path edited = ws.resolve("pipeline/order_pipeline.tap.yml");
        Files.writeString(edited, "# mine now\n");

        Run forced = run(home, new ScriptedPrompter(), "new", "sample", "--yes", "--force", "-w", ws.toString());

        assertThat(forced.code()).isZero();
        assertThat(Files.readString(edited)).isEqualTo(DemoCmd.bundled("pipeline/order_pipeline.tap.yml"));
        assertThat(forced.out())
                .contains("  pipeline/order_pipeline.tap.yml  pipeline order_pipeline: 2 sources, view (replaced)\n");
    }

    // ---- blank ----------------------------------------------------------------------------------

    @Test
    void blankWritesTheTwoSkeletonsVerbatim(@TempDir Path home, @TempDir Path parent) throws IOException {
        Path ws = parent.resolve("fresh");

        Run r = run(home, new ScriptedPrompter(), "new", "blank", "--yes", "-w", ws.toString());

        assertThat(r.code()).as(r.all()).isZero();
        assertThat(r.err()).isEmpty();
        assertThat(Files.readString(ws.resolve("source/example_source.tap.yml")))
                .isEqualTo(BlankRecipe.bundled("source/example_source.tap.yml"));
        assertThat(Files.readString(ws.resolve("pipeline/example_pipeline.tap.yml")))
                .isEqualTo(BlankRecipe.bundled("pipeline/example_pipeline.tap.yml"));
        // the escape hatch writes exactly these two and nothing else - no .env, no .gitignore
        try (var entries = Files.list(ws)) {
            assertThat(entries).hasSize(2);
        }
    }

    /**
     * The stance the contract page pins: a skeleton is a workspace you edit, not a list of errors you
     * clear first, so it parses and validates as written. Were it to arrive invalid, the very first
     * thing the summary tells a reader to run would fail on the recipe whose whole point is being a
     * clean starting point.
     */
    @Test
    void theBlankSkeletonValidatesAsWritten(@TempDir Path home, @TempDir Path parent) {
        Path ws = parent.resolve("fresh");
        run(home, new ScriptedPrompter(), "new", "blank", "--yes", "-w", ws.toString());

        Run validated = run(home, new ScriptedPrompter(), "validate", ws.toString());

        assertThat(validated.code()).as(validated.all()).isZero();
        assertThat(validated.out()).contains("2 resources");
    }

    // ---- mirrored-table -------------------------------------------------------------------------

    @Test
    void mirroredTableWritesTheSourceAndThePipelineToTheGoldens(@TempDir Path home, @TempDir Path ws)
            throws IOException {
        Run r = mirrored(home, ws);

        assertThat(r.code()).as(r.all()).isZero();
        assertThat(r.err()).isEmpty();
        assertThat(Files.readString(ws.resolve("source/orders_src.tap.yml"))).isEqualTo(ORDERS_SRC);
        assertThat(Files.readString(ws.resolve("pipeline/orders_sync.tap.yml"))).isEqualTo(ORDERS_SYNC);
        assertThat(Files.readString(ws.resolve(".env"))).isEqualTo("ORDERS_SRC_PASSWORD=s\n");
        assertThat(Files.readString(ws.resolve(".gitignore"))).isEqualTo(".env\n");
        assertThat(Files.readString(ws.resolve("source/orders_src.tap.yml")))
                .contains("password: ${ORDERS_SRC_PASSWORD}")
                .doesNotContain(": s\n");
        assertThat(r.out()).startsWith("Workspace: " + ws + "\n"
                + "  source/orders_src.tap.yml  source orders_src: mysql, cdc\n"
                + "  pipeline/orders_sync.tap.yml  pipeline orders_sync: 1 source, view — assumed primary_key: id;");
        assertThat(r.out()).contains("\n  .env  ").contains("\n  .gitignore  ");
    }

    /**
     * The workspace has to pass its own first command. This is where the view's {@code from:} is held
     * to what the DSL resolves - a table name, not a source id - and where the secret reference is shown
     * to be inert to validation.
     */
    @Test
    void whatMirroredTableWritesValidates(@TempDir Path home, @TempDir Path ws) {
        assertThat(mirrored(home, ws).code()).isZero();

        Run validated = run(home, new ScriptedPrompter(), "validate", ws.toString());

        assertThat(validated.code()).as(validated.all()).isZero();
        assertThat(validated.all()).contains("2");
    }

    @Test
    void mirroredTableIsByteIdenticalRunToRun(@TempDir Path home, @TempDir Path a, @TempDir Path b)
            throws IOException {
        assertThat(mirrored(home, a).code()).isZero();
        assertThat(mirrored(home, b).code()).isZero();

        for (String file : List.of("source/orders_src.tap.yml", "pipeline/orders_sync.tap.yml", ".env", ".gitignore")) {
            assertThat(Files.readAllBytes(a.resolve(file))).isEqualTo(Files.readAllBytes(b.resolve(file)));
        }
    }

    /**
     * The scripted answers, in the order the questions come: connector, then the connector's
     * required and secret fields (host, port taken as its default, database skipped, username,
     * password masked), then the table, then the view id. Same values as the flag form, same bytes:
     * the two paths share one writer.
     */
    @Test
    void interactiveMirroredTableProducesTheSameFilesAsTheFlagForm(@TempDir Path home, @TempDir Path ws,
                                                                   @TempDir Path flags) throws IOException {
        ScriptedPrompter prompter = new ScriptedPrompter(
                "mysql", "db", "", "", "u", "s", "orders", "orders_view");

        Run r = run(home, prompter, "new", "mirrored-table", "-w", ws.toString());

        assertThat(r.code()).as(r.all()).isZero();
        assertThat(prompter.secretQuestions).hasSize(1);
        assertThat(prompter.offered).hasSize(1);
        assertThat(prompter.offered.get(0)).startsWith("mysql");
        assertThat(mirrored(home, flags).code()).isZero();
        for (String file : List.of("source/orders_src.tap.yml", "pipeline/orders_sync.tap.yml", ".env", ".gitignore")) {
            assertThat(Files.readString(ws.resolve(file))).isEqualTo(Files.readString(flags.resolve(file)));
        }
    }

    @Test
    void mirroredTableViewDefaultsToTheTableName(@TempDir Path home, @TempDir Path ws) throws IOException {
        Run r = run(home, new ScriptedPrompter(), "new", "mirrored-table", "--yes",
                "--set", "host=db", "--table", "orders", "-w", ws.toString());

        assertThat(r.code()).as(r.all()).isZero();
        assertThat(Files.readString(ws.resolve("pipeline/orders_sync.tap.yml"))).contains("  id: orders\n");
        // no secret answered, so nothing to keep out of the artifact
        assertThat(ws.resolve(".env")).doesNotExist();
        assertThat(ws.resolve(".gitignore")).doesNotExist();
    }

    @Test
    void mirroredTableAppendsToAnExistingEnvAndGitignore(@TempDir Path home, @TempDir Path ws) throws IOException {
        Files.createDirectories(ws);
        Files.writeString(ws.resolve(".env"), "OTHER=1\nORDERS_SRC_PASSWORD=old\n");
        Files.writeString(ws.resolve(".gitignore"), "target/\n");

        Run r = mirrored(home, ws);

        assertThat(r.code()).as(r.all()).isZero();
        assertThat(Files.readString(ws.resolve(".env"))).isEqualTo("OTHER=1\nORDERS_SRC_PASSWORD=s\n");
        assertThat(Files.readString(ws.resolve(".gitignore"))).isEqualTo("target/\n.env\n");
    }

    @Test
    void mirroredTableRefusesToOverwriteBeforeWritingAnything(@TempDir Path home, @TempDir Path ws)
            throws IOException {
        Files.createDirectories(ws.resolve("pipeline"));
        Files.writeString(ws.resolve("pipeline/orders_sync.tap.yml"), "# mine\n");

        Run r = mirrored(home, ws);

        assertThat(r.code()).isEqualTo(NewCmd.EXIT_DIAGNOSTIC);
        assertThat(r.err()).contains("cli.artifact-exists");
        assertThat(ws.resolve("source/orders_src.tap.yml")).doesNotExist();
        assertThat(ws.resolve(".env")).doesNotExist();
        assertThat(Files.readString(ws.resolve("pipeline/orders_sync.tap.yml"))).isEqualTo("# mine\n");
    }

    @Test
    void mirroredTableMissingTableIsAUsageErrorNamingTheFlag(@TempDir Path home, @TempDir Path ws) {
        Run r = run(home, new ScriptedPrompter(), "new", "mirrored-table", "--yes",
                "--set", "host=db", "-w", ws.toString());

        assertThat(r.code()).isEqualTo(NewCmd.EXIT_USAGE);
        assertThat(r.err()).startsWith("new:").contains("--table");
        assertThat(ws.resolve("source")).doesNotExist();
    }

    @Test
    void mirroredTableJsonListsTheFourFilesWithTheirKinds(@TempDir Path home, @TempDir Path ws) {
        Run r = mirrored(home, ws, "-o", "json");

        assertThat(r.code()).as(r.all()).isZero();
        assertThat(r.out())
                .contains("\"status\": \"created\"")
                .contains("\"recipe\": \"mirrored-table\"")
                .contains("\"workspace\": \"" + ws + "\"")
                .contains("\"path\": \"" + ws.resolve("source/orders_src.tap.yml") + "\"")
                .contains("\"kind\": \"source\"")
                .contains("\"kind\": \"pipeline\"")
                .contains("\"kind\": \"env\"")
                .contains("\"kind\": \"gitignore\"")
                .doesNotContain("Workspace:");
        assertThat(r.out().indexOf("\"kind\": \"source\""))
                .isLessThan(r.out().indexOf("\"kind\": \"pipeline\""));
        assertThat(r.out().indexOf("\"kind\": \"pipeline\""))
                .isLessThan(r.out().indexOf("\"kind\": \"env\""));
        assertThat(r.out().indexOf("\"kind\": \"env\""))
                .isLessThan(r.out().indexOf("\"kind\": \"gitignore\""));
    }
}
