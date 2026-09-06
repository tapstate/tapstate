package io.tapstate.cli;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What {@code new} says once a recipe has written ({@code docs/first-run/README.md}, "What new says
 * afterwards"): the workspace and every file in it with what that file is for, the state, the four
 * things to do next, and the one line handing over to an AI assistant. The wording is pinned byte for
 * byte against inline goldens, because it is the last thing a first run reads and the first thing an
 * assistant is pointed at. The file descriptions are {@code ls}'s own one-liners, so {@code ls} is held
 * to a golden here too: the summary it shares must not move under the refactor that shared it.
 *
 * <p>Every run binds a temporary home to the default server through a fake probe, so the URL the
 * {@code up} line names is known and no socket opens.
 */
class NewSummaryTest {

    private static final String SERVER = "http://127.0.0.1:8080";

    /** The tail every recipe run ends with once the file list is out. */
    private static final String ENDING =
            """
            State: not running yet
            Next:
              edit any file above  they are ordinary YAML; the guided commands never hide them
              tapstate validate  check the workspace without a server
              tapstate ls / tapstate desc <id>  see what is here and what each file declares
              tapstate up  bring it to running against http://127.0.0.1:8080
            An AI assistant can take it from here: https://tapstate.dev/docs/first-run
            """;

    private static NewRecipeTest.Run mirrored(Path home, Path ws, String... extra) {
        List<String> args = new java.util.ArrayList<>(List.of(
                "new", "mirrored-table", "--yes", "--connector", "mysql",
                "--set", "host=db", "--set", "username=u", "--set", "password=s",
                "--table", "orders", "--view", "orders_view", "-w", ws.toString()));
        args.addAll(List.of(extra));
        return NewRecipeTest.run(home, new ScriptedPrompter(), args.toArray(String[]::new));
    }

    @Test
    void mirroredTableEndsWithTheFilesTheStateTheNextStepsAndTheHandover(@TempDir Path home, @TempDir Path ws) {
        NewRecipeTest.Run r = mirrored(home, ws);

        assertThat(r.code()).as(r.all()).isZero();
        assertThat(r.err()).isEmpty();
        assertThat(r.out()).isEqualTo(
                "Workspace: " + ws + "\n"
                        + """
                          source/orders_src.tap.yml  source orders_src: mysql, cdc
                          pipeline/orders_sync.tap.yml  pipeline orders_sync: 1 source, view — assumed primary_key: id; edit if the table is keyed otherwise
                          .env  secrets for the files above; not committed
                          .gitignore  keeps .env out of version control
                        """
                        + ENDING);
    }

    @Test
    void blankSaysTheWorkspaceIsEmptyInWords(@TempDir Path home, @TempDir Path parent) {
        Path ws = parent.resolve("fresh");

        NewRecipeTest.Run r = NewRecipeTest.run(home, new ScriptedPrompter(), "new", "blank", "--yes", "-w", ws.toString());

        assertThat(r.code()).as(r.all()).isZero();
        assertThat(r.err()).isEmpty();
        assertThat(r.out()).isEqualTo(
                "Workspace: " + ws + "\n"
                        + "  (empty — write your first file, or run tapstate new again for a starter)\n"
                        + ENDING);
    }

    @Test
    void sampleListsTheThreeDemoFilesWithNoAssumption(@TempDir Path home, @TempDir Path ws) {
        NewRecipeTest.Run r = NewRecipeTest.run(home, new ScriptedPrompter(), "new", "sample", "--yes", "-w", ws.toString());

        assertThat(r.code()).as(r.all()).isZero();
        assertThat(r.err()).isEmpty();
        assertThat(r.out()).isEqualTo(
                "Workspace: " + ws + "\n"
                        + """
                          source/orders_db.tap.yml  source orders_db: mysql, cdc
                          source/fulfillment_db.tap.yml  source fulfillment_db: postgres, cdc
                          pipeline/order_pipeline.tap.yml  pipeline order_pipeline: 2 sources, view
                        """
                        + ENDING);
    }

    /**
     * The machine form carries the same facts as fields and none of the prose. There is no JSON reader
     * on this ring, so the envelope is held to the writer's deterministic layout instead of parsed.
     */
    @Test
    void jsonCarriesRolesStateServerAndNextAsFieldsAndNoProse(@TempDir Path home, @TempDir Path ws) {
        NewRecipeTest.Run r = mirrored(home, ws, "-o", "json");

        assertThat(r.code()).as(r.all()).isZero();
        assertThat(r.out())
                .contains("\"status\": \"created\"")
                .contains("\"recipe\": \"mirrored-table\"")
                .contains("\"workspace\": \"" + ws + "\"")
                .contains("\"role\": \"source orders_src: mysql, cdc\"")
                .contains("\"role\": \"pipeline orders_sync: 1 source, view\"")
                .contains("\"assumed\": \"primary_key: id\"")
                .contains("\"role\": \"secrets for the files above; not committed\"")
                .contains("\"role\": \"keeps .env out of version control\"")
                .contains("\"state\": \"not-running\"")
                .contains("\"server\": \"" + SERVER + "\"")
                .contains("\"next\": [\n    \"validate\",\n    \"ls\",\n    \"desc\",\n    \"up\"\n  ]")
                .doesNotContain("Next:")
                .doesNotContain("State:")
                .doesNotContain("An AI assistant")
                .doesNotContain("Workspace:");
        // the assumption belongs to the pipeline alone: the sources and the two dotfiles assumed nothing
        assertThat(countOf(r.out(), "\"assumed\"")).isEqualTo(1);
        assertThat(r.out().indexOf("\"assumed\"")).isGreaterThan(r.out().indexOf("\"kind\": \"pipeline\""));
        assertThat(r.out().indexOf("\"assumed\"")).isLessThan(r.out().indexOf("\"kind\": \"env\""));
        // the existing keys keep their names and their order
        assertThat(r.out().indexOf("\"path\"")).isLessThan(r.out().indexOf("\"kind\": \"source\""));
        assertThat(r.out().indexOf("\"kind\": \"source\"")).isLessThan(r.out().indexOf("\"role\""));
    }

    @Test
    void yamlCarriesTheSameFieldsAndNoProse(@TempDir Path home, @TempDir Path ws) {
        NewRecipeTest.Run r = mirrored(home, ws, "-o", "yaml");

        assertThat(r.code()).as(r.all()).isZero();
        assertThat(r.out())
                .contains("role: \"source orders_src: mysql, cdc\"")
                .contains("assumed: \"primary_key: id\"")
                .contains("state: not-running")
                .contains("server: \"" + SERVER + "\"")
                .contains("next:\n  - validate\n  - ls\n  - desc\n  - up\n")
                .doesNotContain("Next:")
                .doesNotContain("State:")
                .doesNotContain("An AI assistant");
    }

    /**
     * {@code new --kind} is not the guided first run and keeps its one-line report. Run without an
     * injected prompter: one would force the wizard path, and the flag form is the one being held.
     */
    @Test
    void newKindKeepsItsOwnReport(@TempDir Path ws) {
        CommandLine cl = Cli.newCommandLine();
        StringWriter out = new StringWriter();
        cl.setOut(new PrintWriter(out));
        cl.setErr(new PrintWriter(new StringWriter()));

        int code = cl.execute("new", "--non-interactive", "--kind", "source", "--connector", "mysql",
                "--id", "src_a", "--mode", "cdc", "-w", ws.toString());

        assertThat(code).isZero();
        assertThat(out.toString()).isEqualTo("created " + ws.resolve("source/src_a.tap.yml") + "\n");
    }

    /**
     * {@code ls} over what {@code mirrored-table} wrote, byte for byte: the one-liners {@code new} now
     * borrows are {@code ls}'s, and lending them must not have changed a character of {@code ls}.
     */
    @Test
    void lsOverTheWrittenWorkspaceIsUnchanged(@TempDir Path home, @TempDir Path ws) {
        assertThat(mirrored(home, ws).code()).isZero();

        NewRecipeTest.Run r = NewRecipeTest.run(home, new ScriptedPrompter(), "ls", "-w", ws.toString());

        assertThat(r.code()).as(r.all()).isZero();
        assertThat(r.out()).isEqualTo(
                """
                source (1)
                  orders_src  mysql, cdc
                pipeline (1)
                  orders_sync  1 source, view
                """);
    }

    /** The number of non-overlapping occurrences of {@code needle} in {@code haystack}. */
    private static int countOf(String haystack, String needle) {
        int count = 0;
        for (int i = haystack.indexOf(needle); i >= 0; i = haystack.indexOf(needle, i + needle.length())) {
            count++;
        }
        return count;
    }
}
