package io.tapstate.cli;

import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code new --list} — the recipe catalog, as text and as the machine-readable form. Both renderings
 * come from one static list, and this suite holds each to a golden so the wording cannot drift
 * between them; the last case holds the list itself to the catalog table on the first-run page
 * ({@code docs/first-run/README.md}), which is the contract both are generated from.
 */
class NewListTest {

    private static final List<String> PAGE_IDS = List.of(
            "sample", "mirrored-table", "reshaped-table", "nested-json", "consolidated-table", "blank");

    /** Captured outcome of one one-shot CLI invocation. */
    private record Run(int code, String out, String err) {
    }

    private static Run run(String... args) {
        CommandLine cl = Cli.newCommandLine();
        StringWriter out = new StringWriter();
        StringWriter err = new StringWriter();
        cl.setOut(new PrintWriter(out));
        cl.setErr(new PrintWriter(err));
        int code = cl.execute(args);
        return new Run(code, out.toString(), err.toString());
    }

    @Test
    void listPrintsEveryRecipeAsPlainText() {
        Run r = run("new", "--list");

        assertThat(r.code()).isZero();
        assertThat(r.err()).isEmpty();
        assertThat(r.out()).isEqualTo(
                """
                sample              Try it with sample data
                mirrored-table      Mirror one table, as it changes
                reshaped-table      Mirror a table, renamed / filtered / trimmed
                nested-json         Assemble several tables into one object
                consolidated-table  Consolidate the same table from several databases
                blank               Skeleton files only - I will write it myself
                """);
    }

    @Test
    void listJsonMatchesThePageExactly() {
        Run r = run("new", "--list", "-o", "json");

        assertThat(r.code()).isZero();
        assertThat(r.err()).isEmpty();
        // key order and values are the page's; JsonOut is deterministic, so the raw string is the golden
        assertThat(r.out()).isEqualTo(
                """
                {
                  "recipes": [
                    {
                      "id": "sample",
                      "title": "Try it with sample data",
                      "runnable": true,
                      "uses": []
                    },
                    {
                      "id": "mirrored-table",
                      "title": "Mirror one table, as it changes",
                      "runnable": true,
                      "uses": [
                        "cdc"
                      ]
                    },
                    {
                      "id": "reshaped-table",
                      "title": "Mirror a table, renamed / filtered / trimmed",
                      "runnable": true,
                      "uses": [
                        "cdc",
                        "map",
                        "filter"
                      ]
                    },
                    {
                      "id": "nested-json",
                      "title": "Assemble several tables into one object",
                      "runnable": true,
                      "uses": [
                        "nest"
                      ]
                    },
                    {
                      "id": "consolidated-table",
                      "title": "Consolidate the same table from several databases",
                      "runnable": true,
                      "uses": [
                        "union"
                      ]
                    },
                    {
                      "id": "blank",
                      "title": "Skeleton files only - I will write it myself",
                      "runnable": false,
                      "uses": []
                    }
                  ]
                }
                """);
    }

    @Test
    void listYamlCarriesTheSameIdsInOrder() {
        Run r = run("new", "--list", "-o", "yaml");

        assertThat(r.code()).isZero();
        assertThat(r.err()).isEmpty();
        List<String> ids = new ArrayList<>();
        Matcher m = Pattern.compile("(?m)^  - id: (\\S+)$").matcher(r.out());
        while (m.find()) {
            ids.add(m.group(1));
        }
        assertThat(ids).containsExactlyElementsOf(PAGE_IDS);
        assertThat(r.out()).startsWith("recipes:\n");
    }

    @Test
    void listRefusesScaffoldingFlags() {
        Run r = run("new", "--list", "--kind", "source");

        assertThat(r.code()).isEqualTo(NewCmd.EXIT_USAGE);
        assertThat(r.out()).isEmpty();
        // the command's own refusal, not picocli's unknown-option path (which also exits 2 and names the flag)
        assertThat(r.err()).startsWith("new: --list");
    }

    @Test
    void catalogIdsMatchTheFirstRunPage() throws IOException {
        List<String> onPage = catalogIdsOnPage(Files.readString(firstRunPage()));

        assertThat(onPage).containsExactlyElementsOf(PAGE_IDS);
        assertThat(Recipe.CATALOG).extracting(Recipe::id).containsExactlyElementsOf(onPage);
    }

    /** The first column of the catalog table under "Step 2", in order: every row whose first cell is a backticked id. */
    private static List<String> catalogIdsOnPage(String page) {
        int step2 = page.indexOf("### Step 2");
        int step3 = page.indexOf("### Step 3", step2);
        assertThat(step2).isNotNegative();
        assertThat(step3).isGreaterThan(step2);
        List<String> ids = new ArrayList<>();
        Matcher m = Pattern.compile("(?m)^\\| `([a-z-]+)` \\|").matcher(page.substring(step2, step3));
        while (m.find()) {
            ids.add(m.group(1));
        }
        return ids;
    }

    /** Walks up from the test's working directory (the module dir under surefire) to the repository root. */
    private static Path firstRunPage() {
        Path relative = Path.of("docs", "first-run", "README.md");
        for (Path dir = Path.of("").toAbsolutePath(); dir != null; dir = dir.getParent()) {
            Path candidate = dir.resolve(relative);
            if (Files.isRegularFile(candidate)) {
                return candidate;
            }
        }
        throw new IllegalStateException("docs/first-run/README.md not found above " + Path.of("").toAbsolutePath());
    }
}
