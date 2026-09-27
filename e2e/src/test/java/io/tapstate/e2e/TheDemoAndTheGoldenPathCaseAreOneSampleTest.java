package io.tapstate.e2e;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The demo a stranger runs and the case CI runs are one sample, and this is what makes that true.
 *
 * <p><b>Do not relax this to make anything faster or tidier.</b> The coupling is deliberate. "The
 * recording is the end-to-end case" is the claim the golden path is built on, and until this existed it
 * was a claim about intent: the quickstart generated its workspace from heredocs in a shell script,
 * the case had a directory of its own, the two happened to agree, and nothing anywhere would have said
 * a word when they stopped. Two artifacts that must agree and are checked by nobody agree until the
 * first time it matters.
 *
 * <p><b>What is held identical, and what is not.</b> The pipeline is the sample: the assembly, what is
 * embedded where, what identifies an array element, what the view is keyed by. It is held byte for
 * byte. The two sources are held on everything except the one line that cannot be shared - the address.
 * The demo dials compose service names on a machine it brought up; a case dials whatever containers the
 * harness started that minute. Holding those identical would mean one of the two lying about where its
 * data is.
 *
 * <p>The quickstart script is the original and this case is the copy. That direction is not arbitrary:
 * the script is what a stranger actually runs, so it is the artifact that must read well on its own,
 * and a case generated from it can never be the reason the demo is shaped awkwardly.
 */
class TheDemoAndTheGoldenPathCaseAreOneSampleTest {

    private static final Path QUICKSTART = Path.of("..", "deploy", "quickstart", "quickstart.sh");

    private static final Path EXAMPLES = Path.of("examples");

    /** What {@code tapstate demo} writes, carried beside the command as classpath resources. */
    private static final Path CLI_BUNDLE = Path.of("..", "cli", "src", "main", "resources", "demo");

    private static final Path CASE = EXAMPLES.resolve("the-golden-path-two-engines-become-one-object");

    /** The settings line, which is the one thing the two are allowed to disagree on. */
    private static final String ADDRESS_LINE_PREFIX = "config:";

    @Test
    void thePipelineTheDemoGeneratesIsTheOneTheCaseRuns() throws IOException {
        assertThat(heredoc("work/pipeline/order_pipeline.tap.yml"))
                .as("the demo's pipeline and the golden-path case's pipeline have to be the same text. "
                        + "If this failed because you changed one of them on purpose, change the other "
                        + "in the same commit - do not delete this test")
                .isEqualTo(caseFile("order_pipeline.tap.yml"));
    }

    @Test
    void theSourcesAgreeOnEverythingExceptWhereTheirDatabasesAre() throws IOException {
        for (String source : List.of("orders_db.tap.yml", "fulfillment_db.tap.yml")) {
            List<String> demo = withoutTheAddress(heredoc("work/source/" + source));
            List<String> theCase = withoutTheAddress(caseFile(source));

            assertThat(theCase)
                    .as("%s: the demo and the case may differ on the address line and on nothing else. "
                            + "The id, the connector, the capture mode and the tables are the sample",
                            source)
                    .isEqualTo(demo);
        }
    }

    @Test
    void bothSidesStillCarryAnAddressAtAll() throws IOException {
        // The comparison above works by dropping a line. A source that stopped having one would compare
        // equal to a source that still did, and this test is the reason that cannot pass unnoticed.
        for (String source : List.of("orders_db.tap.yml", "fulfillment_db.tap.yml")) {
            assertThat(heredoc("work/source/" + source)).contains(ADDRESS_LINE_PREFIX);
            assertThat(caseFile(source)).contains(ADDRESS_LINE_PREFIX);
        }
    }

    /**
     * Every other case that carries the demo's pipeline carries the same one.
     *
     * <p>The sameness above is between two files. This is the reason a third copy cannot quietly become
     * a fourth shape: cases about the demo are written by copying it, so the moment one of them is
     * edited in place the demo has forked without anyone deciding to fork it.
     */
    @Test
    void everyCaseCarryingTheDemoPipelineCarriesTheSameOne() throws IOException {
        String demo = heredoc("work/pipeline/order_pipeline.tap.yml");
        List<Path> copies;
        try (var entries = Files.list(EXAMPLES)) {
            copies = entries.map(dir -> dir.resolve("order_pipeline.tap.yml")).filter(Files::exists).toList();
        }

        assertThat(copies)
                .as("the golden path case at least, or this test is watching nothing")
                .isNotEmpty();
        for (Path copy : copies) {
            assertThat(Files.readString(copy))
                    .as("%s carries the demo's pipeline, so it has to carry the demo's pipeline", copy)
                    .isEqualTo(demo);
        }
    }

    /**
     * And the third copy: what {@code tapstate demo} writes.
     *
     * <p>All three, byte for byte, with no address exemption - unlike the case, this one targets the
     * same compose stack the quickstart does, so there is nothing it could legitimately spell
     * differently. The command exists to save a stranger from transcribing these files; a command that
     * wrote its own variant of them would have re-created, in the same release, the drift the other two
     * were just wired together to prevent.
     */
    @Test
    void theCommandWritesWhatTheQuickstartWrites() throws IOException {
        for (String resource : List.of("orders_db.tap.yml", "fulfillment_db.tap.yml")) {
            assertThat(Files.readString(CLI_BUNDLE.resolve(resource)))
                    .as("%s: `tapstate demo` and the quickstart write the same file", resource)
                    .isEqualTo(heredoc("work/source/" + resource));
        }
        assertThat(Files.readString(CLI_BUNDLE.resolve("order_pipeline.tap.yml")))
                .as("the pipeline especially: it is the sample")
                .isEqualTo(heredoc("work/pipeline/order_pipeline.tap.yml"));
    }

    /**
     * The project file too. It names the project the demo's resources are grouped under on the server,
     * so a command that named it differently from the script would put the same demo under two names.
     */
    @Test
    void theCommandNamesTheProjectTheQuickstartNames() throws IOException {
        assertThat(Files.readString(CLI_BUNDLE.resolve("project.tap.yml")))
                .as("`tapstate demo` and the quickstart write the same project file")
                .isEqualTo(heredoc("work/project.tap.yml"));
    }

    /**
     * One resource as the quickstart writes it, read out of the script's own heredoc.
     *
     * <p>Read from the script rather than by running it: running it needs Docker, a network and a
     * platform gate, none of which this is about, and the text is the same either way.
     */
    private static String heredoc(String writtenTo) throws IOException {
        String script = Files.readString(QUICKSTART);
        String opener = "cat > " + writtenTo + " <<'YAML'\n";
        int start = script.indexOf(opener);
        assertThat(start)
                .as("the quickstart no longer writes %s with a heredoc this can read. If the script "
                        + "changed shape, teach this test the new shape - the two artifacts still have "
                        + "to be held to each other", writtenTo)
                .isNotEqualTo(-1);
        int body = start + opener.length();
        int end = script.indexOf("\nYAML\n", body);
        return script.substring(body, end + 1);
    }

    private static String caseFile(String name) throws IOException {
        return Files.readString(CASE.resolve(name));
    }

    /**
     * Every line as it is, except that the settings line keeps its setting <em>names</em> and drops
     * their values.
     *
     * <p>Dropping the whole line was too coarse and keeping the non-address values would be too
     * strict. Every value on that line legitimately differs: the demo dials a compose stack by name,
     * the case interpolates whatever the harness brought up, credentials included. What may not
     * differ is which settings are there at all - a demo that gained a setting the case does not
     * carry is a demo the case is no longer running.
     */
    private static List<String> withoutTheAddress(String resource) {
        return resource.lines()
                .map(line -> line.startsWith(ADDRESS_LINE_PREFIX) ? settingNames(line) : line)
                .toList();
    }

    /** The names on a {@code config: { … }} line, in a stable order, without their values. */
    private static String settingNames(String line) {
        int open = line.indexOf('{');
        int close = line.lastIndexOf('}');
        if (open < 0 || close < open) {
            return ADDRESS_LINE_PREFIX + " <unreadable settings>";
        }
        List<String> names = new ArrayList<>();
        for (String setting : line.substring(open + 1, close).split(",")) {
            int colon = setting.indexOf(':');
            names.add(colon < 0 ? setting.trim() : setting.substring(0, colon).trim());
        }
        Collections.sort(names);
        return ADDRESS_LINE_PREFIX + " settings named " + String.join(", ", names);
    }
}
