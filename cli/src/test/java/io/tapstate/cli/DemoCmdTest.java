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
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * {@code demo} exists to save a stranger from transcribing three files before they can see anything.
 * What it must never do is cost them work they have already done, so the refusal to overwrite is held
 * here as tightly as the writing is - and the all-or-nothing rule with it, because a workspace holding
 * two of three files is a state nobody asked for and neither a re-run nor {@code --force} was designed
 * around.
 */
class DemoCmdTest {

    private record Run(int code, String out, String err) {
        String all() {
            return out + err;
        }
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
    void writesTheThreeResourcesIntoTheirKindDirectories(@TempDir Path dir) {
        Run r = run("demo", "-w", dir.toString());

        assertThat(r.code()).isZero();
        assertThat(dir.resolve("source/orders_db.tap.yml")).exists();
        assertThat(dir.resolve("source/fulfillment_db.tap.yml")).exists();
        assertThat(dir.resolve("pipeline/order_pipeline.tap.yml")).exists();
        assertThat(r.out()).contains("source/orders_db.tap.yml").contains("pipeline/order_pipeline.tap.yml");
    }

    /**
     * What it writes has to be what the rest of the demo is about. Asserted on the shape rather than on
     * the whole text, because the text is locked to the quickstart's own copy by a gate in the e2e
     * module - this one is here so that a resource file emptied or truncated fails where the command is.
     */
    @Test
    void whatItWritesIsTheAssemblyTheDemoIsAbout(@TempDir Path dir) throws IOException {
        run("demo", "-w", dir.toString());

        String pipeline = Files.readString(dir.resolve("pipeline/order_pipeline.tap.yml"));
        assertThat(pipeline)
                .contains("source: [ orders_db, fulfillment_db ]")
                .contains("type: nest")
                .contains("path: shipments")
                .contains("arrayKey")
                .contains("primary_key: id")
                .doesNotContain("serve:");
    }

    @Test
    void refusesRatherThanOverwriteAWorkspaceThatAlreadyHasThem(@TempDir Path dir) throws IOException {
        run("demo", "-w", dir.toString());
        Path edited = dir.resolve("pipeline/order_pipeline.tap.yml");
        Files.writeString(edited, "# mine now\n");

        Run again = run("demo", "-w", dir.toString());

        assertThat(again.code()).isEqualTo(DemoCmd.EXIT_DIAGNOSTIC);
        assertThat(again.all()).contains("cli.demo-workspace-exists");
        assertThat(Files.readString(edited))
                .as("the whole point of the refusal: an edited file survives it")
                .isEqualTo("# mine now\n");
    }

    @Test
    void overwritesOnlyWhenAskedTo(@TempDir Path dir) throws IOException {
        run("demo", "-w", dir.toString());
        Path edited = dir.resolve("pipeline/order_pipeline.tap.yml");
        Files.writeString(edited, "# mine now\n");

        Run forced = run("demo", "-w", dir.toString(), "--force");

        assertThat(forced.code()).isZero();
        assertThat(Files.readString(edited)).contains("type: nest");
    }

    /**
     * All or nothing. Without this a workspace holding one of the three - the common shape, since the
     * pipeline is the file somebody edits - would come back with the other two rewritten and that one
     * refused, which is neither what was asked for nor a state anything else here handles.
     */
    @Test
    void writesNothingAtAllWhenOneOfThemAlreadyExists(@TempDir Path dir) throws IOException {
        Files.createDirectories(dir.resolve("pipeline"));
        Files.writeString(dir.resolve("pipeline/order_pipeline.tap.yml"), "# mine\n");

        Run r = run("demo", "-w", dir.toString());

        assertThat(r.code()).isEqualTo(DemoCmd.EXIT_DIAGNOSTIC);
        assertThat(dir.resolve("source/orders_db.tap.yml"))
                .as("the other two must not have been written before the refusal")
                .doesNotExist();
    }

    /**
     * The all-or-none promise, at the one place it was only an intention. The existence check passes -
     * nothing is there yet - and then a plain file sitting where a kind directory belongs fails the
     * write. Before this, two of the three had already been written by then, and the reader got an
     * uncoded stack trace on top of a half-made workspace.
     */
    @Test
    void leavesNothingBehindWhenADirectoryCannotBeMade(@TempDir Path dir) throws IOException {
        Files.writeString(dir.resolve("pipeline"), "not a directory\n");

        Run r = run("demo", "-w", dir.toString());

        assertThat(r.code()).isEqualTo(DemoCmd.EXIT_DIAGNOSTIC);
        assertThat(r.all()).contains("cli.workspace-not-writable");
        assertThat(dir.resolve("source/orders_db.tap.yml"))
                .as("the sources must not survive a failure that came after them")
                .doesNotExist();
        assertThat(dir.resolve("source"))
                .as("an empty kind directory is acceptable; a file in it is not")
                .satisfiesAnyOf(
                        d -> assertThat(d).doesNotExist(),
                        d -> assertThat(d.toFile().list()).isEmpty());
    }

    /**
     * Under {@code --force} every target that is already there is read before any of them is written,
     * so that the rollback below has something to put back. A target that cannot be read at all - a
     * directory sitting on the path - is therefore refused while the workspace is still untouched,
     * which is a better outcome than writing two files and taking them back again.
     */
    @Test
    void writesNothingWhenATargetCannotEvenBeRead(@TempDir Path dir) throws IOException {
        Files.createDirectories(dir.resolve("pipeline/order_pipeline.tap.yml"));

        Run r = run("demo", "-w", dir.toString(), "--force");

        assertThat(r.code()).isEqualTo(DemoCmd.EXIT_DIAGNOSTIC);
        assertThat(r.all()).contains("cli.workspace-not-writable");
        assertThat(dir.resolve("source/orders_db.tap.yml"))
                .as("the refusal came before the first byte, so this was never written")
                .doesNotExist();
        assertThat(dir.resolve("source/fulfillment_db.tap.yml")).doesNotExist();
    }

    /**
     * The rollback itself, with both of its halves in one run. The last target is readable and not
     * writable, so the first two land and the third cannot: the file this invocation created is taken
     * back, and the file it overwrote is put back with the bytes it held. That second half is the one
     * that matters - deleting it would make a command whose writes did not survive still destroy
     * content on that path, which is the one outcome {@code --force} may not have.
     */
    @Test
    void putsBackWhatItOverwroteAndTakesBackWhatItCreated(@TempDir Path dir) throws IOException {
        Files.createDirectories(dir.resolve("source"));
        Files.createDirectories(dir.resolve("pipeline"));
        Path theirs = dir.resolve("source/orders_db.tap.yml");
        String mine = "# mine, edited\nkind: source\nid: orders_db\n";
        Files.writeString(theirs, mine);
        Path readOnly = dir.resolve("pipeline/order_pipeline.tap.yml");
        Files.writeString(readOnly, "version: tapstate/v1\nkind: pipeline\n");
        readOnly.toFile().setWritable(false, false);
        // Root can write a file with no write bit, and then there is no failure to roll back from.
        assumeTrue(!Files.isWritable(readOnly), "needs a filesystem that enforces the write bit");

        Run r = run("demo", "-w", dir.toString(), "--force");

        assertThat(r.code()).isEqualTo(DemoCmd.EXIT_DIAGNOSTIC);
        assertThat(r.all()).contains("cli.workspace-not-writable");
        assertThat(theirs)
                .as("it was theirs before the run, so the run may not end with it gone or replaced")
                .hasContent(mine);
        assertThat(dir.resolve("source/fulfillment_db.tap.yml"))
                .as("this one the invocation did create, so it goes")
                .doesNotExist();
    }

    @Test
    void printsTheWalkthroughAndWritesNothing(@TempDir Path dir) {
        Run r = run("demo", "-w", dir.toString(), "--print-steps");

        assertThat(r.code()).isZero();
        assertThat(r.out()).contains("curl -sSL https://install.tapstate.dev")
                .contains("start order_pipeline")
                .contains("views.order_state");
        assertThat(dir.resolve("source/orders_db.tap.yml"))
                .as("--print-steps is a read, and a read writes nothing")
                .doesNotExist();
    }

    /**
     * The walkthrough prints commands for a person to type, and a wrong one is not caught by anything
     * that reads it: it looks like a command. This one was wrong in the shipped text - it wrote the
     * watched row as {@code watch views.order_state.find({id:1})}, which the read shell accepts as a
     * *collection literally named* {@code order_state.find({id:1})} and the server then refuses. So the
     * line is parsed here by the same reader the shell uses, and held to naming the collection the demo
     * materializes.
     */
    @Test
    void theWatchLineNamesACollectionRatherThanACallOnOne() {
        String line = DemoCmd.READS.stream()
                .map(read -> read[0])
                .filter(l -> l.startsWith("watch "))
                .findFirst()
                .orElseThrow(() -> new AssertionError("the walkthrough no longer shows the live view"));

        DataBrowserCall parsed = readShellCall(line);

        assertThat(parsed).isInstanceOf(DataBrowserCall.Live.class);
        DataBrowserCall.Live live = (DataBrowserCall.Live) parsed;
        assertThat(live.sourceId()).isEqualTo("views");
        assertThat(live.collection())
                .as("a collection is a name; a call on one belongs after it, as the filter")
                .isEqualTo("order_state");
        assertThat(live.filter())
                .as("the demo watches one order, not the whole collection")
                .isNotNull();
    }

    /**
     * Every read the walkthrough prints has to be one the read shell can read. The failure this guards
     * is silent in review - a malformed call is still a plausible-looking line - and it reaches a
     * stranger as an error on the first thing they were told to type.
     */
    @Test
    void everyReadTheWalkthroughPrintsIsOneTheShellAccepts() {
        List<DataBrowserCall> reads = DemoCmd.READS.stream()
                .map(read -> readShellCall(read[0]))
                .toList();

        assertThat(reads).as("the walkthrough shows the read shell at all").isNotEmpty();
        assertThat(reads)
                .as("each of these is printed for somebody to type; the shell has to take it")
                .noneMatch(DataBrowserCall.Malformed.class::isInstance)
                .doesNotContainNull();
    }

    /** What is printed is what is checked: the calls reach the page through this list, not beside it. */
    @Test
    void theCallsCheckedAboveAreTheOnesPrinted() {
        String printed = String.join("\n", DemoCmd.walkthrough());

        assertThat(DemoCmd.READS).isNotEmpty();
        DemoCmd.READS.forEach(read -> assertThat(printed).contains(read[0]).contains(read[1]));
    }

    /**
     * A demo whose connectors are never registered fails on its first apply, and the walkthrough was
     * missing that step - found by a person following it on a real machine, which is the only place it
     * could have been found.
     */
    @Test
    void theWalkthroughRegistersTheConnectorsItReads() {
        String walkthrough = String.join("\n", commandLines());

        assertThat(walkthrough).contains("register ").contains("mysql-connector.jar")
                .contains("postgres-connector.jar").contains("mongodb-connector.jar");
        assertThat(walkthrough.indexOf("register "))
                .as("registering comes before applying, or the apply is refused")
                .isLessThan(walkthrough.indexOf("apply source/"));
    }

    /**
     * One printed line as the read shell would take it, or null when the shell has no claim on it.
     * The two live views are read by their own entry point because the verb is matched before the
     * operands are - the same split the REPL makes.
     */
    private static DataBrowserCall readShellCall(String line) {
        for (String verb : List.of("watch", "tail")) {
            if (line.startsWith(verb + " ")) {
                return DataBrowserCall.parseLive(verb, line.substring(verb.length() + 1).trim());
            }
        }
        return DataBrowserCall.parse(line);
    }

    /** The {@code example} command object picocli built, so a test can drive one of its seams. */
    private static DemoCmd commandFrom(CommandLine cl) {
        return cl.getSubcommands().get("demo").getCommand();
    }

    /** The indented command lines of the printed walkthrough, trimmed, prose lines dropped. */
    private static List<String> commandLines() {
        Run r = run("demo", "--print-steps");
        assertThat(r.code()).isZero();
        return r.out().lines()
                .filter(l -> l.startsWith("     "))
                .map(String::trim)
                .filter(l -> !l.isBlank())
                .toList();
    }

    @Test
    void reportsAStructuredEnvelopeForScriptsAndAgents(@TempDir Path dir) {
        Run r = run("demo", "-w", dir.toString(), "-o", "json");

        assertThat(r.code()).isZero();
        assertThat(r.out()).contains("\"status\": \"ok\"").contains("\"created\"")
                .contains("order_pipeline.tap.yml");
    }

    @Test
    void reportsTheRefusalInTheSameEnvelope(@TempDir Path dir) {
        run("demo", "-w", dir.toString());

        Run again = run("demo", "-w", dir.toString(), "-o", "json");

        assertThat(again.code()).isEqualTo(DemoCmd.EXIT_DIAGNOSTIC);
        assertThat(again.out()).contains("\"status\": \"error\"")
                .contains("\"code\": \"cli.demo-workspace-exists\"");
    }

    @Test
    void whatItWritesValidates(@TempDir Path dir) {
        run("demo", "-w", dir.toString());

        Run validated = run("validate", dir.toString());

        assertThat(validated.code())
                .as("a demo workspace that does not validate would send a stranger to a diagnostic on "
                        + "their first command: %s", validated.all())
                .isZero();
        assertThat(validated.all()).contains("3");
    }

    @Test
    void isOfferedAsAnOfflineVerb() {
        assertThat(Cli.OFFLINE_VERBS).contains("demo");
        assertThat(run("--help").out()).contains("demo");
    }

    /**
     * The files are written either way - nothing here needs a container - but what they are for does,
     * and a reader who has no Docker is better told now than three commands from now. A note, never a
     * refusal: refusing would be refusing work that succeeded.
     */
    @Test
    void saysSoWhenTheStackTheseResourcesNeedCannotBeStarted(@TempDir Path dir) {
        CommandLine cl = Cli.newCommandLine();
        commandFrom(cl).dockerIsOnThePath = () -> false;
        StringWriter out = new StringWriter();
        cl.setOut(new PrintWriter(out));
        int code = cl.execute("demo", "-w", dir.toString());

        assertThat(code).as("writing files is not the step that needs Docker").isZero();
        assertThat(dir.resolve("pipeline/order_pipeline.tap.yml")).exists();
        assertThat(out.toString()).contains("Docker is not on your PATH");
    }

    @Test
    void saysNothingAboutDockerWhenItIsThere(@TempDir Path dir) {
        CommandLine cl = Cli.newCommandLine();
        commandFrom(cl).dockerIsOnThePath = () -> true;
        StringWriter out = new StringWriter();
        cl.setOut(new PrintWriter(out));
        cl.execute("demo", "-w", dir.toString());

        assertThat(out.toString()).doesNotContain("Docker").contains("--print-steps");
    }

    @Test
    void everyResourceItNamesIsOnTheClasspath() {
        // The bundle is what makes this command a copy of the demo rather than a second one. A missing
        // file is a broken build, and it must not first surface as a half-written workspace.
        for (String resource : DemoCmd.RESOURCES) {
            assertThat(DemoCmd.bundled(resource)).isNotBlank().startsWith("version: tapstate/v1");
        }
        assertThat(DemoCmd.RESOURCES).hasSize(3);
    }

    @Test
    void writesResourcesUnderTheDirectoryTheirKindRequires() {
        // validate refuses a resource whose kind does not match its directory, so the layout is not a
        // convention here - a demo written flat would fail its own first validate.
        assertThat(DemoCmd.RESOURCES)
                .allSatisfy(resource -> assertThat(List.of("source", "pipeline"))
                        .contains(resource.substring(0, resource.indexOf('/'))));
    }
}
