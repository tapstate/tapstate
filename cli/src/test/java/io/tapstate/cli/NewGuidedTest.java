package io.tapstate.cli;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The guided entry behind bare {@code new} and {@code new <recipe>}: the opening line, the one
 * question it asks, and the non-interactive form scripts drive with {@code --yes}.
 *
 * <p>There is nothing about servers here, and that is the point: scaffolding is a local act. Where a
 * workspace is brought up, and what it is brought up against, belongs to {@code up} - see
 * {@link ServerBindingTest}. The last test in this file is the guard that keeps it that way.
 */
class NewGuidedTest {

    private static final String BLANK = "Skeleton files only - I will write it myself";

    private static final List<String> TITLES = List.of(
            "Try it with sample data",
            "Mirror one table, as it changes",
            "Mirror a table, renamed / filtered / trimmed",
            "Assemble several tables into one object",
            "Consolidate the same table from several databases",
            BLANK);

    /** Captured outcome of one one-shot CLI invocation. */
    record Run(int code, String out, String err) {
        String all() {
            return out + err;
        }
    }

    static Run run(Path unusedHome, Prompter prompter, String... args) {
        CommandLine cl = Cli.newCommandLine();
        NewCmd cmd = cl.getSubcommands().get("new").getCommand();
        cmd.prompter = prompter;
        StringWriter out = new StringWriter();
        StringWriter err = new StringWriter();
        cl.setOut(new PrintWriter(out));
        cl.setErr(new PrintWriter(err));
        int code = cl.execute(args);
        return new Run(code, out.toString(), err.toString());
    }

    @Test
    void theOpeningLineNamesWhatIsBeingBuiltBeforeTheQuestionAssumesIt(@TempDir Path home, @TempDir Path ws) {
        ScriptedPrompter prompter = new ScriptedPrompter(BLANK);

        Run r = run(home, prompter, "new", "-w", ws.toString());

        assertThat(r.code()).as(r.all()).isZero();
        // `new` alone is ambiguous - new what? - so the noun comes before the question
        assertThat(r.out()).startsWith(GuidedNew.OPENING_LINE + "\n");
    }

    @Test
    void bareNewOffersTheCatalogInOrderWithBlankLastAndRunsWhatWasPicked(@TempDir Path home, @TempDir Path ws) {
        ScriptedPrompter prompter = new ScriptedPrompter(BLANK);

        Run r = run(home, prompter, "new", "-w", ws.toString());

        assertThat(r.code()).as(r.all()).isZero();
        assertThat(prompter.offered).containsExactly(TITLES);
        assertThat(prompter.asked).as("the outcome is the only thing asked").isEmpty();
        assertThat(ws.resolve("source/example_source.tap.yml")).exists();
    }

    @Test
    void theQuestionIsWordedByOutcomeNotByResourceType(@TempDir Path home, @TempDir Path ws) {
        ScriptedPrompter prompter = new ScriptedPrompter(BLANK);

        run(home, prompter, "new", "-w", ws.toString());

        assertThat(prompter.offered).containsExactly(TITLES);
        // worded by what the workspace is for, never by which resource kind it will contain
        assertThat(GuidedNew.RECIPE_QUESTION).doesNotContainIgnoringCase("kind").doesNotContainIgnoringCase("type");
    }

    @Test
    void aRecipeNamedOnTheCommandLineIsNotAskedAbout(@TempDir Path home, @TempDir Path ws) {
        ScriptedPrompter prompter = new ScriptedPrompter();

        Run r = run(home, prompter, "new", "blank", "--yes", "-w", ws.toString());

        assertThat(r.code()).as(r.all()).isZero();
        assertThat(prompter.asked).isEmpty();
        assertThat(prompter.offered).isEmpty();
    }

    @Test
    void anUnknownRecipeIdIsAUsageErrorPointingAtTheList(@TempDir Path home, @TempDir Path ws) {
        Run r = run(home, new ScriptedPrompter(), "new", "no-such-recipe", "--yes", "-w", ws.toString());

        assertThat(r.code()).isEqualTo(NewCmd.EXIT_USAGE);
        assertThat(r.err()).contains("unknown recipe 'no-such-recipe'").contains("new --list");
    }

    /**
     * The invariant, enforced where it cannot be argued with: {@code new} holds no seam through which a
     * server could be reached. A probe, a stack or an auth service among its fields would mean the
     * scaffolder had learned to talk to something, which is exactly what moving all of that to
     * {@code up} was for - and a behavioural test cannot catch it being added back unused.
     */
    @Test
    void newHoldsNoSeamThatCouldReachAServer() {
        List<Class<?>> forbidden = List.of(ControlPlaneClient.class, LocalStack.class, AuthService.class,
                ContextManager.class, ServerBinding.class);

        List<String> offending = Arrays.stream(NewCmd.class.getDeclaredFields())
                .filter(field -> forbidden.stream().anyMatch(type -> type.isAssignableFrom(field.getType())))
                .map(Field::getName)
                .toList();

        assertThat(offending).as("scaffolding is a local act; everything that reaches a server is up's")
                .isEmpty();
    }
}
