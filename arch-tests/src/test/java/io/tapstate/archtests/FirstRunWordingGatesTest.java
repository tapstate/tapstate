package io.tapstate.archtests;

import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.domain.JavaField;
import com.tngtech.archunit.core.domain.JavaModifier;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import io.tapstate.cli.Cli;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The words the first run is allowed to use, held across every surface a new user meets them on:
 * the questions the guided {@code new} asks, the help of {@code new}, {@code up} and {@code demo},
 * the README's first section, and the first-run contract itself.
 *
 * <p>Three decisions are pinned here because each was made once and drifts one surface at a time.
 * The first run says "Tapstate server", never "context": a context is what the saved-servers verb
 * manages, and a first-time user who is asked for one has been handed the model before the product.
 * The four entry modes - install the CLI, the disposable demo, the guided path, manual operation -
 * are never presented as interchangeable: the root install URL does not appear beside the demo or
 * the databases the demo starts, and the demo's old name is gone. And neither the sample recipe nor
 * the demo is sold as a one-command runner: what one line does is bring up a demo stack, and the
 * phrases below are the ones the product decided against.
 *
 * <p>Every scan here is a proof of absence, so each is paired with what stops it passing
 * vacuously: the surfaces are asserted to have been read, and the recognisers are asserted to
 * recognise the planted spellings they exist to catch.
 */
class FirstRunWordingGatesTest {

    /** Surefire runs a module from its own directory, so the repository root is one level up. */
    private static final Path REPOSITORY = Path.of("..");

    private static final Path README = REPOSITORY.resolve("README.md");
    private static final Path DOCS = REPOSITORY.resolve("docs");
    private static final Path FIRST_RUN_CONTRACT = DOCS.resolve("first-run/README.md");
    private static final Path QUICKSTART = DOCS.resolve("quickstart-online.md");

    /** The README section a new user reads first, and the one the guided path is introduced in. */
    private static final String TRY_IT_HEADING = "## Try it";

    /** The word the first run does not use for "which server", however it is inflected. */
    private static final Pattern CONTEXT = Pattern.compile("(?i)\\bcontexts?\\b");

    /** The one use of the word that is not about servers: the product's positioning phrase. */
    private static final String ALLOWED_CONTEXT = "operational context";

    /**
     * The install site's root, and only the root: not followed by the path that makes it the demo or
     * the explicit CLI route. The root installs the CLI and nothing else, so a line that puts it beside
     * the demo, or beside the databases the demo starts, is presenting one entry mode as another.
     */
    private static final Pattern BARE_INSTALL_ROOT = Pattern.compile("install\\.tapstate\\.dev/?(?![\\w/])");

    private static final Pattern DEMO_OR_ITS_DATABASES = Pattern.compile("(?i)\\bdemo\\b|MySQL|PostgreSQL");

    /** The demo's former name; the command is {@code tapstate demo}, and the old one must not survive in prose. */
    private static final String FORMER_DEMO_NAME = "tapstate example";

    /**
     * The wording the product decided against for the sample recipe and the demo, case-insensitively:
     * neither is a one-command runner of Tapstate, and the demo is not an installation of it.
     */
    private static final List<String> ONE_COMMAND_RUNNER_PHRASES = List.of(
            "one command to run",
            "one-click",
            "run tapstate in one command");

    /** Forbidden only next to the demo: what {@code /demo} brings up is a demo stack, not an installed product. */
    private static final String INSTALLS_TAPSTATE = "installs tapstate";
    private static final Pattern DEMO = Pattern.compile("(?i)\\bdemo\\b");

    /** The guided classes whose string constants are questions or lines a person reads. */
    private static final List<String> GUIDED_CLASSES = List.of("GuidedNew", "RecipeSupport");

    /** The constants that reach a person: every question, and the lines around them. */
    private static final List<String> SPOKEN_CONSTANTS = List.of("OPENING_LINE", "LOCAL_STACK_OFFER");

    private static JavaClasses cliClasses;

    @BeforeAll
    static void importCliClasses() {
        cliClasses = new ClassFileImporter()
                .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                .importPackages("io.tapstate.cli");
    }

    @Test
    @DisplayName("the first run says Tapstate server, never context")
    void theFirstRunNeverSaysContext() {
        Map<String, String> surfaces = new LinkedHashMap<>(guidedLines());
        surfaces.put("new --help", help("new", "--help"));
        surfaces.put("up --help", help("up", "--help"));
        surfaces.put("README.md " + TRY_IT_HEADING, tryItSection());
        surfaces.put(relative(FIRST_RUN_CONTRACT), read(FIRST_RUN_CONTRACT));

        // Positive controls: the questions were found under their own names, and the documents read
        // are the ones with the first run in them. A scan over an empty map passes on nothing.
        assertThat(surfaces.keySet())
                .contains("GuidedNew.SERVER_QUESTION", "GuidedNew.OPENING_LINE", "GuidedNew.RECIPE_QUESTION",
                        "MirroredTableRecipe.TABLE_QUESTION", "RecipeSupport.CONNECTOR_QUESTION");
        assertThat(surfaces.get("README.md " + TRY_IT_HEADING)).contains("tapstate new").contains("tapstate up");
        assertThat(surfaces.get(relative(FIRST_RUN_CONTRACT))).contains("tapstate up");
        assertThat(surfaces.get("new --help")).contains("--server");
        assertThat(surfaces.get("up --help")).contains("--server");

        List<String> offenders = new ArrayList<>();
        surfaces.forEach((surface, text) -> {
            for (String line : text.split("\\R")) {
                if (CONTEXT.matcher(line).find() && !line.contains(ALLOWED_CONTEXT)) {
                    offenders.add(surface + ": " + line.strip());
                }
            }
        });
        assertThat(offenders)
                .as("the first run asks which Tapstate server, never which context: a context is the "
                        + "saved-servers verb's word, and a first-time user asked for one is handed the model "
                        + "before the product")
                .isEmpty();
    }

    @Test
    @DisplayName("the four entry modes do not impersonate each other")
    void theEntryModesDoNotImpersonateEachOther() {
        // The recogniser must tell the root from its routes, or a scan finding nothing means nothing.
        assertThat(List.of(
                "curl -sSL https://install.tapstate.dev | sh",
                "curl -sSL https://install.tapstate.dev/ | sh",
                "`install.tapstate.dev` installs the CLI"))
                .allMatch(line -> BARE_INSTALL_ROOT.matcher(line).find());
        assertThat(List.of(
                "curl -sSL https://install.tapstate.dev/demo | sh",
                "curl -sSL https://install.tapstate.dev/cli | TAPSTATE_INSTALL_DIR=. sh"))
                .noneMatch(line -> BARE_INSTALL_ROOT.matcher(line).find());

        List<String> offenders = new ArrayList<>();
        for (Path document : List.of(README, QUICKSTART)) {
            String text = read(document);
            assertThat(text)
                    .as("%s must name the demo route, or the root would be the only way in it shows", relative(document))
                    .contains("install.tapstate.dev/demo");
            for (String line : text.split("\\R")) {
                if (BARE_INSTALL_ROOT.matcher(line).find() && DEMO_OR_ITS_DATABASES.matcher(line).find()) {
                    offenders.add(relative(document) + ": " + line.strip());
                }
            }
        }
        assertThat(offenders)
                .as("the root install URL installs the CLI and nothing else; a line that puts it beside the "
                        + "demo or the databases the demo starts sells one entry mode as another")
                .isEmpty();

        List<Path> documents = new ArrayList<>(List.of(README));
        documents.addAll(markdownUnder(DOCS));
        assertThat(documents).as("the docs tree must have been walked").hasSizeGreaterThan(3);
        List<String> formerName = new ArrayList<>();
        for (Path document : documents) {
            for (String line : read(document).split("\\R")) {
                if (line.toLowerCase(Locale.ROOT).contains(FORMER_DEMO_NAME)) {
                    formerName.add(relative(document) + ": " + line.strip());
                }
            }
        }
        assertThat(formerName)
                .as("the demo is 'tapstate demo'; its former name is a second entry mode that no longer exists")
                .isEmpty();
    }

    @Test
    @DisplayName("neither sample nor demo is sold as a one-command runner")
    void theSampleAndTheDemoAreNotSoldAsAOneCommandRunner() {
        // What the recogniser catches, before trusting that it caught nothing.
        assertThat(List.of(
                "One command to run Tapstate end to end",
                "a one-click demo",
                "Run Tapstate in one command:",
                "This demo installs Tapstate and starts it"))
                .allMatch(FirstRunWordingGatesTest::isSoldAsAOneCommandRunner);
        assertThat(List.of(
                "curl -sSL https://install.tapstate.dev | sh    # installs the CLI, nothing else",
                "the demo brings up a disposable stack from one command"))
                .noneMatch(FirstRunWordingGatesTest::isSoldAsAOneCommandRunner);

        Map<String, String> surfaces = new LinkedHashMap<>();
        for (Path document : List.of(README, DOCS.resolve("README.md"), FIRST_RUN_CONTRACT, DOCS.resolve("demo.md"))) {
            surfaces.put(relative(document), read(document));
        }
        surfaces.put("demo --help", help("demo", "--help"));
        surfaces.put("new --help", help("new", "--help"));
        assertThat(surfaces.get("demo --help")).contains("demo");
        assertThat(surfaces.get(relative(DOCS.resolve("demo.md")))).contains("install.tapstate.dev/demo");

        List<String> offenders = new ArrayList<>();
        surfaces.forEach((surface, text) -> {
            for (String line : text.split("\\R")) {
                if (isSoldAsAOneCommandRunner(line)) {
                    offenders.add(surface + ": " + line.strip());
                }
            }
        });
        assertThat(offenders)
                .as("sample and demo bring up a demo stack; neither is a one-command runner of Tapstate, and "
                        + "the demo is not an installation of it - this is the wording the product decided against")
                .isEmpty();
    }

    private static boolean isSoldAsAOneCommandRunner(String line) {
        String lower = line.toLowerCase(Locale.ROOT);
        if (ONE_COMMAND_RUNNER_PHRASES.stream().anyMatch(lower::contains)) {
            return true;
        }
        return lower.contains(INSTALLS_TAPSTATE) && DEMO.matcher(line).find();
    }

    /**
     * Every line the guided path speaks, keyed by the constant that holds it: the {@code *_QUESTION}
     * constants of the guided class and of every recipe, and the opening and offer lines. Found by
     * import rather than listed, so a recipe added tomorrow is held to the same words; read through
     * the field because the constants are package-private and this gate is not a reason to open them.
     */
    private static Map<String, String> guidedLines() {
        Map<String, String> lines = new LinkedHashMap<>();
        for (JavaClass type : cliClasses) {
            String name = type.getSimpleName();
            if (!GUIDED_CLASSES.contains(name) && !name.endsWith("Recipe")) {
                continue;
            }
            for (JavaField field : type.getFields()) {
                boolean spoken = field.getName().endsWith("_QUESTION") || SPOKEN_CONSTANTS.contains(field.getName());
                if (spoken && field.getModifiers().contains(JavaModifier.STATIC)
                        && field.getRawType().isEquivalentTo(String.class)) {
                    lines.put(name + "." + field.getName(), constantValue(field));
                }
            }
        }
        return lines;
    }

    private static String constantValue(JavaField field) {
        try {
            Field reflected = field.reflect();
            reflected.setAccessible(true);
            return String.valueOf(reflected.get(null));
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("cannot read " + field.getFullName(), e);
        }
    }

    /** The help of one command, captured the way the front end's own tests capture it. */
    private static String help(String... args) {
        CommandLine commandLine = Cli.newCommandLine();
        StringWriter out = new StringWriter();
        StringWriter err = new StringWriter();
        commandLine.setOut(new PrintWriter(out));
        commandLine.setErr(new PrintWriter(err));
        int code = commandLine.execute(args);
        assertThat(code).as("help of %s must render: %s", String.join(" ", args), err).isZero();
        return out + err.toString();
    }

    /** The README from its "Try it" heading to the next heading of the same level. */
    private static String tryItSection() {
        String readme = read(README);
        int start = readme.indexOf(TRY_IT_HEADING);
        assertThat(start).as("README.md must carry the %s section", TRY_IT_HEADING).isNotNegative();
        int end = readme.indexOf("\n## ", start + TRY_IT_HEADING.length());
        return end < 0 ? readme.substring(start) : readme.substring(start, end);
    }

    private static List<Path> markdownUnder(Path root) {
        try (Stream<Path> files = Files.walk(root)) {
            return files.filter(Files::isRegularFile)
                    .filter(file -> file.getFileName().toString().endsWith(".md"))
                    .sorted()
                    .toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String read(Path file) {
        try {
            return Files.readString(file, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot read " + file, e);
        }
    }

    private static String relative(Path file) {
        return REPOSITORY.toAbsolutePath().normalize().relativize(file.toAbsolutePath().normalize()).toString();
    }
}
