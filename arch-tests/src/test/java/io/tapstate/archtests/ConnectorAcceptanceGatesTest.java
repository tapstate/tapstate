package io.tapstate.archtests;

import io.tapstate.adapters.pdk.ConnectorArtifactRegistrar;
import io.tapstate.app.ConnectorPluginProperties;
import io.tapstate.core.catalog.OfficialConnectors;
import io.tapstate.core.catalog.ModeSource;
import io.tapstate.core.catalog.TapstateCatalog;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The gate behind "the mechanism is here and it is not open": runtime registration exists, and the set
 * of connectors it accepts ships closed.
 *
 * <p>The accepted set is widenable per deployment, and that is exactly what makes this worth a gate.
 * The guard refusing an unsupported connector is a boundary only for as long as nothing a user is
 * handed has already opened it — and opening it takes one line in a compose file or a quickstart
 * script, not a code change. No module test can make that claim: a module sees its own classes, while
 * this would be opened in a file no module compiles.
 *
 * <p>A proof of absence fails by passing vacuously — a scan that reached nothing, or that looked for a
 * spelling nobody uses, reports the same green as a scan that found nothing. So most of the tests here
 * police the scan rather than the product: it must recognise every spelling that would really open the
 * setting, it must have read the artifacts that could open it, and the single file allowed to name the
 * setting must still be naming it.
 */
class ConnectorAcceptanceGatesTest {

    /** Surefire runs a module from its own directory, so the repository root is one level up. */
    private static final Path REPOSITORY = Path.of("..");

    /**
     * The setting's name with everything the binder treats as noise removed — case, dashes and
     * underscores. Every interchangeable spelling collapses onto this one needle: the property
     * {@code tapstate.connectors.also-accept-ids}, the field {@code alsoAcceptIds} it binds to, and both
     * environment-variable forms a container can hand the process — the one that drops the dashes
     * ({@code TAPSTATE_CONNECTORS_ALSOACCEPTIDS}, which is the canonical mapping) and the one that turns
     * them into underscores. Matching literal spellings instead would leave whichever ones nobody
     * thought of as a way straight past this gate.
     */
    private static final String SETTING = "alsoacceptids";

    /**
     * Characters that can sit between a setting's name and its value without changing that it is one:
     * spacing, and the quotes a JSON or YAML key wears. A backtick is not among them — it is how prose
     * marks a name as a name, so treating it as padding turns a settings-reference bullet
     * ({@code - `some.setting`: what it does}) into an accusation of setting the thing it describes.
     */
    private static final String PADDING = " \t\"'";

    /** The one shipped file that must name the setting: the class that declares it. */
    private static final String DECLARATION =
            "app/src/main/java/io/tapstate/app/ConnectorPluginProperties.java";

    /**
     * Artifacts the scan is required to have read. Named individually rather than counted: these are the
     * files a deployment is actually configured in, so a scan that missed them is not a smaller scan, it
     * is the wrong one.
     */
    private static final List<String> MUST_BE_SCANNED = List.of(
            "deploy/quickstart/docker-compose.yml",
            "deploy/quickstart/quickstart.sh",
            "docs/quickstart-online.md");

    /**
     * Directories that are neither shipped nor written by hand: version-control internals, build output,
     * fetched dependencies, and the tool directory a session's own worktree is created under. The last
     * one holds a whole second copy of this repository, so walking it turns every shipped file into a
     * second reading of itself at a path nothing else accounts for.
     */
    private static final Set<String> PRUNED = Set.of(".git", "target", "node_modules", ".claude");

    @Test
    @DisplayName("the accepted connector set ships closed - nothing a release carries widens it")
    void noShippedArtifactWidensTheAcceptedSet() {
        Set<String> offenders = new TreeSet<>();
        for (Path file : shippedFiles()) {
            String relative = relative(file);
            if (relative.equals(DECLARATION)) {
                continue;
            }
            if (assignsTheSetting(read(file))) {
                offenders.add(relative);
            }
        }

        assertThat(offenders)
                .as("the accepted set is widenable per deployment and ships empty everywhere - a release "
                        + "artifact, script or document that gives the setting a value hands users a way "
                        + "past the guard, and turns a supported-connector boundary into a suggestion")
                .isEmpty();
    }

    @Test
    @DisplayName("the scan recognises every spelling that would really open it")
    void theScanRecognisesEverySpellingThatWouldOpenIt() {
        // The gate is only as wide as its recogniser, and a spelling it does not know is a way straight
        // past it that stays green. Relaxed binding accepts all of these for one setting: the property in
        // kebab and camel case, and the environment variable both with the dashes dropped - the canonical
        // mapping, which is the one a hand-written literal list is likeliest to miss - and with them
        // turned into underscores.
        assertThat(List.of(
                "      tapstate.connectors.also-accept-ids: acme",
                "      tapstate.connectors.alsoAcceptIds: acme",
                "      TAPSTATE_CONNECTORS_ALSOACCEPTIDS: acme",
                "      - TAPSTATE_CONNECTORS_ALSO_ACCEPT_IDS=acme",
                "exec java -Dtapstate.connectors.also-accept-ids=acme -jar app.jar",
                "{\"tapstate.connectors.also-accept-ids\": \"acme\"}"))
                .as("a spelling the binder honours but this scan does not is a hole, not a nicety")
                .allMatch(ConnectorAcceptanceGatesTest::assignsTheSetting);

        // And what must NOT redden the build: reading the value, and writing about it. Both are how the
        // setting comes to be understood; neither gives it one.
        assertThat(List.of(
                "        properties.getAlsoAcceptIds());",
                "Set `tapstate.connectors.also-accept-ids` only if you accept leaving the supported set.",
                "the also-accept-ids setting is empty in every shipped artifact",
                // The shape a settings reference is written in: name, then a colon, then what it does.
                // A backtick is prose marking a name as a name, so the colon after it describes rather
                // than assigns - the one line most likely to be written about this setting, and the one a
                // gate that skipped backticks would accuse.
                "- `tapstate.connectors.also-accept-ids`: further connector ids the register path accepts",
                "| `tapstate.connectors.also-accept-ids` | empty | ids accepted beyond the supported set |"))
                .as("a gate that reddens on reading or documenting a setting is accusing the wrong file")
                .noneMatch(ConnectorAcceptanceGatesTest::assignsTheSetting);
    }

    @Test
    @DisplayName("the scan reaches the artifacts that could open it")
    void theScanReachesTheArtifactsThatCouldOpenIt() {
        Set<String> scanned = new TreeSet<>();
        shippedFiles().forEach(file -> scanned.add(relative(file)));

        assertThat(scanned)
                .as("this gate asserts an absence, so a scan that reached nothing would pass exactly like "
                        + "a scan that found nothing - it has to be pinned to files that really exist")
                .containsAll(MUST_BE_SCANNED);
    }

    @Test
    @DisplayName("a second copy of the repository under a tool directory is not scanned")
    void aNestedCheckoutIsNotScanned(@TempDir Path root) throws IOException {
        // Working each session in its own worktree is this repository's own rule, and the tool that makes
        // them puts every one under .claude/worktrees - a whole second copy of the repository, inside the
        // tree this scan walks. Every file in that copy reads as shipped, the declaration among them, and
        // the declaration's copy is not at the path the skip compares against. So the gate accuses it: the
        // most alarming thing this gate can say, said about a working tree that is doing exactly what the
        // project asks for, on a machine where nothing is wrong. CI has no worktrees, so it stays green
        // there and the reading lands only on whoever is following the rule.
        //
        // The real file is written beside it rather than left out, because "nothing was flagged" and
        // "nothing was walked" are the same green - the pruning has to be shown to skip one thing while
        // still reaching another.
        Path nested = root.resolve(".claude/worktrees/a-session/app/src/main/java/io/tapstate/app");
        Files.createDirectories(nested);
        Files.writeString(nested.resolve("ConnectorPluginProperties.java"), "private List<String> alsoAcceptIds;");
        Path shipped = root.resolve("app/src/main/java/io/tapstate/app");
        Files.createDirectories(shipped);
        Files.writeString(shipped.resolve("Real.java"), "// a file the walk must still reach");

        assertThat(shippedFiles(root).stream().map(file -> relative(root, file)).toList())
                .as("a copy of the repository under a tool-managed directory is not something a release "
                        + "carries, so walking into it can only produce accusations about files that are "
                        + "already accounted for at their real paths")
                .containsExactly("app/src/main/java/io/tapstate/app/Real.java");
    }

    @Test
    @DisplayName("the file allowed to name the setting still declares it")
    void theOneFileAllowedToNameTheSettingStillDoes() {
        // A skipped file that no longer declares the setting would sit here forever, quietly excusing
        // whatever else that path came to hold.
        Path declaration = REPOSITORY.resolve(DECLARATION);

        assertThat(Files.isRegularFile(declaration))
                .as("the skipped file must exist: %s", DECLARATION)
                .isTrue();
        assertThat(read(declaration))
                .as("the skipped file is skipped because it declares the setting - if it no longer does, "
                        + "the exception is stale and something else is being let through under it")
                .contains("alsoAcceptIds");
    }

    /**
     * The database kinds the release actually exercises. This independent literal makes adding a
     * kind a deliberate support promise in both the catalog declaration and this gate.
     */
    private static final List<String> VERIFIED_DATABASE_KINDS = List.of("mysql", "postgres", "mongodb");

    /** Counts include each database kind's own id and its explicitly accepted variants. */
    private static final Map<String, Integer> EXPECTED_IDS_PER_DATABASE_KIND = Map.of(
            "mysql", 5,
            "postgres", 5,
            "mongodb", 4);

    /** Every id a shipped deployment accepts out of the box, in refusal-message order. */
    private static final List<String> ACCEPTED_OUT_OF_THE_BOX = List.of(
            "mysql", "aliyun-rds-mysql", "aws-rds-mysql", "polar-db-mysql", "mysql-pxc",
            "postgres", "aliyun-rds-postgres", "aliyun-adb-postgres", "polar-db-postgres",
            "tencent-db-postgres",
            "mongodb", "mongodb-atlas", "aliyun-db-mongodb", "tencent-db-mongodb");

    @Test
    @DisplayName("what a shipped deployment accepts out of the box is exactly this set")
    void whatAShippedDeploymentAcceptsIsExactlyThisSet() {
        // Two facts make the claim, and neither is enough alone: this is the set the register path is
        // built with, and nothing a release carries adds to it (the two tests above). Stating it here,
        // outside every module, is the only place both are in view - the module that owns the list
        // cannot see the deployment assets, and the assets are not compiled by anything.
        //
        // Written out a second time on purpose. The module test pins the field against the field; this
        // pins it against a list maintained apart from it, so widening the set means saying so twice, in
        // two modules, which is the smallest amount of friction that still makes a support promise
        // deliberate. A test that read the same constant it asserts would agree with any value it held.
        assertThat(ConnectorArtifactRegistrar.officialConnectorIds())
                .as("adding an id here is a promise that the product accepts that connector - it is made "
                        + "in two places so that it cannot be made absent-mindedly in one")
                .containsExactlyElementsOf(ACCEPTED_OUT_OF_THE_BOX);

        assertThat(OfficialConnectors.IDS_BY_DATABASE_KIND.keySet())
                .as("the declared database kinds must match the independently verified kinds")
                .containsExactlyElementsOf(VERIFIED_DATABASE_KINDS);

        Map<String, Integer> declaredCounts = new LinkedHashMap<>();
        OfficialConnectors.IDS_BY_DATABASE_KIND.forEach((kind, ids) -> declaredCounts.put(kind, ids.size()));
        assertThat(declaredCounts)
                .as("each database kind has an explicit support count; variants are not uniformly sized")
                .isEqualTo(EXPECTED_IDS_PER_DATABASE_KIND);
    }

    @Test
    @DisplayName("every offered mode of every official connector comes from a capability probe")
    void everyOfficialConnectorModeIsDerived() {
        TapstateCatalog catalog = TapstateCatalog.load();
        assertThat(OfficialConnectors.IDS).as("the official connector scan must not be empty").isNotEmpty();
        assertThat(catalog.ids())
                .as("every official connector must resolve in the bundled catalog")
                .containsAll(OfficialConnectors.IDS);
        for (String id : OfficialConnectors.IDS) {
            var entry = catalog.byId(id);
            assertThat(entry.modes())
                    .as("official connector %s must offer at least one mode", id)
                    .isNotEmpty();
            for (var mode : entry.modes()) {
                assertThat(entry.provenance().modeSource().get(mode))
                        .as("official connector %s mode %s must be derived from registered capabilities", id, mode)
                        .isEqualTo(ModeSource.DERIVED);
            }
        }
    }

    @Test
    @DisplayName("the accepted set is empty until a deployment names something")
    void theAcceptedSetIsEmptyUntilADeploymentNamesSomething() {
        assertThat(new ConnectorPluginProperties().getAlsoAcceptIds())
                .as("the default is what every deployment gets that does not say otherwise, so it is the "
                        + "value that decides whether the guard is on by default")
                .isEmpty();
    }

    /**
     * Whether a file gives the setting a value, as opposed to merely mentioning it. What opens the guard
     * is an assignment — a compose environment entry, a property line, an exported variable, a command
     * flag — so that is what is banned. Banning the name itself would redden the build for the code that
     * reads the value and for any page that documents it, which is an accusation neither one has earned,
     * and a gate that punishes describing a setting teaches people to stop describing it.
     */
    private static boolean assignsTheSetting(String text) {
        String flattened = text.toLowerCase(Locale.ROOT).replace("-", "").replace("_", "");
        int at = flattened.indexOf(SETTING);
        while (at >= 0) {
            int after = at + SETTING.length();
            while (after < flattened.length() && PADDING.indexOf(flattened.charAt(after)) >= 0) {
                after++;
            }
            if (after < flattened.length() && (flattened.charAt(after) == '=' || flattened.charAt(after) == ':')) {
                return true;
            }
            at = flattened.indexOf(SETTING, at + 1);
        }
        return false;
    }

    /** Everything a release carries or a user reads: shipped sources, deployment assets, documentation. */
    private static List<Path> shippedFiles() {
        return shippedFiles(REPOSITORY);
    }

    /**
     * The same walk over a named root. The root is a parameter only so that what the walk refuses to
     * enter can be witnessed over a tree a test builds - pruning is the half of this scan that fails
     * silently, by widening rather than by narrowing.
     */
    private static List<Path> shippedFiles(Path root) {
        List<Path> files = new ArrayList<>();
        try {
            Files.walkFileTree(root, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult preVisitDirectory(Path directory, BasicFileAttributes attributes) {
                    return PRUNED.contains(directory.getFileName().toString())
                            ? FileVisitResult.SKIP_SUBTREE
                            : FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attributes) {
                    if (isShipped(relative(root, file))) {
                        files.add(file);
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            throw new UncheckedIOException("walking the repository at " + root, e);
        }
        return files;
    }

    /** Shipped = a module's main sources, the deployment assets, and the documentation. Tests are not. */
    private static boolean isShipped(String relative) {
        return relative.contains("/src/main/")
                || relative.startsWith("deploy/")
                || relative.startsWith("docs/");
    }

    private static String relative(Path file) {
        return relative(REPOSITORY, file);
    }

    private static String relative(Path root, Path file) {
        return root.relativize(file).toString().replace('\\', '/');
    }

    /**
     * The file's text, decoded so that no byte sequence can throw: the scan crosses whatever a deployment
     * directory happens to hold, and the spellings it looks for are ASCII either way.
     */
    private static String read(Path file) {
        try {
            return new String(Files.readAllBytes(file), StandardCharsets.ISO_8859_1);
        } catch (IOException e) {
            throw new UncheckedIOException("reading " + file, e);
        }
    }
}
