package io.tapstate.cli;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import picocli.CommandLine;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The dual-mode CLI's command table: the offline-verb whitelist (validate / new / explain), the coded
 * not-connected and not-implemented affordances (which must survive the operands these verbs are really
 * typed with), the exit-code contract, and validate wired to the offline DSL link.
 */
class CliTest {

    /** Captured outcome of one one-shot CLI invocation. */
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

    private static Path resource(String name) {
        try {
            return Path.of(CliTest.class.getResource("/" + name).toURI());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    static Stream<String> offlineVerbs() {
        return Cli.OFFLINE_VERBS.stream();
    }

    static Stream<String> connectedVerbs() {
        return Cli.CONNECTED_VERBS.stream();
    }

    static Stream<String> unimplementedCompositeVerbs() {
        return Cli.UNIMPLEMENTED_COMPOSITE_VERBS.stream();
    }

    @Test
    void offlineVerbsAreRegistered() {
        assertThat(Cli.newCommandLine().getSubcommands().keySet())
                .contains("validate", "new", "explain");
    }

    @Test
    void connectedVerbsAreRegisteredNotMissing() {
        // connect is a REPL builtin (session-scoped), not a one-shot subcommand
        assertThat(Cli.newCommandLine().getSubcommands().keySet())
                .contains("apply", "run")
                .doesNotContain("connect");
    }

    @Test
    void liveViewVerbsAreOnlineLaunchesEvenThoughTheyProjectNoOperation() {
        assertThat(Cli.LIVE_VIEW_VERBS).allSatisfy(verb ->
                assertThat(Repl.isOnlineVerb(verb)).as(verb).isTrue());
    }

    @Test
    void mcpLauncherIsAOneShotMetaCommand() {
        Run help = run("mcp", "--help");

        assertThat(help.code()).isZero();
        assertThat(help.out()).contains("Usage: tapstate mcp", "--server", "--allow-write");
        assertThat(help.err()).isEmpty();
    }

    @Test
    void offlineVerbWhitelistMatchesRegisteredSubcommands() {
        // single source of truth: every registered subcommand that needs neither a server nor an
        // implementation it has not got must be exactly the declared offline whitelist (so the
        // recovery hint can never drift)
        TreeSet<String> registeredOffline = new TreeSet<>(Cli.newCommandLine().getSubcommands().keySet());
        registeredOffline.removeAll(Cli.CONNECTED_VERBS);
        registeredOffline.removeAll(Cli.UNIMPLEMENTED_COMPOSITE_VERBS);
        // the live views project no operation, so they are not in the connected list, but they are the
        // opposite of offline: each is a loop over reads that only a server can answer
        registeredOffline.removeAll(Cli.LIVE_VIEW_VERBS);
        // the meta commands are about the CLI, not about a resource: they project no operation and so
        // belong to no verb whitelist. Subtracting them by name keeps this guard's real job -- catching
        // a *product* verb registered without being declared -- rather than widening it to anything new
        registeredOffline.removeAll(Cli.META_VERBS);
        assertThat(registeredOffline).containsExactlyInAnyOrderElementsOf(Cli.OFFLINE_VERBS);
    }

    @ParameterizedTest
    @MethodSource("offlineVerbs")
    void everyOfflineVerbSupportsHelp(String verb) {
        // every offline verb must accept --help like the root command does, printing its own usage
        // rather than being rejected as an unknown option (or, worse, masked behind a required-param
        // error for verbs with a mandatory positional, as desc's ID was before mixinStandardHelpOptions)
        Run r = run(verb, "--help");
        assertThat(r.code()).isZero();
        assertThat(r.out()).contains("Usage: tapstate " + verb);
        assertThat(r.err()).isEmpty();
    }

    @ParameterizedTest
    @MethodSource("offlineVerbs")
    void everyVerbThatAdvertisesVersionPrintsOne(String verb) {
        // mixinStandardHelpOptions registers -V alongside -h, so every verb carrying it *promises* a
        // version in its own usage text. A verb spec with no version prints an empty line and exits 0 —
        // an option the help advertises and that then does nothing, which is worse than the plain
        // "Unknown option: '-V'" it replaced.
        Run r = run(verb, "--help");
        Assumptions.assumeTrue(r.out().contains("-V, --version"), "verb does not advertise -V");

        Run version = run(verb, "-V");
        assertThat(version.code()).isZero();
        assertThat(version.out()).contains(Cli.VERSION);
        assertThat(version.err()).isEmpty();
    }

    @Test
    void theReportedVersionTracksTheProjectVersion() {
        // Cli.VERSION is a compile-time constant so that -V costs neither a manifest nor a bundled
        // resource in the native image. The price of a constant is that it can be left behind by a
        // release, shipping a binary that misreports itself; the build hands the real project version
        // in so that drift fails here instead.
        String projectVersion = System.getProperty("tapstate.project.version");
        assertThat(projectVersion)
                .as("the build must pass -Dtapstate.project.version so this guard can run at all")
                .isNotBlank();
        assertThat(Cli.VERSION).isEqualTo("tapstate " + projectVersion);
    }

    @Test
    void everyRegisteredSubcommandCarriesTheRootVersion() {
        // the version belongs to the binary, not to one verb: whatever the table answers to must report
        // the same string the root does, so `tapstate -V` and `tapstate <verb> -V` can never disagree
        assertThat(Cli.newCommandLine().getSubcommands().values())
                .allSatisfy(sub -> assertThat(sub.getCommandSpec().version())
                        .as("subcommand '%s' has no version to print", sub.getCommandName())
                        .containsExactly(Cli.VERSION));
    }

    @Test
    void validateAcceptsAValidWorkspace() {
        Run r = run("validate", resource("ws-valid").toString());
        assertThat(r.code()).isZero();
        // "invalid" contains "valid", so anchor on the success shape, not a bare substring
        assertThat(r.out()).startsWith("valid:").contains("3 resources");
        assertThat(r.err()).isEmpty();
    }

    @Test
    void validateJudgesAWorkspaceWithoutResolvingItsReferences(@TempDir Path ws) throws Exception {
        // validate is the offline verb: it reads no environment, so a reference stays opaque to it and a
        // workspace that carries one still validates. That is what lets a check run somewhere the
        // variables are not set — a build box, a reviewer's laptop — and it is the standing reason the
        // capability rules skip a value they cannot see. Resolution belongs to apply, which is the verb
        // that has an environment to resolve from.
        Files.createDirectory(ws.resolve("source"));
        Files.writeString(ws.resolve("source").resolve("src.tap.yml"), """
                version: tapstate/v1
                kind: source
                id: src
                connector: mongodb
                config: { uri: "${MONGO_URI}" }
                """);

        Run r = run("validate", ws.toString());

        assertThat(r.code()).isZero();
        assertThat(r.out()).startsWith("valid:");
    }

    @Test
    void validateRejectsAnInvalidWorkspaceWithCodeAndLocation() {
        Run r = run("validate", resource("ws-invalid").toString());
        assertThat(r.code()).isEqualTo(1);
        // the dsl-domain error code surfaces, located at the offending file
        assertThat(r.all()).contains("dsl.unknown-field");
        assertThat(r.all()).contains("src_typo.tap.yml");
    }

    @Test
    void validateHumanOutputRendersTheCatalogMessageNotJustTheCode() {
        Run r = run("validate", resource("ws-invalid").toString());
        assertThat(r.code()).isEqualTo(1);
        // the rendered, user-facing message — not only the bare dev string
        assertThat(r.all()).contains("Unknown field");
        // the canonical code stays visible as the stable, machine-referable identity
        assertThat(r.all()).contains("dsl.unknown-field");
    }

    @Test
    void validateJsonEmitsTheValidEnvelopeOnStdout() {
        Run r = run("validate", "-o", "json", resource("ws-valid").toString());
        assertThat(r.code()).isZero();
        assertThat(r.out()).contains("\"status\": \"valid\"")
                .contains("\"resourceCount\": 3")
                .contains("\"diagnostics\": []");
    }

    @Test
    void validateJsonEmitsDiagnosticsOnStdoutWhenInvalid() {
        Run r = run("validate", "-o", "json", resource("ws-invalid").toString());
        assertThat(r.code()).isEqualTo(1);
        assertThat(r.out()).contains("\"status\": \"invalid\"")
                .contains("\"code\": \"dsl.unknown-field\"")
                .contains("\"severity\": \"ERROR\"")
                .contains("\"source\": \"src_typo.tap.yml\"")
                .contains("Unknown field");
    }

    @Test
    void validateYamlEmitsTheValidEnvelope() {
        Run r = run("validate", "-o", "yaml", resource("ws-valid").toString());
        assertThat(r.code()).isZero();
        assertThat(r.out()).contains("status: valid")
                .contains("resourceCount: 3")
                .contains("diagnostics: []");
    }

    @Test
    void validateRejectsAnUnknownOutputFormat() {
        Run r = run("validate", "-o", "toml", resource("ws-valid").toString());
        assertThat(r.code()).isEqualTo(2);
    }

    @Test
    void validateMissingPathIsAUsageError() {
        Run r = run("validate", "/no/such/tapstate/workspace");
        assertThat(r.code()).isEqualTo(2);
        assertThat(r.err()).containsIgnoringCase("not found");
    }

    @Test
    void validateEmptyWorkspaceIsAUsageErrorNotSilentSuccess(@TempDir Path empty) {
        Run r = run("validate", empty.toString());
        assertThat(r.code()).isEqualTo(2);
        assertThat(r.err()).contains("no *.tap.yml artifacts");
        assertThat(r.out()).doesNotContain("valid:");
    }

    @Test
    void validateRendersIoFaultCleanlyInsteadOfStackTrace(@TempDir Path dir) throws Exception {
        Path artifact = dir.resolve("locked.tap.yml");
        Files.writeString(artifact, "version: tapstate/v1\nkind: source\nid: x\nconnector: mysql\nconfig: {}\n");
        boolean blocked = artifact.toFile().setReadable(false);
        Assumptions.assumeTrue(blocked && !Files.isReadable(artifact),
                "filesystem does not enforce owner-unreadable; skipping IO-fault rendering test");
        Run r = run("validate", dir.toString());
        assertThat(r.code()).isNotZero();
        assertThat(r.err()).contains("cannot read workspace");
        assertThat(r.err()).doesNotContain("Exception");   // a clean diagnostic, not a raw stack
    }

    @Test
    void validateRendersMalformedYamlAsACodedDiagnosticNotARawStack(@TempDir Path dir) throws Exception {
        // a syntactically broken file must surface as a coded dsl.malformed-yaml, never a raw snakeyaml stack
        Path artifact = dir.resolve("broken.tap.yml");
        Files.writeString(artifact, "[unterminated\n");
        Run r = run("validate", artifact.toString());
        assertThat(r.code()).isEqualTo(1);
        assertThat(r.err()).contains("dsl.malformed-yaml");
        assertThat(r.all()).doesNotContain("org.yaml.snakeyaml");
    }

    @Test
    void validateWithNoPathValidatesTheWorkspaceRoot() {
        // no positional path given: validate resolves to the workspace root carried by -w, not "."
        // ws-valid is laid out by kind (source/, pipeline/), so this also exercises the happy path of
        // the workspace-root layout gate — correctly placed artifacts pass enforcement
        String wsRoot = resource("ws-valid").toString();
        Run r = run("validate", "-w", wsRoot);
        assertThat(r.code()).isZero();
        // "invalid" contains "valid", so anchor on the success shape, not a bare substring
        assertThat(r.out()).startsWith("valid:").contains("3 resources");
        // the resolved workspace root is what gets echoed back, proving the root drove the run
        assertThat(r.out()).contains(wsRoot);
        assertThat(r.err()).isEmpty();
    }

    @Test
    void validateExplicitPathOverridesTheWorkspaceRoot() {
        // an explicit positional wins over -w: the bogus workspace root is ignored, the path validated
        String explicit = resource("ws-valid").toString();
        Run r = run("validate", "-w", "/no/such/tapstate/workspace", explicit);
        assertThat(r.code()).isZero();
        assertThat(r.out()).startsWith("valid:");
        // the explicit positional — not the bogus -w root — is the one resolved and echoed
        assertThat(r.out()).contains(explicit).doesNotContain("/no/such/tapstate/workspace");
    }

    @Test
    void validateNoPathReportsTheMissingWorkspaceRoot() {
        // with no positional and a workspace root that does not exist, the missing root is the diagnostic
        Run r = run("validate", "-w", "/no/such/tapstate/workspace");
        assertThat(r.code()).isEqualTo(2);
        assertThat(r.err()).contains("/no/such/tapstate/workspace");
        assertThat(r.err()).containsIgnoringCase("not found");
    }

    @Test
    void validateNoPathEmptyWorkspaceRootIsAUsageError(@TempDir Path empty) {
        // an existing but empty workspace root (resolved via -w, no positional) is a usage error,
        // and the diagnostic names the resolved root — exercising the empty-workspace branch through
        // the workspace mechanism, not just an explicit positional
        Run r = run("validate", "-w", empty.toString());
        assertThat(r.code()).isEqualTo(2);
        assertThat(r.err()).contains("no *.tap.yml artifacts").contains(empty.toString());
        assertThat(r.out()).doesNotContain("valid:");
    }

    @Test
    void validateWorkspaceRootRejectsArtifactInTheWrongKindDirectory() {
        // structure is truth, enforced on the managed workspace root (no positional, -w points at it):
        // a source file dropped into the pipeline/ directory is misplaced — a coded cli diagnostic
        // naming the offending file, not a silent pass
        Run r = run("validate", "-w", resource("ws-misplaced").toString());
        assertThat(r.code()).isEqualTo(1);
        assertThat(r.all()).contains("cli.kind-dir-mismatch");
        assertThat(r.all()).contains("src_misplaced.tap.yml");
        // the rendered, user-facing message — not only the bare code — naming the declared kind
        assertThat(r.all()).contains("declares kind 'source'");
    }

    @Test
    void validateWorkspaceRootRejectsASourceFileInTheViewDirectory() {
        // the canonical structure-is-truth case: a kind:source artifact placed under view/ is
        // misplaced. The gate is kind-generic, but this pins the exact view/ scenario — the coded
        // diagnostic names the offending file, its declared kind, and the directory it sits in.
        Run r = run("validate", "-w", resource("ws-view-misplaced").toString());
        assertThat(r.code()).isEqualTo(1);
        assertThat(r.all()).contains("cli.kind-dir-mismatch");
        assertThat(r.all()).contains("src_in_view.tap.yml");
        assertThat(r.all()).contains("declares kind 'source'");
        // the directory naming half of the message — proving the gate read view/, not some other dir
        // (the quoted phrase pins the {dir} param, so the src_in_view filename cannot satisfy it)
        assertThat(r.all()).contains("'view' directory");
    }

    @Test
    void validateWorkspaceRootRejectsArtifactSittingDirectlyAtTheRoot() {
        // a file directly at the workspace root (not under any kind directory) is misplaced too: its
        // parent is the root directory name, which never equals a kind
        Run r = run("validate", "-w", resource("ws-root-misplaced").toString());
        assertThat(r.code()).isEqualTo(1);
        assertThat(r.all()).contains("cli.kind-dir-mismatch");
        assertThat(r.all()).contains("src_root.tap.yml");
    }

    @Test
    void validateExplicitDirectorySkipsTheKindDirectoryCheck() {
        // an explicit positional directory is ad-hoc validation, not the managed workspace: the same
        // misplaced layout that fails via -w passes here, the gate does not fire
        Run r = run("validate", resource("ws-misplaced").toString());
        assertThat(r.code()).isZero();
        assertThat(r.out()).startsWith("valid:");
        assertThat(r.all()).doesNotContain("cli.kind-dir-mismatch");
    }

    @Test
    void validateSingleFileSkipsTheKindDirectoryCheck() {
        // a single named file carries no workspace-layout claim: the same misplaced source validates
        // fine when pointed at directly, even though it sits in the wrong kind directory
        Run r = run("validate",
                resource("ws-misplaced").resolve("pipeline").resolve("src_misplaced.tap.yml").toString());
        assertThat(r.code()).isZero();
        assertThat(r.out()).startsWith("valid:");
        assertThat(r.all()).doesNotContain("cli.kind-dir-mismatch");
    }

    @Test
    void validateFlatDirectoryFromNewOutIsAccepted(@TempDir Path flat) {
        // the --out flat escape hatch must round-trip: scaffold a source flat, then validate that
        // directory explicitly — the layout gate does not fire on an explicit path, so it is accepted
        Run created = run("new", "--kind", "source", "--id", "src_flat", "--connector", "mysql",
                "--set", "host=10.0.0.1", "--set", "username=u", "--set", "password=p",
                "--out", flat.toString());
        assertThat(created.code()).isZero();
        Run r = run("validate", flat.toString());
        assertThat(r.code()).isZero();
        assertThat(r.out()).startsWith("valid:");
        assertThat(r.all()).doesNotContain("cli.kind-dir-mismatch");
    }

    @Test
    void validateJsonEmitsTheKindDirMismatchDiagnostic() {
        Run r = run("validate", "-o", "json", "-w", resource("ws-misplaced").toString());
        assertThat(r.code()).isEqualTo(1);
        assertThat(r.out()).contains("\"status\": \"invalid\"")
                .contains("\"code\": \"cli.kind-dir-mismatch\"")
                .contains("\"severity\": \"ERROR\"")
                .contains("\"source\": \"src_misplaced.tap.yml\"")
                // all three declared params travel in the structured contract (TreeMap-sorted)
                .contains("\"dir\": \"pipeline\"")
                .contains("\"kind\": \"source\"")
                .contains("\"path\": \"src_misplaced.tap.yml\"");
    }

    @Test
    void validateYamlEmitsTheKindDirMismatchDiagnostic() {
        Run r = run("validate", "-o", "yaml", "-w", resource("ws-misplaced").toString());
        assertThat(r.code()).isEqualTo(1);
        assertThat(r.out()).contains("status: invalid")
                .contains("code: cli.kind-dir-mismatch")
                .contains("source: src_misplaced.tap.yml")
                .contains("dir: pipeline")
                .contains("kind: source");
    }

    @Test
    void connectedVerbReportsNotConnectedRatherThanMissing() {
        Run r = run("apply");
        assertThat(r.code()).isEqualTo(3);
        assertThat(r.err()).contains("cli.not-connected");
    }

    @ParameterizedTest
    @MethodSource("connectedVerbs")
    void everyConnectedVerbReportsItsOwnName(String verb) {
        Run r = run(verb);
        assertThat(r.code()).isEqualTo(3);
        // the shared handler must render a coded diagnostic naming the verb actually typed, for all of
        // them — not just apply, and not a bare string that no catalog or machine reader can resolve
        assertThat(r.err()).contains("cli.not-connected").contains(verb);
    }

    @ParameterizedTest
    @MethodSource("connectedVerbs")
    void aConnectedVerbReportsNotConnectedEvenWhenGivenArguments(String verb) {
        // the affordance has to survive the way these verbs are actually typed — `apply x.yml`, not a
        // bare `apply`. A verb that takes no arguments would reject the operand as unmatched and print
        // usage instead, telling the user nothing about the connection they are missing.
        Run r = run(verb, "some-id", "--force");
        assertThat(r.code()).isEqualTo(3);
        assertThat(r.err()).contains("cli.not-connected").contains(verb);
    }

    @ParameterizedTest
    @MethodSource("unimplementedCompositeVerbs")
    void anUnimplementedCompositeVerbSaysSoRatherThanBlamingTheConnection(String verb) {
        Run r = run(verb);
        assertThat(r.code()).isEqualTo(3);
        // these verbs compose registered operations but have no implementation yet, so "you are not
        // connected" is simply false — connecting would not make them work
        assertThat(r.err()).contains("cli.verb-not-implemented").contains(verb);
        assertThat(r.err()).doesNotContain("cli.not-connected");
    }

    @ParameterizedTest
    @MethodSource("unimplementedCompositeVerbs")
    void anUnimplementedCompositeVerbSaysSoEvenWhenGivenArguments(String verb) {
        Run r = run(verb, "some-id", "--force");
        assertThat(r.code()).isEqualTo(3);
        assertThat(r.err()).contains("cli.verb-not-implemented").contains(verb);
    }

    @Test
    void helpListsEveryReplBuiltin() {
        // tab completion already offered these -- the completer is built from the same list -- so the
        // shell would complete `connect` while `help` denied it existed. The not-connected diagnostic
        // tells the user to run `connect`, which made help the only place that word could not be found.
        Run r = run("help");
        assertThat(r.out()).contains(Repl.BUILTINS);
    }

    @Test
    void helpExplainsTheTwoModesAndWhereTheWorkspaceOptionGoes() {
        // the dual shape is the whole of how this CLI is used and the usage text said nothing about it.
        // -w was the sharp edge: it opens a session in a directory, but before a verb it is an error --
        // which is worth stating, since the obvious guess `tapstate -w DIR validate` is the wrong one
        Run r = run("help");
        assertThat(r.out()).contains("open a session").contains("run one command and exit");
        assertThat(r.out()).contains("tapstate validate -w DIR");
        assertThat(r.out()).contains("$TAPSTATE_WORKDIR");
    }

    @Test
    void theWorkspaceOptionBeforeAVerbIsRefusedNotIgnored() {
        // the top level deliberately does not declare -w: picocli would then parse it into the root and
        // leave the verb on its own default, so the directory the user named would be silently dropped.
        // Refusing it is what makes the help's "put -w after the verb" advice safe to follow
        Run r = run("-w", "somewhere", "validate");
        assertThat(r.code()).isEqualTo(2);
        assertThat(r.err()).contains("-w");
    }

    @Test
    void helpStaysWithinItsOwnUsageWidth() {
        // the session-command list is rendered by hand, so it is the one section picocli does not wrap
        // for us -- a summary edited to a few words longer would push a ragged line past every other
        // section without anything else noticing
        int width = Cli.newCommandLine().getCommandSpec().usageMessage().width();
        assertThat(run("help").out().lines().toList())
                .allSatisfy(line -> assertThat(line.length()).isLessThanOrEqualTo(width));
    }

    @Test
    void everyReplBuiltinIsDescribedInHelp() {
        // pins the help entries to the builtins the REPL actually dispatches, so a builtin cannot be
        // added to one and left out of the other
        assertThat(new TreeSet<>(Cli.BUILTIN_HELP.keySet()))
                .isEqualTo(new TreeSet<>(Repl.BUILTINS));
    }

    @Test
    void aReplBuiltinTypedAsAOneShotSaysWhereItLives() {
        // `connect` is session-scoped and deliberately not a one-shot verb, but answering it with a
        // spelling guess -- "Did you mean: tapstate connectors?" -- treated a correctly spelt word the
        // user was told to type as a typo, and named an unrelated verb as the fix
        Run r = run("connect", "http://127.0.0.1:8080");
        assertThat(r.code()).isEqualTo(3);
        assertThat(r.err()).contains("cli.repl-builtin-only").contains("connect");
        assertThat(r.err()).doesNotContain("Did you mean");
    }

    @Test
    void helpIsAWordTheCliAnswersTo() {
        // the REPL's own banner says "Type 'help' for commands", and that is the word a user carries to
        // the shell. It used to be an unmatched argument there, answered with a spelling guess -- "Did
        // you mean: tapstate schema or tapstate discover-schema?" -- for a word that was never misspelt
        Run r = run("help");
        assertThat(r.code()).isZero();
        assertThat(r.out()).contains("Usage: tapstate").contains("validate");
        assertThat(r.err()).isEmpty();
    }

    @Test
    void helpTakesTheNameOfAVerb() {
        Run r = run("help", "apply");
        assertThat(r.code()).isZero();
        assertThat(r.out()).contains("Usage: tapstate apply")
                .contains(Cli.VERB_HELP.get("apply").summary());
        assertThat(r.err()).isEmpty();
    }

    @Test
    void everyVerbBehindASharedHandlerHasHelpOfItsOwn() {
        // twenty verbs are two classes registered under many names, so an annotation can only give them
        // all one sentence. Without a per-verb entry the table lists a verb and still cannot say what it
        // does — this pins the entries to the registered names, in both directions
        TreeSet<String> registered = new TreeSet<>(Cli.CONNECTED_VERBS);
        registered.addAll(Cli.UNIMPLEMENTED_COMPOSITE_VERBS);
        registered.addAll(Cli.LIVE_VIEW_VERBS);
        assertThat(new TreeSet<>(Cli.VERB_HELP.keySet())).isEqualTo(registered);
    }

    @Test
    void noTwoVerbsAreDescribedTheSameWay() {
        // a copy-pasted summary is the failure this table exists to prevent, and it reads as plausible
        // in review precisely because every one of these verbs is a near neighbour of another
        assertThat(Cli.VERB_HELP.values().stream().map(Cli.VerbHelp::summary).toList())
                .doesNotHaveDuplicates();
    }

    @ParameterizedTest
    @MethodSource("connectedVerbs")
    void everyConnectedVerbHelpShowsTheOperandsItTakes(String verb) {
        // the grammar is the half of the answer a description cannot carry: `schema` taking an optional
        // table, `status` taking --watch, and which verbs accept -o at all were discoverable only by
        // typing the verb wrong and reading the complaint
        Run r = run(verb, "--help");
        assertThat(r.out()).contains(Cli.VERB_HELP.get(verb).operands());
        assertThat(r.out()).contains(Cli.VERB_HELP.get(verb).summary());
    }

    @ParameterizedTest
    @MethodSource("unimplementedCompositeVerbs")
    void everyReservedVerbHelpSaysWhatItIsReservedFor(String verb) {
        Run r = run(verb, "--help");
        assertThat(r.out()).contains(Cli.VERB_HELP.get(verb).summary());
        // and still says it does not exist yet, which is the fact that governs today
        assertThat(r.out()).contains("not implemented yet");
    }

    @ParameterizedTest
    @MethodSource("connectedVerbs")
    void everyConnectedVerbSupportsHelp(String verb) {
        // these verbs swallow their operands so that the connection diagnostic always wins over a usage
        // error, but that swallowed --help along with them: the one question whose answer does not
        // depend on being connected was the one question they could not answer. Asking what a verb is
        // must work in the state the user is in when they ask -- offline, before connecting.
        Run r = run(verb, "--help");
        assertThat(r.code()).isZero();
        assertThat(r.out()).contains("Usage: tapstate " + verb);
        assertThat(r.err()).isEmpty();
    }

    @ParameterizedTest
    @MethodSource("unimplementedCompositeVerbs")
    void everyUnimplementedCompositeVerbSupportsHelp(String verb) {
        // a reserved verb has all the more reason to explain itself: "not implemented yet" is the whole
        // answer only once the user knows what it was going to be
        Run r = run(verb, "--help");
        assertThat(r.code()).isZero();
        assertThat(r.out()).contains("Usage: tapstate " + verb);
        assertThat(r.err()).isEmpty();
    }

    @Test
    void unknownVerbIsAUsageErrorDistinctFromConnectedVerbs() {
        Run r = run("florp");
        // a usage error (exit 2), naming the offending token — not the connected-verb code (3)
        assertThat(r.code()).isEqualTo(2);
        assertThat(r.err()).contains("florp");
    }

    @Test
    void explainRootListsTheResourceKinds() {
        Run r = run("explain");
        assertThat(r.code()).isZero();
        assertThat(r.out()).contains("tapstate/v1");
        assertThat(r.out()).contains("source").contains("pipeline").contains("serve");
    }

    @Test
    void explainScalarFieldRendersTypeAndDescription() {
        Run r = run("explain", "source.connector");
        assertThat(r.code()).isZero();
        assertThat(r.out()).contains("source.connector");
        assertThat(r.out()).contains("string");
        // the field's own description from the schema
        assertThat(r.out()).contains("connector");
        // a required field is marked as such
        assertThat(r.out()).containsIgnoringCase("required");
    }

    @Test
    void explainEnumFieldListsItsAllowedValues() {
        Run r = run("explain", "source.mode");
        assertThat(r.code()).isZero();
        assertThat(r.out()).contains("source.mode").contains("enum");
        assertThat(r.out()).contains("cdc").contains("snapshot").contains("stream").contains("api");
        // each value renders with its schema description, not just the bare token
        assertThat(r.out()).contains("Change data capture");
    }

    @Test
    void explainObjectFieldListsItsChildFieldsWithTypeAndRequiredMarker() {
        Run r = run("explain", "source");
        assertThat(r.code()).isZero();
        assertThat(r.out()).contains("connector").contains("mode").contains("tables");
        // the FIELDS table carries each child's type and marks the required ones
        assertThat(r.out()).contains("enum").contains("string").containsIgnoringCase("required");
    }

    @Test
    void explainUnknownPathIsAUsageError() {
        Run r = run("explain", "source.bogus");
        assertThat(r.code()).isEqualTo(2);
        assertThat(r.err()).contains("source.bogus");
        assertThat(r.out()).doesNotContain("source.bogus");
    }

    @Test
    void explainJsonEmitsTheFieldNodeEnvelope() {
        Run r = run("explain", "-o", "json", "source.mode");
        assertThat(r.code()).isZero();
        assertThat(r.out()).contains("\"path\": \"source.mode\"")
                .contains("\"type\": \"enum\"")
                .contains("\"values\"")
                .contains("cdc");
        // the envelope carries description and required as part of the machine contract
        assertThat(r.out()).contains("\"required\": false").contains("\"description\":");
    }

    @Test
    void explainJsonFieldsCarryEachChildTypeAndRequiredFlag() {
        Run r = run("explain", "-o", "json", "source");
        assertThat(r.code()).isZero();
        // each fields[] entry summarises the child: name + type + required
        assertThat(r.out()).contains("\"name\": \"mode\"").contains("\"type\": \"enum\"");
        assertThat(r.out()).contains("\"name\": \"connector\"").contains("\"required\": true");
    }

    @Test
    void explainJsonRootEmitsTheResourceKinds() {
        Run r = run("explain", "-o", "json");
        assertThat(r.code()).isZero();
        assertThat(r.out()).contains("\"path\": \"\"")
                .contains("\"fields\"")
                .contains("\"name\": \"source\"")
                .contains("\"type\": \"object\"");
    }

    @Test
    void explainYamlEmitsTheFieldNodeEnvelope() {
        Run r = run("explain", "-o", "yaml", "source");
        assertThat(r.code()).isZero();
        assertThat(r.out()).contains("path: source").contains("type: object").contains("fields:");
        // description and required travel in the yaml envelope too
        assertThat(r.out()).contains("required: false").contains("description:");
    }

    @Test
    void explainUnknownPathInJsonModeIsStillAPlainUsageError() {
        // a non-existent path is a CLI usage affordance, not a coded domain diagnostic — even with -o
        Run r = run("explain", "-o", "json", "bogus");
        assertThat(r.code()).isEqualTo(2);
        assertThat(r.err()).contains("bogus");
        assertThat(r.out()).isEmpty();
    }

    @Test
    void versionFlagPrintsTheVersion() {
        Run r = run("--version");
        assertThat(r.code()).isZero();
        assertThat(r.out()).contains("tapstate 0.2.1");
    }

    @Test
    void outputFormatCompletionCandidatesAreTheLowercaseSpelling() {
        // the -o values complete and display as text/json/yaml (the documented spelling), not the
        // upper-case enum constant names; parsing stays case-insensitive
        for (String verb : List.of("validate", "explain")) {
            var option = Cli.newCommandLine().getSubcommands().get(verb)
                    .getCommandSpec().findOption("-o");
            assertThat(option.completionCandidates()).containsExactly("text", "json", "yaml");
        }
    }

    @Test
    void newWithNoAnswersIsAUsageErrorNotAnUnknownCommand() {
        // `new` is a real, routed verb: invoked bare with no answers (and no terminal to prompt at)
        // it explains how to use itself — a usage error (exit 2) carrying its own guidance, not the
        // unmatched-command rejection `florp` gets.
        Run r = run("new");
        assertThat(r.code()).isEqualTo(2);
        assertThat(r.err()).contains("--connector");
    }

    // --- F1d: session-vs-one-shot launch + workspace seed ----------------------------------------

    @Test
    void bareArgsOpenASessionInTheDefaultWorkspace() {
        LaunchOptions launch = LaunchOptions.parse();
        assertThat(launch.isOneShot()).isFalse();
        assertThat(launch.connects()).isFalse();
        assertThat(launch.root()).isEqualTo(Path.of("tap-work"));
    }

    @Test
    void workspaceOnlyArgsOpenASessionSeededWithThatWorkspace() {
        assertThat(LaunchOptions.parse("-w", "foo").isOneShot()).isFalse();
        assertThat(LaunchOptions.parse("--workdir", "foo").isOneShot()).isFalse();
        assertThat(LaunchOptions.parse("--workdir=foo").isOneShot()).isFalse();
        // the seed honours the flag with the option's own precedence
        assertThat(LaunchOptions.parse("-w", "foo").root()).isEqualTo(Path.of("foo"));
        assertThat(LaunchOptions.parse("--workdir=bar").root()).isEqualTo(Path.of("bar"));
    }

    @Test
    void contextIsARootLaunchOptionAndConflictsWithTemporaryConnect() {
        LaunchOptions selected = LaunchOptions.parse("--context", "dev", "ls");

        assertThat(selected.context()).isEqualTo("dev");
        assertThat(selected.command()).containsExactly("ls");
        assertThat(selected.hasConflictingTargets()).isFalse();
        assertThat(LaunchOptions.parse("--connect", "node:8080", "--context", "dev", "ls")
                .hasConflictingTargets()).isTrue();
    }

    @Test
    void aVerbMakesTheLaunchOneShot() {
        assertThat(LaunchOptions.parse("validate").isOneShot()).isTrue();
        assertThat(LaunchOptions.parse("new", "--kind", "source").isOneShot()).isTrue();
        // a verb's own options are captured verbatim rather than parsed here, so the table sees them
        assertThat(LaunchOptions.parse("new", "--kind", "source").command())
                .containsExactly("new", "--kind", "source");
    }

    @Test
    void aVerbAfterTheWorkspaceOptionIsRoutedToTheTableToBeRefused() {
        // parsing -w here would bind the directory to the launch and leave the verb on its own default,
        // dropping the one the user named. It is detected and handed to the table, which says so
        LaunchOptions launch = LaunchOptions.parse("-w", "foo", "validate");
        assertThat(launch.misplacesTheWorkspaceOption("-w", "foo", "validate")).isTrue();
        assertThat(LaunchOptions.parse("validate").misplacesTheWorkspaceOption("validate")).isFalse();
    }

    @Test
    void helpVersionAndUnknownTokensAreOneShot() {
        // --help / --version are not launch options, so they reach the command table; an unknown word is
        // a command name as far as the launch is concerned, and the table is what rejects it
        assertThatThrownBy(() -> LaunchOptions.parse("--help")).isInstanceOf(RuntimeException.class);
        assertThatThrownBy(() -> LaunchOptions.parse("--version")).isInstanceOf(RuntimeException.class);
        assertThat(LaunchOptions.parse("florp").isOneShot()).isTrue();
    }

    @Test
    void resolveThrowsOnAMalformedWorkspaceFlag() {
        // a -w with no value is malformed; resolve must throw so the REPL-launch path in main can fall
        // back to the command table for a loud usage error instead of seeding the REPL with junk
        assertThatThrownBy(() -> WorkspaceOption.resolve("-w")).isInstanceOf(RuntimeException.class);
    }
}
