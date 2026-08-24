package io.tapstate.cli;

import io.tapstate.core.model.canonical.CanonicalHash;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The offline REPL's line dispatch: builtins (help / exit / quit), blank-line tolerance, quote-aware
 * tokenization, and routing every other line through the same verb table the one-shot mode uses.
 * The JLine read loop itself is not unit-tested; {@link Repl#dispatch} is the testable seam.
 */
class ReplTest {

    private record Harness(Repl repl, StringWriter sink) {
    }

    private static Harness harness() {
        return harness(Path.of("tap-work"));
    }

    private static Harness harness(Path workdir) {
        CommandLine cl = Cli.newCommandLine();
        StringWriter sink = new StringWriter();
        PrintWriter pw = new PrintWriter(sink);
        cl.setOut(pw);
        cl.setErr(pw);
        return new Harness(new Repl(cl, workdir), sink);
    }

    private static Harness harness(Path workdir, ControlPlaneClient controlPlane) {
        CommandLine cl = Cli.newCommandLine();
        StringWriter sink = new StringWriter();
        PrintWriter pw = new PrintWriter(sink);
        cl.setOut(pw);
        cl.setErr(pw);
        return new Harness(new Repl(cl, workdir, controlPlane), sink);
    }

    private static Harness harness(Path workdir, ControlPlaneClient controlPlane, Prompter prompter) {
        CommandLine cl = Cli.newCommandLine();
        StringWriter sink = new StringWriter();
        PrintWriter pw = new PrintWriter(sink);
        cl.setOut(pw);
        cl.setErr(pw);
        return new Harness(new Repl(cl, workdir, controlPlane, prompter), sink);
    }

    /** A harness whose interpolation environment is the given map rather than the real process's. */
    private static Harness harness(Path workdir, ControlPlaneClient controlPlane, Prompter prompter,
                                   Map<String, String> env) {
        CommandLine cl = Cli.newCommandLine();
        StringWriter sink = new StringWriter();
        PrintWriter pw = new PrintWriter(sink);
        cl.setOut(pw);
        cl.setErr(pw);
        return new Harness(new Repl(cl, workdir, controlPlane, prompter, env::get), sink);
    }

    /**
     * A network-free stand-in that answers healthy only for the given base URLs and records probes. The
     * connected verbs return their canned outcome when the target base is healthy and {@link
     * ApplyOutcome.Unreachable}-style unreachable when it is not — so a test can knock the landing node
     * down and exercise the failover-and-retry path with the same fake.
     */
    private static final class FakeControlPlane implements ControlPlaneClient {
        private final Set<URI> healthy;
        final List<URI> probed = new ArrayList<>();
        final List<URI> discovered = new ArrayList<>();
        /** The canned login outcome and a log of the login calls made ({@code user:pass@base}). */
        LoginOutcome loginOutcome = new LoginOutcome.Unreachable();
        final List<String> loginCalls = new ArrayList<>();
        SessionExchangeOutcome exchangeOutcome = new SessionExchangeOutcome.Unreachable();
        SessionLogoutOutcome logoutOutcome = new SessionLogoutOutcome.Unreachable();
        final List<String> sessionCalls = new ArrayList<>();
        Runnable beforeLogoutResult = () -> { };

        TokenCreateOutcome tokenCreateOutcome = new TokenCreateOutcome.Unreachable();
        TokenListOutcome tokenListOutcome = new TokenListOutcome.Unreachable();
        TokenRevokeOutcome tokenRevokeOutcome = new TokenRevokeOutcome.Unreachable();
        final List<String> tokenCalls = new ArrayList<>();

        /** The canned connected-verb outcomes (used when the target base is healthy) and their call logs. */
        ApplyOutcome applyOutcome = new ApplyOutcome.Unreachable();
        GetOutcome getOutcome = new GetOutcome.Unreachable();
        DeleteOutcome deleteOutcome = new DeleteOutcome.Unreachable();
        /** Every removal asked for, as {@code credential@base/id#hash} — the hash is what the tests pin. */
        final List<String> deleteCalls = new ArrayList<>();
        ListOutcome listOutcome = new ListOutcome.Unreachable();
        ConnectionTestOutcome testOutcome = new ConnectionTestOutcome.Unreachable();
        ConnectionTestResultOutcome testResultOutcome = new ConnectionTestResultOutcome.Unreachable();
        ConnectionDiscoverSchemaOutcome discoverSchemaOutcome = new ConnectionDiscoverSchemaOutcome.Unreachable();
        ConnectionSchemaOutcome schemaOutcome = new ConnectionSchemaOutcome.Unreachable();
        ConnectorRegisterOutcome registerOutcome = new ConnectorRegisterOutcome.Unreachable();
        /** Per-artifact register outcomes keyed by artifact byte length (for batch/directory tests); falls back to {@link #registerOutcome}. */
        final Map<Integer, ConnectorRegisterOutcome> registerOutcomeByLength = new HashMap<>();
        ConnectorListOutcome connectorListOutcome = new ConnectorListOutcome.Unreachable();
        LifecycleOutcome lifecycleOutcome = new LifecycleOutcome.Unreachable();
        StatusOutcome statusOutcome = new StatusOutcome.Unreachable();
        MetricsOutcome metricsOutcome = new MetricsOutcome.Unreachable();
        SnapshotOutcome snapshotOutcome = new SnapshotOutcome.Unreachable();
        LogsOutcome logsOutcome = new LogsOutcome.Unreachable();
        /** The states a watch stream feeds, and the line batches a follow stream feeds, in order. */
        List<String> watchStates = List.of();
        String streamRefusalCode;
        /** The coded reason carried alongside every emitted watch state, when a test needs one. */
        String watchFailureCode;
        String watchFailureMessage;
        List<List<RemoteLogLine>> followBatches = List.of();
        final List<String> applyCalls = new ArrayList<>();
        /** The drafts each apply carried, kept whole: what reaches the wire is the thing under test. */
        final List<List<LocalDraft>> appliedDrafts = new ArrayList<>();
        final List<String> getCalls = new ArrayList<>();
        final List<String> listCalls = new ArrayList<>();
        final List<String> testCalls = new ArrayList<>();
        final List<String> testResultCalls = new ArrayList<>();
        final List<String> discoverSchemaCalls = new ArrayList<>();
        final List<String> schemaCalls = new ArrayList<>();
        final List<String> registerCalls = new ArrayList<>();
        final List<String> connectorListCalls = new ArrayList<>();
        final List<String> lifecycleCalls = new ArrayList<>();
        final List<String> statusCalls = new ArrayList<>();
        final List<String> metricsCalls = new ArrayList<>();
        final List<String> snapshotCalls = new ArrayList<>();
        final List<String> logsCalls = new ArrayList<>();
        final List<String> watchCalls = new ArrayList<>();
        final List<String> followCalls = new ArrayList<>();

        /** The canned read-shell outcomes, the calls made, and the request the last read carried. */
        DataBrowserOutcome.Collections collectionsOutcome = new DataBrowserOutcome.Collections.Unreachable();
        DataBrowserOutcome.Stats statsOutcome = new DataBrowserOutcome.Stats.Unreachable();
        DataBrowserOutcome.Find findOutcome = new DataBrowserOutcome.Find.Unreachable();
        final List<String> dataBrowserCalls = new ArrayList<>();

        /** Changes the fake streams to a follow, delivered in order before the stream ends. */
        final List<TailChange> tailFrames = new ArrayList<>();

        /** A refusal the follow ends with, or null when it ends because the caller stopped it. */
        String tailRefusal;

        @Override
        public String tail(URI baseUrl, String credential, String sourceId, String collection,
                           Object filter, TailStream sink, java.util.function.BooleanSupplier stop) {
            dataBrowserCalls.add("tail " + sourceId + "." + collection + " filter=" + filter);
            tailFrames.forEach(sink::change);
            return tailRefusal;
        }
        Object lastFindFilter;
        DataBrowserCall.Order lastFindSort;
        Integer lastFindLimit;

        FakeControlPlane(URI... healthy) {
            this.healthy = new LinkedHashSet<>(List.of(healthy));
        }

        @Override
        public boolean isHealthy(URI baseUrl) {
            probed.add(baseUrl);
            return healthy.contains(baseUrl);
        }

        @Override
        public DiscoveryOutcome discover(URI baseUrl) {
            discovered.add(baseUrl);
            return healthy.contains(baseUrl)
                    ? new DiscoveryOutcome.Discovered("urn:tapstate:cluster:test-cluster", "test-cluster",
                            "tapstate/v1", List.of("password", "machine_token"))
                    : new DiscoveryOutcome.Unreachable();
        }

        /** Replaces the reachable set, so a test can knock a landing node down mid-session. */
        void setHealthy(URI... urls) {
            healthy.clear();
            healthy.addAll(List.of(urls));
        }

        @Override
        public LoginOutcome login(URI baseUrl, String username, String password) {
            loginCalls.add(username + ":" + password + "@" + baseUrl);
            return loginOutcome;
        }

        @Override
        public LoginOutcome login(URI baseUrl, String username, String password, boolean createSession) {
            loginCalls.add(username + ":" + password + "@" + baseUrl + " persistent=" + createSession);
            return loginOutcome;
        }

        @Override
        public SessionExchangeOutcome exchangeSession(URI baseUrl, String sessionToken) {
            sessionCalls.add("exchange " + sessionToken + "@" + baseUrl);
            return exchangeOutcome;
        }

        @Override
        public SessionLogoutOutcome logoutSession(URI baseUrl, String sessionToken) {
            sessionCalls.add("logout " + sessionToken + "@" + baseUrl);
            beforeLogoutResult.run();
            return logoutOutcome;
        }

        @Override
        public DataBrowserOutcome.Collections collections(URI baseUrl, String credential, String sourceId) {
            dataBrowserCalls.add("collections " + sourceId);
            return healthy.contains(baseUrl)
                    ? collectionsOutcome : new DataBrowserOutcome.Collections.Unreachable();
        }

        @Override
        public DataBrowserOutcome.Stats stats(
                URI baseUrl, String credential, String sourceId, String collection) {
            dataBrowserCalls.add("stats " + sourceId + "." + collection);
            return healthy.contains(baseUrl) ? statsOutcome : new DataBrowserOutcome.Stats.Unreachable();
        }

        @Override
        public DataBrowserOutcome.Find find(URI baseUrl, String credential, String sourceId,
                                            String collection, Object filter,
                                            DataBrowserCall.Order sort, Integer limit) {
            dataBrowserCalls.add("find " + sourceId + "." + collection
                    + " filter=" + filter + " sort=" + sort + " limit=" + limit);
            lastFindFilter = filter;
            lastFindSort = sort;
            lastFindLimit = limit;
            return healthy.contains(baseUrl) ? findOutcome : new DataBrowserOutcome.Find.Unreachable();
        }

        @Override
        public TokenCreateOutcome tokenCreate(URI baseUrl, String credential, String scope) {
            tokenCalls.add("create " + scope + " " + credential + "@" + baseUrl);
            return healthy.contains(baseUrl) ? tokenCreateOutcome : new TokenCreateOutcome.Unreachable();
        }

        @Override
        public TokenListOutcome tokenList(URI baseUrl, String credential) {
            tokenCalls.add("list " + credential + "@" + baseUrl);
            return healthy.contains(baseUrl) ? tokenListOutcome : new TokenListOutcome.Unreachable();
        }

        @Override
        public TokenRevokeOutcome tokenRevoke(URI baseUrl, String credential, String tokenId) {
            tokenCalls.add("revoke " + tokenId + " " + credential + "@" + baseUrl);
            return healthy.contains(baseUrl) ? tokenRevokeOutcome : new TokenRevokeOutcome.Unreachable();
        }

        @Override
        public ApplyOutcome apply(URI baseUrl, String credential, List<LocalDraft> drafts) {
            applyCalls.add(credential + "@" + baseUrl + " x" + drafts.size());
            appliedDrafts.add(List.copyOf(drafts));
            return healthy.contains(baseUrl) ? applyOutcome : new ApplyOutcome.Unreachable();
        }

        @Override
        public GetOutcome get(URI baseUrl, String credential, String id) {
            getCalls.add(credential + "@" + baseUrl + "/" + id);
            return healthy.contains(baseUrl) ? getOutcome : new GetOutcome.Unreachable();
        }

        @Override
        public DeleteOutcome delete(URI baseUrl, String credential, String id, String expectedContentHash) {
            deleteCalls.add(credential + "@" + baseUrl + "/" + id + "#" + expectedContentHash);
            return healthy.contains(baseUrl) ? deleteOutcome : new DeleteOutcome.Unreachable();
        }

        @Override
        public ListOutcome list(URI baseUrl, String credential, String kind) {
            listCalls.add(credential + "@" + baseUrl + "?" + kind);
            return healthy.contains(baseUrl) ? listOutcome : new ListOutcome.Unreachable();
        }

        @Override
        public ConnectionTestOutcome test(
                URI baseUrl, String credential, String id, String connectorId, Map<String, Object> settings) {
            testCalls.add(credential + "@" + baseUrl + "/" + id + "[" + connectorId + " " + settings + "]");
            return healthy.contains(baseUrl) ? testOutcome : new ConnectionTestOutcome.Unreachable();
        }

        @Override
        public ConnectionTestResultOutcome testResult(URI baseUrl, String credential, String id) {
            testResultCalls.add(credential + "@" + baseUrl + "/" + id);
            return healthy.contains(baseUrl) ? testResultOutcome : new ConnectionTestResultOutcome.Unreachable();
        }

        @Override
        public ConnectionDiscoverSchemaOutcome discoverSchema(
                URI baseUrl, String credential, String id, String connectorId, Map<String, Object> settings) {
            discoverSchemaCalls.add(credential + "@" + baseUrl + "/" + id + "[" + connectorId + " " + settings + "]");
            return healthy.contains(baseUrl) ? discoverSchemaOutcome : new ConnectionDiscoverSchemaOutcome.Unreachable();
        }

        @Override
        public ConnectionSchemaOutcome schema(URI baseUrl, String credential, String id) {
            schemaCalls.add(credential + "@" + baseUrl + "/" + id);
            return healthy.contains(baseUrl) ? schemaOutcome : new ConnectionSchemaOutcome.Unreachable();
        }

        @Override
        public ConnectorRegisterOutcome register(URI baseUrl, String credential, byte[] artifact) {
            registerCalls.add(credential + "@" + baseUrl + " x" + artifact.length);
            if (!healthy.contains(baseUrl)) {
                return new ConnectorRegisterOutcome.Unreachable();
            }
            return registerOutcomeByLength.getOrDefault(artifact.length, registerOutcome);
        }

        @Override
        public ConnectorListOutcome connectorList(URI baseUrl, String credential) {
            connectorListCalls.add(credential + "@" + baseUrl);
            return healthy.contains(baseUrl) ? connectorListOutcome : new ConnectorListOutcome.Unreachable();
        }

        @Override
        public LifecycleOutcome lifecycle(URI baseUrl, String credential, String pipelineId, String verb) {
            lifecycleCalls.add(credential + "@" + baseUrl + " " + verb + " " + pipelineId);
            return healthy.contains(baseUrl) ? lifecycleOutcome : new LifecycleOutcome.Unreachable();
        }

        @Override
        public StatusOutcome status(URI baseUrl, String credential, String pipelineId) {
            statusCalls.add(credential + "@" + baseUrl + "/" + pipelineId);
            return healthy.contains(baseUrl) ? statusOutcome : new StatusOutcome.Unreachable();
        }

        @Override
        public MetricsOutcome metrics(URI baseUrl, String credential, String pipelineId) {
            metricsCalls.add(credential + "@" + baseUrl + "/" + pipelineId);
            return healthy.contains(baseUrl) ? metricsOutcome : new MetricsOutcome.Unreachable();
        }

        @Override
        public SnapshotOutcome snapshot(URI baseUrl, String credential, String pipelineId) {
            snapshotCalls.add(credential + "@" + baseUrl + "/" + pipelineId);
            return healthy.contains(baseUrl) ? snapshotOutcome : new SnapshotOutcome.Unreachable();
        }

        @Override
        public LogsOutcome logs(URI baseUrl, String credential, String pipelineId) {
            logsCalls.add(credential + "@" + baseUrl + "/" + pipelineId);
            return healthy.contains(baseUrl) ? logsOutcome : new LogsOutcome.Unreachable();
        }

        @Override
        public String watchStatus(URI baseUrl, String credential, String pipelineId,
                StatusStream sink, java.util.function.BooleanSupplier stop) {
            watchCalls.add(credential + "@" + baseUrl + "/" + pipelineId);
            for (String state : watchStates) {
                if (stop.getAsBoolean()) {
                    return null;
                }
                sink.state(pipelineId, state, watchFailureCode, watchFailureMessage);
            }
            return streamRefusalCode;
        }

        @Override
        public String followLogs(URI baseUrl, String credential, String pipelineId,
                LogStream sink, java.util.function.BooleanSupplier stop) {
            followCalls.add(credential + "@" + baseUrl + "/" + pipelineId);
            for (List<RemoteLogLine> batch : followBatches) {
                if (stop.getAsBoolean()) {
                    return null;
                }
                sink.lines(pipelineId, batch);
            }
            return streamRefusalCode;
        }
    }

    /** Copies a classpath workspace tree into {@code dest}, preserving the kind-directory layout. */
    private static void copyWorkspace(String resource, Path dest) throws Exception {
        Path src = Path.of(ReplTest.class.getResource(resource).toURI());
        try (var files = Files.walk(src)) {
            for (Path f : files.toList()) {
                Path target = dest.resolve(src.relativize(f).toString());
                if (Files.isDirectory(f)) {
                    Files.createDirectories(target);
                } else {
                    Files.createDirectories(target.getParent());
                    Files.copy(f, target);
                }
            }
        }
    }

    @Test
    void exitStopsTheLoop() {
        assertThat(harness().repl().dispatch("exit")).isFalse();
    }

    @Test
    void quitStopsTheLoop() {
        assertThat(harness().repl().dispatch("quit")).isFalse();
    }

    @Test
    void blankLineContinuesWithoutOutput() {
        Harness h = harness();
        assertThat(h.repl().dispatch("   ")).isTrue();
        assertThat(h.sink().toString()).isEmpty();
    }

    @Test
    void helpPrintsUsageAndContinues() {
        Harness h = harness();
        assertThat(h.repl().dispatch("help")).isTrue();
        assertThat(h.sink().toString()).contains("validate");
    }

    @Test
    void helpTakesTheNameOfAVerbAndDescribesIt() {
        // bare `help` is a builtin handled here, but `help <verb>` falls through to the command table --
        // where it used to be an unmatched argument answered with a spelling guess. Asking about one verb
        // is the more common question of the two, and it was the one that did not work.
        Harness h = harness();
        assertThat(h.repl().dispatch("help status")).isTrue();
        assertThat(h.sink().toString())
                .contains("Usage: tapstate status")
                .contains(Cli.VERB_HELP.get("status").summary());
    }

    @Test
    void verbsDispatchThroughTheSameTable() {
        Harness h = harness();
        assertThat(h.repl().dispatch("explain")).isTrue();
        assertThat(h.sink().toString()).contains("tapstate/v1");
    }

    @Test
    void structuredOutputFlagIsReachableThroughTheRepl() {
        Harness h = harness();
        // the -o flag and its lower-case enum value travel through the REPL's tokeniser and the
        // shared command table (which enables case-insensitive enum matching)
        assertThat(h.repl().dispatch("explain -o json")).isTrue();
        assertThat(h.sink().toString()).contains("\"path\"").contains("source");
    }

    @Test
    void tokenizeSplitsOnWhitespace() {
        assertThat(Repl.tokenize("  validate   /a/b  ")).containsExactly("validate", "/a/b");
    }

    @Test
    void tokenizeKeepsDoubleQuotedSpacesAsOneWord() {
        assertThat(Repl.tokenize("validate \"my workspace\"")).containsExactly("validate", "my workspace");
    }

    @Test
    void tokenizeKeepsSingleQuotedSpacesAsOneWord() {
        assertThat(Repl.tokenize("a 'b c' d")).containsExactly("a", "b c", "d");
    }

    @Test
    void dispatchHandlesAQuotedPathWithSpacesLikeTheOneShotForm(@TempDir Path base) throws Exception {
        Path spaced = Files.createDirectory(base.resolve("my workspace"));
        copyWorkspace("/ws-valid", spaced);
        Harness h = harness();
        boolean cont = h.repl().dispatch("validate \"" + spaced + "\"");
        assertThat(cont).isTrue();
        assertThat(h.sink().toString()).startsWith("valid:").contains("3 resources");
    }

    @Test
    void tokenizeReturnsEmptyForBlank() {
        assertThat(Repl.tokenize("   ")).isEqualTo(List.of());
    }

    // --- F1d: session-state workspace, -w injection, cd / pwd, prompt -----------------------------

    @Test
    void bareValidateUsesTheSessionWorkspaceRootNotTheProcessDefault() throws Exception {
        // a bare `validate` carries no path and no -w: the seeded session workspace must drive it, so
        // the loader sees ws-valid (the session root), not the process-relative tap-work default
        Path wsRoot = Path.of(ReplTest.class.getResource("/ws-valid").toURI());
        Harness h = harness(wsRoot);
        assertThat(h.repl().dispatch("validate")).isTrue();
        assertThat(h.sink().toString())
                .startsWith("valid:").contains("3 resources").contains(wsRoot.toString());
    }

    @Test
    void anExplicitWorkdirFlagWinsOverTheInjectedSession() throws Exception {
        // the session root is bogus; an explicit -w on the line must win, so the run still succeeds —
        // proving the injection does not clobber a user-supplied workspace flag
        Path wsRoot = Path.of(ReplTest.class.getResource("/ws-valid").toURI());
        Harness h = harness(Path.of("/no/such/tapstate/session"));
        assertThat(h.repl().dispatch("validate -w " + wsRoot)).isTrue();
        assertThat(h.sink().toString()).startsWith("valid:").contains("3 resources");
    }

    @Test
    void aVerbWithoutTheWorkspaceOptionGetsNoInjectedWorkdir() {
        // explain declares no --workdir; the REPL must not inject one (it would be an unknown option)
        Harness h = harness(Path.of("tap-work"));
        assertThat(h.repl().dispatch("explain")).isTrue();
        assertThat(h.sink().toString()).contains("tapstate/v1").doesNotContain("Unknown option");
    }

    @Test
    void cdChangesTheSessionWorkspaceToAnExistingSubdirectory(@TempDir Path base) throws Exception {
        Path sub = Files.createDirectory(base.resolve("staging"));
        Harness h = harness(base);
        assertThat(h.repl().dispatch("cd staging")).isTrue();
        assertThat(h.repl().workdir()).isEqualTo(sub);
    }

    @Test
    void cdToTheParentDirectoryResolvesAndNormalizes(@TempDir Path base) throws Exception {
        // `..` must be collapsed (normalize), not left as level1/.. — Path.equals is lexical, so an
        // un-normalized parent would not equal base and would leak into pwd / prompt / the injected -w
        Path sub = Files.createDirectory(base.resolve("level1"));
        Harness h = harness(sub);
        assertThat(h.repl().dispatch("cd ..")).isTrue();
        assertThat(h.repl().workdir()).isEqualTo(base);
    }

    @Test
    void cdToAMissingDirectoryReportsAnErrorAndKeepsTheWorkspace(@TempDir Path base) {
        Harness h = harness(base);
        assertThat(h.repl().dispatch("cd nope")).isTrue();
        assertThat(h.sink().toString()).contains("cd:").contains("nope");
        assertThat(h.repl().workdir()).isEqualTo(base);
    }

    @Test
    void cdWithNoArgumentReportsMissingOperand(@TempDir Path base) {
        Harness h = harness(base);
        assertThat(h.repl().dispatch("cd")).isTrue();
        assertThat(h.sink().toString()).contains("missing operand");
        assertThat(h.repl().workdir()).isEqualTo(base);
    }

    @Test
    void pwdPrintsTheCurrentWorkspace(@TempDir Path base) {
        Harness h = harness(base);
        assertThat(h.repl().dispatch("pwd")).isTrue();
        assertThat(h.sink().toString()).contains(base.toString());
    }

    @Test
    void promptShowsTheWorkspaceName() {
        assertThat(harness(Path.of("tap-work")).repl().prompt()).isEqualTo("tapstate(offline:tap-work)> ");
        assertThat(harness(Path.of("/tmp/projects/demo")).repl().prompt()).isEqualTo("tapstate(offline:demo)> ");
    }

    @Test
    void promptForARootWorkspaceFallsBackToTheFullPath() {
        // a filesystem root has no file name; the prompt must fall back to the path string, not NPE
        assertThat(harness(Path.of("/")).repl().prompt()).isEqualTo("tapstate(offline:/)> ");
    }

    @Test
    void cdUpdatesThePromptAndTheInjectedWorkspace(@TempDir Path base) throws Exception {
        // lay a valid workspace inside a subdirectory; after cd, the prompt names it and a bare
        // validate targets it — cd and -w injection working together end to end
        Path sub = Files.createDirectory(base.resolve("ws"));
        copyWorkspace("/ws-valid", sub);
        Harness h = harness(base);
        h.repl().dispatch("cd ws");
        assertThat(h.repl().prompt()).isEqualTo("tapstate(offline:ws)> ");
        assertThat(h.repl().dispatch("validate")).isTrue();
        assertThat(h.sink().toString()).contains("valid:").contains("3 resources");
    }

    @Test
    void cdMakesLsListTheNewWorkspaceResources(@TempDir Path base) throws Exception {
        // cd must re-point ls at the session workspace: ls over the empty base lists nothing, but after
        // cd into a populated workspace it lists that workspace's resources — the output follows the cd
        Path sub = Files.createDirectory(base.resolve("ws"));
        copyWorkspace("/ws-valid", sub);
        Harness h = harness(base);

        h.repl().dispatch("ls");
        int afterEmptyLs = h.sink().toString().length();
        assertThat(h.sink().toString()).doesNotContain("kfk2my");   // base has no kind dirs

        h.repl().dispatch("cd ws");
        h.repl().dispatch("ls");
        String afterCd = h.sink().toString().substring(afterEmptyLs);
        assertThat(afterCd).contains("source").contains("pipeline").contains("kfk2my");
    }

    @Test
    void cdMakesDescResolveAResourceFromTheNewWorkspace(@TempDir Path base) throws Exception {
        // desc resolves an id against the session workspace: before cd the id is not found in the empty
        // base; after cd into the populated workspace the same id describes its resource
        Path sub = Files.createDirectory(base.resolve("ws"));
        copyWorkspace("/ws-valid", sub);
        Harness h = harness(base);

        h.repl().dispatch("desc kfk2my");
        int afterMiss = h.sink().toString().length();
        assertThat(h.sink().toString()).contains("cli.resource-not-found");   // absent from base

        h.repl().dispatch("cd ws");
        h.repl().dispatch("desc kfk2my");
        String afterCd = h.sink().toString().substring(afterMiss);
        assertThat(afterCd).contains("kfk2my").contains("pipeline").doesNotContain("resource-not-found");
    }

    // --- online-session skeleton: connect / disconnect / prompt / seed parsing --------------------

    @Test
    void connectToAReachableSeedFlipsTheSessionAndPrompt() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        Harness h = harness(Path.of("tap-work"), client);
        assertThat(h.repl().dispatch("connect node1:7900")).isTrue();
        assertThat(h.repl().session().isConnected()).isTrue();
        assertThat(h.repl().session().landingNode()).isEqualTo(URI.create("http://node1:7900"));
        assertThat(h.repl().prompt()).isEqualTo("tapstate(node1:7900)> ");
        assertThat(h.sink().toString()).contains("connected").contains("node1:7900");
    }

    @Test
    void connectTriesSeedsInOrderAndLandsOnTheFirstReachable() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node2:7900"));
        Harness h = harness(Path.of("tap-work"), client);
        assertThat(h.repl().dispatch("connect node1:7900,node2:7900")).isTrue();
        assertThat(h.repl().session().landingNode()).isEqualTo(URI.create("http://node2:7900"));
        assertThat(client.probed)
                .containsExactly(URI.create("http://node1:7900"), URI.create("http://node2:7900"));
        assertThat(h.repl().prompt()).isEqualTo("tapstate(node2:7900)> ");
    }

    @Test
    void connectWithNoReachableSeedRendersConnectFailedAndStaysOffline() {
        FakeControlPlane client = new FakeControlPlane();   // nothing is healthy
        Harness h = harness(Path.of("tap-work"), client);
        assertThat(h.repl().dispatch("connect node1:7900,node2:7900")).isTrue();
        assertThat(h.repl().session().isConnected()).isFalse();
        assertThat(h.repl().prompt()).isEqualTo("tapstate(offline:tap-work)> ");
        assertThat(h.sink().toString())
                .contains("cli.connect-failed")
                .contains("http://node1:7900")
                .contains("http://node2:7900");
    }

    @Test
    void connectWithNoArgumentPrintsUsageAndLeavesTheSessionOffline() {
        FakeControlPlane client = new FakeControlPlane();
        Harness h = harness(Path.of("tap-work"), client);
        assertThat(h.repl().dispatch("connect")).isTrue();
        assertThat(h.repl().session().isConnected()).isFalse();
        assertThat(h.sink().toString()).contains("connect:");
        assertThat(client.probed).isEmpty();
    }

    @Test
    void connectWithABlankSeedListPrintsUsageAndDoesNotProbe() {
        FakeControlPlane client = new FakeControlPlane();
        Harness h = harness(Path.of("tap-work"), client);
        assertThat(h.repl().dispatch("connect \" , \"")).isTrue();
        assertThat(h.repl().session().isConnected()).isFalse();
        assertThat(h.sink().toString()).contains("connect:");
        assertThat(client.probed).isEmpty();
    }

    @Test
    void connectWithAUriIllegalSeedPrintsUsageStaysOfflineAndDoesNotProbe() {
        // `^` is illegal in a URI authority; the token must be treated as a usage-level input error,
        // not bubble an uncaught IllegalArgumentException that would crash the read loop
        FakeControlPlane client = new FakeControlPlane();
        Harness h = harness(Path.of("tap-work"), client);
        assertThat(h.repl().dispatch("connect foo^bar")).isTrue();
        assertThat(h.repl().session().isConnected()).isFalse();
        assertThat(h.sink().toString()).contains("connect:").contains("foo^bar");
        assertThat(client.probed).isEmpty();
    }

    @Test
    void connectWithAPipeIllegalSeedPrintsUsageAndDoesNotProbe() {
        // `|` is likewise illegal in a URI; same total-parse requirement
        FakeControlPlane client = new FakeControlPlane();
        Harness h = harness(Path.of("tap-work"), client);
        assertThat(h.repl().dispatch("connect a|b")).isTrue();
        assertThat(h.repl().session().isConnected()).isFalse();
        assertThat(h.sink().toString()).contains("connect:").contains("a|b");
        assertThat(client.probed).isEmpty();
    }

    @Test
    void connectWithAHostlessSeedPrintsUsageAndStaysOffline() {
        // `foo:bar` parses (a non-numeric port makes the authority registry-based) but has no host;
        // such a seed is unusable and must be rejected as a usage error, not probed
        FakeControlPlane client = new FakeControlPlane();
        Harness h = harness(Path.of("tap-work"), client);
        assertThat(h.repl().dispatch("connect foo:bar")).isTrue();
        assertThat(h.repl().session().isConnected()).isFalse();
        assertThat(h.sink().toString()).contains("connect:").contains("foo:bar");
        assertThat(client.probed).isEmpty();
    }

    @Test
    void connectRejectsTheWholeLineOnABadSeedWithoutProbingTheGoodOne() {
        // the good seed is healthy, but a single invalid seed rejects the whole line before any probe,
        // so a typo can never silently connect to a subset
        FakeControlPlane client = new FakeControlPlane(URI.create("http://goodhost:80"));
        Harness h = harness(Path.of("tap-work"), client);
        assertThat(h.repl().dispatch("connect goodhost:80,foo^bar")).isTrue();
        assertThat(h.repl().session().isConnected()).isFalse();
        assertThat(h.sink().toString()).contains("connect:").contains("foo^bar");
        assertThat(client.probed).isEmpty();
    }

    @Test
    void connectingLeavesTheSessionUnauthenticated() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        Harness h = harness(Path.of("tap-work"), client);
        h.repl().dispatch("connect node1:7900");
        assertThat(h.repl().session().isConnected()).isTrue();
        // connect establishes a transport target only; no credential is obtained until login
        assertThat(h.repl().session().isAuthenticated()).isFalse();
        assertThat(h.repl().session().credential()).isNull();
        assertThat(h.repl().session().principal()).isNull();
    }

    @Test
    void parseSeedsGivesABareHostPortAnHttpScheme() {
        Repl.ParsedSeeds parsed = Repl.parseSeeds("node1:7900");
        assertThat(parsed.valid()).containsExactly(URI.create("http://node1:7900"));
        assertThat(parsed.invalidToken()).isNull();
    }

    @Test
    void parseSeedsKeepsAnExplicitScheme() {
        assertThat(Repl.parseSeeds("http://host:8080").valid())
                .containsExactly(URI.create("http://host:8080"));
        assertThat(Repl.parseSeeds("https://secure:8443").valid())
                .containsExactly(URI.create("https://secure:8443"));
    }

    @Test
    void parseSeedsSplitsCommaSeparatedAndTrimsEach() {
        assertThat(Repl.parseSeeds(" node1:7900 , http://node2:8080 ").valid())
                .containsExactly(URI.create("http://node1:7900"), URI.create("http://node2:8080"));
    }

    @Test
    void parseSeedsDropsBlankElements() {
        assertThat(Repl.parseSeeds("node1:7900,,").valid()).containsExactly(URI.create("http://node1:7900"));
        Repl.ParsedSeeds allBlank = Repl.parseSeeds("  , ");
        assertThat(allBlank.valid()).isEmpty();
        assertThat(allBlank.invalidToken()).isNull();
    }

    @Test
    void parseSeedsReportsTheFirstUriIllegalTokenWithoutThrowing() {
        Repl.ParsedSeeds parsed = Repl.parseSeeds("node1:7900,foo^bar");
        assertThat(parsed.invalidToken()).isEqualTo("foo^bar");
        assertThat(parsed.valid()).isEmpty();
    }

    @Test
    void parseSeedsReportsAHostlessTokenAsInvalid() {
        Repl.ParsedSeeds parsed = Repl.parseSeeds("foo:bar");
        assertThat(parsed.invalidToken()).isEqualTo("foo:bar");
        assertThat(parsed.valid()).isEmpty();
    }

    @Test
    void disconnectReturnsAConnectedSessionToOffline() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        Harness h = harness(Path.of("tap-work"), client);
        h.repl().dispatch("connect node1:7900");
        assertThat(h.repl().dispatch("disconnect")).isTrue();
        assertThat(h.repl().session().isConnected()).isFalse();
        assertThat(h.repl().prompt()).isEqualTo("tapstate(offline:tap-work)> ");
        assertThat(h.sink().toString()).contains("disconnected");
    }

    @Test
    void disconnectWhileOfflinePrintsABenignLine() {
        FakeControlPlane client = new FakeControlPlane();
        Harness h = harness(Path.of("tap-work"), client);
        assertThat(h.repl().dispatch("disconnect")).isTrue();
        assertThat(h.repl().session().isConnected()).isFalse();
        assertThat(h.sink().toString()).contains("not connected");
    }

    // --- login / logout / authenticated prompt ---------------------------------------------------

    @Test
    void loginBeforeConnectReportsNotConnectedAndDoesNotCallTheClient() {
        FakeControlPlane client = new FakeControlPlane();
        Harness h = harness(Path.of("tap-work"), client, new ScriptedPrompter("pw"));
        assertThat(h.repl().dispatch("login alice")).isTrue();
        assertThat(h.repl().session().isAuthenticated()).isFalse();
        // the not-connected state is a coded cli.* diagnostic naming the login verb, not a bare string
        assertThat(h.sink().toString()).contains("cli.not-connected").contains("login");
        assertThat(client.loginCalls).isEmpty();
    }

    @Test
    void loginWithNoUsernameReportsMissingOperandAndDoesNotCallTheClient() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        Harness h = harness(Path.of("tap-work"), client, new ScriptedPrompter("pw"));
        h.repl().dispatch("connect node1:7900");
        assertThat(h.repl().dispatch("login")).isTrue();
        assertThat(h.sink().toString()).contains("login:").contains("missing operand");
        assertThat(client.loginCalls).isEmpty();
    }

    @Test
    void loginReadsAMaskedPasswordAndAuthenticatesOnSuccess() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://localhost:7900"));
        client.loginOutcome = new LoginOutcome.Success("jwt-abc");
        ScriptedPrompter prompter = new ScriptedPrompter("s3cret");
        Harness h = harness(Path.of("tap-work"), client, prompter);
        h.repl().dispatch("connect localhost:7900");
        assertThat(h.repl().dispatch("login alice")).isTrue();
        assertThat(h.repl().session().isAuthenticated()).isTrue();
        assertThat(h.repl().session().principal()).isEqualTo("alice");
        assertThat(h.repl().session().credential()).isEqualTo("jwt-abc");
        assertThat(prompter.secretQuestions).isNotEmpty();   // the password was read masked, never echoed
        assertThat(client.loginCalls).containsExactly("alice:s3cret@http://localhost:7900");
        assertThat(h.sink().toString()).contains("logged in as alice");
    }

    @Test
    void authenticatedPromptShowsThePrincipalAtTheNode() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://localhost:7900"));
        client.loginOutcome = new LoginOutcome.Success("jwt-abc");
        Harness h = harness(Path.of("tap-work"), client, new ScriptedPrompter("s3cret"));
        h.repl().dispatch("connect localhost:7900");
        assertThat(h.repl().prompt()).isEqualTo("tapstate(localhost:7900)> ");   // connected, unauthenticated
        h.repl().dispatch("login alice");
        assertThat(h.repl().prompt()).isEqualTo("tapstate(alice@localhost:7900)> ");
    }

    @Test
    void loginRejectedRendersTheServerErrorAndStaysUnauthenticated() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://localhost:7900"));
        client.loginOutcome = new LoginOutcome.Rejected("control.auth-failed", "Login failed.");
        Harness h = harness(Path.of("tap-work"), client, new ScriptedPrompter("wrong"));
        h.repl().dispatch("connect localhost:7900");
        assertThat(h.repl().dispatch("login alice")).isTrue();
        assertThat(h.repl().session().isAuthenticated()).isFalse();
        assertThat(h.sink().toString()).contains("control.auth-failed").contains("Login failed.");
        assertThat(h.repl().prompt()).isEqualTo("tapstate(localhost:7900)> ");   // stays connected-unauthenticated
    }

    @Test
    void loginUnreachableReportsAndStaysUnauthenticated() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://localhost:7900"));
        client.loginOutcome = new LoginOutcome.Unreachable();
        Harness h = harness(Path.of("tap-work"), client, new ScriptedPrompter("pw"));
        h.repl().dispatch("connect localhost:7900");
        assertThat(h.repl().dispatch("login alice")).isTrue();
        assertThat(h.repl().session().isAuthenticated()).isFalse();
        assertThat(h.sink().toString()).contains("login:").contains("localhost:7900");
    }

    @Test
    void logoutClearsAuthenticationButKeepsTheConnection() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://localhost:7900"));
        client.loginOutcome = new LoginOutcome.Success("jwt");
        Harness h = harness(Path.of("tap-work"), client, new ScriptedPrompter("pw"));
        h.repl().dispatch("connect localhost:7900");
        h.repl().dispatch("login alice");
        assertThat(h.repl().dispatch("logout")).isTrue();
        assertThat(h.repl().session().isAuthenticated()).isFalse();
        assertThat(h.repl().session().isConnected()).isTrue();
        assertThat(h.sink().toString()).contains("logged out");
        assertThat(h.repl().prompt()).isEqualTo("tapstate(localhost:7900)> ");
    }

    @Test
    void logoutWhileNotAuthenticatedPrintsABenignLine() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        Harness h = harness(Path.of("tap-work"), client, new ScriptedPrompter());
        h.repl().dispatch("connect node1:7900");
        assertThat(h.repl().dispatch("logout")).isTrue();
        assertThat(h.sink().toString()).contains("not logged in");
    }

    // --- failover: re-land across the member set on a lost landing node --------------------------

    @Test
    void failoverRelandsOnAnotherHealthyMemberKeepingTheCredential() {
        FakeControlPlane client = new FakeControlPlane(
                URI.create("http://localhost:7900"), URI.create("http://localhost:7901"));
        client.loginOutcome = new LoginOutcome.Success("jwt");
        Harness h = harness(Path.of("tap-work"), client, new ScriptedPrompter("pw"));
        h.repl().dispatch("connect localhost:7900,localhost:7901");
        h.repl().dispatch("login alice");
        client.setHealthy(URI.create("http://localhost:7901"));   // the first seed goes down
        assertThat(h.repl().failover()).isTrue();
        assertThat(h.repl().session().landingNode()).isEqualTo(URI.create("http://localhost:7901"));
        assertThat(h.repl().session().isAuthenticated()).isTrue();   // cluster-wide credential survives
        assertThat(h.repl().session().credential()).isEqualTo("jwt");
        assertThat(h.repl().prompt()).isEqualTo("tapstate(alice@localhost:7901)> ");
    }

    @Test
    void failoverReconnectsToTheSameSingleNodeWhenItIsStillReachable() {
        // the failover path is not omitted for a single-node member list (L1 exercises the same mechanism)
        FakeControlPlane client = new FakeControlPlane(URI.create("http://n1:7900"));
        Harness h = harness(Path.of("tap-work"), client, new ScriptedPrompter());
        h.repl().dispatch("connect n1:7900");
        assertThat(h.repl().failover()).isTrue();
        assertThat(h.repl().session().landingNode()).isEqualTo(URI.create("http://n1:7900"));
        assertThat(h.repl().session().isConnected()).isTrue();
    }

    @Test
    void failoverWithNoReachableMemberLosesTheConnection() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://localhost:7900"));
        client.loginOutcome = new LoginOutcome.Success("jwt");
        Harness h = harness(Path.of("tap-work"), client, new ScriptedPrompter("pw"));
        h.repl().dispatch("connect localhost:7900");
        h.repl().dispatch("login alice");
        client.setHealthy();   // nothing reachable
        assertThat(h.repl().failover()).isFalse();
        assertThat(h.repl().session().isConnected()).isFalse();
        assertThat(h.repl().session().isAuthenticated()).isFalse();
        assertThat(h.repl().prompt()).isEqualTo("tapstate(offline:tap-work)> ");
    }

    @Test
    void failoverWhileOfflineIsANoOpAndProbesNothing() {
        FakeControlPlane client = new FakeControlPlane();
        Harness h = harness(Path.of("tap-work"), client, new ScriptedPrompter());
        assertThat(h.repl().failover()).isFalse();
        assertThat(h.repl().session().isConnected()).isFalse();
        assertThat(client.probed).isEmpty();
        assertThat(h.sink().toString()).isEmpty();   // a true no-op prints nothing (no "connection lost")
    }

    // --- online verbs: apply / get / ls routed to the server once authenticated -------------------

    /** A harness whose stdout and stderr are kept apart, for asserting which stream a line landed on. */
    private record SplitHarness(Repl repl, StringWriter out, StringWriter err) {
    }

    private static SplitHarness splitStreamHarness(Path workdir, ControlPlaneClient controlPlane) {
        CommandLine cl = Cli.newCommandLine();
        StringWriter out = new StringWriter();
        StringWriter err = new StringWriter();
        cl.setOut(new PrintWriter(out));
        cl.setErr(new PrintWriter(err));
        return new SplitHarness(new Repl(cl, workdir, controlPlane, new ScriptedPrompter("pw")), out, err);
    }

    /** An authenticated session with stdout and stderr kept apart. */
    private static SplitHarness onlineSplitStreamSession(Path workdir, FakeControlPlane client) {
        SplitHarness h = splitStreamHarness(workdir, client);
        authenticateOnNode1(h.repl());
        return h;
    }

    /** Connects to a single healthy node and logs in, so a test starts from an authenticated session. */
    private static Harness onlineSession(Path workdir, FakeControlPlane client) {
        Harness h = harness(workdir, client, new ScriptedPrompter("pw"));
        authenticateOnNode1(h.repl());
        return h;
    }

    private static void authenticateOnNode1(Repl repl) {
        URI seed = URI.create("http://node1:7900");
        repl.session().connect(List.of(seed), seed);
        repl.session().authenticate("jwt-tok", "alice", null, List.of(seed));
    }

    // ---- the read shell ----

    @Test
    void showCollectionsNamesEveryDeclaredSourcesCollectionsAsSomethingYouCanType() {
        // The answer to "what can I read" is only useful as the full <source>.<collection> names, because
        // those are exactly what the next line has to say.
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.listOutcome = new ListOutcome.Listed(List.of(new RemoteArtifact("views", "source", "")));
        client.collectionsOutcome =
                new DataBrowserOutcome.Collections.Listed(List.of("order_state", "customers"));
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("show collections")).isTrue();

        String output = h.sink().toString().substring(mark);
        assertThat(output).contains("views.order_state").contains("views.customers");
        assertThat(client.dataBrowserCalls).containsExactly("collections views");
        assertThat(h.repl().lastExitCode()).isZero();
    }

    @Test
    void showCollectionsSaysTheListIsWhatTheDatabaseHoldsRatherThanWhatWasDeclared() {
        // The two are different sets and the declared one is the wrong answer, so the list says which it
        // is. A source referenced purely as a connection supplier declares no tables at all.
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.collectionsOutcome = new DataBrowserOutcome.Collections.Listed(List.of("order_state"));
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        h.repl().dispatch("show collections views");

        assertThat(h.sink().toString().substring(mark))
                .contains("what each source's database holds, not what the workspace declares");
        assertThat(client.dataBrowserCalls).containsExactly("collections views");
    }

    @Test
    void findSendsTheVocabularyRatherThanTheDocumentThatWasTyped() {
        // The filter is written in the shell's syntax and leaves as Tapstate's vocabulary. Sending the
        // typed document instead would be a passthrough with a friendly parser in front of it, which is
        // the one thing this face is not.
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.findOutcome = new DataBrowserOutcome.Find.Read(List.of(), null, false);
        Harness h = onlineSession(Path.of("tap-work"), client);

        h.repl().dispatch(
                "views.order_state.find({status: 'Paid'}).sort({field:'total', dir:'desc'}).limit(5)");

        assertThat(client.lastFindFilter)
                .isEqualTo(Map.of("field", "status", "op", "eq", "value", "Paid"));
        assertThat(client.lastFindSort).isEqualTo(new DataBrowserCall.Order("total", "desc"));
        assertThat(client.lastFindLimit).isEqualTo(5);
    }

    @Test
    void findSaysHowManyThereAreAndThatMoreRemain() {
        // The read is one-shot, so nothing in the rows separates a preview of ten from a collection of
        // ten. This line is the whole of what does.
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.findOutcome = new DataBrowserOutcome.Find.Read(
                List.of(Map.of("order_id", "ord_123")), 512L, true);
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        h.repl().dispatch("views.order_state.find()");

        String output = h.sink().toString().substring(mark);
        assertThat(output).contains("\"order_id\": \"ord_123\"");
        assertThat(output).contains("showing 1 of ~512").contains("more rows remain");
    }

    @Test
    void findWritesEachRowFormattedRatherThanFlattenedOntoOneLine() {
        // A row with anything embedded in it is unreadable on one line: the reader is scanning for one
        // leaf and the whole tree is punctuation between them. The rows are complete either way -- what
        // changes is whether a person can find anything in them.
        java.util.Map<String, Object> item = new java.util.LinkedHashMap<>();
        item.put("sku", "A-1");
        item.put("qty", 2);
        java.util.Map<String, Object> order = new java.util.LinkedHashMap<>();
        order.put("id", "ord_1");
        order.put("items", List.of(item));
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.findOutcome = new DataBrowserOutcome.Find.Read(List.of(order), 1L, false);
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        h.repl().dispatch("views.order_state.find()");

        String output = h.sink().toString().substring(mark);
        assertThat(output).as("an embedded field starts its own indented block rather than continuing "
                        + "the same line")
                .contains("\n  \"items\": [");
        assertThat(output).as("formatting is not allowed to cost content -- every leaf still arrives")
                .contains("\"sku\": \"A-1\"").contains("\"qty\": 2").contains("\"id\": \"ord_1\"");
    }

    @Test
    void findSaysAnUnorderedReadIsNotInAStableOrder() {
        // Asking for no order leaves it to the database, which does not promise the same one twice. A
        // reader who is not told reads the first row as "the first row".
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.findOutcome = new DataBrowserOutcome.Find.Read(List.of(Map.of("a", 1)), 1L, false);
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        h.repl().dispatch("views.order_state.find()");

        assertThat(h.sink().toString().substring(mark))
                .contains("natural order — not stable, and not the newest");
    }

    @Test
    void findNamesTheOrderInsteadWhenOneWasAskedFor() {
        // The other half, so a footer that printed the natural-order caveat unconditionally cannot pass:
        // said over an ordered read it is simply false.
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.findOutcome = new DataBrowserOutcome.Find.Read(List.of(Map.of("a", 1)), 1L, false);
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        h.repl().dispatch("views.order_state.find().sort({field:'total', dir:'desc'})");

        String output = h.sink().toString().substring(mark);
        assertThat(output).contains("ordered by `total` desc");
        assertThat(output).doesNotContain("natural order");
    }

    @Test
    void findGivesNoCountForAFilteredReadRatherThanOneItDidNotPayFor() {
        // Counting a filtered collection is a full scan, so the server withholds it. A footer that
        // rendered the absent count as 0 would state as fact the one thing the read declined to work out.
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.findOutcome = new DataBrowserOutcome.Find.Read(List.of(Map.of("a", 1)), null, true);
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        h.repl().dispatch("views.order_state.find({status: 'Paid'})");

        String output = h.sink().toString().substring(mark);
        assertThat(output).contains("showing 1 ").doesNotContain(" of ~").doesNotContain("of ~0");
        assertThat(output).contains("more rows remain");
    }

    @Test
    void statsReportsTheRowCountAndAverageRowSizeAndSaysTheCountIsAnEstimate() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.statsOutcome = new DataBrowserOutcome.Stats.Reported(512L, 40960L, 80L);
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        h.repl().dispatch("views.order_state.stats()");

        String output = h.sink().toString().substring(mark);
        assertThat(output).contains("~512").contains("80 bytes").contains("not counted");
        assertThat(client.dataBrowserCalls).containsExactly("stats views.order_state");
    }

    @Test
    void statsReportsASizeTheConnectorWithheldAsUnreportedRatherThanZero() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.statsOutcome = new DataBrowserOutcome.Stats.Reported(null, null, null);
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        h.repl().dispatch("views.order_state.stats()");

        String output = h.sink().toString().substring(mark);
        assertThat(output).contains("not reported").doesNotContain("~0").doesNotContain("0 bytes");
    }

    @Test
    void aReadShellLineOffLineReportsThereIsNoConnectionRatherThanAnUnknownVerb() {
        // It falls to the verb table otherwise, which answers a correctly typed read with a spelling
        // suggestion for a word that was spelt right.
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        Harness h = harness(Path.of("tap-work"), client, new ScriptedPrompter());
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("views.order_state.find()")).isTrue();

        assertThat(h.sink().toString().substring(mark)).contains("cli.not-connected");
        assertThat(client.dataBrowserCalls).isEmpty();
    }

    @Test
    void aReadShellLineItCannotParseSaysWhyAndAsksTheServerNothing() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("views.order_state.drop()")).isTrue();

        assertThat(h.sink().toString().substring(mark)).contains("is not a read");
        assertThat(client.dataBrowserCalls).isEmpty();
        assertThat(h.repl().lastExitCode()).isEqualTo(Cli.EXIT_USAGE);
    }

    @Test
    void theSameReadIsReachableAsAVerbForAOneShotInvocation() {
        // A session takes the bare line, but a script gets one command; without the verb the shell would
        // be unreachable from anything that is not a person at a prompt.
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.findOutcome = new DataBrowserOutcome.Find.Read(List.of(), null, false);
        Harness h = onlineSession(Path.of("tap-work"), client);

        h.repl().dispatch(List.of("data-browser", "views.order_state.find()"));

        assertThat(client.dataBrowserCalls).containsExactly(
                "find views.order_state filter=null sort=null limit=null");
    }

    @Test
    void tokenCreatePrintsTheNewBearerExactlyOnce() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.tokenCreateOutcome = new TokenCreateOutcome.Issued(
                new RemoteCreatedToken("tok_01", "WRITE", "ts_live_secret", "2026-07-30T01:02:03Z"));
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("token create --scope write -o json")).isTrue();

        String output = h.sink().toString().substring(mark);
        assertThat(output).contains("\"tokenId\": \"tok_01\"").contains("\"token\": \"ts_live_secret\"");
        assertThat(output.split("ts_live_secret", -1)).hasSize(2);
        assertThat(client.tokenCalls).containsExactly("create write jwt-tok@http://node1:7900");
        assertThat(h.repl().lastExitCode()).isZero();
    }

    @Test
    void tokenListRendersSecretFreeMetadata() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.tokenListOutcome = new TokenListOutcome.Listed(List.of(
                new RemoteToken("tok_01", "READ", false, "2026-07-30T01:02:03Z")));
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("token list -o yaml")).isTrue();

        String output = h.sink().toString().substring(mark);
        assertThat(output).contains("tokenId: tok_01").contains("scope: READ").contains("revoked: false");
        assertThat(output).doesNotContain("secret").doesNotContain("token:");
        assertThat(client.tokenCalls).containsExactly("list jwt-tok@http://node1:7900");
    }

    @Test
    void tokenListReportsWhenNoMachineTokensExist() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.tokenListOutcome = new TokenListOutcome.Listed(List.of());
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("token list")).isTrue();

        assertThat(h.sink().toString().substring(mark)).contains("no machine tokens");
    }

    @Test
    void tokenRevokeRequiresAnIdAndConfirmsTheRevocation() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.tokenRevokeOutcome = new TokenRevokeOutcome.Revoked();
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("token revoke tok_01")).isTrue();

        assertThat(h.sink().toString().substring(mark)).contains("revoked").contains("tok_01");
        assertThat(client.tokenCalls).containsExactly("revoke tok_01 jwt-tok@http://node1:7900");
        assertThat(h.repl().lastExitCode()).isZero();
    }

    @Test
    void tokenRequiresARecognizedActionAndValidatesCreateOptions() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        Harness h = onlineSession(Path.of("tap-work"), client);

        assertThat(h.repl().dispatch("token")).isTrue();
        assertThat(h.sink().toString()).contains("missing action");
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("token unknown")).isTrue();
        assertThat(h.sink().toString().substring(mark)).contains("unknown action");
        mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("token create")).isTrue();
        assertThat(h.sink().toString().substring(mark)).contains("--scope must be read, write, or admin");
        mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("token create --scope nope")).isTrue();
        assertThat(h.sink().toString().substring(mark)).contains("--scope must be read, write, or admin");
        mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("token create --scope read --output xml")).isTrue();
        assertThat(h.sink().toString().substring(mark)).contains("unknown output format");
        assertThat(client.tokenCalls).isEmpty();
    }

    @Test
    void tokenCreateRejectsUnknownOptionsAndRendersServerRejections() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        Harness h = onlineSession(Path.of("tap-work"), client);

        assertThat(h.repl().dispatch("token create --scope read --unexpected")).isTrue();
        assertThat(h.sink().toString()).contains("unknown or incomplete option");
        assertThat(client.tokenCalls).isEmpty();

        client.tokenCreateOutcome = new TokenCreateOutcome.Rejected(
                "control.forbidden", "Token creation is forbidden.");
        int mark = h.sink().toString().length();
        assertThat(h.repl().dispatch("token create --scope admin")).isTrue();
        assertThat(h.sink().toString().substring(mark))
                .contains("control.forbidden")
                .contains("Token creation is forbidden.");
    }

    @Test
    void tokenListAndRevokeValidateFormatsAndRenderFailures() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        Harness h = onlineSession(Path.of("tap-work"), client);

        assertThat(h.repl().dispatch("token list --output")).isTrue();
        assertThat(h.sink().toString()).contains("unknown or incomplete option");
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("token list -o xml")).isTrue();
        assertThat(h.sink().toString().substring(mark)).contains("unknown output format");
        mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("token revoke")).isTrue();
        assertThat(h.sink().toString().substring(mark)).contains("missing token id");
        mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("token revoke tok_01 --output xml")).isTrue();
        assertThat(h.sink().toString().substring(mark)).contains("unknown output format");
        mark = h.sink().toString().length();

        client.tokenListOutcome = new TokenListOutcome.Unreachable();
        assertThat(h.repl().dispatch("token list")).isTrue();
        assertThat(h.sink().toString().substring(mark)).contains("request failed");
        assertThat(client.tokenCalls).containsExactly(
                "list jwt-tok@http://node1:7900", "list jwt-tok@http://node1:7900");
    }

    /** An authenticated session whose interpolation reads {@code env} instead of the process environment. */
    private static Harness onlineSession(Path workdir, FakeControlPlane client, Map<String, String> env) {
        Harness h = harness(workdir, client, new ScriptedPrompter("pw"), env);
        authenticateOnNode1(h.repl());
        return h;
    }

    @Test
    void getWhileAuthenticatedFetchesTheArtifactFromTheServer() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.getOutcome = new GetOutcome.Found(
                new RemoteArtifact("src_kfk", "source", "kind: source\nid: src_kfk\n"));
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();
        assertThat(h.repl().dispatch("get src_kfk")).isTrue();
        String out = h.sink().toString().substring(mark);
        assertThat(out).contains("kind: source").contains("src_kfk");
        // the credential travels to the current landing node
        assertThat(client.getCalls).containsExactly("jwt-tok@http://node1:7900/src_kfk");
    }

    /**
     * Every verb that needs a connection must be routed to the server once one exists. The routing list
     * is written out separately from the projection (it also carries {@code ls}, which browses locally
     * when offline), and nothing reconciled the two: a verb added to the projection but not to the
     * routing list is registered, listed, helped, completed — and then answers "run connect first" to a
     * user who is already connected, which reads as a broken server rather than a missing line here.
     * Measured on {@code delete}, which did exactly that until this test was written.
     */
    @Test
    void everyVerbThatNeedsAConnectionIsRoutedOnceThereIsOne() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        Harness h = onlineSession(Path.of("tap-work"), client);

        for (String verb : Cli.CONNECTED_VERBS) {
            int mark = h.sink().toString().length();
            h.repl().dispatch(verb);
            assertThat(h.sink().toString().substring(mark))
                    .as(verb + " is projected as a connected verb, so a connected session must route it")
                    .doesNotContain("cli.not-connected");
        }
    }

    /**
     * Without {@code --if-match} the verb reads first and removes the version it read. The hash sent is
     * asserted against the canonical bytes that came back: sending anything else — a blank, a literal
     * null, a hash of something else — would make the removal unconditional in effect, which is the one
     * property the precondition exists to provide.
     */
    @Test
    void deleteWithoutAPreconditionReadsTheArtifactAndRemovesThatExactVersion() {
        String canonical = "kind: source\nid: src_kfk\n";
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.getOutcome = new GetOutcome.Found(new RemoteArtifact("src_kfk", "source", canonical));
        client.deleteOutcome = new DeleteOutcome.Removed("src_kfk");
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("delete src_kfk")).isTrue();

        assertThat(h.sink().toString().substring(mark)).contains("deleted").contains("source").contains("src_kfk");
        assertThat(client.deleteCalls).containsExactly(
                "jwt-tok@http://node1:7900/src_kfk#" + CanonicalHash.of(canonical));
    }

    @Test
    void deleteWithAnExplicitPreconditionSkipsTheReadAndSendsTheHashGiven() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.deleteOutcome = new DeleteOutcome.Removed("src_kfk");
        Harness h = onlineSession(Path.of("tap-work"), client);
        String hash = "c".repeat(64);

        assertThat(h.repl().dispatch("delete src_kfk --if-match " + hash)).isTrue();

        assertThat(client.deleteCalls).containsExactly("jwt-tok@http://node1:7900/src_kfk#" + hash);
        // The caller already holds a version, so no read is issued on its behalf.
        assertThat(client.getCalls).isEmpty();
    }

    @Test
    void deleteOfAnIdThatIsNotThereReportsNotFoundAndAsksTheServerToRemoveNothing() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.getOutcome = new GetOutcome.Absent();
        Harness h = onlineSession(Path.of("tap-work"), client);

        assertThat(h.repl().dispatch("delete nope")).isTrue();

        assertThat(h.repl().lastExitCode()).isEqualTo(Cli.EXIT_DIAGNOSTIC);
        assertThat(client.deleteCalls).isEmpty();
    }

    @Test
    void deleteOnTheMachineFaceEmitsAStructuredResultInsteadOfASentence() {
        // A script removing resources needs to know what it removed, and "deleted source src_kfk" is a
        // sentence, not a result. The precondition actually used is part of it: without --if-match the
        // verb picks the version itself, and the caller has no other way to learn which one went.
        String canonical = "kind: source\nid: src_kfk\n";
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.getOutcome = new GetOutcome.Found(new RemoteArtifact("src_kfk", "source", canonical));
        client.deleteOutcome = new DeleteOutcome.Removed("src_kfk");
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("delete src_kfk -o json")).isTrue();

        String out = h.sink().toString().substring(mark);
        assertThat(out)
                .contains("\"id\": \"src_kfk\"")
                .contains("\"kind\": \"source\"")
                .contains("\"removed\": true")
                .contains("\"expectedContentHash\": \"" + CanonicalHash.of(canonical) + "\"");
        assertThat(h.repl().lastExitCode()).isZero();
    }

    /**
     * A read may be replayed on another member; a removal may not. The replay cannot tell its own first
     * attempt's success apart from the id never having been there — both answer {@code
     * artifact.not-found} — so a removal that landed and lost only its reply comes back reported as a
     * failure, taking a scripted teardown down with it on the non-zero exit.
     */
    @Test
    void aRemovalThatGetsNoAnswerIsSentOnceAndNeverReplayedElsewhere() {
        FakeControlPlane client = new FakeControlPlane(
                URI.create("http://localhost:7900"), URI.create("http://localhost:7901"));
        client.loginOutcome = new LoginOutcome.Success("jwt");
        Harness h = harness(Path.of("tap-work"), client, new ScriptedPrompter("pw"));
        h.repl().dispatch("connect localhost:7900,localhost:7901");
        h.repl().dispatch("login alice");
        client.setHealthy(URI.create("http://localhost:7901"));    // the first seed takes the removal, then goes quiet
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("delete src_kfk --if-match " + "a".repeat(64))).isTrue();

        // One call, to the node that went quiet — the second seed is reachable and would have taken a replay.
        assertThat(client.deleteCalls).hasSize(1);
        assertThat(client.deleteCalls.get(0)).contains("http://localhost:7900");
        // And the report says which it is: neither "deleted" nor "not found", both of which would be
        // asserting something nobody here knows.
        assertThat(h.sink().toString().substring(mark)).contains("may or may not have been applied");
        assertThat(h.repl().lastExitCode()).isEqualTo(Cli.EXIT_DIAGNOSTIC);
    }

    /**
     * {@code -o json} has to hold for every way the verb can fail, not just the refusals. The implicit
     * pre-read is an implementation detail of {@code delete} — the caller never asked for it — so its
     * failures must answer in the shape the caller did ask for. A machine face that covers some failures
     * and not others is one a script cannot parse against at all.
     */
    @Test
    void aPreReadFailureOnTheMachineFaceIsStillADocument() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.getOutcome = new GetOutcome.Absent();
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("delete nope -o json")).isTrue();

        String out = h.sink().toString().substring(mark);
        assertThat(out).contains("\"code\": \"artifact.not-found\"").contains("nope");
        assertThat(client.deleteCalls).isEmpty();
        assertThat(h.repl().lastExitCode()).isEqualTo(Cli.EXIT_DIAGNOSTIC);
    }

    @Test
    void deleteOnTheMachineFaceRendersYamlWhenAskedFor() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.deleteOutcome = new DeleteOutcome.Removed("src_kfk");
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("delete src_kfk --if-match " + "c".repeat(64) + " -o yaml")).isTrue();

        assertThat(h.sink().toString().substring(mark))
                .contains("id: src_kfk")
                .contains("removed: true");
    }

    /**
     * The machine face must not be a worse face. A refusal's parameters are what the text face turns into
     * the next step — who is still referencing it, what state the pipeline is really in — so a machine
     * document carrying only the code and the message would leave a script strictly less able to act than
     * a person reading the same refusal, and the obvious implementation (reuse the shared error document)
     * does exactly that.
     */
    @Test
    void aRefusalOnTheMachineFaceKeepsTheParametersTheTextFaceTurnsIntoNextSteps() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.deleteOutcome = new DeleteOutcome.Rejected(
                "artifact.in-use", "Resource 'src_kfk' is still referenced.",
                Map.of("id", "src_kfk", "referrers", List.of("kfk2my", "kfk2pg")));
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("delete src_kfk --if-match " + "d".repeat(64) + " -o json")).isTrue();

        String out = h.sink().toString().substring(mark);
        assertThat(out).contains("\"code\": \"artifact.in-use\"");
        assertThat(out).contains("kfk2my").contains("kfk2pg");
        assertThat(h.repl().lastExitCode()).isNotZero();
    }

    @Test
    void deleteRefusesAnOutputFormatItDoesNotKnow() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        Harness h = onlineSession(Path.of("tap-work"), client);

        assertThat(h.repl().dispatch("delete src_kfk -o toml")).isTrue();

        assertThat(h.repl().lastExitCode()).isEqualTo(Cli.EXIT_USAGE);
        // Nothing was read and nothing was removed: an unparseable request stops before it acts.
        assertThat(client.deleteCalls).isEmpty();
        assertThat(client.getCalls).isEmpty();
    }

    /**
     * A refusal has to leave the user with the next move, not just a code. Both grounds name something
     * the caller must decide on — and this verb deliberately does neither of them itself, so saying what
     * to do is the whole of the help it can give.
     */
    @Test
    void aReferencedRefusalNamesTheReferrersAndDoesNotOfferToRemoveThem() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.deleteOutcome = new DeleteOutcome.Rejected(
                "artifact.in-use", "Resource 'src_kfk' is still referenced.",
                Map.of("id", "src_kfk", "referrers", List.of("kfk2my", "kfk2pg")));
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("delete src_kfk --if-match " + "d".repeat(64))).isTrue();

        String out = h.sink().toString().substring(mark);
        assertThat(out).contains("artifact.in-use").contains("kfk2my").contains("kfk2pg");
        assertThat(out).contains("remove those first");
    }

    /**
     * The remedy is rendered with the refusal's own parameters, or it is not printed at all.
     *
     * <p>An unbound name is left verbatim by the catalog, so rendering a solution with no arguments
     * does not suppress it - it prints the template, braces and all. This refusal has its parameters
     * in hand and used them on the very next line, while the remedy above it was rendered from
     * nothing: "then delete `{id}`" reached the reader as those eight characters.
     */
    @Test
    void aReferencedRefusalRendersItsRemedyWithTheIdRatherThanAPlaceholder() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.deleteOutcome = new DeleteOutcome.Rejected(
                "artifact.in-use", "Resource 'src_kfk' is still referenced.",
                Map.of("id", "src_kfk", "referrers", List.of("kfk2my")));
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("delete src_kfk --if-match " + "d".repeat(64))).isTrue();

        String out = h.sink().toString().substring(mark);
        assertThat(out).as("no template escapes to the reader").doesNotContain("{id}");
        assertThat(out).contains("then delete `src_kfk`");
    }

    /**
     * A remedy that cannot be filled in is left out rather than printed raw.
     *
     * <p>The call sites that hold parameters now pass them, but most refusals carry none at all, and
     * every one of those reaches the same renderer. Suppressing a solution that still has an unbound
     * name after substitution is what keeps the next caller from reintroducing the raw template - the
     * reader loses a sentence they could not have used, not one they could.
     */
    @Test
    void aRefusalWithNoParametersPrintsNoRemedyRatherThanTheRawTemplate() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.deleteOutcome = new DeleteOutcome.Rejected(
                "artifact.in-use", "Resource 'src_kfk' is still referenced.", Map.of());
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("delete src_kfk --if-match " + "d".repeat(64))).isTrue();

        String out = h.sink().toString().substring(mark);
        assertThat(out).contains("artifact.in-use");
        assertThat(out).doesNotContain("{id}").doesNotContain("Delete or rewrite those resources first");
    }

    @Test
    void aRunningPipelineRefusalShowsBothHalvesOfTheStateAndPointsAtStop() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.deleteOutcome = new DeleteOutcome.Rejected(
                "artifact.pipeline-not-stopped", "Pipeline 'kfk2my' is not stopped.",
                Map.of("id", "kfk2my", "actual", "RUNNING", "desired", "RUNNING"));
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("delete kfk2my --if-match " + "e".repeat(64))).isTrue();

        String out = h.sink().toString().substring(mark);
        // Both halves are shown: the desired state alone would not explain a pipeline that is still
        // draining after a stop, and the actual alone would not explain one that is about to come back up.
        assertThat(out).contains("actual=RUNNING").contains("desired=RUNNING");
        assertThat(out).contains("stop kfk2my");
    }

    @Test
    void theStopGuidanceNamesTheTypedIdEvenWhenTheRefusalCarriesNone() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        // A refusal that names the state but not the id: the guidance is a command the user is meant
        // to paste, so filling the id slot from an absent parameter prints `stop null` and hands them
        // a line that cannot work. The verb already knows which id was typed.
        client.deleteOutcome = new DeleteOutcome.Rejected(
                "artifact.pipeline-not-stopped", "Pipeline 'kfk2my' is not stopped.",
                Map.of("actual", "RUNNING", "desired", "RUNNING"));
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("delete kfk2my --if-match " + "e".repeat(64))).isTrue();

        String out = h.sink().toString().substring(mark);
        assertThat(out).contains("stop kfk2my");
        assertThat(out).doesNotContain("stop null");
    }

    @Test
    void aPartlyExecutedRemovalSaysTheResourceIsGoneAndNamesTheResidueInsteadOfInvitingARetry() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        // The one failure on this verb that is not a refusal: the artifact is destroyed and some of its
        // bookkeeping is not. Rendered like the refusals above, the operator retries — which can only
        // answer artifact.not-found — and the residue that does need clearing is never mentioned.
        client.deleteOutcome = new DeleteOutcome.Rejected(
                "artifact.reclaim-incomplete", "Artifact 'kfk2my' was removed, but bookkeeping was left.",
                Map.of("id", "kfk2my", "reason", "step-failed",
                        "residue", List.of("desired", "mining-chain-consumer")));
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("delete kfk2my --if-match " + "e".repeat(64))).isTrue();

        String out = h.sink().toString().substring(mark);
        assertThat(out).contains("'kfk2my' is gone").contains("Do not retry");
        assertThat(out).contains("desired").contains("mining-chain-consumer");
    }

    @Test
    void aRemovalThatStoppedBecauseThePipelineCameBackUpSaysToStopItBeforeClearingAnything() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        // The other way a removal ends incomplete: nothing was cleared, deliberately, because the
        // pipeline was started while the removal ran. "Clear the listed records by hand" is the wrong
        // next step here — deleting the checkpoint of a running job discards its fencing epoch.
        client.deleteOutcome = new DeleteOutcome.Rejected(
                "artifact.reclaim-incomplete", "Artifact 'kfk2my' was removed, but bookkeeping was left.",
                Map.of("id", "kfk2my", "reason", "pipeline-live",
                        "residue", List.of("mining-chain-consumer", "desired", "state", "observation")));
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("delete kfk2my --if-match " + "e".repeat(64))).isTrue();

        String out = h.sink().toString().substring(mark);
        assertThat(out).contains("stop kfk2my");
        assertThat(out).contains("started again");
    }

    @Test
    void aVersionConflictTellsTheUserToReadAgainRatherThanToRetryWithoutThePrecondition() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.deleteOutcome = new DeleteOutcome.Rejected(
                "artifact.version-conflict", "Resource 'src_kfk' has changed.", Map.of("id", "src_kfk"));
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("delete src_kfk --if-match " + "f".repeat(64))).isTrue();

        String out = h.sink().toString().substring(mark);
        assertThat(out).contains("artifact.version-conflict").contains("read it again");
        // Dropping the precondition and retrying is the one recovery that must never be suggested: it
        // turns a refused removal into one that discards whatever the other writer just put there.
        assertThat(out).doesNotContain("--if-match to skip").doesNotContain("without --if-match");
    }

    @Test
    void deleteRejectsAMalformedInvocationBeforeTouchingTheServer() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        Harness h = onlineSession(Path.of("tap-work"), client);

        for (String line : List.of("delete", "delete a b", "delete a --if-match", "delete a --nope")) {
            assertThat(h.repl().dispatch(line)).as(line).isTrue();
            assertThat(h.repl().lastExitCode()).as(line).isEqualTo(Cli.EXIT_USAGE);
        }
        assertThat(client.deleteCalls).isEmpty();
        assertThat(client.getCalls).isEmpty();
    }

    @Test
    void getForAMissingIdReportsNotFound() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.getOutcome = new GetOutcome.Absent();
        Harness h = onlineSession(Path.of("tap-work"), client);
        h.repl().dispatch("get nope");
        assertThat(h.sink().toString()).contains("not found").contains("nope");
    }

    @Test
    void getWhileConnectedButNotAuthenticatedReportsAndDoesNotCallTheServer() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        Harness h = harness(Path.of("tap-work"), client, new ScriptedPrompter());
        h.repl().dispatch("connect node1:7900");   // connected, never logged in
        assertThat(h.repl().dispatch("get x")).isTrue();
        // the not-authenticated state is a coded cli.* diagnostic naming the verb, not a bare string
        assertThat(h.sink().toString()).contains("cli.not-authenticated").contains("get");
        assertThat(client.getCalls).isEmpty();
    }

    @Test
    void anUnauthenticatedOnlineVerbRendersThroughTheSharedCatalogRendererNamingTheVerb() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        Harness h = harness(Path.of("tap-work"), client, new ScriptedPrompter());
        h.repl().dispatch("connect node1:7900");   // connected, never logged in
        int mark = h.sink().toString().length();
        assertThat(h.repl().dispatch("apply")).isTrue();
        String out = h.sink().toString().substring(mark);
        // rendered like every other coded diagnostic: an `error: <code>` header, the catalog message,
        // and the solution hint — proving it goes through the shared renderer, not a hand-rolled string
        assertThat(out).contains("error:").contains("cli.not-authenticated");
        assertThat(out).contains("apply");        // the {verb} placeholder is bound to the verb name
        assertThat(out).containsIgnoringCase("login");   // the solution points at the recovery verb
        assertThat(client.applyCalls).isEmpty();
    }

    @Test
    void getWithNoIdReportsMissingOperandAndDoesNotCallTheServer() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();
        assertThat(h.repl().dispatch("get")).isTrue();
        assertThat(h.sink().toString().substring(mark)).contains("get:").contains("missing operand");
        assertThat(client.getCalls).isEmpty();
    }

    @Test
    void getRenderingAServerRejectionShowsTheCodeAndMessage() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.getOutcome = new GetOutcome.Rejected("control.forbidden", "You lack the grade.");
        Harness h = onlineSession(Path.of("tap-work"), client);
        h.repl().dispatch("get x");
        assertThat(h.sink().toString()).contains("control.forbidden").contains("You lack the grade.");
    }

    @Test
    void lsWhileConnectedListsServerArtifactsNotTheLocalWorkspace() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.listOutcome = new ListOutcome.Listed(List.of(
                new RemoteArtifact("src_kfk", "source", "kind: source\n"),
                new RemoteArtifact("kfk2my", "pipeline", "kind: pipeline\n")));
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();
        assertThat(h.repl().dispatch("ls")).isTrue();
        String out = h.sink().toString().substring(mark);
        assertThat(out).contains("src_kfk").contains("kfk2my").contains("source").contains("pipeline");
        assertThat(client.listCalls).containsExactly("jwt-tok@http://node1:7900?null");
    }

    @Test
    void lsWithAKindPassesTheKindFilterToTheServer() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.listOutcome = new ListOutcome.Listed(List.of());
        Harness h = onlineSession(Path.of("tap-work"), client);
        h.repl().dispatch("ls source");
        assertThat(client.listCalls).containsExactly("jwt-tok@http://node1:7900?source");
    }

    // --- online verb: `connectors` lists the online catalog view (registered rows included) --------

    @Test
    void connectorsWhileAuthenticatedListsTheOnlineCatalogWithOriginTags() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.connectorListOutcome = new ConnectorListOutcome.Listed(List.of(
                new CatalogConnector("mysql", "MySQL", "database", List.of("snapshot", "cdc"), true, "bundled"),
                new CatalogConnector("acme", "Acme", "database", List.of("snapshot"), false, "registered")));
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();
        assertThat(h.repl().dispatch("connectors")).isTrue();
        String out = h.sink().toString().substring(mark);
        assertThat(out).contains("mysql").contains("acme")
                .contains("bundled").contains("registered").contains("snapshot");
        assertThat(client.connectorListCalls).containsExactly("jwt-tok@http://node1:7900");
    }

    @Test
    void connectorsWithJsonOutputEmitsTheMachineForm() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.connectorListOutcome = new ConnectorListOutcome.Listed(List.of(
                new CatalogConnector("acme", "Acme", "database", List.of("snapshot"), false, "registered")));
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();
        assertThat(h.repl().dispatch("connectors -o json")).isTrue();
        String out = h.sink().toString().substring(mark);
        assertThat(out).contains("connectors").contains("acme").contains("origin").contains("registered");
    }

    @Test
    void connectorsRenderingAServerRejectionShowsTheCodeAndMessage() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.connectorListOutcome = new ConnectorListOutcome.Rejected("control.forbidden", "You lack the grade.");
        Harness h = onlineSession(Path.of("tap-work"), client);
        h.repl().dispatch("connectors");
        assertThat(h.sink().toString()).contains("control.forbidden").contains("You lack the grade.");
    }

    @Test
    void connectorsRejectsAStrayOperandWithAUsageLine() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();
        h.repl().dispatch("connectors nope");
        assertThat(h.sink().toString().substring(mark)).contains("connectors:").contains("takes no operand");
        assertThat(client.connectorListCalls).isEmpty();
    }

    // --- connection test: `test <id>` sources the stored connection, then probes it and renders the report ---

    /** A stored source connection whose canonical form carries the connector id and its connection config. */
    private static GetOutcome.Found storedConnection() {
        return new GetOutcome.Found(new RemoteArtifact("my-mongo", "source",
                "kind: source\nid: my-mongo\nconnector: mongodb\nconfig:\n  host: db.internal\n  username: cdc\n"));
    }

    private static ConnectionTestOutcome.Tested passedReport() {
        return new ConnectionTestOutcome.Tested(new ConnectionReport("my-mongo", "mongodb", "PASSED",
                List.of(new ConnectionReport.Check("ping", "PASSED", null, null, null, null),
                        new ConnectionReport.Check("version", "WARNING", "server is old", null, null, null)),
                1752000000000L));
    }

    @Test
    void testFetchesTheStoredConnectionThenProbesItAndRendersTheReport() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.getOutcome = storedConnection();
        client.testOutcome = passedReport();
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("test my-mongo")).isTrue();

        String out = h.sink().toString().substring(mark);
        // the report renders the overall outcome, the connection + connector, and each check
        assertThat(out).contains("PASSED").contains("my-mongo").contains("mongodb");
        assertThat(out).contains("ping").contains("version").contains("WARNING").contains("server is old");
        // server-as-truth: it fetched the stored connection first, then posted the probe with the parsed
        // connector + settings under the session credential
        assertThat(client.getCalls).containsExactly("jwt-tok@http://node1:7900/my-mongo");
        assertThat(client.testCalls).containsExactly(
                "jwt-tok@http://node1:7900/my-mongo[mongodb {host=db.internal, username=cdc}]");
    }

    /**
     * A connector that can say why a check failed and what to do about it says so through reason and
     * solution, and until now the plain-text surface printed neither - they were reachable only by
     * knowing to pass -o json, which is knowledge the person who needs them least likely has. The
     * remedy the connector already wrote is the whole value of the check having run.
     */
    @Test
    void testRendersTheReasonAndSolutionOnThePlainTextSurface() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.getOutcome = storedConnection();
        client.testOutcome = new ConnectionTestOutcome.Tested(new ConnectionReport(
                "my-mongo", "mongodb", "PASSED",
                List.of(new ConnectionReport.Check("read log", "WARNING", "Cdc cannot start",
                        "wal_level is replica, logical decoding needs logical",
                        "Set wal_level=logical and restart the server", "CREATE_SLOT_FAILED")),
                1752000000000L));
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("test my-mongo")).isTrue();

        String out = h.sink().toString().substring(mark);
        assertThat(out).contains("Cdc cannot start");
        assertThat(out).contains("wal_level is replica");
        assertThat(out).contains("Set wal_level=logical");
        assertThat(out).contains("CREATE_SLOT_FAILED");
    }

    /**
     * A diagnostic the catalog does not know is shown as it arrived, not dropped.
     *
     * <p>This used to be decided by shape - dotted lowercase segments were taken for an unresolvable
     * key and suppressed. The shape does not separate the two: {@code 10.10.0.5}, {@code db.internal}
     * and {@code 8.0.13} all match it, and all three are exactly what a host or version check reports.
     * Suppressing them left a failed check with nothing on it at all, which is less than the reader
     * had before the wording feature existed. The keys the connector API defines are a closed set the
     * catalog holds in full, so anything absent from it is treated as a value and printed.
     */
    @Test
    void testPrintsADiagnosticTheCatalogDoesNotKnowRatherThanDroppingIt() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.getOutcome = storedConnection();
        client.testOutcome = new ConnectionTestOutcome.Tested(new ConnectionReport(
                "my-mongo", "mongodb", "FAILED",
                List.of(new ConnectionReport.Check("Check host port is valid", "FAILED",
                        "10.10.0.5", "db.internal", "8.0.13", "CONN_REFUSED")),
                1752000000000L));
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("test my-mongo")).isTrue();

        String out = h.sink().toString().substring(mark);
        assertThat(out)
                .as("a host, a hostname and a version all look like keys and are none of them")
                .contains("10.10.0.5").contains("db.internal").contains("8.0.13");
        assertThat(out).contains("CONN_REFUSED");
    }

    /**
     * A diagnostic far longer than anything a pattern should be run over is carried, not crashed on.
     *
     * <p>Deciding whether a string was a key by matching a repeated group is recursive in this
     * platform's engine - one frame per repetition, so a few thousand dotted segments exhausted the
     * stack, and the failure was a StackOverflowError thrown out of printing a connection report. The
     * string is a connector's own, arriving from a database nobody here configured, and nothing in
     * this repository bounds its length. Nothing matches it now - the catalog is asked instead - and
     * this case is what keeps a matcher from coming back.
     */
    @Test
    void testCarriesADiagnosticFarTooLongToMatchAgainst() {
        String enormous = "a" + ".a".repeat(100_000);
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.getOutcome = storedConnection();
        client.testOutcome = new ConnectionTestOutcome.Tested(new ConnectionReport(
                "my-mongo", "mongodb", "PASSED",
                List.of(new ConnectionReport.Check("read log", "WARNING", "Access denied",
                        enormous, null, "CDC_PRIVILEGE")),
                1752000000000L));
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("test my-mongo")).isTrue();

        String out = h.sink().toString().substring(mark);
        assertThat(out).contains("Access denied").contains("CDC_PRIVILEGE");
        assertThat(out).as("the catalog does not know it, so it is shown as it arrived").contains(enormous);
    }

    /**
     * For the keys the connector API actually defines, the catalog supplies the wording the connector
     * never carried, so the checks that decide whether a stranger's own database can be read at all
     * say what to do about it rather than naming a key nobody can resolve.
     */
    @Test
    void testRendersOurOwnWordingForAConnectorApiDiagnosticKey() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.getOutcome = storedConnection();
        client.testOutcome = new ConnectionTestOutcome.Tested(new ConnectionReport(
                "my-mongo", "mongodb", "PASSED",
                List.of(new ConnectionReport.Check("read log", "WARNING", "Access denied",
                        "check.cdc.privilege.reason", "check.cdc.privilege.solution", "CDC_PRIVILEGE")),
                1752000000000L));
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("test my-mongo")).isTrue();

        String out = h.sink().toString().substring(mark);
        assertThat(out).contains("privileges a change stream needs");
        assertThat(out).contains("Grant the replication privileges");
        assertThat(out).doesNotContain("check.cdc.privilege");
    }

    /**
     * The change-stream check is the one whose failure is invisible: connectors report it as a warning,
     * a warning does not fail the overall outcome, and so a database with change capture switched off
     * answers "PASSED". The person then builds a pipeline whose capture half never runs and is told
     * nothing, anywhere. The outcome is left alone - it is a connection verb, and the connection is
     * genuinely usable for a snapshot - but the consequence is spelled out where it cannot be missed.
     */
    @Test
    void testSaysPlainlyWhenChangeCaptureWillNotWorkEvenThoughTheTestPassed() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.getOutcome = storedConnection();
        client.testOutcome = new ConnectionTestOutcome.Tested(new ConnectionReport(
                "my-mongo", "mongodb", "PASSED",
                List.of(new ConnectionReport.Check("ping", "PASSED", null, null, null, null),
                        new ConnectionReport.Check("Read log", "WARNING",
                                "SELECT pg_create_logical_replication_slot('t','pgoutput')",
                                null, null, "410003")),
                1752000000000L));
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("test my-mongo")).isTrue();

        String out = h.sink().toString().substring(mark);
        assertThat(out).contains("PASSED");
        assertThat(out).containsIgnoringCase("change capture");
        assertThat(out).contains("snapshot");
    }

    /**
     * The advisory names the check it is about, because "the check above" need not be the one that
     * failed. Checks are printed in the order the connector reports them, and nothing puts the
     * change-stream check last - so a passing check after it made the advisory point at a result that
     * is fine, and blame the wrong thing to go and fix.
     */
    @Test
    void testNamesTheChangeStreamCheckWhenAPassingCheckIsPrintedAfterIt() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.getOutcome = storedConnection();
        client.testOutcome = new ConnectionTestOutcome.Tested(new ConnectionReport(
                "my-mongo", "mongodb", "PASSED",
                List.of(new ConnectionReport.Check("Read log", "WARNING", "no replication slot",
                                null, null, "410003"),
                        new ConnectionReport.Check("ping", "PASSED", null, null, null, null)),
                1752000000000L));
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("test my-mongo")).isTrue();

        String out = h.sink().toString().substring(mark);
        assertThat(out).contains("Read log check passes");
        assertThat(out).as("the last check printed is a passing one, so 'above' would misname it")
                .doesNotContain("the check above");
    }

    /** A connection whose change-stream check passed says nothing of the sort. */
    @Test
    void testDoesNotWarnAboutChangeCaptureWhenTheLogCheckPassed() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.getOutcome = storedConnection();
        client.testOutcome = new ConnectionTestOutcome.Tested(new ConnectionReport(
                "my-mongo", "mongodb", "PASSED",
                List.of(new ConnectionReport.Check("Read log", "PASSED", null, null, null, null)),
                1752000000000L));
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("test my-mongo")).isTrue();

        assertThat(h.sink().toString().substring(mark)).doesNotContainIgnoringCase("change capture");
    }

    /**
     * A refusal that names what is wrong but not what to do leaves the reader stuck. The catalog carries
     * a solution for the code, and the server sends the named parameters that fill it in, so the remedy
     * can be rendered here from the same catalog the message came from - and until it is, the most
     * carefully written half of an error is the half nobody sees.
     */
    @Test
    void aRefusedApplyShowsTheRemedyAndNotOnlyWhatWentWrong(@TempDir Path base) throws Exception {
        copyWorkspace("/ws-valid", base);
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.applyOutcome = new ApplyOutcome.Rejected("dsl.upsert-needs-key",
                "The sync at serve.sync upserts table `events` of source `mydb`, but that table declares "
                        + "no key, so no write can be matched to the row it belongs to.",
                Map.of("table", "events", "source", "mydb", "path", "serve.sync"));
        Harness h = onlineSession(base, client);
        int mark = h.sink().toString().length();

        h.repl().dispatch("apply");

        String out = h.sink().toString().substring(mark);
        assertThat(out).contains("dsl.upsert-needs-key").contains("declares no key");
        assertThat(out)
                .as("the catalog's solution for the code, with its parameters filled in")
                .contains("Give `events` a primary key")
                .contains("write_mode: append");
    }

    /**
     * The connector API's keys reach the message field too, not only reason and solution - a host/port
     * check that fails carries {@code check.host.port.fail} as its message. Suppressing keys in two
     * fields and printing them in the third would put the unreadable one where the eye lands first.
     */
    @Test
    void testRendersOurOwnWordingWhenTheMessageItselfIsADiagnosticKey() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.getOutcome = storedConnection();
        client.testOutcome = new ConnectionTestOutcome.Tested(new ConnectionReport(
                "my-mongo", "mongodb", "FAILED",
                List.of(new ConnectionReport.Check("Check host port is valid", "FAILED",
                        "check.host.port.fail", "check.host.port.reason", "check.host.port.solution",
                        null)),
                1752000000000L));
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("test my-mongo")).isTrue();

        String out = h.sink().toString().substring(mark);
        assertThat(out).contains("did not accept a connection");
        assertThat(out).doesNotContain("check.host.port.fail");
    }

    @Test
    void testRendersTheReportAsJsonWithTheOutputFlag() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.getOutcome = storedConnection();
        client.testOutcome = passedReport();
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("test my-mongo -o json")).isTrue();

        String out = h.sink().toString().substring(mark);
        assertThat(out).contains("\"connectionId\"").contains("\"my-mongo\"")
                .contains("\"outcome\"").contains("\"PASSED\"").contains("\"ping\"");
    }

    @Test
    void testRendersTheReportAsYamlWithTheOutputFlag() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.getOutcome = storedConnection();
        client.testOutcome = passedReport();
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("test my-mongo -o yaml")).isTrue();

        String out = h.sink().toString().substring(mark);
        assertThat(out).contains("connectionId: my-mongo").contains("outcome: PASSED").contains("name: ping");
    }

    @Test
    void testOnANonSourceIdReportsNotATestableConnectionAndDoesNotProbe() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.getOutcome = new GetOutcome.Found(
                new RemoteArtifact("kfk2my", "pipeline", "kind: pipeline\nid: kfk2my\n"));
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("test kfk2my")).isTrue();

        String out = h.sink().toString().substring(mark);
        assertThat(out).contains("kfk2my").containsIgnoringCase("not a testable connection").contains("pipeline");
        assertThat(client.testCalls).isEmpty();   // a non-connection is never probed
    }

    @Test
    void testForAMissingConnectionReportsNotFoundAndDoesNotProbe() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.getOutcome = new GetOutcome.Absent();
        Harness h = onlineSession(Path.of("tap-work"), client);

        h.repl().dispatch("test nope");

        assertThat(h.sink().toString()).contains("not found").contains("nope");
        assertThat(client.testCalls).isEmpty();
    }

    @Test
    void testWithNoIdReportsMissingOperandAndDoesNotCallTheServer() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("test")).isTrue();

        assertThat(h.sink().toString().substring(mark)).contains("test:").contains("missing operand");
        assertThat(client.getCalls).isEmpty();
        assertThat(client.testCalls).isEmpty();
    }

    @Test
    void testWhileConnectedButNotAuthenticatedReportsAndDoesNotCallTheServer() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        Harness h = harness(Path.of("tap-work"), client, new ScriptedPrompter());
        h.repl().dispatch("connect node1:7900");   // connected, never logged in

        assertThat(h.repl().dispatch("test x")).isTrue();

        assertThat(h.sink().toString()).contains("cli.not-authenticated").contains("test");
        assertThat(client.getCalls).isEmpty();
        assertThat(client.testCalls).isEmpty();
    }

    @Test
    void testRenderingAServerRejectionShowsTheCodeAndMessage() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.getOutcome = storedConnection();
        client.testOutcome = new ConnectionTestOutcome.Rejected("control.forbidden", "You lack the grade.");
        Harness h = onlineSession(Path.of("tap-work"), client);

        h.repl().dispatch("test my-mongo");

        assertThat(h.sink().toString()).contains("control.forbidden").contains("You lack the grade.");
    }

    @Test
    void testRenderingATimeoutIsCodedAndDistinctFromUnreachable() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.getOutcome = storedConnection();
        client.testOutcome = new ConnectionTestOutcome.TimedOut();
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        h.repl().dispatch("test my-mongo");

        // A busy server is not a gone server: the timeout carries its own code and names the landing node.
        String out = h.sink().toString().substring(mark);
        assertThat(out).contains("cli.request-timed-out").contains("node1:7900");
        assertThat(out).doesNotContain("unreachable");
    }

    @Test
    void testOfflineReportsThatAConnectionIsRequired() {
        Harness h = harness(Path.of("tap-work"));

        assertThat(h.repl().dispatch("test my-mongo")).isTrue();

        assertThat(h.sink().toString()).contains("cli.not-connected").contains("test");
    }

    // --- connection test result: `test-result <id>` reads back the connection's latest stored result ---

    /** A stored FAILED result carrying full per-check diagnostics — the read peer returns whatever last ran. */
    private static ConnectionTestResultOutcome.Found storedResult() {
        return new ConnectionTestResultOutcome.Found(new ConnectionReport("my-mongo", "mongodb", "FAILED",
                List.of(new ConnectionReport.Check("Login", "FAILED", "auth failed", "SCRAM rejected",
                        "check the password", "11000")),
                1752000000000L));
    }

    @Test
    void testResultReadsBackTheStoredReportWithoutRunningAProbe() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.testResultOutcome = storedResult();
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("test-result my-mongo")).isTrue();

        String out = h.sink().toString().substring(mark);
        // the report renders the last outcome, the connection + connector, and each check with its diagnostics
        assertThat(out).contains("FAILED").contains("my-mongo").contains("mongodb")
                .contains("Login").contains("auth failed");
        // it read the result under the session credential from the current landing node — no probe, no fetch
        assertThat(client.testResultCalls).containsExactly("jwt-tok@http://node1:7900/my-mongo");
        assertThat(client.testCalls).isEmpty();
        assertThat(client.getCalls).isEmpty();
    }

    @Test
    void testResultRendersTheStoredReportAsJsonWithTheOutputFlag() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.testResultOutcome = storedResult();
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("test-result my-mongo -o json")).isTrue();

        String out = h.sink().toString().substring(mark);
        assertThat(out).contains("\"connectionId\"").contains("\"my-mongo\"")
                .contains("\"outcome\"").contains("\"FAILED\"").contains("\"Login\"")
                .contains("\"connectorErrorCode\"").contains("\"11000\"");
    }

    @Test
    void testResultForANeverTestedConnectionReportsNotTestedYet() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.testResultOutcome = new ConnectionTestResultOutcome.Absent();
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("test-result my-mongo")).isTrue();

        // never-tested is a benign line, not a coded error nor a rendered report
        assertThat(h.sink().toString().substring(mark)).contains("my-mongo").containsIgnoringCase("not been tested");
    }

    @Test
    void testResultWithNoIdReportsMissingOperandAndDoesNotCallTheServer() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("test-result")).isTrue();

        assertThat(h.sink().toString().substring(mark)).contains("test-result:").contains("missing operand");
        assertThat(client.testResultCalls).isEmpty();
    }

    @Test
    void testResultWhileConnectedButNotAuthenticatedReportsAndDoesNotCallTheServer() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        Harness h = harness(Path.of("tap-work"), client, new ScriptedPrompter());
        h.repl().dispatch("connect node1:7900");   // connected, never logged in

        assertThat(h.repl().dispatch("test-result x")).isTrue();

        // the not-authenticated state is a coded cli.* diagnostic naming the verb, not a bare string
        assertThat(h.sink().toString()).contains("cli.not-authenticated").contains("test-result");
        assertThat(client.testResultCalls).isEmpty();
    }

    @Test
    void testResultRenderingAServerRejectionShowsTheCodeAndMessage() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.testResultOutcome =
                new ConnectionTestResultOutcome.Rejected("control.forbidden", "You lack the grade.");
        Harness h = onlineSession(Path.of("tap-work"), client);

        h.repl().dispatch("test-result my-mongo");

        assertThat(h.sink().toString()).contains("control.forbidden").contains("You lack the grade.");
    }

    @Test
    void testResultOfflineReportsThatAConnectionIsRequired() {
        Harness h = harness(Path.of("tap-work"));

        assertThat(h.repl().dispatch("test-result my-mongo")).isTrue();

        assertThat(h.sink().toString()).contains("cli.not-connected").contains("test-result");
    }

    // --- schema discovery: `discover-schema <id>` discovers a stored connection's source model ---

    /** A discovered two-table model the schema verbs render — orders (with pk + index) and customers. */
    private static ConnectionSchema discoveredSchema() {
        return new ConnectionSchema("my-mongo", "mongodb",
                List.of(
                        new ConnectionSchema.Table("orders",
                                List.of(new ConnectionSchema.Field("id", "bigint"),
                                        new ConnectionSchema.Field("note", null)),
                                List.of("id"),
                                List.of(new ConnectionSchema.Index("pk_orders", List.of("id"), true))),
                        new ConnectionSchema.Table("customers",
                                List.of(new ConnectionSchema.Field("email", "varchar")),
                                List.of("email"),
                                List.of())),
                1752000000000L);
    }

    @Test
    void discoverSchemaFetchesTheStoredConnectionThenDiscoversAndRendersTheTables() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.getOutcome = storedConnection();
        client.discoverSchemaOutcome = new ConnectionDiscoverSchemaOutcome.Discovered(discoveredSchema());
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("discover-schema my-mongo")).isTrue();

        String out = h.sink().toString().substring(mark);
        // the summary renders the connection + connector and one line per discovered table
        assertThat(out).contains("my-mongo").contains("mongodb")
                .contains("orders").contains("customers").contains("2 tables");
        // server-as-truth: it fetched the stored connection first, then posted the discovery with the
        // parsed connector + settings under the session credential
        assertThat(client.getCalls).containsExactly("jwt-tok@http://node1:7900/my-mongo");
        assertThat(client.discoverSchemaCalls).containsExactly(
                "jwt-tok@http://node1:7900/my-mongo[mongodb {host=db.internal, username=cdc}]");
    }

    @Test
    void discoverSchemaRendersTheModelAsJsonWithTheOutputFlag() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.getOutcome = storedConnection();
        client.discoverSchemaOutcome = new ConnectionDiscoverSchemaOutcome.Discovered(discoveredSchema());
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("discover-schema my-mongo -o json")).isTrue();

        String out = h.sink().toString().substring(mark);
        assertThat(out).contains("\"connectionId\"").contains("\"my-mongo\"")
                .contains("\"tables\"").contains("\"orders\"").contains("\"primaryKey\"")
                .contains("\"discoveredAt\"");
    }

    @Test
    void discoverSchemaOnANonSourceIdReportsNotDiscoverableAndDoesNotDiscover() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.getOutcome = new GetOutcome.Found(
                new RemoteArtifact("kfk2my", "pipeline", "kind: pipeline\nid: kfk2my\n"));
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("discover-schema kfk2my")).isTrue();

        String out = h.sink().toString().substring(mark);
        assertThat(out).contains("kfk2my").containsIgnoringCase("not a discoverable connection").contains("pipeline");
        assertThat(client.discoverSchemaCalls).isEmpty();
    }

    @Test
    void discoverSchemaWithNoIdReportsMissingOperandAndDoesNotCallTheServer() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("discover-schema")).isTrue();

        assertThat(h.sink().toString().substring(mark)).contains("discover-schema:").contains("missing operand");
        assertThat(client.getCalls).isEmpty();
        assertThat(client.discoverSchemaCalls).isEmpty();
    }

    @Test
    void discoverSchemaRenderingAServerRejectionShowsTheCodeAndMessage() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.getOutcome = storedConnection();
        client.discoverSchemaOutcome =
                new ConnectionDiscoverSchemaOutcome.Rejected("control.forbidden", "You lack the grade.");
        Harness h = onlineSession(Path.of("tap-work"), client);

        h.repl().dispatch("discover-schema my-mongo");

        assertThat(h.sink().toString()).contains("control.forbidden").contains("You lack the grade.");
    }

    @Test
    void discoverSchemaRenderingATimeoutIsCodedAndDistinctFromUnreachable() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.getOutcome = storedConnection();
        client.discoverSchemaOutcome = new ConnectionDiscoverSchemaOutcome.TimedOut();
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        h.repl().dispatch("discover-schema my-mongo");

        // Discovery drives a live round-trip to the remote database; a slow answer must not read as "gone".
        String out = h.sink().toString().substring(mark);
        assertThat(out).contains("cli.request-timed-out").contains("node1:7900");
        assertThat(out).doesNotContain("unreachable");
    }

    @Test
    void discoverSchemaOfflineReportsThatAConnectionIsRequired() {
        Harness h = harness(Path.of("tap-work"));

        assertThat(h.repl().dispatch("discover-schema my-mongo")).isTrue();

        assertThat(h.sink().toString()).contains("cli.not-connected").contains("discover-schema");
    }

    // --- register: `register <path>` uploads a local artifact to the server -----------------------

    @Test
    void registerUploadsALocalArtifactAndRendersTheRegistration(@TempDir Path workdir) throws Exception {
        Files.write(workdir.resolve("orders.jar"), new byte[] {1, 2, 3, 4});
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.registerOutcome = new ConnectorRegisterOutcome.Registered(
                new RegisteredConnector("orders", "hash-abc", "1.3.5", true));
        Harness h = onlineSession(workdir, client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("register orders.jar")).isTrue();

        String out = h.sink().toString().substring(mark);
        assertThat(out).contains("registered").contains("orders").contains("hash-abc");
        // the artifact bytes (4) travel to the current landing node under the session credential
        assertThat(client.registerCalls).containsExactly("jwt-tok@http://node1:7900 x4");
    }

    @Test
    void registerRendersATimedOutOutcomeAsACodedDiagnosticDistinctFromUnreachable(@TempDir Path workdir)
            throws Exception {
        Files.write(workdir.resolve("orders.jar"), new byte[] {1, 2, 3, 4});
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.registerOutcome = new ConnectorRegisterOutcome.TimedOut();
        Harness h = onlineSession(workdir, client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("register orders.jar")).isTrue();

        // A timeout is a distinct, coded outcome — it must not be reported as the server being unreachable.
        String out = h.sink().toString().substring(mark);
        assertThat(out).contains("cli.request-timed-out").contains("node1:7900");
        assertThat(out).doesNotContain("unreachable");
    }

    @Test
    void registerRendersAnAlreadyRegisteredArtifactAsANoOp(@TempDir Path workdir) throws Exception {
        Files.write(workdir.resolve("orders.jar"), new byte[] {1, 2, 3});
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.registerOutcome = new ConnectorRegisterOutcome.Registered(
                new RegisteredConnector("orders", "hash-abc", "1.3.5", false));
        Harness h = onlineSession(workdir, client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("register orders.jar")).isTrue();

        assertThat(h.sink().toString().substring(mark)).contains("already registered").contains("orders");
    }

    @Test
    void registerRendersTheRegistrationAsJsonWithTheOutputFlag(@TempDir Path workdir) throws Exception {
        Files.write(workdir.resolve("orders.jar"), new byte[] {1, 2, 3, 4});
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.registerOutcome = new ConnectorRegisterOutcome.Registered(
                new RegisteredConnector("orders", "hash-abc", "1.3.5", true));
        Harness h = onlineSession(workdir, client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("register orders.jar -o json")).isTrue();

        String out = h.sink().toString().substring(mark);
        assertThat(out).contains("\"connectorId\"").contains("\"orders\"")
                .contains("\"contentHash\"").contains("\"newlyRegistered\"");
    }

    @Test
    void registerRendersTheRegistrationAsYamlWithTheOutputFlag(@TempDir Path workdir) throws Exception {
        Files.write(workdir.resolve("orders.jar"), new byte[] {1, 2, 3, 4});
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.registerOutcome = new ConnectorRegisterOutcome.Registered(
                new RegisteredConnector("orders", "hash-abc", "1.3.5", true));
        Harness h = onlineSession(workdir, client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("register orders.jar -o yaml")).isTrue();

        String out = h.sink().toString().substring(mark);
        assertThat(out).contains("connectorId:").contains("orders")
                .contains("contentHash:").contains("hash-abc")
                .contains("newlyRegistered:");
    }

    @Test
    void registerForAMissingFileReportsCannotReadAndDoesNotCallTheServer(@TempDir Path workdir) {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        Harness h = onlineSession(workdir, client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("register nope.jar")).isTrue();

        assertThat(h.sink().toString().substring(mark)).contains("register:").containsIgnoringCase("cannot read");
        assertThat(client.registerCalls).isEmpty();
    }

    @Test
    void registerWithNoOperandReportsMissingOperandAndDoesNotCallTheServer(@TempDir Path workdir) {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        Harness h = onlineSession(workdir, client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("register")).isTrue();

        assertThat(h.sink().toString().substring(mark)).contains("register:").contains("missing operand");
        assertThat(client.registerCalls).isEmpty();
    }

    @Test
    void registerRenderingAServerRejectionShowsTheCodeAndMessage(@TempDir Path workdir) throws Exception {
        Files.write(workdir.resolve("orders.jar"), new byte[] {1, 2, 3, 4});
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.registerOutcome = new ConnectorRegisterOutcome.Rejected(
                "connector.registration-conflict", "A different artifact already holds that id.");
        Harness h = onlineSession(workdir, client);

        h.repl().dispatch("register orders.jar");

        assertThat(h.sink().toString())
                .contains("connector.registration-conflict").contains("A different artifact already holds that id.");
    }

    @Test
    void registerRenderingAServerRejectionAsJsonEmitsAStructuredErrorDocument(@TempDir Path workdir) throws Exception {
        Files.write(workdir.resolve("orders.jar"), new byte[] {1, 2, 3, 4});
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.registerOutcome = new ConnectorRegisterOutcome.Rejected(
                "connector.registration-conflict", "A different artifact already holds that id.");
        Harness h = onlineSession(workdir, client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("register orders.jar -o json")).isTrue();

        String out = h.sink().toString().substring(mark);
        assertThat(out).contains("\"error\"")
                .contains("\"code\"").contains("connector.registration-conflict")
                .contains("\"message\"").contains("A different artifact already holds that id.");
    }

    @Test
    void registerRenderingAServerRejectionAsYamlEmitsAStructuredErrorDocument(@TempDir Path workdir) throws Exception {
        Files.write(workdir.resolve("orders.jar"), new byte[] {1, 2, 3, 4});
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.registerOutcome = new ConnectorRegisterOutcome.Rejected(
                "connector.spec-invalid", "The artifact spec is not valid JSON.");
        Harness h = onlineSession(workdir, client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("register orders.jar -o yaml")).isTrue();

        String out = h.sink().toString().substring(mark);
        assertThat(out).contains("error:").contains("code:").contains("connector.spec-invalid")
                .contains("message:").contains("The artifact spec is not valid JSON.");
    }

    @Test
    void registerUploadsEveryJarInADirectoryAndReportsEachOutcome(@TempDir Path workdir) throws Exception {
        Path dir = Files.createDirectory(workdir.resolve("connectors"));
        Files.write(dir.resolve("orders.jar"), new byte[] {1});           // registered
        Files.write(dir.resolve("billing.jar"), new byte[] {1, 2});       // already registered (no-op)
        Files.write(dir.resolve("broken.jar"), new byte[] {1, 2, 3});     // rejected
        Files.write(dir.resolve("notes.txt"), new byte[] {1, 2, 3, 4});   // not a jar: skipped
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.registerOutcomeByLength.put(1, new ConnectorRegisterOutcome.Registered(
                new RegisteredConnector("orders", "h1", "1.0", true)));
        client.registerOutcomeByLength.put(2, new ConnectorRegisterOutcome.Registered(
                new RegisteredConnector("billing", "h2", "1.0", false)));
        client.registerOutcomeByLength.put(3, new ConnectorRegisterOutcome.Rejected(
                "connector.spec-invalid", "The artifact spec is not valid JSON."));
        Harness h = onlineSession(workdir, client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("register connectors")).isTrue();

        String out = h.sink().toString().substring(mark);
        assertThat(out).contains("orders").contains("billing")
                .contains("already registered")
                .contains("connector.spec-invalid")
                .contains("3 artifacts");
        // only the three jars were uploaded; the .txt was skipped
        assertThat(client.registerCalls).hasSize(3);
    }

    @Test
    void registerUploadsADirectoryAsJsonWithAnArtifactArrayAndSummary(@TempDir Path workdir) throws Exception {
        Path dir = Files.createDirectory(workdir.resolve("connectors"));
        Files.write(dir.resolve("orders.jar"), new byte[] {1});
        Files.write(dir.resolve("broken.jar"), new byte[] {1, 2, 3});
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.registerOutcomeByLength.put(1, new ConnectorRegisterOutcome.Registered(
                new RegisteredConnector("orders", "h1", "1.0", true)));
        client.registerOutcomeByLength.put(3, new ConnectorRegisterOutcome.Rejected(
                "connector.spec-invalid", "bad spec"));
        Harness h = onlineSession(workdir, client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("register connectors -o json")).isTrue();

        String out = h.sink().toString().substring(mark);
        assertThat(out).contains("\"artifacts\"").contains("\"summary\"")
                .contains("\"artifact\"").contains("orders.jar").contains("broken.jar")
                .contains("\"connectorId\"").contains("orders")
                .contains("\"error\"").contains("connector.spec-invalid")
                .contains("\"total\"");
    }

    @Test
    void registerBatchCountsATimedOutArtifactSeparatelyAndKeepsUploadingTheRest(@TempDir Path workdir)
            throws Exception {
        Path dir = Files.createDirectory(workdir.resolve("connectors"));
        // Filename order drives the batch, so the timeout sits between two healthy uploads.
        Files.write(dir.resolve("a-ok.jar"), new byte[] {1});
        Files.write(dir.resolve("b-slow.jar"), new byte[] {1, 2});
        Files.write(dir.resolve("c-ok.jar"), new byte[] {1, 2, 3});
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.registerOutcomeByLength.put(1, new ConnectorRegisterOutcome.Registered(
                new RegisteredConnector("alpha", "h1", "1.0", true)));
        client.registerOutcomeByLength.put(2, new ConnectorRegisterOutcome.TimedOut());
        client.registerOutcomeByLength.put(3, new ConnectorRegisterOutcome.Registered(
                new RegisteredConnector("charlie", "h3", "1.0", true)));
        Harness h = onlineSession(workdir, client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("register connectors")).isTrue();

        String out = h.sink().toString().substring(mark);
        // The timed-out jar gets its own state and its own tally, kept apart from rejected and unreachable.
        assertThat(out).contains("b-slow.jar").contains("timed out").contains("1 timed out")
                .doesNotContain("unreachable");
        // Unlike an unreachable server, a timeout does not end the batch: the jar after it still uploads.
        assertThat(client.registerCalls).hasSize(3);
    }

    @Test
    void registerBatchAsJsonCarriesTheTimeoutCodeOnTheRowAndCountsItInTheSummary(@TempDir Path workdir)
            throws Exception {
        Path dir = Files.createDirectory(workdir.resolve("connectors"));
        Files.write(dir.resolve("a-ok.jar"), new byte[] {1});
        Files.write(dir.resolve("b-slow.jar"), new byte[] {1, 2});
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.registerOutcomeByLength.put(1, new ConnectorRegisterOutcome.Registered(
                new RegisteredConnector("alpha", "h1", "1.0", true)));
        client.registerOutcomeByLength.put(2, new ConnectorRegisterOutcome.TimedOut());
        Harness h = onlineSession(workdir, client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("register connectors -o json")).isTrue();

        // The machine surface must carry the code, not a prose sentence a caller would have to match on.
        String out = h.sink().toString().substring(mark);
        assertThat(out).contains("b-slow.jar")
                .contains("\"error\"").contains("cli.request-timed-out")
                .contains("\"timedOut\"");
    }

    @Test
    void registerUploadsADirectoryAsYamlWithAnArtifactArrayAndSummary(@TempDir Path workdir) throws Exception {
        Path dir = Files.createDirectory(workdir.resolve("connectors"));
        Files.write(dir.resolve("orders.jar"), new byte[] {1});
        Files.write(dir.resolve("broken.jar"), new byte[] {1, 2, 3});
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.registerOutcomeByLength.put(1, new ConnectorRegisterOutcome.Registered(
                new RegisteredConnector("orders", "h1", "1.0", true)));
        client.registerOutcomeByLength.put(3, new ConnectorRegisterOutcome.Rejected(
                "connector.spec-invalid", "bad spec"));
        Harness h = onlineSession(workdir, client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("register connectors -o yaml")).isTrue();

        String out = h.sink().toString().substring(mark);
        assertThat(out).contains("artifacts:").contains("summary:")
                .contains("artifact:").contains("orders.jar").contains("broken.jar")
                .contains("connectorId:").contains("orders")
                .contains("error:").contains("connector.spec-invalid")
                .contains("total:");
    }

    @Test
    void registerOfADirectoryWithNoJarsReportsThatNoneWereFound(@TempDir Path workdir) throws Exception {
        Files.createDirectory(workdir.resolve("connectors"));
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        Harness h = onlineSession(workdir, client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("register connectors")).isTrue();

        assertThat(h.sink().toString().substring(mark)).containsIgnoringCase("no connector jars");
        assertThat(client.registerCalls).isEmpty();
    }

    @Test
    void registerOfADirectoryStopsAndTakesTheSessionOfflineWhenTheServerIsUnreachable(@TempDir Path workdir) throws Exception {
        Path dir = Files.createDirectory(workdir.resolve("connectors"));
        Files.write(dir.resolve("a-first.jar"), new byte[] {1});
        Files.write(dir.resolve("b-second.jar"), new byte[] {1, 2});
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        Harness h = onlineSession(workdir, client);
        client.setHealthy();   // the server is now down: no member is reachable
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("register connectors")).isTrue();

        // a-first is attempted, fails over to no healthy member, and the batch stops before b-second
        assertThat(client.registerCalls).hasSize(1);
        assertThat(h.repl().session().isConnected()).isFalse();   // failover took the session offline
        String out = h.sink().toString().substring(mark);
        assertThat(out).containsIgnoringCase("connection lost")
                .containsIgnoringCase("not attempted");           // the un-tried jar is signalled, not dropped silently
    }

    @Test
    void registerContinuesPastAnUnreadableJarInADirectoryAndReportsIt(@TempDir Path workdir) throws Exception {
        Path dir = Files.createDirectory(workdir.resolve("connectors"));
        Files.write(dir.resolve("a-open.jar"), new byte[] {1});
        Path locked = dir.resolve("b-locked.jar");
        Files.write(locked, new byte[] {1, 2});
        Files.write(dir.resolve("c-open.jar"), new byte[] {1, 2, 3});
        assumeTrue(Files.getFileAttributeView(locked, PosixFileAttributeView.class) != null,
                "POSIX permissions required");
        Files.setPosixFilePermissions(locked, PosixFilePermissions.fromString("---------"));
        assumeTrue(!Files.isReadable(locked), "permission enforcement required (skips when running as root)");
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.registerOutcome = new ConnectorRegisterOutcome.Registered(
                new RegisteredConnector("c", "h", "1.0", true));
        Harness h = onlineSession(workdir, client);
        int mark = h.sink().toString().length();
        try {
            assertThat(h.repl().dispatch("register connectors")).isTrue();

            // a-open and c-open still upload; the unreadable b-locked is recorded, not fatal
            assertThat(client.registerCalls).hasSize(2);
            assertThat(h.sink().toString().substring(mark))
                    .containsIgnoringCase("cannot read").contains("b-locked.jar").contains("3 artifacts");
        } finally {
            Files.setPosixFilePermissions(locked, PosixFilePermissions.fromString("rwx------"));
        }
    }

    @Test
    void registerADirectoryOnTheMachineSurfaceDoesNotEchoUploadingLines(@TempDir Path workdir) throws Exception {
        Path dir = Files.createDirectory(workdir.resolve("connectors"));
        Files.write(dir.resolve("orders.jar"), new byte[] {1});
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.registerOutcome = new ConnectorRegisterOutcome.Registered(
                new RegisteredConnector("orders", "h", "1.0", true));
        Harness h = onlineSession(workdir, client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("register connectors -o json")).isTrue();

        assertThat(h.sink().toString().substring(mark)).doesNotContain("uploading");
    }

    @Test
    void registerEchoesTheArtifactNameAndSizeBeforeUploading(@TempDir Path workdir) throws Exception {
        Files.write(workdir.resolve("orders.jar"), new byte[2048]);   // 2 KB
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.registerOutcome = new ConnectorRegisterOutcome.Registered(
                new RegisteredConnector("orders", "hash-abc", "1.3.5", true));
        Harness h = onlineSession(workdir, client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("register orders.jar")).isTrue();

        assertThat(h.sink().toString().substring(mark))
                .containsIgnoringCase("uploading").contains("orders.jar").contains("KB");
    }

    @Test
    void registerEchoesEachArtifactBeforeUploadingItInADirectoryBatch(@TempDir Path workdir) throws Exception {
        Path dir = Files.createDirectory(workdir.resolve("connectors"));
        Files.write(dir.resolve("orders.jar"), new byte[1024]);
        Files.write(dir.resolve("billing.jar"), new byte[2048]);
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.registerOutcome = new ConnectorRegisterOutcome.Registered(
                new RegisteredConnector("c", "h", "1.0", true));
        Harness h = onlineSession(workdir, client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("register connectors")).isTrue();

        assertThat(h.sink().toString().substring(mark))
                .contains("uploading orders.jar").contains("uploading billing.jar");
    }

    @Test
    void registerWhileConnectedButNotAuthenticatedReportsAndDoesNotCallTheServer() {
        // The not-authenticated guard fires before the file is even read, so no file need exist.
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        Harness h = harness(Path.of("tap-work"), client, new ScriptedPrompter());
        h.repl().dispatch("connect node1:7900");   // connected, never logged in

        assertThat(h.repl().dispatch("register orders.jar")).isTrue();

        assertThat(h.sink().toString()).contains("cli.not-authenticated").contains("register");
        assertThat(client.registerCalls).isEmpty();
    }

    @Test
    void registerOfflineReportsThatAConnectionIsRequired() {
        Harness h = harness(Path.of("tap-work"));

        assertThat(h.repl().dispatch("register orders.jar")).isTrue();

        assertThat(h.sink().toString()).contains("cli.not-connected").contains("register");
    }

    // --- schema read-back: `schema <id> [table]` reads the stored model without discovering ---

    @Test
    void schemaReadsBackTheStoredModelWithoutDiscovering() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.schemaOutcome = new ConnectionSchemaOutcome.Found(discoveredSchema());
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("schema my-mongo")).isTrue();

        String out = h.sink().toString().substring(mark);
        assertThat(out).contains("my-mongo").contains("mongodb").contains("orders").contains("customers");
        // it read the stored model under the session credential — no discovery, no artifact fetch
        assertThat(client.schemaCalls).containsExactly("jwt-tok@http://node1:7900/my-mongo");
        assertThat(client.discoverSchemaCalls).isEmpty();
        assertThat(client.getCalls).isEmpty();
    }

    @Test
    void schemaWithATableOperandRendersJustThatTable() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.schemaOutcome = new ConnectionSchemaOutcome.Found(discoveredSchema());
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("schema my-mongo orders")).isTrue();

        String out = h.sink().toString().substring(mark);
        // the single-table view names the table, its fields with types, the pk marker and the index
        assertThat(out).contains("orders").contains("id").contains("bigint").contains("pk")
                .contains("pk_orders").contains("unique");
        assertThat(out).doesNotContain("customers");
    }

    @Test
    void schemaNarrowedToATableNamesThatTableInTheView() {
        // `customers` shares no substring with its fields or indexes, so this witnesses the table name
        // itself being rendered — not an accident of an index name embedding it.
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.schemaOutcome = new ConnectionSchemaOutcome.Found(discoveredSchema());
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("schema my-mongo customers")).isTrue();

        String out = h.sink().toString().substring(mark);
        assertThat(out).contains("customers").contains("email").contains("varchar");
        assertThat(out).doesNotContain("orders");
    }

    @Test
    void schemaForATableNotInTheModelReportsItAndTheAvailableTables() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.schemaOutcome = new ConnectionSchemaOutcome.Found(discoveredSchema());
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("schema my-mongo no_such")).isTrue();

        String out = h.sink().toString().substring(mark);
        // an unknown table is a benign line naming the miss, not a crash or a rendered model
        assertThat(out).contains("no_such").containsIgnoringCase("not in the discovered model");
    }

    @Test
    void schemaRendersTheFilteredModelAsJsonWithTheOutputFlag() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.schemaOutcome = new ConnectionSchemaOutcome.Found(discoveredSchema());
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("schema my-mongo orders -o json")).isTrue();

        String out = h.sink().toString().substring(mark);
        // the machine form keeps the envelope shape with tables filtered to the requested one
        assertThat(out).contains("\"connectionId\"").contains("\"orders\"").doesNotContain("\"customers\"");
    }

    @Test
    void schemaForANeverDiscoveredConnectionReportsNotDiscoveredYet() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.schemaOutcome = new ConnectionSchemaOutcome.Absent();
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("schema my-mongo")).isTrue();

        assertThat(h.sink().toString().substring(mark))
                .contains("my-mongo").containsIgnoringCase("not been discovered");
    }

    @Test
    void schemaRenderingAServerRejectionShowsTheCodeAndMessage() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.schemaOutcome = new ConnectionSchemaOutcome.Rejected("control.forbidden", "You lack the grade.");
        Harness h = onlineSession(Path.of("tap-work"), client);

        h.repl().dispatch("schema my-mongo");

        assertThat(h.sink().toString()).contains("control.forbidden").contains("You lack the grade.");
    }

    @Test
    void schemaRendersTheModelAsYamlWithTheOutputFlag() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.schemaOutcome = new ConnectionSchemaOutcome.Found(discoveredSchema());
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("schema my-mongo -o yaml")).isTrue();

        String out = h.sink().toString().substring(mark);
        assertThat(out).contains("connectionId: my-mongo").contains("name: orders").contains("unique: true");
    }

    @Test
    void schemaWithNoIdReportsMissingOperandAndDoesNotCallTheServer() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("schema")).isTrue();

        assertThat(h.sink().toString().substring(mark)).contains("schema:").contains("missing operand");
        assertThat(client.schemaCalls).isEmpty();
    }

    @Test
    void schemaWithAnUnknownOptionReportsItAndDoesNotCallTheServer() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("schema my-mongo -x")).isTrue();

        assertThat(h.sink().toString().substring(mark)).contains("schema:").contains("unknown option");
        assertThat(client.schemaCalls).isEmpty();
    }

    @Test
    void schemaWithAnUnknownOutputFormatReportsItAndDoesNotCallTheServer() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("schema my-mongo -o xml")).isTrue();

        assertThat(h.sink().toString().substring(mark))
                .contains("schema:").contains("unknown output format").contains("xml");
        assertThat(client.schemaCalls).isEmpty();
    }

    @Test
    void schemaWithTooManyOperandsReportsUsageAndDoesNotCallTheServer() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("schema my-mongo orders extra")).isTrue();

        assertThat(h.sink().toString().substring(mark)).contains("schema:").contains("too many operands");
        assertThat(client.schemaCalls).isEmpty();
    }

    @Test
    void schemaOfflineReportsThatAConnectionIsRequired() {
        Harness h = harness(Path.of("tap-work"));

        assertThat(h.repl().dispatch("schema my-mongo")).isTrue();

        assertThat(h.sink().toString()).contains("cli.not-connected").contains("schema");
    }

    // --- server-as-truth: a connected read verb sources the server store, never the local workspace ---

    @Test
    void onlineLsSourcesTheServerAndNeverTheLocalWorkspace(@TempDir Path base) throws Exception {
        copyWorkspace("/ws-valid", base);   // a real local workspace: source/src_kfk + pipeline/kfk2my
        FakeControlPlane client = new FakeControlPlane(URI.create("http://localhost:7900"));
        client.listOutcome = new ListOutcome.Listed(List.of());
        Harness h = harness(base, client, new ScriptedPrompter("pw"));
        // precondition: offline, ls really does read these local artifacts (so the guard below is load-bearing)
        h.repl().dispatch("ls");
        assertThat(h.sink().toString()).contains("src_kfk").contains("kfk2my");
        // once online, the same session's ls sources the (empty) server, not that local workspace
        client.loginOutcome = new LoginOutcome.Success("jwt-tok");
        h.repl().dispatch("connect localhost:7900");
        h.repl().dispatch("login alice");
        int mark = h.sink().toString().length();
        h.repl().dispatch("ls");
        String out = h.sink().toString().substring(mark);
        assertThat(out).contains("no resources");                    // the empty server ...
        assertThat(out).doesNotContain("src_kfk").doesNotContain("kfk2my");   // ... not the local files it just listed
    }

    @Test
    void onlineGetSourcesTheServerCanonicalNotTheLocalWorkspaceFileWithTheSameId(@TempDir Path base) throws Exception {
        copyWorkspace("/ws-valid", base);   // a real local source/src_kfk.tap.yml exists in the workspace
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.getOutcome = new GetOutcome.Found(new RemoteArtifact(
                "src_kfk", "source", "kind: source\nid: src_kfk\nserver_marker: REMOTE\n"));
        Harness h = onlineSession(base, client);
        int mark = h.sink().toString().length();
        h.repl().dispatch("get src_kfk");
        String out = h.sink().toString().substring(mark);
        // the server canonical is returned — carrying a marker the local file does not have — via the server call
        assertThat(out).contains("server_marker: REMOTE");
        assertThat(client.getCalls).containsExactly("jwt-tok@http://node1:7900/src_kfk");
    }

    @Test
    void applyWhileAuthenticatedSendsTheWorkspaceDraftsAndReportsTheOutcomes(@TempDir Path base) throws Exception {
        copyWorkspace("/ws-valid", base);
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.applyOutcome = new ApplyOutcome.Applied(List.of(
                new ApplyOutcome.Item("src_kfk", "source", "CREATED"),
                new ApplyOutcome.Item("kfk2my", "pipeline", "UNCHANGED")));
        Harness h = onlineSession(base, client);
        int mark = h.sink().toString().length();
        assertThat(h.repl().dispatch("apply")).isTrue();
        String out = h.sink().toString().substring(mark);
        assertThat(out).contains("created").contains("src_kfk").contains("unchanged").contains("kfk2my");
        // one apply call carrying the three ws-valid drafts to the landing node with the credential
        assertThat(client.applyCalls).hasSize(1);
        assertThat(client.applyCalls.get(0)).startsWith("jwt-tok@http://node1:7900 x3");
    }

    @Test
    void applyRendersTheServerWarningsOnStderrAndStillSucceeds(@TempDir Path base) throws Exception {
        // Any catalogued code serves as the stand-in: what is under test is that the CLI renders a
        // server-sent code from its own bundled catalog, message and solution alike, and that the note
        // lands on stderr so a piped stdout carries only the per-artifact outcome lines.
        copyWorkspace("/ws-valid", base);
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.applyOutcome = new ApplyOutcome.Applied(
                List.of(new ApplyOutcome.Item("src_kfk", "source", "CREATED")),
                List.of(new ApplyOutcome.Warning("cli.artifact-exists", Map.of("path", "source/src_kfk.tap.yml"))));
        SplitHarness h = onlineSplitStreamSession(base, client);

        assertThat(h.repl().dispatch("apply")).isTrue();

        assertThat(h.out().toString())
                .as("stdout stays the machine-readable outcome lines")
                .contains("created").contains("src_kfk")
                .doesNotContain("warning:");
        assertThat(h.err().toString())
                .contains("warning:")
                .contains("cli.artifact-exists")
                .as("the params are substituted into the catalogued message, not printed raw")
                .contains("An artifact already exists at source/src_kfk.tap.yml.")
                .as("the catalogued next-step hint rides along")
                .contains("Choose a different id or --out");
        assertThat(h.repl().lastExitCode())
                .as("a warning is a note about a batch that applied — it never changes the exit status")
                .isEqualTo(Cli.EXIT_OK);
    }

    @Test
    void applyRendersAWarningWhoseCodeTheCatalogDoesNotKnowAsItsBareCode(@TempDir Path base) throws Exception {
        // A server one version ahead can send a code this CLI's catalog has never heard of. Rendering it
        // as its bare code keeps the finding visible and machine-greppable; dropping it would turn a
        // version skew into silence, which is the exact failure this channel exists to end.
        copyWorkspace("/ws-valid", base);
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.applyOutcome = new ApplyOutcome.Applied(
                List.of(new ApplyOutcome.Item("src_kfk", "source", "CREATED")),
                List.of(new ApplyOutcome.Warning("nest.a-code-from-a-newer-server", Map.of())));
        SplitHarness h = onlineSplitStreamSession(base, client);

        assertThat(h.repl().dispatch("apply")).isTrue();

        assertThat(h.err().toString()).contains("warning:").contains("nest.a-code-from-a-newer-server");
        assertThat(h.out().toString()).contains("created").contains("src_kfk");
    }

    @Test
    void applyWithNoWarningsPrintsNothingOnStderr(@TempDir Path base) throws Exception {
        // The positive control for the two above: the same apply, minus the findings, leaves stderr
        // untouched — so a "warning:" line can only have come from a server that sent one.
        copyWorkspace("/ws-valid", base);
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.applyOutcome = new ApplyOutcome.Applied(
                List.of(new ApplyOutcome.Item("src_kfk", "source", "CREATED")), List.of());
        SplitHarness h = onlineSplitStreamSession(base, client);
        int mark = h.err().toString().length();

        assertThat(h.repl().dispatch("apply")).isTrue();

        assertThat(h.err().toString().substring(mark)).isEmpty();
        assertThat(h.out().toString()).contains("created").contains("src_kfk");
    }

    @Test
    void applyOfOneResourceWithAPreconditionCarriesItOnThatDraft(@TempDir Path base) throws Exception {
        copyWorkspace("/ws-valid", base);
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.applyOutcome = new ApplyOutcome.Applied(
                List.of(new ApplyOutcome.Item("src_kfk", "source", "UPDATED")));
        Harness h = onlineSession(base, client);
        String hash = "e".repeat(64);

        assertThat(h.repl().dispatch("apply source/src_kfk.tap.yml --if-match " + hash)).isTrue();

        assertThat(client.appliedDrafts).hasSize(1);
        assertThat(client.appliedDrafts.get(0))
                .singleElement()
                .extracting(LocalDraft::expectedContentHash)
                .isEqualTo(hash);
    }

    /**
     * A batch precondition would be a lie: one hash cannot describe several resources, and picking one to
     * attach it to would silently leave the rest unguarded. Refusing before the request is the whole point
     * — the alternative is an edit that lands for every resource the caller was not thinking about.
     */
    @Test
    void applyOfAMultiResourceBatchWithAPreconditionIsRefusedBeforeAnythingIsSent(@TempDir Path base)
            throws Exception {
        copyWorkspace("/ws-valid", base);   // three resources
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        Harness h = onlineSession(base, client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("apply --if-match " + "e".repeat(64))).isTrue();

        String out = h.sink().toString().substring(mark);
        assertThat(out).contains("error:").contains("cli.if-match-needs-one-resource");
        assertThat(out).contains("3");
        assertThat(client.applyCalls).isEmpty();
    }

    @Test
    void applyWithoutAPreconditionSendsDraftsThatDeclareNone(@TempDir Path base) throws Exception {
        // The backward-compatibility half, asserted on the wire value rather than on the outcome: a draft
        // that silently acquired a hash would start refusing callers who never asked for the check.
        copyWorkspace("/ws-valid", base);
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.applyOutcome = new ApplyOutcome.Applied(List.of());
        Harness h = onlineSession(base, client);

        assertThat(h.repl().dispatch("apply")).isTrue();

        assertThat(client.appliedDrafts).hasSize(1);
        assertThat(client.appliedDrafts.get(0))
                .allSatisfy(d -> assertThat(d.expectedContentHash()).isNull());
    }

    @Test
    void applySubstitutesReferencesFromTheEnvironmentBeforeSendingTheDrafts(@TempDir Path base) throws Exception {
        Files.createDirectory(base.resolve("source"));
        Files.writeString(base.resolve("source").resolve("tgt.tap.yml"),
                "version: tapstate/v1\nkind: source\nid: tgt\nconnector: mongodb\n"
                        + "config: { uri: \"${MONGO_URI}\" }\n");
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.applyOutcome = new ApplyOutcome.Applied(List.of(new ApplyOutcome.Item("tgt", "source", "CREATED")));
        Harness h = onlineSession(base, client, Map.of("MONGO_URI", "mongodb://127.0.0.1:27017/demo"));

        assertThat(h.repl().dispatch("apply")).isTrue();

        // the value reaches the wire, not the reference: the server is handed a config it can dial
        assertThat(client.appliedDrafts).hasSize(1);
        String sent = client.appliedDrafts.get(0).get(0).content();
        assertThat(sent).contains("mongodb://127.0.0.1:27017/demo").doesNotContain("${");
    }

    @Test
    void applyRefusesAnUndefinedVariableAndSendsNothing(@TempDir Path base) throws Exception {
        Files.createDirectory(base.resolve("source"));
        Files.writeString(base.resolve("source").resolve("tgt.tap.yml"),
                "version: tapstate/v1\nkind: source\nid: tgt\nconnector: mongodb\n"
                        + "config: { uri: \"${MONGO_URI}\" }\n");
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        Harness h = onlineSession(base, client, Map.of());
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("apply")).isTrue();

        String out = h.sink().toString().substring(mark);
        assertThat(out).contains("dsl.undefined-variable").contains("MONGO_URI").contains("tgt.tap.yml");
        // nothing left the client: a reference that cannot be resolved is not the server's problem to
        // discover, and a literal ${...} on the wire would surface as a connector failure far from here
        assertThat(client.applyCalls).isEmpty();
    }

    @Test
    void applyReportsBenignlyWhenTheWorkspaceTreeCannotBeReadInsteadOfCrashing(@TempDir Path base) throws Exception {
        // a subdirectory that cannot be listed makes Files.walk raise UncheckedIOException mid-traversal;
        // apply must render a benign "cannot read" line, not let that escape and crash the REPL session
        Path locked = Files.createDirectory(base.resolve("source"));
        Files.writeString(locked.resolve("s.tap.yml"), "kind: source\nid: x\n");
        assumeTrue(Files.getFileAttributeView(locked, PosixFileAttributeView.class) != null,
                "POSIX permissions required to make a subdirectory unreadable");
        Files.setPosixFilePermissions(locked, PosixFilePermissions.fromString("---------"));
        assumeTrue(!Files.isReadable(locked), "permission enforcement required (skips when running as root)");
        try {
            FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
            Harness h = onlineSession(base, client);
            int mark = h.sink().toString().length();
            assertThat(h.repl().dispatch("apply")).isTrue();   // must not throw out of dispatch
            assertThat(h.sink().toString().substring(mark)).contains("apply:").contains("cannot read");
            assertThat(client.applyCalls).isEmpty();
        } finally {
            Files.setPosixFilePermissions(locked, PosixFilePermissions.fromString("rwx------"));
        }
    }

    @Test
    void applyWithNoDraftsInTheWorkspaceReportsBenignlyAndDoesNotCallTheServer(@TempDir Path base) {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        Harness h = onlineSession(base, client);
        int mark = h.sink().toString().length();
        assertThat(h.repl().dispatch("apply")).isTrue();
        assertThat(h.sink().toString().substring(mark)).contains("apply:").contains("no");
        assertThat(client.applyCalls).isEmpty();
    }

    @Test
    void applyRenderingAServerRejectionShowsTheCodeAndMessage(@TempDir Path base) throws Exception {
        copyWorkspace("/ws-valid", base);
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.applyOutcome = new ApplyOutcome.Rejected("dsl.illegal-value", "Not a known kind.");
        Harness h = onlineSession(base, client);
        int mark = h.sink().toString().length();
        h.repl().dispatch("apply");
        assertThat(h.sink().toString().substring(mark)).contains("dsl.illegal-value").contains("Not a known kind.");
    }

    @Test
    void lsRenderingAServerRejectionShowsTheCodeAndMessageAndDoesNotFailOver() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.listOutcome = new ListOutcome.Rejected("control.forbidden", "You lack the grade.");
        Harness h = onlineSession(Path.of("tap-work"), client);
        int probesBefore = client.probed.size();
        int mark = h.sink().toString().length();
        h.repl().dispatch("ls");
        String out = h.sink().toString().substring(mark);
        assertThat(out).contains("control.forbidden").contains("You lack the grade.");
        // a coded refusal is not a transport failure: it must not trigger failover
        assertThat(out).doesNotContain("reconnected").doesNotContain("connection lost");
        assertThat(client.probed.size()).isEqualTo(probesBefore);
    }

    @Test
    void lsWithAnEmptyServerStorePrintsNoResources() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.listOutcome = new ListOutcome.Listed(List.of());
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();
        h.repl().dispatch("ls");
        assertThat(h.sink().toString().substring(mark)).contains("no resources");
    }

    @Test
    void applyWhileOfflineFallsThroughToTheConnectionRequiredNotice() {
        Harness h = harness();   // offline: apply is a connected verb, so the offline notice fires
        assertThat(h.repl().dispatch("apply")).isTrue();
        assertThat(h.sink().toString()).contains("cli.not-connected");
    }

    @Test
    void getWhileOfflineFallsThroughToTheConnectionRequiredNotice() {
        Harness h = harness();   // offline: get is a connected verb, discoverable rather than unknown
        assertThat(h.repl().dispatch("get x")).isTrue();
        assertThat(h.sink().toString()).contains("cli.not-connected");
    }

    // --- pipeline lifecycle verbs route online to POST /api/pipelines/{id}:{verb} ------------------

    @Test
    void startWhileAuthenticatedRoutesToTheServerAndPrintsTheNewState() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.lifecycleOutcome = new LifecycleOutcome.Accepted("pl1", "RUNNING", "rev-abc");
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();
        assertThat(h.repl().dispatch("start pl1")).isTrue();
        assertThat(h.sink().toString().substring(mark)).contains("pl1").contains("running");
        assertThat(client.lifecycleCalls).containsExactly("jwt-tok@http://node1:7900 start pl1");
    }

    @Test
    void theFourLifecycleVerbsEachRouteToTheirOwnServerVerb() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.lifecycleOutcome = new LifecycleOutcome.Accepted("pl1", "RUNNING", "rev-abc");
        Harness h = onlineSession(Path.of("tap-work"), client);
        h.repl().dispatch("start pl1");
        h.repl().dispatch("pause pl1");
        h.repl().dispatch("resume pl1");
        h.repl().dispatch("stop pl1");
        assertThat(client.lifecycleCalls).containsExactly(
                "jwt-tok@http://node1:7900 start pl1",
                "jwt-tok@http://node1:7900 pause pl1",
                "jwt-tok@http://node1:7900 resume pl1",
                "jwt-tok@http://node1:7900 stop pl1");
    }

    @Test
    void aLifecycleVerbWithoutAPipelineIdIsABenignUsageLineAndCallsNothing() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();
        assertThat(h.repl().dispatch("start")).isTrue();
        assertThat(h.sink().toString().substring(mark)).contains("start").contains("missing operand");
        assertThat(client.lifecycleCalls).isEmpty();
    }

    @Test
    void aLifecycleVerbRejectionShowsTheCodeAndMessageAndDoesNotFailOver() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.lifecycleOutcome = new LifecycleOutcome.Rejected(
                "lifecycle.illegal-transition", "Cannot pause a pipeline that is not running.");
        Harness h = onlineSession(Path.of("tap-work"), client);
        int probesBefore = client.probed.size();
        int mark = h.sink().toString().length();
        h.repl().dispatch("pause pl1");
        String out = h.sink().toString().substring(mark);
        assertThat(out).contains("lifecycle.illegal-transition").contains("Cannot pause");
        // a coded refusal is not a transport failure: it must not trigger failover
        assertThat(out).doesNotContain("reconnected").doesNotContain("connection lost");
        assertThat(client.probed.size()).isEqualTo(probesBefore);
    }

    @Test
    void anUnauthenticatedLifecycleVerbSaysRunLoginAndCallsNothing() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        Harness h = harness(Path.of("tap-work"), client, new ScriptedPrompter("pw"));
        h.repl().dispatch("connect node1:7900");   // connected, not authenticated
        int mark = h.sink().toString().length();
        assertThat(h.repl().dispatch("start pl1")).isTrue();
        assertThat(h.sink().toString().substring(mark)).contains("login");
        assertThat(client.lifecycleCalls).isEmpty();
    }

    @Test
    void startWhileOfflineFallsThroughToTheConnectionRequiredNotice() {
        Harness h = harness();   // offline: start is a connected verb, discoverable rather than unknown
        assertThat(h.repl().dispatch("start pl1")).isTrue();
        assertThat(h.sink().toString()).contains("cli.not-connected");
    }

    @Test
    void aLifecycleVerbFailsOverToAHealthyMemberAndRetriesOnceOnTheNewNode() {
        FakeControlPlane client = new FakeControlPlane(
                URI.create("http://localhost:7900"), URI.create("http://localhost:7901"));
        client.loginOutcome = new LoginOutcome.Success("jwt-tok");
        client.lifecycleOutcome = new LifecycleOutcome.Accepted("pl1", "RUNNING", "rev-abc");
        Harness h = harness(Path.of("tap-work"), client, new ScriptedPrompter("pw"));
        h.repl().dispatch("connect localhost:7900,localhost:7901");
        h.repl().dispatch("login alice");
        client.setHealthy(URI.create("http://localhost:7901"));   // the first seed goes down before the request
        int mark = h.sink().toString().length();
        h.repl().dispatch("start pl1");
        String out = h.sink().toString().substring(mark);
        assertThat(out).contains("reconnected to localhost:7901");
        assertThat(out).contains("pl1").contains("running");
    }

    // --- observation read verbs route online to GET /api/pipelines/{id}/{face} --------------------

    @Test
    void statusWhileAuthenticatedRoutesToTheServerAndPrintsTheState() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.statusOutcome = new StatusOutcome.Found("pl1", "RUNNING");
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();
        assertThat(h.repl().dispatch("status pl1")).isTrue();
        assertThat(h.sink().toString().substring(mark)).contains("pl1").contains("running");
        assertThat(client.statusCalls).containsExactly("jwt-tok@http://node1:7900/pl1");
    }

    @Test
    void statusOfAFailedPipelinePrintsWhyItDied() {
        // A bare "failed" sends the user hunting through logs. The reason arrives coded with its arguments,
        // so the CLI renders it from the bundled catalog the same way it renders every other coded message.
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.statusOutcome = new StatusOutcome.Found("pl1", "FAILED", "engine.job-failed",
                "Pipeline pl1 stopped because its job failed: the sink rejected the batch.");
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("status pl1")).isTrue();

        String out = h.sink().toString().substring(mark);
        assertThat(out).contains("failed");
        // Assert the code, not the prose: the code is the stable identity, and only this path can emit it.
        assertThat(out).contains("engine.job-failed");
        assertThat(out).contains("the sink rejected the batch");
    }

    @Test
    void statusOfAFailedPipelinePrintsTheReasonToStdoutNotStderr() {
        // The read succeeded -- it is not a refusal -- so the reason must land on the same stream as the
        // state line above it. A caller separating the streams (status pl1 > out.txt 2> err.txt) must see
        // both in out.txt; the merged single-sink harness the other status tests use cannot tell this apart.
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.statusOutcome = new StatusOutcome.Found("pl1", "FAILED", "engine.job-failed",
                "Pipeline pl1 stopped because its job failed: the sink rejected the batch.");
        SplitHarness h = onlineSplitStreamSession(Path.of("tap-work"), client);
        int outMark = h.out().toString().length();
        int errMark = h.err().toString().length();

        assertThat(h.repl().dispatch("status pl1")).isTrue();

        String stdout = h.out().toString().substring(outMark);
        String stderr = h.err().toString().substring(errMark);
        assertThat(stdout).contains("failed").contains("engine.job-failed").contains("the sink rejected the batch");
        assertThat(stderr).isEmpty();
    }

    @Test
    void statusOfAHealthyPipelinePrintsNoFailureLine() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.statusOutcome = new StatusOutcome.Found("pl1", "RUNNING");
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("status pl1")).isTrue();

        assertThat(h.sink().toString().substring(mark)).doesNotContain("engine.");
    }

    @Test
    void metricsWhileAuthenticatedPrintsEachStat() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.metricsOutcome = new MetricsOutcome.Found("pl1", Map.of("recordCount", 42L, "errorCount", 0L));
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();
        assertThat(h.repl().dispatch("metrics pl1")).isTrue();
        String out = h.sink().toString().substring(mark);
        assertThat(out).contains("recordCount").contains("42").contains("errorCount");
        assertThat(client.metricsCalls).containsExactly("jwt-tok@http://node1:7900/pl1");
    }

    @Test
    void metricsSaysItsNamesAreUnstableSoNobodyBuildsAlertsOnThemUnwarned() {
        // The metric names are not a compatibility promise in this preview. That has to be visible where
        // the names are, not only in a document nobody reads before wiring up a dashboard.
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.metricsOutcome = new MetricsOutcome.Found("pl1", Map.of("recordCount", 42L));
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("metrics pl1")).isTrue();

        assertThat(h.sink().toString().substring(mark)).contains("unstable");
    }

    @Test
    void theUnstableNoticeIsAbsentWhenThereAreNoMetricsToMislabel() {
        // Nothing was named, so there is no naming promise to disclaim; the benign "no metrics" line
        // should not be dressed up as a warning.
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.metricsOutcome = new MetricsOutcome.Found("pl1", Map.of());
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("metrics pl1")).isTrue();

        String out = h.sink().toString().substring(mark);
        assertThat(out).contains("no metrics");
        assertThat(out).doesNotContain("unstable");
    }

    @Test
    void metricsPrintsPerTableOffsetLinesAlongsideTheStats() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.metricsOutcome = new MetricsOutcome.Found(
                "pl1", Map.of("recordCount", 6L), Map.of("orders", "w7"));
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();
        h.repl().dispatch("metrics pl1");
        String out = h.sink().toString().substring(mark);
        assertThat(out).contains("recordCount").contains("perTableOffset.orders").contains("w7");
    }

    @Test
    void metricsWithOnlyPerTableOffsetPrintsItRatherThanNoMetrics() {
        // Positions-only: numeric stats empty but a per-table position is wired, so the offset prints and
        // "no metrics" must not — it fires only when both sources are empty.
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.metricsOutcome = new MetricsOutcome.Found("pl1", Map.of(), Map.of("orders", "w7"));
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();
        h.repl().dispatch("metrics pl1");
        String out = h.sink().toString().substring(mark);
        assertThat(out).contains("perTableOffset.orders").contains("w7").doesNotContain("no metrics");
    }

    @Test
    void metricsWithNoMetricsPrintsABenignNoMetricsLine() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.metricsOutcome = new MetricsOutcome.Found("pl1", Map.of());
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();
        h.repl().dispatch("metrics pl1");
        assertThat(h.sink().toString().substring(mark)).contains("no metrics");
    }

    @Test
    void snapshotWhileAuthenticatedPrintsPerTableProgressAndAnUnavailableTotal() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.snapshotOutcome = new SnapshotOutcome.Found("pl1", Map.of(
                "orders", new RemoteTableSnapshot(10, 100L, 10),
                "events", new RemoteTableSnapshot(5, null, null)));
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();
        h.repl().dispatch("snapshot pl1");
        String out = h.sink().toString().substring(mark);
        // a table with a total shows its percentage; a table with no total is honest partial data, not 0/100
        assertThat(out).contains("orders").contains("10/100").contains("10%");
        assertThat(out).contains("events").contains("5/?");
    }

    @Test
    void snapshotWithNoTablesPrintsABenignNoSnapshotLine() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.snapshotOutcome = new SnapshotOutcome.Found("pl1", Map.of());
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();
        h.repl().dispatch("snapshot pl1");
        assertThat(h.sink().toString().substring(mark)).contains("no snapshot");
    }

    @Test
    void logsWhileAuthenticatedPrintsTheTailOldestToNewest() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.logsOutcome = new LogsOutcome.Found("pl1", List.of(
                new RemoteLogLine(1_700_000_000_000L, "INFO", "submitted job"),
                new RemoteLogLine(1_700_000_000_100L, "WARN", "slow tick")));
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();
        h.repl().dispatch("logs pl1");
        String out = h.sink().toString().substring(mark);
        assertThat(out).contains("submitted job").contains("INFO").contains("slow tick").contains("WARN");
        // oldest to newest
        assertThat(out.indexOf("submitted job")).isLessThan(out.indexOf("slow tick"));
    }

    @Test
    void logsWithNoLinesPrintsABenignNoLogsLine() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.logsOutcome = new LogsOutcome.Found("pl1", List.of());
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();
        h.repl().dispatch("logs pl1");
        assertThat(h.sink().toString().substring(mark)).contains("no logs");
    }

    // --- status --watch / logs --follow stream over the websocket channel ------------------------

    @Test
    void statusWatchStreamsEachStateToTheOutputInOrder() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.watchStates = List.of("RUNNING", "PAUSED");
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();
        assertThat(h.repl().dispatch("status pl1 --watch")).isTrue();
        String out = h.sink().toString().substring(mark);
        assertThat(out).contains("running").contains("paused");
        assertThat(out.indexOf("running")).isLessThan(out.indexOf("paused"));
        assertThat(client.watchCalls).containsExactly("jwt-tok@http://node1:7900/pl1");
    }

    @Test
    void statusWatchPrintsWhyAStreamedFailedStateDied() {
        // Mirrors statusOfAFailedPipelinePrintsWhyItDied for the watch path: the frame reporting a death
        // is the only frame that will ever carry the reason (state does not change again on its own), so
        // the watcher must render it right there rather than needing a separate poll.
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.watchStates = List.of("FAILED");
        client.watchFailureCode = "engine.job-failed";
        client.watchFailureMessage = "Pipeline pl1 stopped because its job failed: the sink rejected the batch.";
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("status pl1 --watch")).isTrue();

        String out = h.sink().toString().substring(mark);
        assertThat(out).contains("failed");
        assertThat(out).contains("engine.job-failed");
        assertThat(out).contains("the sink rejected the batch");
    }

    @Test
    void statusWatchPrintsTheFailureReasonToStdoutNotStderr() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.watchStates = List.of("FAILED");
        client.watchFailureCode = "engine.job-failed";
        client.watchFailureMessage = "Pipeline pl1 stopped because its job failed: the sink rejected the batch.";
        SplitHarness h = onlineSplitStreamSession(Path.of("tap-work"), client);
        int outMark = h.out().toString().length();
        int errMark = h.err().toString().length();

        assertThat(h.repl().dispatch("status pl1 --watch")).isTrue();

        String stdout = h.out().toString().substring(outMark);
        String stderr = h.err().toString().substring(errMark);
        assertThat(stdout).contains("failed").contains("engine.job-failed").contains("the sink rejected the batch");
        assertThat(stderr).isEmpty();
    }

    @Test
    void statusWatchOfAHealthyStreamedStatePrintsNoFailureLine() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.watchStates = List.of("RUNNING");
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        assertThat(h.repl().dispatch("status pl1 --watch")).isTrue();

        assertThat(h.sink().toString().substring(mark)).doesNotContain("engine.");
    }

    @Test
    void statusWatchOfAnUnknownPipelineRendersTheCodedRefusalInsteadOfWatchingForever() {
        // The one-shot `status ghost` is refused at once; its --watch twin must not silently outlive it.
        // The server ends the stream deliberately with the coded reason, the client hands that code back,
        // and the watch renders it as the refusal it is -- on stderr, like every other refused command.
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.streamRefusalCode = "lifecycle.unknown-pipeline";
        SplitHarness h = onlineSplitStreamSession(Path.of("tap-work"), client);
        int errMark = h.err().toString().length();

        assertThat(h.repl().dispatch("status ghost --watch")).isTrue();

        String stderr = h.err().toString().substring(errMark);
        assertThat(stderr).contains("error:").contains("lifecycle.unknown-pipeline");
        // The catalog's message renders locally with the id the watch was for, not a bare code.
        assertThat(stderr).contains("ghost");
    }

    @Test
    void logsFollowOfAnUnknownPipelineRendersTheCodedRefusalInsteadOfFollowingForever() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.streamRefusalCode = "lifecycle.unknown-pipeline";
        SplitHarness h = onlineSplitStreamSession(Path.of("tap-work"), client);
        int errMark = h.err().toString().length();

        assertThat(h.repl().dispatch("logs ghost --follow")).isTrue();

        String stderr = h.err().toString().substring(errMark);
        assertThat(stderr).contains("error:").contains("lifecycle.unknown-pipeline");
    }

    @Test
    void logsFollowStreamsEachAppendedLineBatchToTheOutputInOrder() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.followBatches = List.of(
                List.of(new RemoteLogLine(1_700_000_000_000L, "INFO", "submitted job")),
                List.of(new RemoteLogLine(1_700_000_000_100L, "WARN", "slow tick")));
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();
        assertThat(h.repl().dispatch("logs pl1 --follow")).isTrue();
        String out = h.sink().toString().substring(mark);
        assertThat(out).contains("submitted job").contains("slow tick");
        assertThat(out.indexOf("submitted job")).isLessThan(out.indexOf("slow tick"));
        assertThat(client.followCalls).containsExactly("jwt-tok@http://node1:7900/pl1");
    }

    @Test
    void statusWatchWithoutAPipelineIdIsABenignUsageLineAndStreamsNothing() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.watchStates = List.of("RUNNING");
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();
        assertThat(h.repl().dispatch("status --watch")).isTrue();
        assertThat(h.sink().toString().substring(mark)).contains("missing operand");
        assertThat(client.watchCalls).isEmpty();
    }

    @Test
    void aReadVerbRejectionShowsTheCodeAndMessageAndDoesNotFailOver() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.statusOutcome = new StatusOutcome.Rejected(
                "monitor.no-observation", "No observation is available for pipeline pl1.");
        Harness h = onlineSession(Path.of("tap-work"), client);
        int probesBefore = client.probed.size();
        int mark = h.sink().toString().length();
        h.repl().dispatch("status pl1");
        String out = h.sink().toString().substring(mark);
        assertThat(out).contains("monitor.no-observation").contains("No observation is available");
        // a coded refusal is not a transport failure: it must not trigger failover
        assertThat(out).doesNotContain("reconnected").doesNotContain("connection lost");
        assertThat(client.probed.size()).isEqualTo(probesBefore);
    }

    @Test
    void aReadVerbWithoutAPipelineIdIsABenignUsageLineAndCallsNothing() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();
        assertThat(h.repl().dispatch("status")).isTrue();
        assertThat(h.sink().toString().substring(mark)).contains("status").contains("missing operand");
        assertThat(client.statusCalls).isEmpty();
    }

    @Test
    void anUnauthenticatedReadVerbSaysRunLoginAndCallsNothing() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        Harness h = harness(Path.of("tap-work"), client, new ScriptedPrompter("pw"));
        h.repl().dispatch("connect node1:7900");   // connected, not authenticated
        int mark = h.sink().toString().length();
        assertThat(h.repl().dispatch("status pl1")).isTrue();
        assertThat(h.sink().toString().substring(mark)).contains("login");
        assertThat(client.statusCalls).isEmpty();
    }

    @Test
    void readVerbsWhileOfflineFallThroughToTheConnectionRequiredNotice() {
        // status is a connected verb offline; metrics, snapshot and logs are too, so all four are
        // discoverable rather than reported as unknown.
        for (String verb : List.of("status pl1", "metrics pl1", "snapshot pl1", "logs pl1")) {
            Harness h = harness();
            assertThat(h.repl().dispatch(verb)).isTrue();
            assertThat(h.sink().toString()).contains("cli.not-connected");
        }
    }

    @Test
    void aReadVerbFailsOverToAHealthyMemberAndRetriesOnceOnTheNewNode() {
        FakeControlPlane client = new FakeControlPlane(
                URI.create("http://localhost:7900"), URI.create("http://localhost:7901"));
        client.loginOutcome = new LoginOutcome.Success("jwt-tok");
        client.statusOutcome = new StatusOutcome.Found("pl1", "RUNNING");
        Harness h = harness(Path.of("tap-work"), client, new ScriptedPrompter("pw"));
        h.repl().dispatch("connect localhost:7900,localhost:7901");
        h.repl().dispatch("login alice");
        client.setHealthy(URI.create("http://localhost:7901"));   // the first seed goes down before the request
        int mark = h.sink().toString().length();
        h.repl().dispatch("status pl1");
        String out = h.sink().toString().substring(mark);
        assertThat(out).contains("reconnected to localhost:7901");
        assertThat(out).contains("pl1").contains("running");
    }

    // --- online verbs fail over on a request the landing node cannot answer ------------------------

    @Test
    void anOnlineVerbRejectsDashOptionsRatherThanMisreadingThemAsOperands() {
        // `-o json` must not be silently read as a kind filter (which would list nothing); connected verbs
        // take positional operands only until structured output lands for them
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.listOutcome = new ListOutcome.Listed(List.of());
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();
        assertThat(h.repl().dispatch("ls -o json")).isTrue();
        assertThat(h.sink().toString().substring(mark)).contains("options are not supported");
        assertThat(client.listCalls).isEmpty();
    }

    @Test
    void anOnlineVerbFailsOverToAHealthyMemberAndRetriesOnceOnTheNewNode() {
        FakeControlPlane client = new FakeControlPlane(
                URI.create("http://localhost:7900"), URI.create("http://localhost:7901"));
        client.loginOutcome = new LoginOutcome.Success("jwt-tok");
        client.getOutcome = new GetOutcome.Found(new RemoteArtifact("src_kfk", "source", "kind: source\n"));
        Harness h = harness(Path.of("tap-work"), client, new ScriptedPrompter("pw"));
        h.repl().dispatch("connect localhost:7900,localhost:7901");
        h.repl().dispatch("login alice");
        client.setHealthy(URI.create("http://localhost:7901"));   // the first seed goes down before the request

        int mark = h.sink().toString().length();
        assertThat(h.repl().dispatch("get src_kfk")).isTrue();
        String out = h.sink().toString().substring(mark);
        assertThat(out).contains("reconnected to localhost:7901").contains("kind: source");
        assertThat(h.repl().session().landingNode()).isEqualTo(URI.create("http://localhost:7901"));
        // the verb was attempted on the first seed (unreachable) then retried after failover, credential intact
        assertThat(client.getCalls).containsExactly(
                "jwt-tok@http://localhost:7900/src_kfk", "jwt-tok@http://localhost:7901/src_kfk");
    }

    @Test
    void anOnlineVerbWithNoReachableMemberLosesTheConnectionAndReportsItOnce() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://localhost:7900"));
        client.loginOutcome = new LoginOutcome.Success("jwt-tok");
        client.getOutcome = new GetOutcome.Found(new RemoteArtifact("x", "source", "kind: source\n"));
        Harness h = harness(Path.of("tap-work"), client, new ScriptedPrompter("pw"));
        h.repl().dispatch("connect localhost:7900");
        h.repl().dispatch("login alice");
        client.setHealthy();   // every member is down

        int mark = h.sink().toString().length();
        assertThat(h.repl().dispatch("get x")).isTrue();
        String out = h.sink().toString().substring(mark);
        assertThat(out).contains("connection lost");        // failover reported the loss
        assertThat(out).doesNotContain("request failed");   // and it is not double-reported
        assertThat(h.repl().session().isConnected()).isFalse();
    }

    // --- exit codes: a dispatched line yields the code a one-shot invocation would exit with -------

    @Test
    void aSuccessfulOnlineVerbYieldsSuccess() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.getOutcome = new GetOutcome.Found(
                new RemoteArtifact("src_kfk", "source", "kind: source\nid: src_kfk\n"));
        Harness h = onlineSession(Path.of("tap-work"), client);
        h.repl().dispatch("get src_kfk");
        assertThat(h.repl().lastExitCode()).isZero();
    }

    @Test
    void aRefusedOnlineVerbYieldsTheDiagnosticCode() {
        // a coded refusal from the server is the same class of outcome as a coded local diagnostic, so it
        // exits the same way -- a script that reads only the exit status must not read a refusal as success
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.lifecycleOutcome = new LifecycleOutcome.Rejected(
                "pipeline.illegal-transition", "The pipeline cannot start from this state.");
        Harness h = onlineSession(Path.of("tap-work"), client);
        h.repl().dispatch("start sync_orders");
        assertThat(h.repl().lastExitCode()).isEqualTo(1);
    }

    @Test
    void anOnlineVerbMissingItsOperandYieldsTheUsageCode() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        Harness h = onlineSession(Path.of("tap-work"), client);
        h.repl().dispatch("start");
        assertThat(h.repl().lastExitCode()).isEqualTo(2);
    }

    @Test
    void anUnauthenticatedOnlineVerbYieldsTheUnavailableCode() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        Harness h = harness(Path.of("tap-work"), client, new ScriptedPrompter());
        h.repl().dispatch("connect node1:7900");   // connected, never logged in
        h.repl().dispatch("get x");
        assertThat(h.repl().lastExitCode()).isEqualTo(Cli.EXIT_VERB_UNAVAILABLE);
    }

    @Test
    void anOfflineVerbKeepsTheCodeItsCommandReturned() {
        // the offline path already had an exit code -- picocli returns one from execute -- and the REPL
        // discarded it. It has to survive too, or a one-shot `validate` would report success on a
        // workspace it had just called invalid
        Harness h = harness();
        h.repl().dispatch("validate does-not-exist");
        assertThat(h.repl().lastExitCode()).isNotZero();
    }

    @Test
    void aBuiltinThatSucceedsYieldsSuccess() {
        Harness h = harness();
        h.repl().dispatch("pwd");
        assertThat(h.repl().lastExitCode()).isZero();
    }

    @Test
    void aBuiltinGivenNoOperandYieldsTheUsageCode() {
        Harness h = harness();
        h.repl().dispatch("cd");
        assertThat(h.repl().lastExitCode()).isEqualTo(2);
    }

    // --- one-line launch: -c / -u establish the session before anything runs ------------------------

    @Test
    void aOneLineLaunchConnectsSignsInAndRunsTheCommand() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://localhost:7900"));
        client.loginOutcome = new LoginOutcome.Success("jwt-tok");
        client.listOutcome = new ListOutcome.Listed(List.of(
                new RemoteArtifact("src_kfk", "source", "")));
        LaunchOptions launch = LaunchOptions.parse("-c", "localhost:7900", "-u", "admin", "ls")
                .withEnv(name -> "TAPSTATE_PASSWORD".equals(name) ? "pw" : null);
        int code = Cli.runSession(launch, client, () -> new ScriptedPrompter());
        assertThat(code).isZero();
        // the whole chain ran off one line: probe, credential exchange, then the verb against the server
        assertThat(client.discovered).containsExactly(URI.create("http://localhost:7900"));
        assertThat(client.loginCalls).containsExactly("admin:pw@http://localhost:7900");
        assertThat(client.listCalls).hasSize(1);
    }

    @Test
    void aRemotePlaintextLaunchStopsBeforeReadingOrSendingThePassword() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        ScriptedPrompter prompter = new ScriptedPrompter("should-not-be-read");
        LaunchOptions launch = LaunchOptions.parse("-c", "node1:7900", "-u", "admin", "ls")
                .withEnv(name -> null);

        int code = Cli.runSession(launch, client, () -> prompter);

        assertThat(code).isNotZero();
        assertThat(prompter.secretQuestions).isEmpty();
        assertThat(client.discovered).isEmpty();
        assertThat(client.loginCalls).isEmpty();
        assertThat(client.listCalls).isEmpty();
    }

    @Test
    void aRemotePlaintextReplLoginStopsBeforePromptingOrSendingThePassword() {
        URI seed = URI.create("http://node1:7900");
        FakeControlPlane client = new FakeControlPlane(seed);
        ScriptedPrompter prompter = new ScriptedPrompter("should-not-be-read");
        Harness harness = harness(Path.of("tap-work"), client, prompter);

        harness.repl().dispatch("connect node1:7900");
        harness.repl().dispatch("login admin");

        assertThat(prompter.secretQuestions).isEmpty();
        assertThat(client.discovered).isEmpty();
        assertThat(client.loginCalls).isEmpty();
        assertThat(harness.sink().toString()).contains("cli.remote-plaintext");
    }

    @Test
    void onlineDispatcherLazilyConnectsTheExactWorkspaceContext(@TempDir Path home) throws IOException {
        Path workspace = Files.createDirectory(home.resolve("orders"));
        URI seed = URI.create("http://dev:7900");
        ContextDefinition definition = new ContextDefinition(
                java.util.UUID.fromString("018f0d7a-7b2e-7e30-a8dd-6f78fc0d8ff2"), List.of(seed),
                new ContextTls(true),
                java.util.UUID.fromString("5c199643-04da-4f72-9831-3a77e3590eed"));
        ContextConfig config = new ContextConfig(1, null, Map.of("dev", definition),
                Map.of(workspace.toRealPath().toString(), "dev"));
        ContextResolver resolver = new ContextResolver(() -> config, name -> null);
        FakeControlPlane client = new FakeControlPlane(seed);
        CommandLine commandLine = Cli.newCommandLine();
        StringWriter sink = new StringWriter();
        PrintWriter writer = new PrintWriter(sink);
        commandLine.setOut(writer);
        commandLine.setErr(writer);
        Repl repl = new Repl(commandLine, workspace, client, new ScriptedPrompter(), name -> null,
                resolver, null);

        repl.dispatch(List.of("ls"));

        assertThat(client.probed).containsExactly(seed);
        assertThat(repl.session().isConnected()).isTrue();
        assertThat(sink.toString()).contains("cli.not-authenticated");
    }

    @Test
    void authCommandsPersistResumeReportAndRemotelyRevokeTheNamedContext(@TempDir Path home)
            throws IOException {
        Path workspace = Files.createDirectory(home.resolve("orders"));
        URI seed = URI.create("http://127.0.0.1:7900");
        UUID contextId = UUID.fromString("018f0d7a-7b2e-7e30-a8dd-6f78fc0d8ff2");
        UUID authRef = UUID.fromString("5c199643-04da-4f72-9831-3a77e3590eed");
        ContextDefinition definition = new ContextDefinition(
                contextId, List.of(seed), new ContextTls(true), authRef);
        ContextConfig config = new ContextConfig(1, null, Map.of("dev", definition),
                Map.of(workspace.toRealPath().toString(), "dev"));
        ContextResolver resolver = new ContextResolver(() -> config, name -> null);
        FakeControlPlane client = new FakeControlPlane(seed);
        Instant now = Instant.parse("2026-08-17T10:00:00Z");
        client.loginOutcome = new LoginOutcome.Success(
                "jwt-login", now.plusSeconds(900), "urn:tapstate:cluster:test-cluster", "alice",
                List.of("read", "write"), "tss_s01.session-secret", now.plusSeconds(2_592_000),
                now.plusSeconds(7_776_000));
        client.exchangeOutcome = new SessionExchangeOutcome.Success(
                "jwt-resumed", now.plusSeconds(900), "urn:tapstate:cluster:test-cluster", "alice",
                List.of("read", "write"));
        client.logoutOutcome = new SessionLogoutOutcome.Success();
        client.listOutcome = new ListOutcome.Listed(List.of());
        AuthFileStore store = AuthFileStore.underHome(home);
        Clock clock = Clock.fixed(now, ZoneOffset.UTC);

        CommandLine loginLine = Cli.newCommandLine();
        StringWriter loginOut = new StringWriter();
        StringWriter loginErr = new StringWriter();
        loginLine.setOut(new PrintWriter(loginOut));
        loginLine.setErr(new PrintWriter(loginErr));
        Repl login = new Repl(loginLine, workspace, client, new ScriptedPrompter("pw"),
                name -> null, resolver, null, new AuthService(client, store, clock));
        login.terminalCheck(() -> true);
        login.dispatch(List.of("auth", "login", "alice"), true);

        assertThat(login.lastExitCode()).isZero();
        assertThat(loginOut.toString()).isEmpty();
        assertThat(loginErr.toString()).contains("signed in as alice").doesNotContain("pw");
        assertThat(login.session().credential()).isEqualTo("jwt-login");
        assertThat(client.loginCalls).containsExactly("alice:pw@http://127.0.0.1:7900 persistent=true");
        assertThat(store.load(authRef, contextId)).get()
                .extracting(AuthSessionRecord::sessionToken).isEqualTo("tss_s01.session-secret");
        login.dispatch(List.of("disconnect"), true);
        assertThat(login.session().isConnected()).isFalse();
        assertThat(store.load(authRef, contextId)).isPresent();

        CommandLine resumedLine = Cli.newCommandLine();
        StringWriter resumedOutput = new StringWriter();
        resumedLine.setOut(new PrintWriter(resumedOutput));
        resumedLine.setErr(new PrintWriter(resumedOutput));
        Repl resumed = new Repl(resumedLine, workspace, client, new ScriptedPrompter(), name -> null,
                resolver, null, new AuthService(client, store, clock));
        resumed.dispatch(List.of("ls"), true);
        resumed.dispatch(List.of("auth", "status"), true);
        resumed.dispatch(List.of("auth", "logout"), true);

        assertThat(client.sessionCalls).contains(
                "exchange tss_s01.session-secret@http://127.0.0.1:7900",
                "logout tss_s01.session-secret@http://127.0.0.1:7900");
        assertThat(resumedOutput.toString()).contains("signed in as alice").contains("session revoked");
        assertThat(store.load(authRef, contextId)).isEmpty();
    }

    @Test
    void temporaryConnectAndLegacyLoginNeverCreateAnAuthCache(@TempDir Path home) {
        URI seed = URI.create("http://127.0.0.1:7900");
        FakeControlPlane client = new FakeControlPlane(seed);
        client.loginOutcome = new LoginOutcome.Success("jwt-temporary");
        CommandLine commandLine = Cli.newCommandLine();
        StringWriter output = new StringWriter();
        commandLine.setOut(new PrintWriter(output));
        commandLine.setErr(new PrintWriter(output));
        Repl repl = new Repl(commandLine, home, client, new ScriptedPrompter("pw"), name -> null,
                new ContextResolver(ContextConfig::empty, name -> null), null,
                new AuthService(client, AuthFileStore.underHome(home), Clock.systemUTC()));

        repl.dispatch(List.of("connect", seed.toString()));
        repl.dispatch(List.of("login", "alice"));

        assertThat(repl.lastExitCode()).isZero();
        assertThat(repl.session().credential()).isEqualTo("jwt-temporary");
        assertThat(client.loginCalls).containsExactly("alice:pw@http://127.0.0.1:7900");
        assertThat(home.resolve(".tapstate/auth")).doesNotExist();
    }

    @Test
    void authHelpDoesNotResolveAContextOrTouchTheNetwork(@TempDir Path workspace) {
        AtomicInteger configReads = new AtomicInteger();
        ContextResolver resolver = new ContextResolver(() -> {
            configReads.incrementAndGet();
            return ContextConfig.empty();
        }, name -> null);
        FakeControlPlane client = new FakeControlPlane();
        CommandLine commandLine = Cli.newCommandLine();
        StringWriter output = new StringWriter();
        commandLine.setOut(new PrintWriter(output));
        commandLine.setErr(new PrintWriter(output));
        Repl repl = new Repl(commandLine, workspace, client, new ScriptedPrompter(), name -> null,
                resolver, null, null);

        repl.dispatch(List.of("auth", "--help"), true);

        assertThat(repl.lastExitCode()).isZero();
        assertThat(output.toString()).contains("tapstate auth <login|status|logout>")
                .contains("--local-only");
        assertThat(configReads).hasValue(0);
        assertThat(client.probed).isEmpty();
    }

    @Test
    void contextCommandCreatesAndBindsThroughTheSharedManager(@TempDir Path home) throws IOException {
        Path workspace = Files.createDirectory(home.resolve("orders"));
        ContextConfigStore store = ContextConfigStore.underHome(home);
        ScriptedPrompter prompter = new ScriptedPrompter(
                "Create a context", "dev", "http://127.0.0.1:7900", "", "y");
        CommandLine commandLine = Cli.newCommandLine();
        StringWriter output = new StringWriter();
        commandLine.setOut(new PrintWriter(output));
        commandLine.setErr(new PrintWriter(output));
        Repl repl = new Repl(commandLine, workspace, new FakeControlPlane(), prompter, name -> null,
                new ContextResolver(store, name -> null), null, null, new ContextManager(store));

        repl.dispatch(List.of("context"), true);

        ContextConfig saved = store.load();
        assertThat(repl.lastExitCode()).isZero();
        assertThat(saved.contexts()).containsOnlyKeys("dev");
        assertThat(saved.contexts().get("dev").seeds()).containsExactly(URI.create("http://127.0.0.1:7900"));
        assertThat(saved.workspaceBindings()).containsEntry(workspace.toRealPath().toString(), "dev");
        assertThat(output.toString()).contains("created context dev").contains("bound dev");
    }

    @Test
    void ctxBuiltinCanChooseEditUnbindAndDeleteThroughTheSharedManager(@TempDir Path home)
            throws IOException {
        Path workspace = Files.createDirectory(home.resolve("orders"));
        ContextConfigStore store = ContextConfigStore.underHome(home);
        ContextManager manager = new ContextManager(store);
        manager.create("dev", List.of(URI.create("http://127.0.0.1:7900")), true);
        manager.create("prod", List.of(URI.create("https://prod.example.com")), true);
        manager.bind(workspace, "dev");
        ScriptedPrompter prompter = new ScriptedPrompter(
                "Choose a context", "prod",
                "Edit a context", "prod", "https://prod2.example.com", "n",
                "Unbind this workspace",
                "Delete a context", "prod", "yes", "yes");
        CommandLine commandLine = Cli.newCommandLine();
        StringWriter output = new StringWriter();
        commandLine.setOut(new PrintWriter(output));
        commandLine.setErr(new PrintWriter(output));
        Repl repl = new Repl(commandLine, workspace, new FakeControlPlane(), prompter, name -> null,
                new ContextResolver(store, name -> null), null, null, manager);

        repl.dispatch(":ctx");
        repl.dispatch(":ctx");
        repl.dispatch(":ctx");
        repl.dispatch(":ctx");

        ContextConfig saved = store.load();
        assertThat(repl.lastExitCode()).isZero();
        assertThat(saved.lastContext()).isNull();
        assertThat(saved.contexts()).containsOnlyKeys("dev");
        assertThat(saved.workspaceBindings()).isEmpty();
        assertThat(output.toString()).contains("chose context prod")
                .contains("updated context prod")
                .contains("unbound dev")
                .contains("authRef")
                .contains("deleted context prod");
    }

    @Test
    void failedRemoteLogoutKeepsTheCacheWhileLocalOnlyRemovesItWithAWarning(@TempDir Path home)
            throws IOException {
        Path workspace = Files.createDirectory(home.resolve("orders"));
        URI seed = URI.create("http://127.0.0.1:7900");
        UUID contextId = UUID.fromString("018f0d7a-7b2e-7e30-a8dd-6f78fc0d8ff2");
        UUID authRef = UUID.fromString("5c199643-04da-4f72-9831-3a77e3590eed");
        ContextDefinition definition = new ContextDefinition(contextId, List.of(seed), new ContextTls(true), authRef);
        ContextConfig config = new ContextConfig(1, null, Map.of("dev", definition),
                Map.of(workspace.toRealPath().toString(), "dev"));
        ContextResolver resolver = new ContextResolver(() -> config, name -> null);
        FakeControlPlane client = new FakeControlPlane(seed);
        Instant now = Instant.parse("2026-08-17T10:00:00Z");
        AuthSessionRecord record = new AuthSessionRecord(1, authRef, contextId,
                "urn:tapstate:cluster:test-cluster", "alice", List.of("read"),
                "tss_s01.session-secret", now, now.plusSeconds(2_592_000), now.plusSeconds(7_776_000));
        AuthFileStore store = AuthFileStore.underHome(home);
        store.save(record, false);
        CommandLine commandLine = Cli.newCommandLine();
        StringWriter output = new StringWriter();
        commandLine.setOut(new PrintWriter(output));
        commandLine.setErr(new PrintWriter(output));
        Repl repl = new Repl(commandLine, workspace, client, new ScriptedPrompter(), name -> null,
                resolver, null, new AuthService(client, store, Clock.fixed(now, ZoneOffset.UTC)));

        repl.dispatch(List.of("auth", "logout"), true);
        assertThat(repl.lastExitCode()).isNotZero();
        assertThat(store.load(authRef, contextId)).contains(record);
        assertThat(output.toString()).contains("local cache was kept");

        repl.dispatch(List.of("auth", "logout", "--local-only"), true);
        assertThat(repl.lastExitCode()).isZero();
        assertThat(store.load(authRef, contextId)).isEmpty();
        assertThat(output.toString()).contains("remote session remains valid until expiry");
    }

    @Test
    void freshProcessLocalOnlyLogoutResolvesNamedContextWithoutAnyNetwork(@TempDir Path home)
            throws IOException {
        Path workspace = Files.createDirectory(home.resolve("orders"));
        URI seed = URI.create("http://127.0.0.1:7900");
        UUID contextId = UUID.fromString("018f0d7a-7b2e-7e30-a8dd-6f78fc0d8ff2");
        UUID authRef = UUID.fromString("5c199643-04da-4f72-9831-3a77e3590eed");
        ContextDefinition definition = new ContextDefinition(
                contextId, List.of(seed), new ContextTls(true), authRef);
        ContextConfig config = new ContextConfig(1, null, Map.of("dev", definition),
                Map.of(workspace.toRealPath().toString(), "dev"));
        FakeControlPlane unreachable = new FakeControlPlane();
        Instant now = Instant.parse("2026-08-17T10:00:00Z");
        AuthFileStore store = AuthFileStore.underHome(home);
        store.save(new AuthSessionRecord(1, authRef, contextId,
                "urn:tapstate:cluster:test-cluster", "alice", List.of("read"),
                "tss_s01.session-secret", now, now.plusSeconds(2_592_000), now.plusSeconds(7_776_000)),
                false);
        CommandLine commandLine = Cli.newCommandLine();
        StringWriter output = new StringWriter();
        commandLine.setOut(new PrintWriter(output));
        commandLine.setErr(new PrintWriter(output));
        Repl repl = new Repl(commandLine, workspace, unreachable, new ScriptedPrompter(), name -> null,
                new ContextResolver(() -> config, name -> null), null,
                new AuthService(unreachable, store, Clock.fixed(now, ZoneOffset.UTC)));

        repl.dispatch(List.of("auth", "logout", "--local-only"), true);

        assertThat(repl.lastExitCode()).isZero();
        assertThat(store.load(authRef, contextId)).isEmpty();
        assertThat(unreachable.probed).isEmpty();
        assertThat(unreachable.discovered).isEmpty();
        assertThat(unreachable.sessionCalls).isEmpty();
        assertThat(output.toString()).contains("remote session remains valid until expiry");
    }

    @Test
    void remoteLogoutOfOldSessionNeverDeletesConcurrentReplacement(@TempDir Path home)
            throws IOException {
        Path workspace = Files.createDirectory(home.resolve("orders"));
        URI seed = URI.create("http://127.0.0.1:7900");
        UUID contextId = UUID.fromString("018f0d7a-7b2e-7e30-a8dd-6f78fc0d8ff2");
        UUID authRef = UUID.fromString("5c199643-04da-4f72-9831-3a77e3590eed");
        ContextDefinition definition = new ContextDefinition(
                contextId, List.of(seed), new ContextTls(true), authRef);
        ContextConfig config = new ContextConfig(1, null, Map.of("dev", definition),
                Map.of(workspace.toRealPath().toString(), "dev"));
        FakeControlPlane client = new FakeControlPlane(seed);
        client.logoutOutcome = new SessionLogoutOutcome.Success();
        Instant now = Instant.parse("2026-08-17T10:00:00Z");
        AuthSessionRecord oldRecord = new AuthSessionRecord(1, authRef, contextId,
                "urn:tapstate:cluster:test-cluster", "alice", List.of("read"),
                "tss_s01.old-secret", now, now.plusSeconds(2_592_000), now.plusSeconds(7_776_000));
        AuthSessionRecord replacement = new AuthSessionRecord(1, authRef, contextId,
                "urn:tapstate:cluster:test-cluster", "bob", List.of("read"),
                "tss_s02.new-secret", now.plusSeconds(1), now.plusSeconds(2_592_001),
                now.plusSeconds(7_776_001));
        AuthFileStore store = AuthFileStore.underHome(home);
        store.save(oldRecord, false);
        client.beforeLogoutResult = () -> AuthFileStore.underHome(home).save(replacement, false);
        CommandLine commandLine = Cli.newCommandLine();
        StringWriter output = new StringWriter();
        commandLine.setOut(new PrintWriter(output));
        commandLine.setErr(new PrintWriter(output));
        Repl repl = new Repl(commandLine, workspace, client, new ScriptedPrompter(), name -> null,
                new ContextResolver(() -> config, name -> null), null,
                new AuthService(client, store, Clock.fixed(now, ZoneOffset.UTC)));

        repl.dispatch(List.of("auth", "logout"), true);

        assertThat(repl.lastExitCode()).isNotZero();
        assertThat(store.load(authRef, contextId)).contains(replacement);
        assertThat(client.sessionCalls).containsExactly("logout tss_s01.old-secret@" + seed);
        assertThat(output.toString()).contains("cli.auth-logout-cache-changed")
                .doesNotContain("tss_s01.old-secret", "tss_s02.new-secret");
    }

    @Test
    void cachedNamedSessionResumesForDataBrowserAliasesAndLiveViews(@TempDir Path home)
            throws IOException {
        Path workspace = Files.createDirectory(home.resolve("orders"));
        URI seed = URI.create("http://127.0.0.1:7900");
        UUID contextId = UUID.fromString("018f0d7a-7b2e-7e30-a8dd-6f78fc0d8ff2");
        UUID authRef = UUID.fromString("5c199643-04da-4f72-9831-3a77e3590eed");
        ContextDefinition definition = new ContextDefinition(
                contextId, List.of(seed), new ContextTls(true), authRef);
        ContextConfig config = new ContextConfig(1, null, Map.of("dev", definition),
                Map.of(workspace.toRealPath().toString(), "dev"));
        Instant now = Instant.parse("2026-08-17T10:00:00Z");
        AuthFileStore store = AuthFileStore.underHome(home);
        store.save(new AuthSessionRecord(1, authRef, contextId,
                "urn:tapstate:cluster:test-cluster", "alice", List.of("read"),
                "tss_s01.session-secret", now, now.plusSeconds(2_592_000), now.plusSeconds(7_776_000)),
                false);
        List<String> commands = List.of(
                "data-browser show collections views",
                "show collections views",
                "views.orders.find()",
                "tail views.orders");

        for (String command : commands) {
            FakeControlPlane client = new FakeControlPlane(seed);
            client.exchangeOutcome = new SessionExchangeOutcome.Success(
                    "jwt-resumed", now.plusSeconds(900), "urn:tapstate:cluster:test-cluster", "alice",
                    List.of("read"));
            client.collectionsOutcome = new DataBrowserOutcome.Collections.Listed(List.of("orders"));
            client.findOutcome = new DataBrowserOutcome.Find.Read(List.of(), null, false);
            client.tailRefusal = "data-browser.follow-idle";
            CommandLine commandLine = Cli.newCommandLine();
            StringWriter output = new StringWriter();
            commandLine.setOut(new PrintWriter(output));
            commandLine.setErr(new PrintWriter(output));
            Repl repl = new Repl(commandLine, workspace, client, new ScriptedPrompter(), name -> null,
                    new ContextResolver(() -> config, name -> null), null,
                    new AuthService(client, store, Clock.fixed(now, ZoneOffset.UTC)));

            repl.dispatch(command);

            assertThat(client.discovered).as(command).containsExactly(seed);
            assertThat(client.sessionCalls).as(command)
                    .containsExactly("exchange tss_s01.session-secret@" + seed);
            assertThat(client.dataBrowserCalls).as(command).isNotEmpty();
        }
    }

    @Test
    void cachedSessionIsNeverExchangedWhenAnonymousDiscoveryFindsAnotherIssuer(@TempDir Path home)
            throws IOException {
        Path workspace = Files.createDirectory(home.resolve("orders"));
        URI seed = URI.create("http://127.0.0.1:7900");
        UUID contextId = UUID.fromString("018f0d7a-7b2e-7e30-a8dd-6f78fc0d8ff2");
        UUID authRef = UUID.fromString("5c199643-04da-4f72-9831-3a77e3590eed");
        ContextDefinition definition = new ContextDefinition(contextId, List.of(seed), new ContextTls(true), authRef);
        ContextConfig config = new ContextConfig(1, null, Map.of("dev", definition),
                Map.of(workspace.toRealPath().toString(), "dev"));
        ContextResolver resolver = new ContextResolver(() -> config, name -> null);
        FakeControlPlane client = new FakeControlPlane(seed);
        Instant now = Instant.parse("2026-08-17T10:00:00Z");
        AuthFileStore store = AuthFileStore.underHome(home);
        store.save(new AuthSessionRecord(1, authRef, contextId,
                "urn:tapstate:cluster:replaced", "alice", List.of("read"),
                "tss_must-not-be-sent.secret", now, now.plusSeconds(2_592_000), now.plusSeconds(7_776_000)),
                false);
        CommandLine commandLine = Cli.newCommandLine();
        StringWriter output = new StringWriter();
        commandLine.setOut(new PrintWriter(output));
        commandLine.setErr(new PrintWriter(output));
        Repl repl = new Repl(commandLine, workspace, client, new ScriptedPrompter(), name -> null,
                resolver, null, new AuthService(client, store, Clock.fixed(now, ZoneOffset.UTC)));

        repl.dispatch("show collections views");

        assertThat(repl.lastExitCode()).isNotZero();
        assertThat(client.discovered).containsExactly(seed);
        assertThat(client.sessionCalls).isEmpty();
        assertThat(output.toString()).contains("cli.auth-issuer-mismatch")
                .doesNotContain("tss_must-not-be-sent.secret");
    }

    @Test
    void liveViewDispatcherLazilyConnectsTheExactWorkspaceContext(@TempDir Path home) throws IOException {
        Path workspace = Files.createDirectory(home.resolve("orders"));
        URI seed = URI.create("http://dev:7900");
        ContextDefinition definition = new ContextDefinition(
                java.util.UUID.fromString("018f0d7a-7b2e-7e30-a8dd-6f78fc0d8ff2"), List.of(seed),
                new ContextTls(true),
                java.util.UUID.fromString("5c199643-04da-4f72-9831-3a77e3590eed"));
        ContextConfig config = new ContextConfig(1, null, Map.of("dev", definition),
                Map.of(workspace.toRealPath().toString(), "dev"));
        ContextResolver resolver = new ContextResolver(() -> config, name -> null);
        FakeControlPlane client = new FakeControlPlane(seed);
        CommandLine commandLine = Cli.newCommandLine();
        StringWriter sink = new StringWriter();
        PrintWriter writer = new PrintWriter(sink);
        commandLine.setOut(writer);
        commandLine.setErr(writer);
        Repl repl = new Repl(commandLine, workspace, client, new ScriptedPrompter(), name -> null,
                resolver, null);

        repl.dispatch(List.of("tail", "src.orders"));

        assertThat(client.probed).containsExactly(seed);
        assertThat(repl.session().isConnected()).isTrue();
        assertThat(sink.toString()).contains("cli.not-authenticated").doesNotContain("cli.not-connected");
    }

    @Test
    void onlineDispatcherReportsContextRequiredWithoutGuessingATarget(@TempDir Path home) {
        ContextResolver resolver = new ContextResolver(ContextConfig::empty, name -> null);
        FakeControlPlane client = new FakeControlPlane();
        CommandLine commandLine = Cli.newCommandLine();
        StringWriter sink = new StringWriter();
        PrintWriter writer = new PrintWriter(sink);
        commandLine.setOut(writer);
        commandLine.setErr(writer);
        Repl repl = new Repl(commandLine, home, client, new ScriptedPrompter(), name -> null,
                resolver, null);

        repl.dispatch(List.of("get", "missing"));

        assertThat(client.probed).isEmpty();
        assertThat(repl.session().isConnected()).isFalse();
        assertThat(sink.toString()).contains("cli.context-required").doesNotContain("cli.not-connected");
    }

    @Test
    void aOneShotLaunchKeepsItsConnectionNoiseOffTheCommandsOutput() {
        // the point of the one-line form is piping the command's output somewhere; two lines about
        // having connected, ahead of it, land in whatever is reading the result
        FakeControlPlane client = new FakeControlPlane(URI.create("http://localhost:7900"));
        client.loginOutcome = new LoginOutcome.Success("jwt-tok");
        client.listOutcome = new ListOutcome.Listed(List.of(new RemoteArtifact("src", "source", "")));
        StringWriter sink = new StringWriter();
        Repl repl = replWritingTo(sink, client);
        repl.signIn("localhost:7900", "admin", () -> "pw", true);
        assertThat(sink.toString()).doesNotContain("connected to").doesNotContain("logged in as");
    }

    @Test
    void aSessionLaunchStillConfirmsThatItConnected() {
        // in a session those lines are the answer to what was just typed, so they stay
        FakeControlPlane client = new FakeControlPlane(URI.create("http://localhost:7900"));
        client.loginOutcome = new LoginOutcome.Success("jwt-tok");
        StringWriter sink = new StringWriter();
        Repl repl = replWritingTo(sink, client);
        repl.signIn("localhost:7900", "admin", () -> "pw", false);
        assertThat(sink.toString()).contains("connected to").contains("logged in as");
    }

    private static Repl replWritingTo(StringWriter sink, FakeControlPlane client) {
        CommandLine cl = Cli.newCommandLine();
        PrintWriter pw = new PrintWriter(sink);
        cl.setOut(pw);
        cl.setErr(pw);
        return new Repl(cl, Path.of("tap-work"), client, new ScriptedPrompter());
    }

    @Test
    void aOneLineLaunchYieldsTheCommandsOwnStatus() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://localhost:7900"));
        client.loginOutcome = new LoginOutcome.Success("jwt-tok");
        client.getOutcome = new GetOutcome.Absent();
        LaunchOptions launch = LaunchOptions.parse("-c", "localhost:7900", "-u", "admin", "get", "nope")
                .withEnv(name -> "TAPSTATE_PASSWORD".equals(name) ? "pw" : null);
        assertThat(Cli.runSession(launch, client, () -> new ScriptedPrompter())).isNotZero();
    }

    @Test
    void aLaunchThatCannotConnectStopsBeforeRunningTheCommand() {
        FakeControlPlane client = new FakeControlPlane();   // nothing is healthy
        LaunchOptions launch = LaunchOptions.parse("-c", "node1:7900", "-u", "admin", "ls")
                .withEnv(name -> "TAPSTATE_PASSWORD".equals(name) ? "pw" : null);
        int code = Cli.runSession(launch, client, () -> new ScriptedPrompter());
        // running the verb anyway would report a missing connection rather than why there is none
        assertThat(code).isNotZero();
        assertThat(client.loginCalls).isEmpty();
        assertThat(client.listCalls).isEmpty();
    }

    @Test
    void aLaunchThatCannotSignInStopsBeforeRunningTheCommand() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://localhost:7900"));
        client.loginOutcome = new LoginOutcome.Rejected("auth.bad-credentials", "Wrong password.");
        LaunchOptions launch = LaunchOptions.parse("-c", "localhost:7900", "-u", "admin", "ls")
                .withEnv(name -> "TAPSTATE_PASSWORD".equals(name) ? "nope" : null);
        assertThat(Cli.runSession(launch, client, () -> new ScriptedPrompter())).isNotZero();
        assertThat(client.listCalls).isEmpty();
    }

    @Test
    void connectingWithoutAUserLandsConnectedButNotSignedIn() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        LaunchOptions launch = LaunchOptions.parse("-c", "node1:7900", "ls");
        Cli.runSession(launch, client, () -> new ScriptedPrompter());
        // connecting alone is a usable state, so it is not an error -- but nothing was signed in with
        assertThat(client.loginCalls).isEmpty();
    }

    @Test
    void thePasswordIsOnlyAskedForOnceTheConnectionIsMade() {
        // asking for a password to a server that is not there wastes the one interaction the user has
        FakeControlPlane client = new FakeControlPlane();   // nothing is healthy
        ScriptedPrompter prompter = new ScriptedPrompter("secret");
        LaunchOptions launch = LaunchOptions.parse("-c", "node1:7900", "-u", "admin")
                .withEnv(name -> null);
        Cli.runSession(launch, client, () -> prompter);
        assertThat(client.loginCalls).isEmpty();
    }

    @Test
    void anOmittedPasswordIsAskedForWhenTheEnvironmentHasNone() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://localhost:7900"));
        client.loginOutcome = new LoginOutcome.Success("jwt-tok");
        LaunchOptions launch = LaunchOptions.parse("-c", "localhost:7900", "-u", "admin", "ls")
                .withEnv(name -> null);
        Cli.runSession(launch, client, () -> new ScriptedPrompter("asked"));
        assertThat(client.loginCalls).containsExactly("admin:asked@http://localhost:7900");
    }

    @Test
    void anOmittedPasswordFallsBackToTheEnvironmentBeforeAsking() {
        // the point of the fallback is that a script need not put the password in argv, where the
        // process list and the shell history can both read it
        FakeControlPlane client = new FakeControlPlane(URI.create("http://localhost:7900"));
        client.loginOutcome = new LoginOutcome.Success("jwt-tok");
        LaunchOptions launch = LaunchOptions.parse("-c", "localhost:7900", "-u", "admin", "ls")
                .withEnv(name -> "TAPSTATE_PASSWORD".equals(name) ? "from-env" : null);
        Cli.runSession(launch, client, () -> new ScriptedPrompter("asked"));
        assertThat(client.loginCalls).containsExactly("admin:from-env@http://localhost:7900");
    }

    // ---- the in-place view ----

    @Test
    void watchRefusesWhereItsOutputIsNotATerminalAndNamesBothAlternatives() {
        // The refusal is the whole point: down a pipe, an in-place redraw is not a worse view, it is
        // cursor-control bytes in the middle of whatever the reader piped it into. And a reader who
        // reached for this verb wants one of the two things it names, so a refusal that named neither
        // would leave them with nothing to type next.
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        Harness h = onlineSession(Path.of("tap-work"), client);
        h.repl().terminalCheck(() -> false);
        int mark = h.sink().toString().length();

        h.repl().dispatch("watch views.order_state");

        String output = h.sink().toString().substring(mark);
        assertThat(output).contains("cli.watch-needs-a-terminal");
        assertThat(output).contains("tail").contains("find");
        assertThat(client.dataBrowserCalls)
                .as("it refuses before asking, so a piped watch never opens a read it cannot render")
                .isEmpty();
    }

    @Test
    void watchSaysWhatANamespaceLooksLikeRatherThanReportingAnUnknownVerb() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        Harness h = onlineSession(Path.of("tap-work"), client);
        h.repl().terminalCheck(() -> true);
        int mark = h.sink().toString().length();

        h.repl().dispatch("watch views");

        String output = h.sink().toString().substring(mark);
        assertThat(output)
                .as("the verb was matched, so a line it cannot read is that verb written wrongly; "
                        + "falling through would answer it by complaining about an unknown command")
                .contains("<source>.<collection>")
                .doesNotContain("Unknown");
    }

    @Test
    void watchNeedsASessionBeforeItNeedsATerminal() {
        // Offline the view has nothing to watch at all, and saying "this needs a terminal" to somebody
        // who has not connected sends them to fix the wrong thing.
        Harness h = harness();
        h.repl().terminalCheck(() -> false);
        int mark = h.sink().toString().length();

        h.repl().dispatch("watch views.order_state");

        assertThat(h.sink().toString().substring(mark)).contains("cli.not-connected");
    }

    // ---- the appended view ----

    @Test
    void tailStreamsEveryChangeAndSaysWhatItCannotPromise() {
        // The note is load-bearing. An appended stream reads as "everything that happened", and what
        // reaches the store is the settled version of a row -- rapid changes are folded before they are
        // written, upstream of the store, so no better transport would recover them.
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.tailFrames.add(new TailChange(TailChange.Kind.INSERT, "14:22:11", null,
                Map.of("status", "Paid")));
        client.tailFrames.add(new TailChange(TailChange.Kind.UPDATE, "14:22:15",
                Map.of("status", "Paid"), Map.of("status", "Shipped")));
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        h.repl().dispatch("tail views.order_state");

        String output = h.sink().toString().substring(mark);
        assertThat(output).contains("not every intermediate version");
        assertThat(output).contains("insert").contains("update");
        assertThat(output)
                .as("both sides of the alteration are shown because this change carried both; nothing "
                        + "is worked out from an earlier event")
                .contains("Paid").contains("Shipped");
    }

    @Test
    void tailSaysWhyItStoppedRatherThanNamingACodeAndNothingElse() {
        // A follow that ends by itself - its stream failed, or it was reclaimed after showing nothing
        // for long enough - arrives as a code and nothing else, because a close frame carries one
        // field. The reader is somebody watching a screen that just stopped updating, so the code has
        // to be turned back into a sentence here; there is nowhere else it could be done.
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.tailRefusal = "data-browser.follow-idle";
        Harness h = onlineSession(Path.of("tap-work"), client);
        int mark = h.sink().toString().length();

        h.repl().dispatch("tail views.order_state");

        String output = h.sink().toString().substring(mark);
        assertThat(output).contains("data-browser.follow-idle");
        assertThat(output)
                .as("the catalog sentence, not the literal text of a missing one")
                .contains("no changes")
                .doesNotContain("null");
    }

    @Test
    void tailNeedsNoTerminalBecauseItOnlyEverAppends() {
        // The opposite of the in-place view, and the reason both exist: this one is the pipeable half.
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        client.tailFrames.add(new TailChange(TailChange.Kind.INSERT, "14:22:11", null,
                Map.of("status", "Paid")));
        Harness h = onlineSession(Path.of("tap-work"), client);
        h.repl().terminalCheck(() -> false);
        int mark = h.sink().toString().length();

        h.repl().dispatch("tail views.order_state");

        String output = h.sink().toString().substring(mark);
        assertThat(output).doesNotContain("cli.watch-needs-a-terminal");
        assertThat(output).contains("insert");
    }

    @Test
    void tailSendsItsFilterToTheServerRatherThanNarrowingLocally() {
        FakeControlPlane client = new FakeControlPlane(URI.create("http://node1:7900"));
        Harness h = onlineSession(Path.of("tap-work"), client);

        h.repl().dispatch("tail views.order_state {status: \"Paid\"}");

        assertThat(client.dataBrowserCalls)
                .as("narrowing on the client would still carry every change of the whole collection "
                        + "over the wire, which is the cost the filter exists to cut")
                .anySatisfy(call -> assertThat(call).contains("tail views.order_state")
                        .contains("status").contains("Paid"));
    }
}
