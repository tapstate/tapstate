package io.tapstate.cli;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code tapstate up} ({@code docs/first-run/README.md}, "tapstate up"): the composite that brings a
 * bound workspace to running through the same calls the individual verbs make, in a fixed order, and
 * stops at the first stage that fails, naming it. The workspace is the one {@code new mirrored-table}
 * writes, bound to the default server in a temporary home; the session is a saved one, resumed the way
 * every other online one-shot resumes it; the control plane is a fake that records every call and
 * answers what each test scripts, so the order of calls and the words printed are both pinned.
 */
class UpCmdTest {

    private static final URI SERVER = URI.create("http://127.0.0.1:8080");
    private static final Instant NOW = Instant.parse("2026-09-06T10:00:00Z");
    private static final String ISSUER = "urn:tapstate:cluster:test-cluster";

    /** The tail every successful run ends with once the workspace lines are out. */
    private static final String NEXT =
            """
            Next:
              tapstate status orders_sync  watch it
              tapstate logs orders_sync  see what it is doing
              tapstate apply / tapstate start  the same thing, one step at a time
              edit any file above, then tapstate up again  it converges
            An AI assistant can take it from here: https://tapstate.dev/docs/first-run
            """;

    private record Run(int code, String out, String err) {
        String all() {
            return out + err;
        }
    }

    /** Writes the mirrored-table workspace and binds it to the default server, as a first run would. */
    private static void scaffold(Path home, Path ws) {
        NewRecipeTest.Run r = NewRecipeTest.run(home, new ScriptedPrompter(),
                "new", "mirrored-table", "--yes", "--connector", "mysql",
                "--set", "host=db", "--set", "username=u", "--set", "password=s",
                "--table", "orders", "--view", "orders_view", "-w", ws.toString());
        assertThat(r.code()).as(r.all()).isZero();
    }

    /** Saves a session for the bound context, so the run resumes it exactly as any online verb does. */
    private static void signIn(Path home) {
        ContextDefinition local = ContextConfigStore.underHome(home).load().contexts().get("local");
        AuthSessionRecord record = new AuthSessionRecord(AuthSessionRecord.CURRENT_VERSION,
                local.authRef(), local.id(), ISSUER, "alice", List.of("read", "write"),
                "tss_s01.session-secret", NOW, NOW.plusSeconds(2_592_000), NOW.plusSeconds(7_776_000));
        AuthFileStore.underHome(home).save(record, false);
    }

    private static Run up(Path home, FakeUpControlPlane client, UnaryOperator<String> env, String... args) {
        LaunchOptions launch = LaunchOptions.parse(args).withEnv(env);
        ContextResolver resolver = new ContextResolver(ContextConfigStore.underHome(home), env);
        AuthService auth = new AuthService(client, AuthFileStore.underHome(home), Clock.fixed(NOW, ZoneOffset.UTC));
        CommandLine cl = Cli.newCommandLine();
        StringWriter out = new StringWriter();
        StringWriter err = new StringWriter();
        cl.setOut(new PrintWriter(out));
        cl.setErr(new PrintWriter(err));
        int code = Cli.runSession(launch, client, () -> new ScriptedPrompter(), resolver, auth, cl);
        return new Run(code, out.toString(), err.toString());
    }

    private static Run up(Path home, FakeUpControlPlane client, String... args) {
        return up(home, client, name -> null, args);
    }

    // ---- the happy path ---------------------------------------------------------------------------

    @Test
    void bringsAFreshWorkspaceToRunningInOrderAndSaysSo(@TempDir Path home, @TempDir Path ws) {
        scaffold(home, ws);
        signIn(home);
        FakeUpControlPlane client = new FakeUpControlPlane();

        Run r = up(home, client, "up", "-w", ws.toString());

        assertThat(r.code()).as(r.all()).isZero();
        assertThat(r.err()).isEmpty();
        assertThat(client.calls).containsExactly(
                "isHealthy",
                "connectorList",
                "test orders_src",
                "apply[source]",
                "discoverSchema orders_src",
                "apply[pipeline]",
                "lifecycle start orders_sync",
                "status orders_sync");
        assertThat(r.out()).isEqualTo(
                "Workspace: " + ws + "\n"
                        + """
                          pipeline orders_sync: running
                          source orders_src: applied
                        State: running
                        """
                        + NEXT);
    }

    @Test
    void aSecondRunConvergesWithoutStartingAnythingAndSaysSoPerStage(@TempDir Path home, @TempDir Path ws) {
        scaffold(home, ws);
        signIn(home);
        FakeUpControlPlane client = new FakeUpControlPlane();
        client.applyChange = "UNCHANGED";
        client.schemaOutcome = new ConnectionSchemaOutcome.Found(client.schema);
        client.pipelineState = "RUNNING";

        Run r = up(home, client, "up", "-w", ws.toString());

        assertThat(r.code()).as(r.all()).isZero();
        assertThat(r.err()).isEmpty();
        // Reachability is checked on every run: a converged workspace whose database has since gone
        // away is exactly the case the preflight exists to catch before anything else is asked.
        assertThat(client.calls).contains("test orders_src")
                .doesNotContain("lifecycle start orders_sync")
                .doesNotContain("discoverSchema orders_src");
        assertThat(r.out()).isEqualTo(
                "Workspace: " + ws + "\n"
                        + """
                          pipeline orders_sync: running (apply: unchanged; start: already running)
                          source orders_src: applied (apply: unchanged; discover: already discovered)
                        State: running (nothing to do)
                        """
                        + NEXT);
    }

    @Test
    void anUnchangedSourceNeverDiscoveredIsStillDiscovered(@TempDir Path home, @TempDir Path ws) {
        scaffold(home, ws);
        signIn(home);
        FakeUpControlPlane client = new FakeUpControlPlane();
        client.applyChange = "UNCHANGED";
        client.schemaOutcome = new ConnectionSchemaOutcome.Absent();
        client.pipelineState = "STOPPED";

        Run r = up(home, client, "up", "-w", ws.toString());

        assertThat(r.code()).as(r.all()).isZero();
        assertThat(client.calls).containsSubsequence(
                "schema orders_src", "discoverSchema orders_src", "status orders_sync",
                "lifecycle start orders_sync", "status orders_sync");
        assertThat(r.out()).contains("State: running\n").doesNotContain("nothing to do");
    }

    @Test
    void theSessionAGuidedNewSignedInWithIsReusedWithoutAnyCredentialFlag(@TempDir Path home, @TempDir Path ws) {
        // the first run signs in through the guided entry; no session is written by hand here
        NewGuidedTest.Run first = NewGuidedTest.run(home, new ScriptedPrompter(), new NewGuidedTest.Fakes(true),
                NewGuidedTest.PASSWORD_IN_ENV,
                "new", "mirrored-table", "--yes", "--connector", "mysql",
                "--set", "host=db", "--set", "username=u", "--set", "password=s",
                "--table", "orders", "--view", "orders_view", "-w", ws.toString());
        assertThat(first.code()).as(first.all()).isZero();
        FakeUpControlPlane client = new FakeUpControlPlane();
        client.principal = "admin";

        Run r = up(home, client, "up", "-w", ws.toString());

        assertThat(r.code()).as(r.all()).isZero();
        assertThat(r.err()).isEmpty();
        // the session the first run saved is the one presented, and the stages ran on it
        assertThat(client.exchanged).containsExactly("tss_s01.first-run-secret");
        assertThat(client.calls).startsWith("isHealthy", "connectorList").endsWith("status orders_sync");
        assertThat(r.out()).contains("State: running\n");
    }

    // ---- .env ---------------------------------------------------------------------------------------

    @Test
    void dotEnvIsReadFirstAndTheProcessEnvironmentSecond(@TempDir Path home, @TempDir Path ws) throws IOException {
        scaffold(home, ws);
        signIn(home);
        Path source = ws.resolve("source/orders_src.tap.yml");
        Files.writeString(source, Files.readString(source).replace(
                "  host: db\n", "  database: ${ORDERS_SRC_DATABASE}\n  host: db\n"));
        Files.writeString(ws.resolve(".env"), "# secrets\n\nORDERS_SRC_PASSWORD=s\n");
        FakeUpControlPlane client = new FakeUpControlPlane();
        UnaryOperator<String> process = name -> switch (name) {
            case "ORDERS_SRC_DATABASE" -> "shop";
            case "ORDERS_SRC_PASSWORD" -> "from-the-process";
            default -> null;
        };

        Run r = up(home, client, process, "up", "-w", ws.toString());

        assertThat(r.code()).as(r.all()).isZero();
        String submitted = client.applied.get(0).get(0).content();
        assertThat(submitted).contains("password: s\n").contains("database: shop\n")
                .doesNotContain("${").doesNotContain("from-the-process");
    }

    // ---- the stages that refuse ---------------------------------------------------------------------

    @Test
    void aConnectorMissingFromTheServerStopsPreflightBeforeAnythingIsApplied(@TempDir Path home, @TempDir Path ws) {
        scaffold(home, ws);
        signIn(home);
        FakeUpControlPlane client = new FakeUpControlPlane();
        client.registeredConnectors = List.of("postgres");

        Run r = up(home, client, "up", "-w", ws.toString());

        assertThat(r.code()).as(r.all()).isEqualTo(Cli.EXIT_DIAGNOSTIC);
        assertThat(r.err()).contains("up: preflight failed on orders_src: connector.not-registered")
                .contains("mysql")
                .containsIgnoringCase("register");
        assertThat(client.calls).containsExactly("isHealthy", "connectorList");
        assertThat(r.out()).isEmpty();
    }

    @Test
    void anUnreachableDiscoveryStopsAtDiscoverNamingTheSource(@TempDir Path home, @TempDir Path ws) {
        scaffold(home, ws);
        signIn(home);
        FakeUpControlPlane client = new FakeUpControlPlane();
        client.discoverOutcome = new ConnectionDiscoverSchemaOutcome.Unreachable();

        Run r = up(home, client, "up", "-w", ws.toString());

        assertThat(r.code()).as(r.all()).isEqualTo(Cli.EXIT_DIAGNOSTIC);
        assertThat(r.err()).contains("up: discover failed on orders_src: ");
        assertThat(client.calls).contains("apply[source]")
                .doesNotContain("apply[pipeline]")
                .doesNotContain("lifecycle start orders_sync");
    }

    @Test
    void aRefusedWorkspaceApplyStopsBeforeStartWithTheServersCode(@TempDir Path home, @TempDir Path ws) {
        scaffold(home, ws);
        signIn(home);
        FakeUpControlPlane client = new FakeUpControlPlane();
        client.pipelineApply = new ApplyOutcome.Rejected("actuation.source-schema-not-discovered",
                "Source `orders_src` needs a discovered schema before its tables can be selected.",
                Map.of("source", "orders_src"));

        Run r = up(home, client, "up", "-w", ws.toString());

        assertThat(r.code()).as(r.all()).isEqualTo(Cli.EXIT_DIAGNOSTIC);
        assertThat(r.err()).contains(
                "up: apply workspace failed on orders_sync: actuation.source-schema-not-discovered — "
                        + "Source `orders_src` needs a discovered schema before its tables can be selected.");
        assertThat(client.calls).contains("apply[pipeline]").doesNotContain("lifecycle start orders_sync");
    }

    @Test
    void aFailureInJsonIsTheDiagnosticEnvelopeWithTheStage(@TempDir Path home, @TempDir Path ws) {
        scaffold(home, ws);
        signIn(home);
        FakeUpControlPlane client = new FakeUpControlPlane();
        client.registeredConnectors = List.of();

        Run r = up(home, client, "up", "-w", ws.toString(), "-o", "json");

        assertThat(r.code()).as(r.all()).isEqualTo(Cli.EXIT_DIAGNOSTIC);
        assertThat(r.out())
                .contains("\"code\": \"connector.not-registered\"")
                .contains("\"stage\": \"preflight\"")
                .contains("\"on\": \"orders_src\"")
                .doesNotContain("Next:");
        assertThat(r.err()).isEmpty();
    }

    @Test
    void anUnboundDirectoryIsRefusedTheWayEveryOnlineVerbRefusesIt(@TempDir Path home, @TempDir Path ws) {
        FakeUpControlPlane client = new FakeUpControlPlane();

        Run r = up(home, client, "up", "-w", ws.toString());

        assertThat(r.code()).as(r.all()).isEqualTo(Cli.EXIT_VERB_UNAVAILABLE);
        assertThat(r.err()).contains("cli.context-required").contains("`up`");
        assertThat(client.calls).isEmpty();
    }

    @Test
    void aWorkspaceWithNoPipelineIsRefusedAtPreflight(@TempDir Path home, @TempDir Path ws) throws IOException {
        scaffold(home, ws);
        signIn(home);
        Files.delete(ws.resolve("pipeline/orders_sync.tap.yml"));
        FakeUpControlPlane client = new FakeUpControlPlane();

        Run r = up(home, client, "up", "-w", ws.toString());

        assertThat(r.code()).as(r.all()).isEqualTo(Cli.EXIT_DIAGNOSTIC);
        assertThat(r.err()).contains("up: preflight failed on " + ws + ": cli.workspace-has-no-pipeline")
                .contains("tapstate new");
        // Nothing beyond the connect's own probe: the workspace is read before the server is asked anything.
        assertThat(client.calls).containsExactly("isHealthy");
        assertThat(r.out()).isEmpty();
    }

    @Test
    void anUnreadableWorkspaceIsRefusedAtPreflightWithACode(@TempDir Path home, @TempDir Path ws) throws IOException {
        scaffold(home, ws);
        signIn(home);
        Path pipelines = ws.resolve("pipeline");
        Set<PosixFilePermission> restored = Files.getPosixFilePermissions(pipelines);
        Files.setPosixFilePermissions(pipelines, Set.of());
        // Running as a user the permission bits do not bind (root) makes the directory readable anyway.
        Assumptions.assumeFalse(Files.isReadable(pipelines));
        FakeUpControlPlane client = new FakeUpControlPlane();
        try {
            Run r = up(home, client, "up", "-w", ws.toString());

            assertThat(r.code()).as(r.all()).isEqualTo(Cli.EXIT_DIAGNOSTIC);
            assertThat(r.err()).contains("up: preflight failed on " + ws + ": cli.workspace-unreadable")
                    .contains(pipelines.toString());
            assertThat(client.calls).containsExactly("isHealthy");
            assertThat(r.out()).isEmpty();
        } finally {
            Files.setPosixFilePermissions(pipelines, restored);
        }
    }

    // ---- each source reachable, through the test verb's own call --------------------------------------

    @Test
    void aSourceWhoseTestFailsStopsPreflightWithTheConnectorsCodeAndRemedy(@TempDir Path home, @TempDir Path ws) {
        scaffold(home, ws);
        signIn(home);
        FakeUpControlPlane client = new FakeUpControlPlane();
        client.testOutcome = new ConnectionTestOutcome.Tested(new ConnectionReport("orders_src", "mysql", "FAILED",
                List.of(new ConnectionReport.Check("Connect", "PASSED", null, null, null, null),
                        new ConnectionReport.Check("Read privilege", "FAILED", "Cannot read table orders",
                                "The account u has no SELECT grant on shop.orders.",
                                "Grant SELECT on shop.orders to u, then test again.", "MYSQL-1142")),
                0L));

        Run r = up(home, client, "up", "-w", ws.toString());

        assertThat(r.code()).as(r.all()).isEqualTo(Cli.EXIT_DIAGNOSTIC);
        assertThat(r.err()).startsWith(
                """
                error: up: preflight failed on orders_src: MYSQL-1142 — Cannot read table orders
                  The account u has no SELECT grant on shop.orders.
                  Grant SELECT on shop.orders to u, then test again.
                """);
        assertThat(client.calls).containsExactly("isHealthy", "connectorList", "test orders_src");
        assertThat(r.out()).isEmpty();
    }

    @Test
    void aSourceTestTheServerRefusesStopsPreflightWithTheServersCode(@TempDir Path home, @TempDir Path ws) {
        scaffold(home, ws);
        signIn(home);
        FakeUpControlPlane client = new FakeUpControlPlane();
        client.testOutcome = new ConnectionTestOutcome.Rejected("connector.auth-failed",
                "Authentication failed for user u.");

        Run r = up(home, client, "up", "-w", ws.toString());

        assertThat(r.code()).as(r.all()).isEqualTo(Cli.EXIT_DIAGNOSTIC);
        assertThat(r.err()).startsWith(
                "error: up: preflight failed on orders_src: connector.auth-failed — Authentication failed for user u.\n");
        assertThat(client.calls).containsExactly("isHealthy", "connectorList", "test orders_src");
    }

    @Test
    void aSourceTestThatCannotReachTheServerIsTheConnectFailedCode(@TempDir Path home, @TempDir Path ws) {
        scaffold(home, ws);
        signIn(home);
        FakeUpControlPlane client = new FakeUpControlPlane();
        client.testOutcome = new ConnectionTestOutcome.Unreachable();

        Run r = up(home, client, "up", "-w", ws.toString());

        assertThat(r.code()).as(r.all()).isEqualTo(Cli.EXIT_DIAGNOSTIC);
        assertThat(r.err()).contains("up: preflight failed on orders_src: cli.connect-failed");
        // Failover re-probes and retries the one call before giving up, as it does for every stage.
        assertThat(client.calls).startsWith("isHealthy", "connectorList", "test orders_src")
                .doesNotContain("apply[source]");
    }

    @Test
    void aSourceTestThatTimesOutIsTheTimedOutCode(@TempDir Path home, @TempDir Path ws) {
        scaffold(home, ws);
        signIn(home);
        FakeUpControlPlane client = new FakeUpControlPlane();
        client.testOutcome = new ConnectionTestOutcome.TimedOut();

        Run r = up(home, client, "up", "-w", ws.toString());

        assertThat(r.code()).as(r.all()).isEqualTo(Cli.EXIT_DIAGNOSTIC);
        assertThat(r.err()).contains("up: preflight failed on orders_src: cli.request-timed-out");
        assertThat(client.calls).containsExactly("isHealthy", "connectorList", "test orders_src");
    }

    @Test
    void aSourceTestFailureInJsonIsTheDiagnosticEnvelopeWithTheStage(@TempDir Path home, @TempDir Path ws) {
        scaffold(home, ws);
        signIn(home);
        FakeUpControlPlane client = new FakeUpControlPlane();
        client.testOutcome = new ConnectionTestOutcome.Rejected("connector.auth-failed",
                "Authentication failed for user u.");

        Run r = up(home, client, "up", "-w", ws.toString(), "-o", "json");

        assertThat(r.code()).as(r.all()).isEqualTo(Cli.EXIT_DIAGNOSTIC);
        assertThat(r.out())
                .contains("\"code\": \"connector.auth-failed\"")
                .contains("\"stage\": \"preflight\"")
                .contains("\"on\": \"orders_src\"");
        assertThat(r.err()).isEmpty();
    }

    // ---- the machine surface and the help -----------------------------------------------------------

    @Test
    void jsonCarriesTheFactsAsFieldsAndNoProse(@TempDir Path home, @TempDir Path ws) {
        scaffold(home, ws);
        signIn(home);
        FakeUpControlPlane client = new FakeUpControlPlane();

        Run r = up(home, client, "up", "-w", ws.toString(), "-o", "json");

        assertThat(r.code()).as(r.all()).isZero();
        assertThat(r.out())
                .contains("\"status\": \"up\"")
                .contains("\"workspace\": \"" + ws + "\"")
                .contains("\"pipelines\": [")
                .contains("\"id\": \"orders_sync\"")
                .contains("\"state\": \"running\"")
                .contains("\"sources\": [")
                .contains("\"orders_src\"")
                .contains("\"next\": [")
                .doesNotContain("Next:")
                .doesNotContain("An AI assistant")
                .doesNotContain("Workspace:");
    }

    @Test
    void helpSaysWhatItDoesAndListsTheStages() {
        CommandLine cl = Cli.newCommandLine();
        StringWriter out = new StringWriter();
        cl.setOut(new PrintWriter(out));
        cl.setErr(new PrintWriter(new StringWriter()));

        int code = cl.execute("up", "--help");

        assertThat(code).isZero();
        assertThat(out.toString())
                .contains("Usage: tapstate up")
                .contains(Cli.VERB_HELP.get("up").summary())
                .contains("preflight").contains("apply sources").contains("discover")
                .contains("apply workspace").contains("start")
                .contains("--server").contains("--yes");
    }

    /**
     * A control plane that answers what each test scripts and writes down every call the run makes,
     * in order. Session establishment (issuer discovery, the session exchange) is answered but not
     * written down: it is how every online one-shot starts, and the order under test is the verb's.
     * Anything the verb has no business calling fails the test.
     */
    static final class FakeUpControlPlane implements ControlPlaneClient {
        final List<String> calls = new ArrayList<>();
        /** Every session token presented for exchange, in order. */
        final List<String> exchanged = new ArrayList<>();
        /** The drafts each apply carried, kept whole: what reaches the wire is the thing under test. */
        final List<List<LocalDraft>> applied = new ArrayList<>();
        List<String> registeredConnectors = List.of("mysql", "postgres");
        /** What every applied item reports back: CREATED on a first run, UNCHANGED on a converged one. */
        String applyChange = "CREATED";
        /** When set, what a batch holding the pipeline is answered with instead of its items. */
        ApplyOutcome pipelineApply;
        final ConnectionSchema schema = new ConnectionSchema("orders_src", "mysql", List.of(
                new ConnectionSchema.Table("orders", List.of(new ConnectionSchema.Field("id", "int")),
                        List.of("id"), List.of())), 0L);
        ConnectionSchemaOutcome schemaOutcome = new ConnectionSchemaOutcome.Absent();
        ConnectionDiscoverSchemaOutcome discoverOutcome = new ConnectionDiscoverSchemaOutcome.Discovered(schema);
        String pipelineState = "RUNNING";
        /** What each source's connection test answers: a passing report unless a test scripts otherwise. */
        ConnectionTestOutcome testOutcome = new ConnectionTestOutcome.Tested(new ConnectionReport(
                "orders_src", "mysql", "PASSED",
                List.of(new ConnectionReport.Check("Connect", "PASSED", null, null, null, null)), 0L));
        /** Whom the saved session belongs to, as the exchange answers it; the record's principal must match. */
        String principal = "alice";

        @Override
        public boolean isHealthy(URI baseUrl) {
            calls.add("isHealthy");
            return true;
        }

        @Override
        public String serverVersion(URI baseUrl) {
            return Cli.VERSION_NUMBER;
        }

        @Override
        public DiscoveryOutcome discover(URI baseUrl) {
            return new DiscoveryOutcome.Discovered(ISSUER, "test-cluster", "tapstate/v1",
                    List.of("password", "machine_token"));
        }

        @Override
        public SessionExchangeOutcome exchangeSession(URI baseUrl, String sessionToken) {
            exchanged.add(sessionToken);
            return new SessionExchangeOutcome.Success("jwt-resumed", NOW.plusSeconds(900), ISSUER, principal,
                    List.of("read", "write"));
        }

        @Override
        public ConnectorListOutcome connectorList(URI u, String c) {
            calls.add("connectorList");
            return new ConnectorListOutcome.Listed(registeredConnectors.stream()
                    .map(id -> new CatalogConnector(id, id, "database", List.of("cdc"), true, "registered"))
                    .toList());
        }

        @Override
        public ApplyOutcome apply(URI baseUrl, String credential, List<LocalDraft> drafts) {
            List<String> kinds = drafts.stream().map(d -> d.source().substring(0, d.source().indexOf('/'))).distinct().toList();
            calls.add("apply[" + String.join(",", kinds) + "]");
            applied.add(List.copyOf(drafts));
            if (pipelineApply != null && kinds.contains("pipeline")) {
                return pipelineApply;
            }
            Function<LocalDraft, ApplyOutcome.Item> item = d -> new ApplyOutcome.Item(
                    d.source().substring(d.source().indexOf('/') + 1).replace(".tap.yml", ""),
                    d.source().substring(0, d.source().indexOf('/')), applyChange);
            return new ApplyOutcome.Applied(drafts.stream().map(item).toList());
        }

        @Override
        public ConnectionSchemaOutcome schema(URI u, String c, String id) {
            calls.add("schema " + id);
            return schemaOutcome;
        }

        @Override
        public ConnectionDiscoverSchemaOutcome discoverSchema(URI u, String c, String id, String connector, Map<String, Object> s) {
            calls.add("discoverSchema " + id);
            return discoverOutcome;
        }

        @Override
        public LifecycleOutcome lifecycle(URI u, String c, String id, String verb) {
            calls.add("lifecycle " + verb + " " + id);
            pipelineState = "RUNNING";
            return new LifecycleOutcome.Accepted(id, "RUNNING", "1");
        }

        @Override
        public StatusOutcome status(URI u, String c, String id) {
            calls.add("status " + id);
            return new StatusOutcome.Found(id, pipelineState);
        }

        @Override public LoginOutcome login(URI baseUrl, String username, String password) { throw new AssertionError(); }
        @Override public GetOutcome get(URI baseUrl, String credential, String id) { throw new AssertionError(); }
        @Override public DeleteOutcome delete(URI baseUrl, String credential, String id, String hash) { throw new AssertionError(); }
        @Override public ListOutcome list(URI baseUrl, String credential, String kind) { throw new AssertionError(); }
        @Override
        public ConnectionTestOutcome test(URI u, String c, String id, String connector, Map<String, Object> s) {
            calls.add("test " + id);
            return testOutcome;
        }

        @Override public ConnectionTestResultOutcome testResult(URI u, String c, String id) { throw new AssertionError(); }
        @Override public ConnectorRegisterOutcome register(URI u, String c, byte[] a) { throw new AssertionError(); }
        @Override public DataBrowserOutcome.Collections collections(URI u, String c, String id) { throw new AssertionError(); }
        @Override public DataBrowserOutcome.Stats stats(URI u, String c, String id, String collection) { throw new AssertionError(); }
        @Override public DataBrowserOutcome.Find find(URI u, String c, String id, String collection, Object f, DataBrowserCall.Order o, Integer l) { throw new AssertionError(); }
        @Override public MetricsOutcome metrics(URI u, String c, String id) { throw new AssertionError(); }
        @Override public SnapshotOutcome snapshot(URI u, String c, String id) { throw new AssertionError(); }
        @Override public LogsOutcome logs(URI u, String c, String id) { throw new AssertionError(); }
        @Override public String watchStatus(URI u, String c, String id, StatusStream s, java.util.function.BooleanSupplier stop) { throw new AssertionError(); }
        @Override public String followLogs(URI u, String c, String id, LogStream s, java.util.function.BooleanSupplier stop) { throw new AssertionError(); }
        @Override public String tail(URI u, String c, String id, String collection, Object f, TailStream s, java.util.function.BooleanSupplier stop) { throw new AssertionError(); }
    }
}
