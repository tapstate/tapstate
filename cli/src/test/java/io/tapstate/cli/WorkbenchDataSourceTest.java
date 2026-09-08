package io.tapstate.cli;

import io.tapstate.core.common.TapstateException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkbenchDataSourceTest {

    private static final URI FIRST = URI.create("https://first.example");
    private static final URI SECOND = URI.create("https://second.example");

    @TempDir
    Path workspace;

    @Test
    void disconnectedWithoutTargetKeepsLocalWorkspaceAsNotConfigured() throws Exception {
        writeSource("local");
        Harness harness = harness(null, null);

        WorkbenchSnapshot snapshot = load(harness.repl());

        assertThat(snapshot.workspace().remoteState())
                .isInstanceOf(WorkbenchRemoteState.NotConfigured.class);
        assertThat(snapshot.workspace().rows()).extracting(row -> row.key().id())
                .containsExactly("local");
        assertThat(snapshot.workspace().rows().getFirst().alignment())
                .isEqualTo(WorkbenchAlignment.UNKNOWN);
        assertThat(snapshot.session().connection()).isEqualTo(WorkbenchConnection.NO_CONTEXT);
        assertThat(harness.client().calls("list")).isZero();
    }

    @Test
    void reachableNamedContextConnectsSilentlyAsSignedOutWithoutListing() throws Exception {
        writeSource("local");
        NamedFixture named = namedFixture();
        RecordingClient client = new RecordingClient();
        client.discoveryOutcome = discovered();
        Harness harness = harness(named.resolver(), "dev", client);

        WorkbenchSnapshot first = load(harness.repl());
        WorkbenchSnapshot second = load(harness.repl());

        assertThat(first.workspace().remoteState())
                .isInstanceOf(WorkbenchRemoteState.SignedOut.class);
        assertThat(second.workspace().remoteState())
                .isInstanceOf(WorkbenchRemoteState.SignedOut.class);
        assertThat(first.workspace().rows()).hasSize(1);
        assertNamedContext(first, "dev", ResolvedContext.Source.EXPLICIT);
        assertNamedContext(second, "dev", ResolvedContext.Source.EXPLICIT);
        assertThat(second.session().connection()).isEqualTo(WorkbenchConnection.CONNECTED);
        assertThat(second.session().authentication()).isEqualTo(WorkbenchAuthentication.SIGNED_OUT);
        assertThat(second.session().landingNode()).contains(FIRST);
        assertThat(harness.client().calls("discover")).isEqualTo(1);
        assertThat(harness.client().calls("list")).isZero();
        assertThat(harness.out().toString()).isEmpty();
        assertThat(harness.err().toString()).isEmpty();
    }

    @Test
    void unreachableNamedContextKeepsLocalRowsAndReportsOfflineSilently() throws Exception {
        writeSource("local");
        ContextDefinition firstContext = new ContextDefinition(
                UUID.randomUUID(), List.of(FIRST), new ContextTls(true), UUID.randomUUID());
        ContextDefinition secondContext = new ContextDefinition(
                UUID.randomUUID(), List.of(SECOND), new ContextTls(true), UUID.randomUUID());
        Map<String, ContextDefinition> contexts = Map.of("dev", firstContext, "next", secondContext);
        String workspacePath = workspace.toRealPath().toString();
        AtomicReference<ContextConfig> current = new AtomicReference<>(
                new ContextConfig(1, null, contexts, Map.of(workspacePath, "dev")));
        ContextResolver resolver = new ContextResolver(current::get, name -> null);
        RecordingClient client = new RecordingClient();
        Harness harness = harness(resolver, null, client);

        WorkbenchSnapshot first = load(harness.repl());
        current.set(new ContextConfig(1, null, contexts, Map.of(workspacePath, "next")));
        WorkbenchSnapshot second = load(harness.repl());

        assertThat(first.workspace().remoteState())
                .isInstanceOf(WorkbenchRemoteState.Offline.class);
        assertThat(second.workspace().remoteState())
                .isInstanceOf(WorkbenchRemoteState.Offline.class);
        assertThat(first.workspace().rows()).hasSize(1);
        assertNamedContext(first, "dev", ResolvedContext.Source.WORKSPACE_BINDING);
        assertNamedContext(second, "next", ResolvedContext.Source.WORKSPACE_BINDING);
        assertThat(harness.repl().session().isConnected()).isFalse();
        assertThat(client.calls("discover")).isEqualTo(2);
        assertThat(harness.client().calls("list")).isZero();
        assertThat(harness.out().toString()).isEmpty();
        assertThat(harness.err().toString()).isEmpty();
    }

    @Test
    void cachedNamedSessionKeepsMetadataAndListsOncePerLoad() throws Exception {
        NamedFixture named = namedFixture();
        RecordingClient client = new RecordingClient();
        client.discoveryOutcome = discovered();
        client.exchangeOutcome = new SessionExchangeOutcome.Success(
                "resumed-access-token",
                Instant.parse("2026-09-08T00:15:00Z"),
                "urn:tapstate:cluster:test-cluster",
                "alice",
                List.of("read"));
        client.outcomes.add(new ListOutcome.Listed(List.of()));
        client.outcomes.add(new ListOutcome.Listed(List.of()));
        Path home = Files.createDirectories(workspace.resolve("home"));
        AuthFileStore store = AuthFileStore.underHome(home);
        Instant now = Instant.parse("2026-09-08T00:00:00Z");
        store.save(new AuthSessionRecord(
                AuthSessionRecord.CURRENT_VERSION,
                named.definition().authRef(),
                named.definition().id(),
                "urn:tapstate:cluster:test-cluster",
                "alice",
                List.of("read"),
                "tss_s01.cached-session-secret",
                now.minusSeconds(60),
                now.plusSeconds(3600),
                now.plusSeconds(7200)), false);
        AuthService authService = new AuthService(
                client.proxy, store, Clock.fixed(now, ZoneOffset.UTC));
        Harness harness = harness(named.resolver(), "dev", client, authService);

        WorkbenchSnapshot first = load(harness.repl());
        WorkbenchSnapshot second = load(harness.repl());

        assertThat(first.workspace().remoteState())
                .isEqualTo(new WorkbenchRemoteState.Available(0));
        assertThat(second.workspace().remoteState())
                .isEqualTo(new WorkbenchRemoteState.Available(0));
        assertNamedContext(first, "dev", ResolvedContext.Source.EXPLICIT);
        assertNamedContext(second, "dev", ResolvedContext.Source.EXPLICIT);
        assertThat(second.session().authentication()).isEqualTo(WorkbenchAuthentication.SIGNED_IN);
        assertThat(second.session().principal()).contains("alice");
        assertThat(first.toString() + second).doesNotContain(
                "resumed-access-token", "cached-session-secret");
        assertThat(client.calls("discover")).isEqualTo(1);
        assertThat(client.calls("exchangeSession")).isEqualTo(1);
        assertThat(client.calls("list")).isEqualTo(2);
        assertThat(harness.out().toString()).isEmpty();
        assertThat(harness.err().toString()).isEmpty();
    }

    @Test
    void namedMachineTokenActivatesAfterDiscoveryAndListsSilently() throws Exception {
        NamedFixture named = namedFixture();
        RecordingClient client = new RecordingClient();
        client.discoveryOutcome = discovered();
        client.outcomes.add(new ListOutcome.Listed(List.of()));
        client.outcomes.add(new ListOutcome.Listed(List.of()));
        Harness harness = harness(named.resolver(), "dev", client);
        harness.repl().installMachineToken("machine-token-secret");

        WorkbenchSnapshot first = load(harness.repl());
        WorkbenchSnapshot second = load(harness.repl());

        assertThat(first.workspace().remoteState())
                .isEqualTo(new WorkbenchRemoteState.Available(0));
        assertThat(second.workspace().remoteState())
                .isEqualTo(new WorkbenchRemoteState.Available(0));
        assertNamedContext(first, "dev", ResolvedContext.Source.EXPLICIT);
        assertNamedContext(second, "dev", ResolvedContext.Source.EXPLICIT);
        assertThat(second.session().connection()).isEqualTo(WorkbenchConnection.CONNECTED);
        assertThat(second.session().authentication()).isEqualTo(WorkbenchAuthentication.MACHINE);
        assertThat(second.session().principal()).contains("machine");
        assertThat(first.toString() + second).doesNotContain("machine-token-secret");
        assertThat(client.credentials).containsExactly("machine-token-secret", "machine-token-secret");
        assertThat(client.calls("discover")).isEqualTo(1);
        assertThat(client.calls("list")).isEqualTo(2);
        assertThat(harness.out().toString()).isEmpty();
        assertThat(harness.err().toString()).isEmpty();
    }

    @Test
    void connectedWithoutCredentialIsSignedOutAndDoesNotList() throws Exception {
        writeSource("local");
        NamedFixture bound = boundNamedFixture();
        Harness harness = harness(bound.resolver(), null);
        harness.repl().session().connect(List.of(FIRST), FIRST, "9.1");

        WorkbenchSnapshot snapshot = load(harness.repl());

        assertThat(snapshot.workspace().remoteState())
                .isInstanceOf(WorkbenchRemoteState.SignedOut.class);
        assertThat(snapshot.workspace().rows()).hasSize(1);
        assertThat(snapshot.session().connection()).isEqualTo(WorkbenchConnection.CONNECTED);
        assertThat(snapshot.session().authentication()).isEqualTo(WorkbenchAuthentication.SIGNED_OUT);
        assertThat(snapshot.session().landingNode()).contains(FIRST);
        assertThat(snapshot.session().contextName()).isEmpty();
        assertThat(snapshot.session().contextSource()).isEmpty();
        assertThat(harness.client().calls("discover")).isZero();
        assertThat(harness.client().calls("list")).isZero();
    }

    @Test
    void rejectedListingsUseOnlyStableLocalDiagnosticsAndKeepLocalRows() throws Exception {
        writeSource("local");
        String serverPath = "/private/tmp/server-controlled-secret-path";
        String serverCode = "\u001B[31mserver.denied\u001B[0m";
        String serverMessage = "\u0000token=credential-secret password=sensitive-value " + serverPath;
        RecordingClient rejectedClient = new RecordingClient();
        rejectedClient.outcomes.add(new ListOutcome.Rejected(serverCode, serverMessage));
        Harness rejected = harness(null, null, rejectedClient);
        authenticate(rejected.repl(), "human-secret");

        WorkbenchSnapshot rejectedSnapshot = load(rejected.repl());

        assertThat(serverCode + serverMessage)
                .contains("\u001B", "\u0000", "credential-secret", "sensitive-value", serverPath);
        assertThat(rejectedSnapshot.workspace().remoteState()).isEqualTo(
                new WorkbenchRemoteState.Rejected("workbench.remote-rejected", "Request rejected"));
        assertThat(rejectedSnapshot.workspace().rows()).hasSize(1);
        assertThat(rejectedSnapshot.toString())
                .doesNotContain(
                        serverCode,
                        serverMessage,
                        "credential-secret",
                        "human-secret",
                        "sensitive-value",
                        serverPath)
                .doesNotContainPattern("[\\x00-\\x1f\\x7f]");
        assertThat(rejectedClient.calls("list")).isEqualTo(1);
        assertThat(rejected.out().toString()).isEmpty();
        assertThat(rejected.err().toString()).isEmpty();
    }

    @Test
    void emptyListingRemainsDistinctFromRejection() throws Exception {
        writeSource("local");

        RecordingClient emptyClient = new RecordingClient();
        emptyClient.outcomes.add(new ListOutcome.Listed(List.of()));
        Harness empty = harness(null, null, emptyClient);
        authenticate(empty.repl(), "human-secret");

        WorkbenchSnapshot emptySnapshot = load(empty.repl());

        assertThat(emptySnapshot.workspace().remoteState())
                .isEqualTo(new WorkbenchRemoteState.Available(0));
        assertThat(emptySnapshot.workspace().rows()).hasSize(1);
        assertThat(emptySnapshot.workspace().rows().getFirst().alignment())
                .isEqualTo(WorkbenchAlignment.LOCAL_ONLY);
        assertThat(emptyClient.calls("list")).isEqualTo(1);
    }

    @Test
    void listedArtifactsProduceOneSharedProjectionWithSanitizedSessionMetadata() throws Exception {
        RecordingClient client = new RecordingClient();
        client.outcomes.add(new ListOutcome.Listed(List.of(
                new RemoteArtifact("orders", "pipeline", null, false))));
        Harness harness = harness(null, null, client);
        harness.repl().session().connect(List.of(FIRST), FIRST, "9.1");
        harness.repl().session().authenticate("top-secret-token", "alice", null, List.of(FIRST));

        WorkbenchSnapshot snapshot = harness.repl().workbenchDataSource()
                .load(4, 9, new RefreshRequest.CancellationToken());

        assertThat(snapshot.contextGeneration()).isEqualTo(4);
        assertThat(snapshot.requestSequence()).isEqualTo(9);
        assertThat(snapshot.pipelines().rows()).extracting(row -> row.key().id())
                .containsExactly("orders");
        assertThat(snapshot.session().authentication()).isEqualTo(WorkbenchAuthentication.SIGNED_IN);
        assertThat(snapshot.session().principal()).contains("alice");
        assertThat(snapshot.session().versions()).contains("cli ", "server 9.1");
        assertThat(snapshot.toString()).doesNotContain("top-secret-token", "password");
        assertThat(client.credentials).containsExactly("top-secret-token");
        assertThat(client.kinds).hasSize(1);
        assertThat(client.kinds.getFirst()).isNull();
        assertThat(harness.out().toString()).isEmpty();
        assertThat(harness.err().toString()).isEmpty();
    }

    @Test
    void landingEndpointDropsUserInfoPathQueryAndFragment() throws Exception {
        URI unsafe = URI.create(
                "https://user:userinfo-secret@landing.example:8443/private?token=query-secret#fragment-secret");
        RecordingClient client = new RecordingClient();
        client.outcomes.add(new ListOutcome.Listed(List.of()));
        Harness harness = harness(null, null, client);
        harness.repl().session().connect(List.of(unsafe), unsafe, "9.1");
        harness.repl().session().authenticate("access-secret", "alice", null, List.of(unsafe));

        WorkbenchSnapshot snapshot = load(harness.repl());

        assertThat(snapshot.session().landingNode())
                .contains(URI.create("https://landing.example:8443"));
        assertThat(snapshot.toString()).doesNotContain(
                "userinfo-secret", "query-secret", "fragment-secret", "/private", "access-secret");
    }

    @Test
    void contextDiagnosticsDropSecretFieldsAndRawFailureReasons() throws Exception {
        ContextResolver resolver = new ContextResolver(
                () -> {
                    throw new TapstateException(CliError.WORKSPACE_NOT_WRITABLE,
                            Map.of("path", workspace, "reason", "password=raw-secret"), null);
                },
                name -> null);
        Harness harness = harness(resolver, "dev");

        WorkbenchSnapshot snapshot = load(harness.repl());

        assertThat(snapshot.workspace().remoteState())
                .isInstanceOfSatisfying(WorkbenchRemoteState.Diagnostic.class, diagnostic -> {
                    assertThat(diagnostic.code()).isEqualTo(CliError.WORKSPACE_NOT_WRITABLE);
                    assertThat(diagnostic.arguments()).isEmpty();
                });
        assertThat(snapshot.toString()).doesNotContain("raw-secret", "password=");
        assertThat(harness.out().toString()).isEmpty();
        assertThat(harness.err().toString()).isEmpty();
    }

    @Test
    void unreachableListingRetriesAtMostOnceAfterSilentBoundedFailover() throws Exception {
        RecordingClient client = new RecordingClient();
        client.outcomesByEndpoint.put(FIRST, new ListOutcome.Unreachable());
        client.outcomesByEndpoint.put(SECOND, new ListOutcome.Listed(List.of()));
        client.healthy.add(FIRST);
        client.healthy.add(SECOND);
        Harness harness = harness(null, null, client);
        harness.repl().session().connect(List.of(FIRST, SECOND), FIRST, "9.1");
        harness.repl().session().authenticate("token", "alice", null, List.of(FIRST, SECOND));

        WorkbenchSnapshot snapshot = load(harness.repl());

        assertThat(snapshot.workspace().remoteState())
                .isEqualTo(new WorkbenchRemoteState.Available(0));
        assertThat(client.calls("list")).isEqualTo(2);
        assertThat(client.listEndpoints).containsExactly(FIRST, SECOND);
        assertThat(client.healthEndpoints).containsExactly(SECOND);
        assertThat(harness.repl().session().landingNode()).isEqualTo(SECOND);
        assertThat(harness.out().toString()).isEmpty();
        assertThat(harness.err().toString()).isEmpty();
    }

    @Test
    void unreachableAfterFailoverIsOfflineAndNeverRetriesAgain() throws Exception {
        RecordingClient client = new RecordingClient();
        client.outcomes.add(new ListOutcome.Unreachable());
        client.outcomes.add(new ListOutcome.Unreachable());
        client.healthy.add(SECOND);
        Harness harness = harness(null, null, client);
        authenticate(harness.repl(), "token", List.of(FIRST, SECOND));

        WorkbenchSnapshot snapshot = load(harness.repl());

        assertThat(snapshot.workspace().remoteState())
                .isInstanceOf(WorkbenchRemoteState.Offline.class);
        assertThat(client.calls("list")).isEqualTo(2);
    }

    @Test
    void remoteCardinalityNeverCreatesPerResourceCalls() throws Exception {
        List<RemoteArtifact> artifacts = new ArrayList<>();
        for (int index = 0; index < 100; index++) {
            artifacts.add(new RemoteArtifact("source-" + index, "source", null, false));
        }
        RecordingClient client = new RecordingClient();
        client.outcomes.add(new ListOutcome.Listed(artifacts));
        Harness harness = harness(null, null, client);
        authenticate(harness.repl(), "token");

        WorkbenchSnapshot snapshot = load(harness.repl());

        assertThat(snapshot.workspace().rows()).hasSize(100);
        assertThat(client.calls("list")).isEqualTo(1);
        assertThat(client.calledMethods()).containsExactly("list");
    }

    @Test
    void cancellationStopsBeforeWorkAndBetweenNetworkAndProjection() throws Exception {
        RecordingClient before = new RecordingClient();
        Harness beforeHarness = harness(null, null, before);
        authenticate(beforeHarness.repl(), "token");
        RefreshRequest.CancellationToken alreadyCancelled = new RefreshRequest.CancellationToken();
        alreadyCancelled.cancel();

        assertThatThrownBy(() -> beforeHarness.repl().workbenchDataSource().load(1, 1, alreadyCancelled))
                .isInstanceOf(InterruptedException.class);
        assertThat(before.totalCalls()).isZero();

        RecordingClient during = new RecordingClient();
        RefreshRequest.CancellationToken cancelledByList = new RefreshRequest.CancellationToken();
        during.onList = cancelledByList::cancel;
        during.outcomes.add(new ListOutcome.Unreachable());
        during.healthy.add(SECOND);
        Harness duringHarness = harness(null, null, during);
        authenticate(duringHarness.repl(), "token", List.of(FIRST, SECOND));

        assertThatThrownBy(() -> duringHarness.repl().workbenchDataSource().load(1, 1, cancelledByList))
                .isInstanceOf(InterruptedException.class);
        assertThat(during.calls("list")).isEqualTo(1);
        assertThat(during.calls("isHealthy")).isZero();
    }

    @Test
    void cancellationAfterHealthReturnsCannotRelandOrDisconnectTheSession() throws Exception {
        assertCanceledHealthDoesNotMutateSession(true);
        assertCanceledHealthDoesNotMutateSession(false);
    }

    @Test
    void cancellationAfterNamedDiscoveryReturnsCannotConnectTheSession() throws Exception {
        NamedFixture named = namedFixture();
        RecordingClient client = new RecordingClient();
        client.discoveryOutcome = discovered();
        CountDownLatch discoveryEntered = new CountDownLatch(1);
        CountDownLatch releaseDiscovery = new CountDownLatch(1);
        client.onDiscover = () -> {
            discoveryEntered.countDown();
            awaitIgnoringInterrupt(releaseDiscovery);
        };
        Harness harness = harness(named.resolver(), "dev", client);
        RefreshRequest.CancellationToken token = new RefreshRequest.CancellationToken();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread worker = loadInThread(harness.repl(), token, failure);
        assertThat(discoveryEntered.await(2, TimeUnit.SECONDS)).isTrue();

        token.cancel();
        releaseDiscovery.countDown();
        worker.join(TimeUnit.SECONDS.toMillis(2));

        assertThat(worker.isAlive()).isFalse();
        assertThat(failure.get()).isInstanceOf(InterruptedException.class);
        assertThat(harness.repl().session().isConnected()).isFalse();
        assertThat(client.calls("list")).isZero();
    }

    private WorkbenchSnapshot load(Repl repl) throws Exception {
        return repl.workbenchDataSource().load(1, 1, new RefreshRequest.CancellationToken());
    }

    private void writeSource(String id) throws Exception {
        Path source = Files.createDirectories(workspace.resolve("source")).resolve(id + ".tap.yml");
        Files.writeString(source, """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mysql
                """.formatted(id));
    }

    private NamedFixture namedFixture() {
        ContextDefinition definition = new ContextDefinition(
                UUID.randomUUID(), List.of(FIRST), new ContextTls(true), UUID.randomUUID());
        ContextConfig config = new ContextConfig(1, null, Map.of("dev", definition), Map.of());
        return new NamedFixture(definition, new ContextResolver(() -> config, name -> null));
    }

    private NamedFixture boundNamedFixture() throws Exception {
        ContextDefinition definition = new ContextDefinition(
                UUID.randomUUID(), List.of(SECOND), new ContextTls(true), UUID.randomUUID());
        ContextConfig config = new ContextConfig(
                1,
                null,
                Map.of("bound", definition),
                Map.of(workspace.toRealPath().toString(), "bound"));
        return new NamedFixture(definition, new ContextResolver(() -> config, name -> null));
    }

    private static void assertNamedContext(
            WorkbenchSnapshot snapshot,
            String name,
            ResolvedContext.Source source) {
        assertThat(snapshot.session().contextName()).contains(name);
        assertThat(snapshot.session().contextSource()).contains(source);
    }

    private Harness harness(ContextResolver resolver, String explicitContext) {
        return harness(resolver, explicitContext, new RecordingClient());
    }

    private Harness harness(ContextResolver resolver, String explicitContext, RecordingClient client) {
        return harness(resolver, explicitContext, client, null);
    }

    private Harness harness(
            ContextResolver resolver,
            String explicitContext,
            RecordingClient client,
            AuthService authService) {
        CommandLine commandLine = Cli.newCommandLine();
        StringWriter out = new StringWriter();
        StringWriter err = new StringWriter();
        commandLine.setOut(new PrintWriter(out));
        commandLine.setErr(new PrintWriter(err));
        Repl repl = new Repl(commandLine, workspace, client.proxy, new ScriptedPrompter(),
                name -> null, resolver, explicitContext, authService, null);
        return new Harness(repl, client, out, err);
    }

    private void assertCanceledHealthDoesNotMutateSession(boolean healthy) throws Exception {
        RecordingClient client = new RecordingClient();
        client.outcomes.add(new ListOutcome.Unreachable());
        if (healthy) {
            client.healthy.add(SECOND);
        }
        CountDownLatch healthEntered = new CountDownLatch(1);
        CountDownLatch releaseHealth = new CountDownLatch(1);
        client.onHealthy = () -> {
            healthEntered.countDown();
            awaitIgnoringInterrupt(releaseHealth);
        };
        Harness harness = harness(null, null, client);
        harness.repl().session().connect(List.of(SECOND), FIRST, "9.1");
        harness.repl().session().authenticate("token", "alice", null, List.of(SECOND));
        RefreshRequest.CancellationToken token = new RefreshRequest.CancellationToken();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread worker = loadInThread(harness.repl(), token, failure);
        assertThat(healthEntered.await(2, TimeUnit.SECONDS)).isTrue();

        token.cancel();
        releaseHealth.countDown();
        worker.join(TimeUnit.SECONDS.toMillis(2));

        assertThat(worker.isAlive()).isFalse();
        assertThat(failure.get()).isInstanceOf(InterruptedException.class);
        assertThat(harness.repl().session().isConnected()).isTrue();
        assertThat(harness.repl().session().landingNode()).isEqualTo(FIRST);
    }

    private Thread loadInThread(
            Repl repl,
            RefreshRequest.CancellationToken token,
            AtomicReference<Throwable> failure) {
        Thread worker = new Thread(() -> {
            try {
                repl.workbenchDataSource().load(1, 1, token);
            } catch (Throwable thrown) {
                failure.set(thrown);
            }
        }, "workbench-data-source-test");
        worker.start();
        return worker;
    }

    private static DiscoveryOutcome.Discovered discovered() {
        return new DiscoveryOutcome.Discovered(
                "urn:tapstate:cluster:test-cluster",
                "test-cluster",
                "tapstate/v1",
                List.of("password", "machine_token"));
    }

    private static void awaitIgnoringInterrupt(CountDownLatch latch) {
        boolean interrupted = false;
        while (true) {
            try {
                latch.await();
                break;
            } catch (InterruptedException ignored) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static void authenticate(Repl repl, String credential) {
        authenticate(repl, credential, List.of(FIRST));
    }

    private static void authenticate(Repl repl, String credential, List<URI> members) {
        repl.session().connect(members, members.getFirst(), "9.1");
        repl.session().authenticate(credential, "alice", null, members);
    }

    private record Harness(Repl repl, RecordingClient client, StringWriter out, StringWriter err) {
    }

    private record NamedFixture(ContextDefinition definition, ContextResolver resolver) {
    }

    private static final class RecordingClient {
        private final Map<String, Integer> calls = new HashMap<>();
        private final ArrayDeque<ListOutcome> outcomes = new ArrayDeque<>();
        private final Map<URI, ListOutcome> outcomesByEndpoint = new HashMap<>();
        private final Set<URI> healthy = new HashSet<>();
        private final List<String> credentials = new ArrayList<>();
        private final List<String> kinds = new ArrayList<>();
        private final List<URI> listEndpoints = new ArrayList<>();
        private final List<URI> healthEndpoints = new ArrayList<>();
        private Runnable onList = () -> { };
        private Runnable onDiscover = () -> { };
        private Runnable onHealthy = () -> { };
        private DiscoveryOutcome discoveryOutcome = new DiscoveryOutcome.Unreachable();
        private SessionExchangeOutcome exchangeOutcome = new SessionExchangeOutcome.Unreachable();
        private final ControlPlaneClient proxy = (ControlPlaneClient) Proxy.newProxyInstance(
                ControlPlaneClient.class.getClassLoader(),
                new Class<?>[]{ControlPlaneClient.class},
                (ignored, method, arguments) -> {
                    String name = method.getName();
                    if (name.equals("toString")) {
                        return "RecordingClient";
                    }
                    calls.merge(name, 1, Integer::sum);
                    return switch (name) {
                        case "list" -> {
                            URI endpoint = (URI) arguments[0];
                            listEndpoints.add(endpoint);
                            credentials.add((String) arguments[1]);
                            kinds.add((String) arguments[2]);
                            onList.run();
                            yield outcomesByEndpoint.containsKey(endpoint)
                                    ? outcomesByEndpoint.get(endpoint)
                                    : outcomes.isEmpty() ? new ListOutcome.Unreachable() : outcomes.removeFirst();
                        }
                        case "isHealthy" -> {
                            healthEndpoints.add((URI) arguments[0]);
                            onHealthy.run();
                            yield healthy.contains((URI) arguments[0]);
                        }
                        case "discover" -> {
                            onDiscover.run();
                            yield discoveryOutcome;
                        }
                        case "exchangeSession" -> exchangeOutcome;
                        case "close" -> null;
                        default -> defaultValue(method.getReturnType());
                    };
                });

        private int calls(String method) {
            return calls.getOrDefault(method, 0);
        }

        private int totalCalls() {
            return calls.values().stream().mapToInt(Integer::intValue).sum();
        }

        private List<String> calledMethods() {
            return calls.entrySet().stream()
                    .filter(entry -> entry.getValue() > 0)
                    .map(Map.Entry::getKey)
                    .sorted()
                    .toList();
        }

        private static Object defaultValue(Class<?> type) {
            if (type == boolean.class) {
                return false;
            }
            if (type == int.class) {
                return 0;
            }
            if (type == long.class) {
                return 0L;
            }
            return null;
        }
    }
}
