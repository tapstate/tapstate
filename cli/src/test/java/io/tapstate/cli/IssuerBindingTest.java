package io.tapstate.cli;

import io.tapstate.core.common.TapstateException;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IssuerBindingTest {

    private static final String ISSUER = "urn:tapstate:cluster:01J5FIXTURE";

    @Test
    void allSeedsMustAgreeBeforeAStoredCredentialCanBeUsed() {
        URI first = URI.create("https://one.example.com");
        URI second = URI.create("https://two.example.com");
        RecordingDiscovery client = new RecordingDiscovery(Map.of(
                first, discovered(ISSUER),
                second, discovered(ISSUER)));
        AtomicInteger credentialBytes = new AtomicInteger();

        IssuerBinding.Verified verified = new IssuerBinding(client).verify(context(first, second), ISSUER);
        String answer = verified.withCredential("cached-secret", (seed, credential) -> {
            credentialBytes.addAndGet(credential.getBytes().length);
            return seed.toString();
        });

        assertThat(answer).isEqualTo(first.toString());
        assertThat(client.authorizationHeaders).containsExactly(null, null);
        assertThat(credentialBytes).hasValue("cached-secret".getBytes().length);
    }

    @Test
    void seedConsistencyIsBoundToIssuerRatherThanIncidentalDiscoveryMetadata() {
        URI first = URI.create("https://one.example.com");
        URI second = URI.create("https://two.example.com");
        RecordingDiscovery client = new RecordingDiscovery(Map.of(
                first, discovered(ISSUER),
                second, new DiscoveryOutcome.Discovered(ISSUER, "01J5FIXTURE", "tapstate/v1",
                        List.of("machine_token", "password"))));

        assertThat(new IssuerBinding(client).verify(context(first, second), ISSUER).issuer())
                .isEqualTo(ISSUER);
    }

    @Test
    void aReplacementSeedWithAnotherIssuerFailsBeforeAnyCredentialByteIsExposed() {
        URI first = URI.create("https://one.example.com");
        URI replacement = URI.create("https://replacement.example.com");
        RecordingDiscovery client = new RecordingDiscovery(Map.of(
                first, discovered(ISSUER),
                replacement, discovered("urn:tapstate:cluster:OTHER")));
        AtomicInteger credentialBytes = new AtomicInteger();

        assertThatThrownBy(() -> new IssuerBinding(client).verify(context(first, replacement), ISSUER)
                .withCredential("cached-secret", (seed, credential) -> {
                    credentialBytes.addAndGet(credential.length());
                    return null;
                }))
                .isInstanceOfSatisfying(TapstateException.class, error ->
                        assertThat(error.code().code()).isEqualTo("cli.auth-issuer-mismatch"));

        assertThat(client.authorizationHeaders).containsExactly(null, null);
        assertThat(credentialBytes).hasValue(0);
    }

    @Test
    void remotePlaintextIsRejectedBeforeDiscoveryButLoopbackHttpRemainsAvailable() {
        URI remote = URI.create("http://tapstate.example.com:8080");
        RecordingDiscovery remoteClient = new RecordingDiscovery(Map.of(remote, discovered(ISSUER)));

        assertThatThrownBy(() -> new IssuerBinding(remoteClient).verify(context(remote), ISSUER))
                .isInstanceOfSatisfying(TapstateException.class, error ->
                        assertThat(error.code().code()).isEqualTo("cli.remote-plaintext"));
        assertThat(remoteClient.requests).hasValue(0);

        for (URI loopback : List.of(
                URI.create("http://localhost:8080"),
                URI.create("http://127.0.0.1:8080"),
                URI.create("http://[::1]:8080"))) {
            RecordingDiscovery loopbackClient = new RecordingDiscovery(Map.of(loopback, discovered(ISSUER)));
            assertThat(new IssuerBinding(loopbackClient).verify(context(loopback), ISSUER).issuer())
                    .isEqualTo(ISSUER);
            assertThat(loopbackClient.requests).hasValue(1);
        }
    }

    @Test
    void malformedDiscoveryIsAnInvalidSeedCodedDiagnostic() {
        URI seed = URI.create("https://broken.example.com");
        RecordingDiscovery client = new RecordingDiscovery(Map.of(seed,
                new DiscoveryOutcome.Discovered("not-a-cluster-issuer", "cluster-a", "tapstate/v1",
                        List.of("password"))));

        assertThatThrownBy(() -> new IssuerBinding(client).verify(context(seed), ISSUER))
                .isInstanceOfSatisfying(TapstateException.class, error -> {
                    assertThat(error.code().code()).isEqualTo("cli.issuer-discovery-invalid");
                    assertThat(error.args()).containsEntry("seed", seed.toString());
                });
    }

    @Test
    void discoveryWithoutAnIssuerOrClusterIdIsRejectedAsInvalid() {
        URI seed = URI.create("https://broken.example.com");
        RecordingDiscovery client = new RecordingDiscovery(Map.of(seed,
                new DiscoveryOutcome.Discovered(null, null, "tapstate/v1",
                        List.of("password", "machine_token"))));

        assertThatThrownBy(() -> new IssuerBinding(client).verify(context(seed), null))
                .isInstanceOfSatisfying(TapstateException.class, error ->
                        assertThat(error.code().code()).isEqualTo("cli.issuer-discovery-invalid"));
    }

    private static DiscoveryOutcome.Discovered discovered(String issuer) {
        String clusterId = issuer.substring(issuer.lastIndexOf(':') + 1);
        return new DiscoveryOutcome.Discovered(
                issuer, clusterId, "tapstate/v1", List.of("password", "machine_token"));
    }

    private static ContextDefinition context(URI... seeds) {
        return new ContextDefinition(UUID.randomUUID(), List.of(seeds), new ContextTls(true), UUID.randomUUID());
    }

    private static final class RecordingDiscovery implements ControlPlaneClient {
        private final Map<URI, DiscoveryOutcome> outcomes;
        private final List<String> authorizationHeaders = new java.util.ArrayList<>();
        private final AtomicInteger requests = new AtomicInteger();

        private RecordingDiscovery(Map<URI, DiscoveryOutcome> outcomes) {
            this.outcomes = new LinkedHashMap<>(outcomes);
        }

        @Override
        public DiscoveryOutcome discover(URI baseUrl) {
            requests.incrementAndGet();
            authorizationHeaders.add(null);
            return outcomes.getOrDefault(baseUrl, new DiscoveryOutcome.Unreachable());
        }

        @Override public boolean isHealthy(URI baseUrl) { return false; }
        @Override public LoginOutcome login(URI baseUrl, String username, String password) { throw new AssertionError(); }
        @Override public ApplyOutcome apply(URI baseUrl, String credential, List<LocalDraft> drafts) { throw new AssertionError(); }
        @Override public GetOutcome get(URI baseUrl, String credential, String id) { throw new AssertionError(); }
        @Override public DeleteOutcome delete(URI baseUrl, String credential, String id, String hash) { throw new AssertionError(); }
        @Override public ListOutcome list(URI baseUrl, String credential, String kind) { throw new AssertionError(); }
        @Override public ConnectionTestOutcome test(URI u, String c, String id, String connector, Map<String, Object> s) { throw new AssertionError(); }
        @Override public ConnectionTestResultOutcome testResult(URI u, String c, String id) { throw new AssertionError(); }
        @Override public ConnectionDiscoverSchemaOutcome discoverSchema(URI u, String c, String id, String connector, Map<String, Object> s) { throw new AssertionError(); }
        @Override public ConnectionSchemaOutcome schema(URI u, String c, String id) { throw new AssertionError(); }
        @Override public ConnectorRegisterOutcome register(URI u, String c, byte[] a) { throw new AssertionError(); }
        @Override public ConnectorListOutcome connectorList(URI u, String c) { throw new AssertionError(); }
        @Override public DataBrowserOutcome.Collections collections(URI u, String c, String id) { throw new AssertionError(); }
        @Override public DataBrowserOutcome.Stats stats(URI u, String c, String id, String collection) { throw new AssertionError(); }
        @Override public DataBrowserOutcome.Find find(URI u, String c, String id, String collection, Object f, DataBrowserCall.Order o, Integer l) { throw new AssertionError(); }
        @Override public LifecycleOutcome lifecycle(URI u, String c, String id, String v) { throw new AssertionError(); }
        @Override public StatusOutcome status(URI u, String c, String id) { throw new AssertionError(); }
        @Override public MetricsOutcome metrics(URI u, String c, String id) { throw new AssertionError(); }
        @Override public SnapshotOutcome snapshot(URI u, String c, String id) { throw new AssertionError(); }
        @Override public LogsOutcome logs(URI u, String c, String id) { throw new AssertionError(); }
        @Override public String watchStatus(URI u, String c, String id, StatusStream s, java.util.function.BooleanSupplier stop) { throw new AssertionError(); }
        @Override public String followLogs(URI u, String c, String id, LogStream s, java.util.function.BooleanSupplier stop) { throw new AssertionError(); }
        @Override public String tail(URI u, String c, String id, String collection, Object f, TailStream s, java.util.function.BooleanSupplier stop) { throw new AssertionError(); }
    }
}
