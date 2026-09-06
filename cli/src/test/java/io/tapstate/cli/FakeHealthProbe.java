package io.tapstate.cli;

import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A control plane for the guided first run: it answers the health probe with a verdict a test can
 * flip, and the sign-in that follows a healthy answer - issuer discovery and the login itself, with
 * whatever outcome the test scripted (a saved-session success by default). It remembers what was
 * probed and every login attempted, so a test can assert the order and the credentials. The one call
 * after sign-in it answers is the connector list, which a stack that just came up is polled for: the
 * staged connectors are listed as {@code bundled} from the first answer and flip to {@code registered}
 * after as many lists as the test scripted, or never. Every other call fails
 * the test: the first run has no business talking to the server beyond these, so another call is a
 * defect, not something to stub.
 */
final class FakeHealthProbe implements ControlPlaneClient {

    static final String ISSUER = "urn:tapstate:cluster:test-cluster";

    /** What the probe answers; a test flips it when the fake stack it scripted "comes up". */
    boolean healthy;
    /** When set, only a server on this host answers the probe, whatever {@link #healthy} says. */
    String healthyHost;
    final List<URI> probed = new ArrayList<>();
    /** Every login attempted, as {@code <username>:<password>}, in order. */
    final List<String> logins = new ArrayList<>();
    /** What a login is answered with; null means a success carrying a persistent session. */
    LoginOutcome loginOutcome;
    /** How many lists answer with the staged connectors still {@code bundled} before they register; 0 means at once. */
    int connectorListsBeforeSeeded;
    /** When set, the staged connectors stay {@code bundled} for good, however often the list is asked for. */
    boolean connectorsNeverSeeded;
    /** How many times the connector list was asked for. */
    int connectorLists;

    FakeHealthProbe(boolean healthy) {
        this.healthy = healthy;
    }

    @Override
    public boolean isHealthy(URI baseUrl) {
        probed.add(baseUrl);
        return healthyHost != null ? healthyHost.equals(baseUrl.getHost()) : healthy;
    }

    @Override
    public DiscoveryOutcome discover(URI baseUrl) {
        return new DiscoveryOutcome.Discovered(ISSUER, "test-cluster", "tapstate/v1",
                List.of("password", "machine_token"));
    }

    @Override
    public LoginOutcome login(URI baseUrl, String username, String password, boolean createSession) {
        logins.add(username + ":" + password);
        if (loginOutcome != null) {
            return loginOutcome;
        }
        Instant now = Instant.now();
        return new LoginOutcome.Success("jwt-first-run", now.plusSeconds(900), ISSUER, username,
                List.of("read", "write"), "tss_s01.first-run-secret",
                now.plusSeconds(2_592_000), now.plusSeconds(7_776_000));
    }

    @Override public String serverVersion(URI baseUrl) { throw new AssertionError(); }
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
    @Override
    public ConnectorListOutcome connectorList(URI baseUrl, String credential) {
        connectorLists++;
        boolean loaded = !connectorsNeverSeeded && connectorLists > connectorListsBeforeSeeded;
        String origin = loaded ? "registered" : "bundled";
        return new ConnectorListOutcome.Listed(LocalStack.CONNECTOR_JARS.stream()
                .map(id -> new CatalogConnector(id, id, "database", List.of("cdc"), true, origin))
                .toList());
    }
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
