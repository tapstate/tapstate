package io.tapstate.cli;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A control plane that answers the health probe with a fixed verdict and remembers what was probed.
 * Every other call fails the test: the guided first run has no business talking to the server beyond
 * the probe, so a second call is a defect, not something to stub.
 */
final class FakeHealthProbe implements ControlPlaneClient {
    private final boolean healthy;
    final List<URI> probed = new ArrayList<>();

    FakeHealthProbe(boolean healthy) {
        this.healthy = healthy;
    }

    @Override
    public boolean isHealthy(URI baseUrl) {
        probed.add(baseUrl);
        return healthy;
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
