package io.tapstate.runtime.probe;

import io.tapstate.spi.store.ConnectionConfig;
import io.tapstate.spi.store.ConnectionTestResult;
import io.tapstate.spi.store.ConnectionTester;
import java.util.Objects;

/**
 * The runtime-side connection probe: it fulfils the synchronous control-to-runtime seam by driving the
 * connector-plane connection-tester port and returning its normalized result. The tester implementation
 * — the one that opens the connector through the PDK — is injected by the app assembly root, so the
 * runtime ring never compiles against an adapter. In the first landing this delegation is a direct
 * in-process call; when the control and runtime roles split, this probe is the engine-side handler the
 * control plane reaches across instances.
 */
public final class DelegatingConnectionProbe implements ConnectionProbe {

    private final ConnectionTester tester;

    public DelegatingConnectionProbe(ConnectionTester tester) {
        this.tester = Objects.requireNonNull(tester, "tester");
    }

    @Override public ConnectionTestResult probeDefault(ConnectionConfig config, long deadlineNanos) {
        return tester.testDefault(config, deadlineNanos);
    }

    @Override
    public ConnectionTestResult probe(ConnectionConfig config) {
        return tester.test(config);
    }
}
