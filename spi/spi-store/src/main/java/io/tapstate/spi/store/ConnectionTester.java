package io.tapstate.spi.store;

/**
 * Tests a stored connection by driving the configured connector's own connection test and normalizing
 * the outcome into a {@link ConnectionTestResult}: an overall pass/fail plus the per-item checks the
 * connector reported.
 *
 * <p>A connector that reports a failed check is a normal result whose {@link ConnectionTestResult.Outcome}
 * is FAILED — not a thrown error; a warning check never fails the outcome. Only a failure that prevents
 * the test from running at all (the connector cannot be loaded / level-gated, or throws out of its own
 * test) surfaces as a coded exception. The port carries no connector-framework types.
 *
 * <p>An {@link ExecutionPort}: it drives a connector, so it runs on the runtime side and control
 * reaches it only through the whitelisted probe seam.
 */
public interface ConnectionTester extends ExecutionPort {

    /** Tests {@code config} by driving its connector and returns the normalized result. */
    ConnectionTestResult test(ConnectionConfig config);

    /** The default Source carries its accepting node's monotonic deadline through the same test action. */
    default ConnectionTestResult testDefault(ConnectionConfig config, long deadlineNanos) { return test(config); }
}
