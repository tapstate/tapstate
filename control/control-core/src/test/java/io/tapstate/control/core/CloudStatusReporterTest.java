package io.tapstate.control.core;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class CloudStatusReporterTest {

    @Test
    void everyReportUsesTheSingleProjectionAndAFreshNonce() {
        CloudRuntimeStatus status = new CloudRuntimeStatus(
                "0.6.0", 42_000L, 3, Instant.parse("2026-09-28T11:59:00Z"));
        List<String> sent = new ArrayList<>();
        List<String> nonces = new ArrayList<>(List.of("nonce-a", "nonce-b"));
        CloudStatusReporter reporter = new CloudStatusReporter(
                "cluster-a",
                () -> status,
                (clusterId, nonce, snapshot) -> sent.add(clusterId + ":" + nonce + ":" + snapshot.activePipelines()),
                () -> nonces.removeFirst());

        assertThat(reporter.report()).isSameAs(status);
        assertThat(reporter.report()).isSameAs(status);
        assertThat(sent).containsExactly("cluster-a:nonce-a:3", "cluster-a:nonce-b:3");
    }
}
