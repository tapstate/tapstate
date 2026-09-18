package io.tapstate.app;

import io.tapstate.adapters.otel.OtelMetricsExport;
import io.tapstate.spi.metrics.MetricsExport;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Nothing configured means nothing started: the convergence loop offers through the port's no-op, no
 * SDK is brought up, no port is opened. The read faces never depended on this bean, so with it at none()
 * they are, by construction, what they were before export existed. One key set is what starts the
 * adapter -- and it is closed again here, because a listener a test opened and left is a port the next
 * test cannot have.
 */
class MetricsExportIsOptInTest {

    @Test
    void withNeitherEndpointNorPortTheExportIsTheNoOp() {
        MetricsExportProperties properties = new MetricsExportProperties();

        MetricsExport export = RuntimeConvergenceConfiguration.metricsExportFor(properties);

        assertThat(export).isSameAs(MetricsExport.none());
        assertThat(properties.settings().exportsAnything()).isFalse();
    }

    @Test
    void theScrapeEndpointListensOnTheLoopbackUnlessItIsWidenedOnPurpose() {
        // Setting the port alone is what an operator following the commented example does. What the
        // endpoint then serves is an inventory rather than a summary -- pipeline, table, chain and nest
        // namespace ids all ride as attribute values -- and a scrape endpoint carries no authentication
        // anywhere, which is why the interface it listens on is the loopback until somebody widens it.
        MetricsExportProperties defaults = new MetricsExportProperties();
        defaults.getPrometheus().setPort(9464);

        assertThat(defaults.settings().prometheusHost()).isEqualTo("127.0.0.1");

        MetricsExportProperties widened = new MetricsExportProperties();
        widened.getPrometheus().setPort(9464);
        widened.getPrometheus().setHost("0.0.0.0");

        assertThat(widened.settings().prometheusHost()).isEqualTo("0.0.0.0");
    }

    @Test
    void aPrometheusPortAloneStartsTheAdapterOnThatPort() throws IOException {
        int port;
        try (ServerSocket socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        }
        MetricsExportProperties properties = new MetricsExportProperties();
        properties.getPrometheus().setHost("127.0.0.1");
        properties.getPrometheus().setPort(port);

        try (MetricsExport export = RuntimeConvergenceConfiguration.metricsExportFor(properties)) {
            assertThat(export).isInstanceOf(OtelMetricsExport.class);
            assertThat(((OtelMetricsExport) export).settings().prometheusPort()).isEqualTo(port);
            assertThat(((OtelMetricsExport) export).settings().otlpEndpoint()).isNull();
        }
    }
}
