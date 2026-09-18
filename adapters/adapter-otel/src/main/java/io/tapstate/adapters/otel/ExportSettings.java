package io.tapstate.adapters.otel;

import java.time.Duration;
import java.util.Objects;

/**
 * Where the facts go, and how often. Two destinations, either or both: a Prometheus that pulls from a
 * scrape endpoint this process serves, and a collector that is pushed to over OTLP on a cadence. Nothing
 * configured is a valid setting, and it is not this adapter's to serve: the assembly offers through
 * {@code MetricsExport.none()} instead, so the SDK is never started for nobody.
 *
 * @param otlpEndpoint   the OTLP endpoint to push to, or null when nothing is pushed. Over HTTP it is the
 *                       full URL including the {@code /v1/metrics} path; over gRPC it is the host and port
 * @param otlpProtocol   how the push travels; ignored without an endpoint
 * @param otlpInterval   how often the push happens; required with an endpoint
 * @param prometheusHost the interface the scrape endpoint listens on, or null to listen on all of them
 * @param prometheusPort the port the scrape endpoint listens on, or null when nothing is served
 */
public record ExportSettings(String otlpEndpoint, Protocol otlpProtocol, Duration otlpInterval,
        String prometheusHost, Integer prometheusPort) {

    /** How an OTLP push travels. */
    public enum Protocol {
        HTTP_PROTOBUF,
        GRPC
    }

    public ExportSettings {
        otlpProtocol = otlpProtocol == null ? Protocol.HTTP_PROTOBUF : otlpProtocol;
        if (otlpEndpoint != null) {
            if (otlpEndpoint.isBlank()) {
                throw new IllegalArgumentException("an OTLP endpoint is a URL, not blank");
            }
            Objects.requireNonNull(otlpInterval, "otlpInterval");
            if (otlpInterval.isZero() || otlpInterval.isNegative()) {
                throw new IllegalArgumentException("an OTLP push interval is a positive length of time: " + otlpInterval);
            }
        }
        if (prometheusPort != null && (prometheusPort < 0 || prometheusPort > 65_535)) {
            throw new IllegalArgumentException("a port is between 0 and 65535: " + prometheusPort);
        }
    }

    /** Whether anything at all is configured to receive the facts. */
    public boolean exportsAnything() {
        return otlpEndpoint != null || prometheusPort != null;
    }

    /** A scrape endpoint and nothing else. */
    public static ExportSettings prometheusOn(String host, int port) {
        return new ExportSettings(null, null, null, host, port);
    }

    /** A push and nothing else. */
    public static ExportSettings otlpTo(String endpoint, Protocol protocol, Duration interval) {
        return new ExportSettings(endpoint, protocol, interval, null, null);
    }
}
