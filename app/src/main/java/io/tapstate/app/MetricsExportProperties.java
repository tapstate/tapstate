package io.tapstate.app;

import io.tapstate.adapters.otel.ExportSettings;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Where the measured facts are exported to, if anywhere. Export is opt-in: with neither an OTLP
 * endpoint nor a Prometheus port configured nothing is started, no listener is opened and no push is
 * made, and every read face behaves exactly as it does with them configured. The facts, their names
 * and their attributes are the same either way; this only says who else gets to see them.
 */
@ConfigurationProperties("tapstate.metrics.export")
public class MetricsExportProperties {

    private final Otlp otlp = new Otlp();
    private final Prometheus prometheus = new Prometheus();

    public Otlp getOtlp() {
        return otlp;
    }

    public Prometheus getPrometheus() {
        return prometheus;
    }

    /** What the properties configure, as the adapter reads them. */
    ExportSettings settings() {
        return new ExportSettings(otlp.getEndpoint(), otlp.getProtocol(), otlp.getInterval(),
                prometheus.getHost(), prometheus.getPort());
    }

    /** A collector that is pushed to. */
    public static class Otlp {

        /**
         * The endpoint to push to; unset means no push. Over http-protobuf it is the full URL including
         * the {@code /v1/metrics} path; over grpc it is the scheme, host and port.
         */
        private String endpoint;

        /** {@code http-protobuf} or {@code grpc}. */
        private ExportSettings.Protocol protocol = ExportSettings.Protocol.HTTP_PROTOBUF;

        /** How often the push happens. */
        private Duration interval = Duration.ofSeconds(60);

        public String getEndpoint() {
            return endpoint;
        }

        public void setEndpoint(String endpoint) {
            this.endpoint = endpoint;
        }

        public ExportSettings.Protocol getProtocol() {
            return protocol;
        }

        public void setProtocol(ExportSettings.Protocol protocol) {
            this.protocol = protocol;
        }

        public Duration getInterval() {
            return interval;
        }

        public void setInterval(Duration interval) {
            this.interval = interval;
        }
    }

    /** A Prometheus that pulls from a scrape endpoint this process serves. */
    public static class Prometheus {

        /** The port the scrape endpoint listens on; unset means none is served. */
        private Integer port;

        /**
         * The interface it listens on. The loopback, unless it is widened here on purpose.
         *
         * <p>What the endpoint serves is an inventory rather than a summary: pipeline ids, table ids,
         * chain ids and nest namespaces all ride as attribute values, and there is no authentication in
         * front of it — a scrape endpoint has none anywhere, which is why the usual place for one is a
         * network a scraper is already inside. A Prometheus on this host reaches the loopback; one in
         * another container reaches this process by its container address, which is what widening this
         * is for.
         */
        private String host = "127.0.0.1";

        public Integer getPort() {
            return port;
        }

        public void setPort(Integer port) {
            this.port = port;
        }

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }
    }
}
