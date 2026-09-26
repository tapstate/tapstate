package io.tapstate.adapters.otel;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.exporter.prometheus.PrometheusHttpServer;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.SdkMeterProviderBuilder;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.metrics.MetricsExport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * The metrics export port carried out by the OpenTelemetry SDK: the facts each pipeline last reported are
 * held by a producer the SDK collects from, and served to a Prometheus scrape endpoint, pushed to an OTLP
 * endpoint, or both, as configured. The SDK owns the wire and the cadence; the facts own everything the
 * numbers mean.
 *
 * <p>Started only when something is configured to receive the facts. An assembly with no endpoint does
 * not start this and offers through the port's no-op instead, so a process nobody monitors carries no
 * exporter, no listener and no push.
 */
public final class OtelMetricsExport implements MetricsExport {

    private static final Logger LOG = LoggerFactory.getLogger(OtelMetricsExport.class);

    /** The service name every exported series carries, for a backend that keys by it. */
    static final String SERVICE_NAME = "tapstate";

    private final SdkMeterProvider provider;
    private final FactsMetricProducer producer;
    private final ExportSettings settings;

    private OtelMetricsExport(SdkMeterProvider provider, FactsMetricProducer producer, ExportSettings settings) {
        this.provider = provider;
        this.producer = producer;
        this.settings = settings;
    }

    /**
     * Brings up the SDK with a reader per configured destination. Refuses a setting that names none: the
     * caller meant {@link MetricsExport#none()}, and an SDK started for nobody would be a listener and a
     * thread that no configuration accounts for.
     */
    public static OtelMetricsExport start(ExportSettings settings) {
        Objects.requireNonNull(settings, "settings");
        if (!settings.exportsAnything()) {
            throw new IllegalArgumentException(
                    "nothing is configured to receive the facts: name an OTLP endpoint or a Prometheus port, "
                            + "or offer through MetricsExport.none()");
        }
        FactsMetricProducer producer = new FactsMetricProducer(Instant.now());
        SdkMeterProviderBuilder builder = SdkMeterProvider.builder()
                .setResource(Resource.getDefault().merge(Resource.create(
                        Attributes.of(AttributeKey.stringKey("service.name"), SERVICE_NAME))))
                .registerMetricProducer(producer);
        if (settings.prometheusPort() != null) {
            builder.registerMetricReader(PrometheusHttpServer.builder()
                    .setHost(settings.prometheusHost() == null ? "0.0.0.0" : settings.prometheusHost())
                    .setPort(settings.prometheusPort())
                    .build());
        }
        if (settings.otlpEndpoint() != null) {
            MetricExporter exporter = switch (settings.otlpProtocol()) {
                case GRPC -> OtlpGrpcMetricExporter.builder().setEndpoint(settings.otlpEndpoint()).build();
                case HTTP_PROTOBUF -> OtlpHttpMetricExporter.builder().setEndpoint(settings.otlpEndpoint()).build();
            };
            builder.registerMetricReader(PeriodicMetricReader.builder(exporter)
                    .setInterval(settings.otlpInterval())
                    .build());
        }
        SdkMeterProvider provider = builder.build();
        LOG.info("Metrics export started: prometheus={} otlp={} ({} every {})",
                settings.prometheusPort() == null ? "off"
                        : (settings.prometheusHost() == null ? "0.0.0.0" : settings.prometheusHost())
                                + ":" + settings.prometheusPort(),
                settings.otlpEndpoint() == null ? "off" : settings.otlpEndpoint(),
                settings.otlpProtocol(), settings.otlpInterval());
        return new OtelMetricsExport(provider, producer, settings);
    }

    @Override
    public void offer(String pipelineId, PipelineState state, Instant observedAt, List<MetricFact> facts) {
        producer.offer(pipelineId, state, observedAt, facts);
    }

    @Override
    public void observeProcess(Supplier<List<MetricFact>> facts) {
        producer.observeProcess(facts);
    }

    @Override
    public void forgetPipelinesOutside(Collection<String> pipelineIds) {
        producer.forgetPipelinesOutside(pipelineIds);
    }

    /** Pushes what is held to every push reader now, for a caller that cannot wait for the cadence; true when every push completed. */
    public boolean flush(Duration within) {
        return provider.forceFlush().join(within.toMillis(), TimeUnit.MILLISECONDS).isSuccess();
    }

    /** What this export was started with. */
    public ExportSettings settings() {
        return settings;
    }

    @Override
    public void close() {
        provider.close();
        LOG.info("Metrics export stopped");
    }
}
