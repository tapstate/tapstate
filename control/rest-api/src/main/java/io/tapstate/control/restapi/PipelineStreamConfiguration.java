package io.tapstate.control.restapi;

import io.tapstate.control.core.ControlOperations;
import io.tapstate.control.core.CredentialAuthenticator;
import io.tapstate.control.core.PipelineCatalogService;
import io.tapstate.control.core.PipelineLogQueryService;
import io.tapstate.control.core.PipelineObservationQueryService;
import io.tapstate.messages.MessageCatalog;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;

import java.time.Duration;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * Mounts the two thin streaming read channels on the same HTTP face as the one-shot verbs: a status
 * watch at {@code /api/pipelines/{id}/status/watch} and a logs follow at
 * {@code /api/pipelines/{id}/logs/follow}. They carry only the two category-2 CLI sugars (watch /
 * follow) and add no registry operation — each rides the grade of the underlying read it streams, so its
 * handshake is guarded by a {@link PipelineStreamHandshakeInterceptor} bound to that read operation.
 *
 * <p>This is Spring's raw websocket-handler mechanism, never a STOMP message broker: no destinations, no
 * subscriptions, no fan-out — one bounded poll loop per connection over one control-core read face. The
 * poll cadence is configurable; its default is a working single-node value.
 *
 * <p>Servlet-web only: websocket upgrade is a servlet-container concern, so this config (and its
 * {@link EnableWebSocket}) load only in a servlet web context. In a non-web assembly — a substrate check
 * or a bean-graph test that stands up the control plane without an HTTP surface — it self-excludes, so it
 * never drags a servlet requirement into a context that has no servlet container.
 */
@Configuration
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
@EnableWebSocket
public class PipelineStreamConfiguration {

    /** The poll cadence for both stream channels, ISO-8601 duration; a working single-node default. */
    private static final String DEFAULT_POLL_INTERVAL = "PT1S";

    static Predicate<String> knownPipeline(PipelineCatalogService pipelines) {
        Objects.requireNonNull(pipelines, "pipelines");
        return pipelines::containsDefinition;
    }

    /**
     * A small dedicated scheduler for the stream poll loops, kept off the request-serving threads. Managed
     * by Spring so it is shut down with the context; daemon threads so it never holds the JVM up.
     */
    @Bean
    TaskScheduler pipelineStreamScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(2);
        scheduler.setThreadNamePrefix("pipeline-stream-");
        scheduler.setDaemon(true);
        scheduler.initialize();
        return scheduler;
    }

    @Bean
    WebSocketConfigurer pipelineStreamEndpoints(
            TaskScheduler pipelineStreamScheduler,
            PipelineObservationQueryService observations,
            PipelineCatalogService pipelines,
            PipelineLogQueryService logs,
            CredentialAuthenticator credentials,
            MessageCatalog catalog,
            @Value("${tapstate.control.stream.poll-interval:" + DEFAULT_POLL_INTERVAL + "}") Duration pollInterval) {
        return registry -> {
            registry.addHandler(
                            new PipelineStatusWatchHandler(
                                    observations,
                                    knownPipeline(pipelines),
                                    catalog, pipelineStreamScheduler, pollInterval),
                            "/api/pipelines/*/status/watch")
                    .addInterceptors(new PipelineStreamHandshakeInterceptor(
                            credentials, ControlOperations.PIPELINE_STATUS));
            registry.addHandler(
                            new PipelineLogsFollowHandler(logs, pipelineStreamScheduler, pollInterval),
                            "/api/pipelines/*/logs/follow")
                    .addInterceptors(new PipelineStreamHandshakeInterceptor(
                            credentials, ControlOperations.PIPELINE_LOGS));
        };
    }
}
