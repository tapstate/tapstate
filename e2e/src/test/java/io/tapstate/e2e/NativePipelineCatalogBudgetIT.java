package io.tapstate.e2e;

import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.adapters.mongostore.MongoConnectionSettings;
import io.tapstate.adapters.mongostore.MongoHttpDiagnosticsScope;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.testsupport.RequiresDocker;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.filter.OncePerRequestFilter;

import static org.assertj.core.api.Assertions.assertThat;

@RequiresDocker
class NativePipelineCatalogBudgetIT {
    private static final ConcurrentLinkedQueue<Map<String, Object>> READINGS = new ConcurrentLinkedQueue<>();

    @Test void aMixedCatalogPageDoesNotAddConfigurationCursorCommands(@TempDir Path directory) throws Exception {
        READINGS.clear();
        try (var storage = PreparedNativeStore.create();
             var server = InProcessServer.startPrepared(storage,
                     List.of("--spring.main.sources=" + ProbeConfiguration.class.getName()))) {
            var running = RunningPipeline.started(server, directory, storage);
            running.stopAndSettle(false);
            var control = running.control();
            assertThat(control.pipelineStatus(running.pipelineId())).containsEntry("stopState", "RETAINED");

            seedUnpreparedPipelines(storage, running);

            assertThat(control.pipelineCatalogIds(1)).hasSize(200).startsWith("unprepared_000").endsWith("unprepared_199");
            long unpreparedGetMores = configurationGetMores(storage);
            assertThat(control.pipelineCatalogIds()).hasSize(200).startsWith(running.pipelineId()).endsWith("unprepared_198");
            long mixedGetMores = configurationGetMores(storage);
            System.out.printf("CATALOG_CONFIG_GET_MORE unprepared=%d mixed=%d%n", unpreparedGetMores, mixedGetMores);
            assertThat(unpreparedGetMores).isPositive();
            assertThat(mixedGetMores).as("CONFIG getMore commands for one prepared and 199 unprepared pipelines")
                    .isEqualTo(unpreparedGetMores);
        } finally {
            READINGS.clear();
        }
    }

    private static void seedUnpreparedPipelines(PreparedNativeStore storage, RunningPipeline running) {
        var parser = new DslParser();
        var definitions = IntStream.range(0, 200)
                .mapToObj(index -> String.format(Locale.ROOT, "unprepared_%03d", index))
                .map(id -> parser.parse(Workspaces.pipelineYaml(id, running.sourceId(), running.targetId(), "orders")))
                .toList();
        try (var configuration = MongoConfigurationDomain.openExisting(
                new MongoConnectionSettings(storage.configurationUri(), null, Duration.ofSeconds(5)))) {
            configuration.artifacts().saveAll(definitions);
        }
    }

    private static long configurationGetMores(PreparedNativeStore storage) {
        var reading = Await.answered("the catalog HTTP request's Mongo receipt",
                Duration.ofSeconds(5), () -> Optional.ofNullable(READINGS.poll()));
        assertThat(reading).containsEntry("complete", true);
        return ((List<?>) reading.get("mongo_groups")).stream().map(group -> (Map<?, ?>) group)
                .filter(group -> storage.configurationDatabase().equals(group.get("database")))
                .filter(group -> "getMore".equalsIgnoreCase((String) group.get("command")))
                .mapToLong(group -> ((Number) group.get("started")).longValue()).sum();
    }

    @Configuration(proxyBeanMethods = false)
    public static class ProbeConfiguration {
        @Bean FilterRegistrationBean<OncePerRequestFilter> nativeCatalogCommandProbe() {
            var registration = new FilterRegistrationBean<OncePerRequestFilter>();
            registration.setOrder(Integer.MIN_VALUE);
            registration.setFilter(new OncePerRequestFilter() {
                @Override protected boolean shouldNotFilter(HttpServletRequest request) {
                    return !"GET".equals(request.getMethod()) || !"/api/pipelines".equals(request.getRequestURI());
                }

                @Override protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
                        FilterChain chain) throws ServletException, IOException {
                    try (var scope = MongoHttpDiagnosticsScope.open()) {
                        try { chain.doFilter(request, response); }
                        finally { READINGS.add(scope.snapshot()); }
                    }
                }
            });
            return registration;
        }
    }
}
