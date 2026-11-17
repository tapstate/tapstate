package io.tapstate.e2e;

import io.tapstate.adapters.mongostore.MongoHttpDiagnosticsScope;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.testsupport.RequiresDocker;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.filter.OncePerRequestFilter;

import static org.assertj.core.api.Assertions.assertThat;

/** Counts the actual request-thread Mongo work, including security and transaction completion. */
@RequiresDocker
class NativePipelineDeleteBudgetIT {
    // One-table fixture budgets include authentication, lazy schema checks, and transaction completion.
    private static final int PREPARED_MAX_COMMANDS = 8500;
    private static final int UNPREPARED_MAX_COMMANDS = 3000;
    private static final ConcurrentLinkedQueue<Map<String, Object>> READINGS = new ConcurrentLinkedQueue<>();

    @Configuration(proxyBeanMethods = false)
    public static class ProbeConfiguration {
        @Bean FilterRegistrationBean<OncePerRequestFilter> nativeDeletionCommandProbe() {
            var registration = new FilterRegistrationBean<OncePerRequestFilter>();
            registration.setOrder(Integer.MIN_VALUE);
            registration.setFilter(new OncePerRequestFilter() {
                @Override protected boolean shouldNotFilter(HttpServletRequest request) {
                    return !"DELETE".equals(request.getMethod());
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

    @Test void preparedAndUnpreparedFixturesStayWithinTheirRequestThreadCommandBudgets(@TempDir Path directory) throws Exception {
        READINGS.clear();
        try (var storage = PreparedNativeStore.create();
             var server = InProcessServer.startPrepared(storage,
                     java.util.List.of("--spring.main.sources=" + ProbeConfiguration.class.getName()))) {
            var running = RunningPipeline.started(server, directory, storage);
            var control = running.control();
            running.stopAndSettle(false);
            control.deleteArtifact(running.pipelineId(), control.contentHash(running.pipelineId()));
            var prepared = Await.answered("the prepared DELETE request's own Mongo receipt",
                    Duration.ofSeconds(5), () -> Optional.ofNullable(READINGS.poll()));
            requireBudget(prepared, PREPARED_MAX_COMMANDS);
            assertThat(control.artifact(running.pipelineId())).isEmpty();
            System.out.println("DELETE_MONGO_BUDGET prepared=" + JsonWriter.write(prepared));

            String empty = "unstarted_delete_budget";
            control.apply(Map.of("source.tap.yml", Workspaces.cdcSourceYaml(running.sourceId(), directory.resolve(running.sourceId())),
                    "target.tap.yml", Workspaces.targetYaml(running.targetId(), running.targetDirectory()),
                    "unstarted.tap.yml", Workspaces.pipelineYaml(empty, running.sourceId(), running.targetId(), "orders")));
            control.deleteArtifact(empty, control.contentHash(empty));
            var unprepared = Await.answered("the unprepared DELETE request's own Mongo receipt",
                    Duration.ofSeconds(5), () -> Optional.ofNullable(READINGS.poll()));
            requireBudget(unprepared, UNPREPARED_MAX_COMMANDS);
            assertThat(control.artifact(empty)).isEmpty();
            assertThat(READINGS).isEmpty();
            System.out.println("DELETE_MONGO_BUDGET unprepared=" + JsonWriter.write(unprepared));
        } finally { READINGS.clear(); }
    }
    private static void requireBudget(Map<String, Object> receipt, int maximum) {
        assertThat(receipt).containsEntry("complete", true).containsEntry("pending_commands", 0)
                .containsEntry("unmatched_completions", 0).containsEntry("unclosed_stages", 0);
        var driver = (Map<?, ?>) receipt.get("mongo");
        long started = ((Number) driver.get("started")).longValue();
        assertThat(started).isBetween(1L, (long) maximum);
        assertThat(((Number) driver.get("completed")).longValue()).isEqualTo(started);
    }

}
