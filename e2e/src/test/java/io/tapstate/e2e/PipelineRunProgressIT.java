package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.MongoHttpDiagnosticsScope;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.testsupport.DockerGate;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.filter.OncePerRequestFilter;

/** Two packaged controllers share CACHE while the real sink acknowledgement is held. */
class PipelineRunProgressIT {
    private static final String ID = "orders_progress";
    private static final String START_PATH = "/api/pipelines/" + ID + ":start";
    private static final String PROGRESS_PATH = "/api/pipelines/" + ID + "/run-progress";
    private static final Duration TIMEOUT = Duration.ofSeconds(70);

    @BeforeAll static void requireDocker() { DockerGate.require(); }

    @Test
    void anAcceptedStartReportsTheHeldExecutionPlanBeforeEngineSubmission(@TempDir Path directory) throws Exception {
        var source = Files.createDirectory(directory.resolve("source"));
        var target = Files.createDirectory(directory.resolve("target"));
        FileEndpoints.replaceTable(source.resolve("orders.csv"), "id,seq\n1,1\n");
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (var storage = PreparedNativeStore.create();
             var server = InProcessServer.startPrepared(storage, List.of("--spring.main.sources=" + Measurements.class.getName()))) {
            var control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);
            control.registerConnector(E2eConnectorJar.CONNECTOR_ID, Files.readAllBytes(E2eConnectorJar.buildInto(directory)));
            control.apply(Map.of("input.tap.yml", Workspaces.cdcSourceYaml("input", source),
                    "target.tap.yml", Workspaces.targetYaml("target", target)));
            control.discoverSchema("input", E2eConnectorJar.CONNECTOR_ID, Map.of("uri", source.toString()));
            publishDraft(control);

            var actuator = server.nativeLifecycleActuator();
            var field = actuator.getClass().getDeclaredField("dagSource");
            field.setAccessible(true);
            var original = field.get(actuator);
            var dags = field.getType();
            field.set(actuator, Proxy.newProxyInstance(dags.getClassLoader(), new Class<?>[]{dags}, (proxy, method, arguments) -> {
                if (method.getName().equals("prepareStart") && method.getParameterCount() == 3) {
                    entered.countDown();
                    if (!release.await(40, TimeUnit.SECONDS)) throw new IllegalStateException("execution plan hold expired");
                }
                method.setAccessible(true);
                try {
                    return method.invoke(original, arguments);
                } catch (InvocationTargetException failure) {
                    throw failure.getCause();
                }
            }));
            try {
                control.lifecycle(ID, LifecycleVerb.START);
                assertThat(entered.await(30, TimeUnit.SECONDS)).as("background execution plan preparation entered").isTrue();
                var held = Await.answered("the held startup activity to be visible over HTTP", Duration.ofSeconds(12), () -> {
                    var progress = progress(control);
                    return ((List<?>) progress.get("active")).stream().map(entry -> (Map<?, ?>) entry)
                            .anyMatch(entry -> "EXECUTION_PLAN_PREPARING".equals(entry.get("stage")))
                            ? Optional.of(progress) : Optional.empty();
                });
                assertThat(stages(held)).contains("START_ACCEPTED").doesNotContain("ENGINE_RUNNING");
                var start = Measurements.await(START_PATH);
                var read = Measurements.await(PROGRESS_PATH);
                assertThat(start).containsEntry("complete", true);
                assertThat(read).containsEntry("complete", true);
                assertThat(progressCommands(start)).isEmpty();
                assertThat(progressCommands(read)).singleElement().satisfies(group -> {
                    assertThat(group.get("command")).isEqualTo("find");
                    assertThat(((Number) group.get("started")).longValue()).isOne();
                });
                System.out.println("RUN_PROGRESS_MONGO_COMMANDS start=" + commands(start) + " progress=" + commands(read));
            } finally {
                release.countDown();
                field.set(actuator, original);
                Measurements.READINGS.clear();
            }
            Await.until("the released execution plan to reach RUNNING", TIMEOUT,
                    () -> stages(progress(control)).contains("ENGINE_RUNNING"), () -> String.valueOf(progress(control)));
        }
    }

    @Test
    void bothMembersReadTheSameRunThroughTargetConfirmationAndQuietCdc(@TempDir Path directory) throws Exception {
        var source = Files.createDirectory(directory.resolve("source"));
        var target = Files.createDirectory(directory.resolve("target"));
        var holds = new Holds(directory.resolve("holds"));
        FileEndpoints.replaceTable(source.resolve("orders.csv"), "id,seq\n1,1\n2,2\n");
        holds.after("orders", 2);
        try (var storage = PreparedNativeStore.create();
             var cluster = TwoMemberCluster.startPreparedSequentially(storage);
             var cacheClient = MongoClients.create(storage.cacheUri())) {
            cluster.awaitBothMembers();
            var first = cluster.first();
            var second = cluster.second();
            first.registerConnector(E2eConnectorJar.CONNECTOR_ID, Files.readAllBytes(E2eConnectorJar.buildInto(directory)));
            first.apply(Map.of("input.tap.yml", Workspaces.cdcSourceYaml("input", source),
                    "target.tap.yml", Workspaces.targetYaml("target", target, Map.of("hold", holds.directory().toString()))));
            first.discoverSchema("input", E2eConnectorJar.CONNECTOR_ID, Map.of("uri", source.toString()));
            publishDraft(first);
            first.lifecycle(ID, LifecycleVerb.START);
            try {
                Await.answered("the sink to write the last row without acknowledging it", TIMEOUT,
                        () -> holds.holderAfter("orders", 2));
                Await.until("both members to report the confirmed running engine", TIMEOUT,
                        () -> stages(progress(first)).contains("ENGINE_RUNNING") && stages(progress(second)).contains("ENGINE_RUNNING"),
                        () -> "first=" + progress(first) + ", second=" + progress(second) + ", status=" + first.pipelineStatus(ID));
                var held = progress(second);
                assertThat(stages(held)).contains("START_ACCEPTED", "EXECUTION_PLAN_PREPARED", "CONNECTORS_READY", "ENGINE_RUNNING")
                        .doesNotContain("INITIAL_LOAD_COMPLETED");
                assertThat(held.get("runId")).isEqualTo(progress(first).get("runId"));
                assertThat(Files.readString(target.resolve("orders.csv"))).contains("2");
            } finally {
                holds.releaseAfter("orders", 2);
            }
            Await.until("both members to report full landing and an idle change consumer", TIMEOUT,
                    () -> complete(progress(first)) && complete(progress(second)),
                    () -> "first=" + progress(first) + ", second=" + progress(second) + ", status=" + first.pipelineStatus(ID));
            var completed = progress(second);
            assertThat(completed).isEqualTo(progress(first));
            assertThat((Map<?, ?>) completed.get("initialLoad"))
                    .hasSize(2).satisfies(counts -> {
                        assertThat(((Number) counts.get("landed")).intValue()).isOne();
                        assertThat(((Number) counts.get("total")).intValue()).isOne();
                    });
            first.stop(ID, false);
            Await.until("both members to observe the retained stopped run", TIMEOUT,
                    () -> stopped(first) && stopped(second), () -> "status=" + first.pipelineStatus(ID));
            assertThat(progress(second).get("runId")).isEqualTo(completed.get("runId"));
            assertThat((List<?>) progress(second).get("active")).isEmpty();

            second.lifecycle(ID, LifecycleVerb.START);
            Await.until("both members to report the running engine for the replacement run", TIMEOUT, () -> {
                var a = progress(first);
                var b = progress(second);
                return a.get("runId") instanceof String runId && !runId.equals(completed.get("runId"))
                        && runId.equals(b.get("runId"))
                        && stages(a).contains("ENGINE_RUNNING") && stages(b).contains("ENGINE_RUNNING");
            }, () -> "first=" + progress(first) + ", second=" + progress(second));
            assertThat(SystemCollections.CACHE_PIPELINE_RUN_PROGRESS.on(cacheClient.getDatabase(storage.cacheDatabase()))
                    .countDocuments()).isOne();
        }
    }

    private static boolean complete(Map<?, ?> progress) {
        return stages(progress).containsAll(List.of("ENGINE_RUNNING", "INITIAL_LOAD_COMPLETED", "CDC_READY"));
    }

    private static void publishDraft(ControlPlane control) {
        String draft = "/api/pipelines/" + ID + "/draft";
        control.createPipelineDraft(ID, Map.of("pipelineId", ID, "mode", "wizard", "name", "Orders progress",
                "wizard", Map.of("root", Map.of("id", "root", "sourceId", "input", "table", "orders", "key", List.of("id")),
                        "related", List.of(), "transforms", List.of(),
                        "output", Map.of("kind", "source", "config", Map.of("sourceId", "target", "table", "orders",
                                "writeMode", "upsert", "onFullLoad", "append")))));
        control.sourceRequest("POST", draft + ":preview", Map.of("revision", 1), null, 200);
        var publication = control.sourceRequest("POST", draft + ":publish", Map.of("revision", 1), null, 200);
        assertThat(((Map<?, ?>) JsonReader.parse(publication.body())).get("published")).isEqualTo(true);
    }

    private static boolean stopped(ControlPlane control) {
        return "STOPPED".equals(control.pipelineStatus(ID).get("state"));
    }

    private static Map<?, ?> progress(ControlPlane control) {
        return (Map<?, ?>) JsonReader.parse(control.sourceRequest("GET", PROGRESS_PATH, null, null, 200).body());
    }

    private static List<Object> stages(Map<?, ?> progress) {
        if (!(progress.get("milestones") instanceof List<?> entries)) return List.of();
        return entries.stream().map(entry -> ((Map<?, ?>) entry).get("stage")).collect(java.util.stream.Collectors.toList());
    }

    private static List<Map<?, ?>> progressCommands(Map<String, Object> snapshot) {
        return ((List<?>) snapshot.get("mongo_groups")).stream().<Map<?, ?>>map(entry -> (Map<?, ?>) entry)
                .filter(entry -> SystemCollections.CACHE_PIPELINE_RUN_PROGRESS.collectionName().equals(entry.get("collection"))).toList();
    }

    private static long commands(Map<String, Object> snapshot) {
        return ((Number) ((Map<?, ?>) snapshot.get("mongo")).get("started")).longValue();
    }

    @Configuration(proxyBeanMethods = false)
    public static class Measurements {
        private static final Map<String, Map<String, Object>> READINGS = new ConcurrentHashMap<>();

        @Bean FilterRegistrationBean<OncePerRequestFilter> runProgressMeasurements() {
            var registration = new FilterRegistrationBean<OncePerRequestFilter>();
            registration.setOrder(Integer.MIN_VALUE);
            registration.setFilter(new OncePerRequestFilter() {
                @Override protected boolean shouldNotFilter(HttpServletRequest request) {
                    String path = request.getRequestURI();
                    return !START_PATH.equals(path) && !PROGRESS_PATH.equals(path);
                }

                @Override protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
                        FilterChain chain) throws ServletException, IOException {
                    try (var scope = MongoHttpDiagnosticsScope.open()) {
                        try { chain.doFilter(request, response); }
                        finally { READINGS.put(request.getRequestURI(), scope.snapshot()); }
                    }
                }
            });
            return registration;
        }

        private static Map<String, Object> await(String path) {
            return Await.answered("Mongo diagnostics for " + path, Duration.ofSeconds(5),
                    () -> Optional.ofNullable(READINGS.get(path)));
        }
    }
}
