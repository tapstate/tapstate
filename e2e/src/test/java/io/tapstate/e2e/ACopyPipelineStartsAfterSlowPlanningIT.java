package io.tapstate.e2e;

import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.app.Bootstrap;
import io.tapstate.control.core.StartCheckEvaluator;
import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A copy start keeps a usable check budget after slow target planning. The declarative vocabulary
 * cannot advance time inside planning or inspect a start's findings, so this in-process case decorates
 * the clock at the real evaluator's planning boundary and drives the assembled product through HTTP,
 * with a real Mongo store.
 */
@RequiresDocker
class ACopyPipelineStartsAfterSlowPlanningIT {
    private static final String PIPELINE = "slow_planned_copy";
    private static final String TABLE = "orders";

    @Test
    void aCopyStartChecksItsTargetAfterPlanningExceedsTheCheckBudget(@TempDir Path directory) throws Exception {
        EndpointAddress source = EndpointAddress.uri(Files.createDirectories(directory.resolve("source")).toString());
        EndpointAddress target = EndpointAddress.uri(Files.createDirectories(directory.resolve("target")).toString());
        FileEndpoints files = new FileEndpoints();
        files.seed(source, TABLE, SeedRows.generated(2));
        files.seed(target, TABLE, List.of(Map.of("id", 99L, "seq", 99L)));
        Path staging = ServerHandle.privateStagingDirectory();
        try (PreparedNativeStore storage = PreparedNativeStore.create()) {
            try (var context = new SpringApplicationBuilder(Bootstrap.class, SlowPlanning.class).run(
                    "--server.address=127.0.0.1", "--server.port=0",
                    "--tapstate.store.mongo.enabled=true", "--tapstate.store.mongo.uri=" + storage.configurationUri(),
                    "--tapstate.cluster.id=" + storage.clusterId(),
                    "--tapstate.hz.member-port=0", "--tapstate.hz.jet.cooperative-thread-count=2",
                    "--tapstate.connectors.plugins-dir=" + staging);
                    StoreDocuments documents = StoreDocuments.at(storage.configurationUri())) {
                URI base = URI.create("http://127.0.0.1:"
                        + ((WebServerApplicationContext) context).getWebServer().getPort());
                ControlPlane control = new ControlPlane(base);
                storage.loginAndAwaitBusiness(control);
                String connector = E2eConnectorJar.BROWSABLE_CONNECTOR_ID;
                control.registerConnector(connector, Files.readAllBytes(E2eConnectorJar.buildInto(directory, connector)));
                control.discoverSchema("copy_source", connector, source.settings());
                control.apply(Map.of(
                        "source.tap.yml", source("copy_source", connector, source) + "mode: cdc\ntables: [orders]\n",
                        "target.tap.yml", source("copy_target", connector, target),
                        "pipeline.tap.yml", """
                                version: tapstate/v1
                                kind: pipeline
                                id: slow_planned_copy
                                source: copy_source
                                settings: { read_mode: snapshot_and_cdc }
                                serve:
                                  from: orders
                                  sync:
                                    - source: copy_target
                                      on_full_load: clear
                                """));

                PlanningClock clock = context.getBean(PlanningClock.class);
                clock.slowNextPlan.set(true);
                ControlPlane.StartAnswer answer = control.start(PIPELINE, List.of(), null);

                assertThat(clock.slowNextPlan).as("the real target planner consumed the injected delay").isFalse();
                assertThat(answer.status()).as("the start response: %s", answer.body()).isEqualTo(200);
                assertThat(answer.startChecks()).containsEntry("outcome", "READY");
                assertThat(answer.startChecks().get("plan")).asList().singleElement().satisfies(entry ->
                        assertThat(((Map<?, ?>) entry).get("load")).isEqualTo("FULL_LOAD"));
                assertThat(answer.findings()).singleElement().satisfies(finding -> {
                    assertThat(finding).containsEntry("check", "target-not-empty")
                            .containsEntry("evaluation", "COMPLETE")
                            .containsEntry("code", "start-check.target-set-to-clear");
                    assertThat(((Map<?, ?>) finding.get("params")).get("rows")).isEqualTo(1L);
                });
                assertThat(documents.holds(SystemCollections.PIPELINE_INTENTS.collectionName(), PIPELINE)).isTrue();
                assertThat(documents.auditOf("pipeline.start", PIPELINE)).hasSize(1);
                Await.until("the accepted copy to replace the target's old row",
                        () -> files.count(target, TABLE) == 2
                                && files.fetch(target, TABLE, Map.of("id", 99L)).isEmpty(),
                        () -> "target rows=" + files.count(target, TABLE));
                assertThat(files.fetch(target, TABLE, Map.of("id", 1L))).isPresent();
                assertThat(files.fetch(target, TABLE, Map.of("id", 2L))).isPresent();
            }
        } finally {
            ServerHandle.discardStagingDirectory(staging);
        }
    }

    private static String source(String id, String connector, EndpointAddress endpoint) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: %s
                config: { uri: "%s" }
                """.formatted(id, connector, endpoint.text("uri"));
    }

    @Configuration(proxyBeanMethods = false)
    static class SlowPlanning {
        @Bean
        static org.springframework.beans.factory.config.BeanFactoryPostProcessor controlledClock() {
            return factory -> {
                ((org.springframework.beans.factory.support.BeanDefinitionRegistry) factory).removeBeanDefinition("clock");
                factory.registerSingleton("clock", new PlanningClock());
            };
        }
    }

    static final class PlanningClock extends Clock {
        private final AtomicReference<Duration> offset = new AtomicReference<>(Duration.ZERO);
        private final AtomicBoolean slowNextPlan = new AtomicBoolean();
        private final java.util.concurrent.atomic.AtomicInteger planningReads = new java.util.concurrent.atomic.AtomicInteger();

        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return Clock.offset(Clock.system(zone), offset.get()); }
        @Override public Instant instant() {
            // Authentication also reads this clock; only the evaluator's two planning-boundary reads
            // consume the armed delay. The real target planner and check workers remain unchanged.
            if (slowNextPlan.get() && StackWalker.getInstance().walk(frames -> frames.anyMatch(frame ->
                    frame.getClassName().equals(StartCheckEvaluator.class.getName())
                            && frame.getMethodName().equals("evaluate")))
                    && planningReads.incrementAndGet() == 2) {
                offset.updateAndGet(value -> value.plus(StartCheckEvaluator.BUDGET).plusMillis(200));
                slowNextPlan.set(false);
            }
            return Instant.now().plus(offset.get());
        }
    }
}
