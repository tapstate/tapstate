package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.testsupport.RequiresDocker;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Holding one real observation read has no declarative word; HTTP and the real PDK path stay intact. */
@RequiresDocker
class AnIndependentStartDoesNotWaitForAnotherObservationIT {
    @TempDir Path directory;

    @Test void aSecondPipelineStartsAndDeliversBeforeTheFirstPositionReaderIsReleased() throws Exception {
        var input = EndpointAddress.uri(Files.createDirectory(directory.resolve("input")).toString());
        var first = EndpointAddress.uri(Files.createDirectory(directory.resolve("first")).toString());
        var second = EndpointAddress.uri(Files.createDirectory(directory.resolve("second")).toString());
        var endpoints = new FileEndpoints();
        List<Map<String,Object>> rows = List.of(Map.of("id",1L,"seq",7L),Map.of("id",2L,"seq",11L));
        endpoints.seed(input,"orders",rows);
        try (var storage = PreparedNativeStore.create(); var server = InProcessServer.startPrepared(storage)) {
            var control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);
            control.registerConnector(E2eConnectorJar.CONNECTOR_ID,
                    Files.readAllBytes(E2eConnectorJar.buildInto(directory)));
            control.apply(Map.of(
                    "input.tap.yml", source("input",input)+"mode: cdc\ntables: [orders]\n",
                    "first.tap.yml", source("first_target",first),
                    "second.tap.yml", source("second_target",second),
                    "first-pipeline.tap.yml", pipeline("slow_observation","first_target"),
                    "second-pipeline.tap.yml", pipeline("independent_start","second_target")));
            control.discoverSchema("input",E2eConnectorJar.CONNECTOR_ID,input.settings());
            control.lifecycle("slow_observation",LifecycleVerb.START);
            Await.until("the first real Pipeline to run and deliver",Duration.ofSeconds(90),
                    () -> "RUNNING".equals(control.pipelineStatus("slow_observation").get("state"))
                            && endpoints.count(first,"orders")==2,
                    () -> String.valueOf(control.pipelineStatus("slow_observation")));

            // A mutation control can recreate the lost isolation without changing product settings.
            if (Boolean.getBoolean("tapstate.e2e.mutate.serial-convergence")) {
                var workers = server.convergenceWorkers();
                workers.setCorePoolSize(1);workers.setMaximumPoolSize(1);
            }
            try (var barrier = new PositionReadBarrier(server.observationPublisher(),"slow_observation")) {
                assertThat(barrier.entered.await(30,TimeUnit.SECONDS))
                        .as("the actual publisher must reach the held position supplier").isTrue();
                if (Boolean.getBoolean("tapstate.e2e.mutate.serial-convergence")) {
                    var workers = server.convergenceWorkers();
                    Await.until("the mutation to leave exactly one occupied convergence worker",Duration.ofSeconds(10),
                            () -> workers.getPoolSize()==1 && workers.getActiveCount()==1,
                            () -> "pool=" + workers.getPoolSize() + ", active=" + workers.getActiveCount());
                }
                long started = System.nanoTime();
                control.lifecycle("independent_start",LifecycleVerb.START);
                Await.until("the independent engine and target while the original read is still held",Duration.ofSeconds(90),
                        () -> "RUNNING".equals(control.pipelineStatus("independent_start").get("state"))
                                && endpoints.count(second,"orders")==2,
                        () -> String.valueOf(control.pipelineStatus("independent_start")));
                assertThat(barrier.release.getCount()).isEqualTo(1);
                assertThat(barrier.blocked.get()).isTrue();
                assertThat(barrier.prematureExit.get()).isFalse();
                assertThat(endpoints.fetch(second,"orders",Map.of("id",1L))).contains(rows.getFirst());
                assertThat(endpoints.fetch(second,"orders",Map.of("id",2L))).contains(rows.getLast());
                System.out.println("INDEPENDENT_E2E_START_AND_TARGET_MS "
                        + TimeUnit.NANOSECONDS.toMillis(System.nanoTime()-started));
            }
        }
    }

    private static String source(String id,EndpointAddress endpoint) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: %s
                config: { uri: "%s" }
                """.formatted(id,E2eConnectorJar.CONNECTOR_ID,endpoint.text("uri"));
    }

    private static String pipeline(String id,String target) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: input
                settings: { read_mode: snapshot_and_cdc }
                serve:
                  from: orders
                  sync:
                    - source: %s
                """.formatted(id,target);
    }

    private static final class PositionReadBarrier implements AutoCloseable {
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final java.util.concurrent.atomic.AtomicBoolean blocked = new java.util.concurrent.atomic.AtomicBoolean();
        final java.util.concurrent.atomic.AtomicBoolean prematureExit = new java.util.concurrent.atomic.AtomicBoolean();
        private final Object publisher;
        private final java.lang.reflect.Field field;
        private final Object original;

        @SuppressWarnings("unchecked")
        PositionReadBarrier(Object publisher,String pipeline) throws Exception {
            this.publisher=publisher;
            field=publisher.getClass().getDeclaredField("captureMeasurements");field.setAccessible(true);
            original=field.get(publisher);
            var delegate=(java.util.function.BiFunction<String,java.time.Instant,Object>)original;
            field.set(publisher,(java.util.function.BiFunction<String,java.time.Instant,Object>) (id, at) -> {
                if(id.equals(pipeline)) {
                    blocked.set(true);entered.countDown();
                    try { release.await(); }
                    catch(InterruptedException stopped) {Thread.currentThread().interrupt();throw new IllegalStateException(stopped);}
                    finally {if(release.getCount()!=0)prematureExit.set(true);blocked.set(false);}
                }
                return delegate.apply(id, at);
            });
        }

        @Override public void close() throws Exception {release.countDown();field.set(publisher,original);}
    }
}
