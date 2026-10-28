package io.tapstate.runtime.engine.validation;

import com.hazelcast.config.Config;
import com.hazelcast.core.*;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.*;
import io.tapstate.core.validation.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.BooleanSupplier;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import static org.assertj.core.api.Assertions.*;

/** Actual loopback Jet tasklets; these tests do not establish a connector's native drain contract. */
class ValidationProcessorRegistryTest {
    private static final Map<String, Probe> PROBES = new ConcurrentHashMap<>();

    @Test @Timeout(40) void actualSupplierExitIsSeparateFromCancellationAndWaitsForProcessorClose() throws Exception {
        try (var fixture = new Fixture()) {
            String id = UUID.randomUUID().toString(); var probe = new Probe(true); PROBES.put(id, probe);
            var run = fixture.registry.register(identity(id), Set.of("check"), () -> {}); var graph = fixture.graph(id, run);
            Job job = graph.submit(fixture.member.getJet(), () -> {});
            try {
                assertThat(probe.started.await(10, TimeUnit.SECONDS)).isTrue(); run.seal(); job.cancel();
                assertThat(probe.closing.await(10, TimeUnit.SECONDS)).isTrue(); assertThat(run.processorsExited()).isFalse();
                probe.allowClose.countDown(); await(run::processorsExited);
                assertThat(probe.processorsClosed).hasValue(1); assertThat(probe.suppliersClosed).hasValue(1);
                run.forget(); assertThat(graph.reconcile(fixture.member.getJet(), () -> {}).orElseThrow().getId()).isEqualTo(job.getId());
            } finally { probe.allowClose.countDown(); job.cancel(); PROBES.remove(id); }
        }
    }

    @Test @Timeout(40) void ownerLossFailsActualTaskletAndExitReceiptsRemainReadableWithoutOwnerAuthority() throws Exception {
        try (var fixture = new Fixture()) {
            String id = UUID.randomUUID().toString(); var probe = new Probe(false); PROBES.put(id, probe); var authorized = new AtomicBoolean(true);
            var run = fixture.registry.register(identity(id), Set.of("check"), () -> { if (!authorized.get()) throw invalid(); });
            Job job = fixture.graph(id, run).submit(fixture.member.getJet(), () -> {});
            try {
                assertThat(probe.started.await(10, TimeUnit.SECONDS)).isTrue(); authorized.set(false);
                assertThatThrownBy(() -> job.getFuture().get(10, TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class).hasMessageContaining(ValidationError.INVALID_CONTRACT.code());
                run.seal(); await(run::processorsExited); assertThat(probe.processorsClosed).hasValue(1); run.forget();
            } finally { job.cancel(); PROBES.remove(id); }
        }
    }

    @Test @Timeout(40) void aQueuedOldTokenCannotEnterAReplacementRegistryOnTheSameMember() throws Exception {
        try (var fixture = new Fixture()) {
            String id = UUID.randomUUID().toString(); var probe = new Probe(false); PROBES.put(id, probe);
            var old = fixture.registry.register(identity(id), Set.of("check"), () -> {}); var graph = fixture.graph(id, old);
            fixture.registry.close(); assertThat(old.processorsExited()).isTrue();
            try (var replacement = new ValidationProcessorRegistry(fixture.member, () -> {})) {
                var current = replacement.register(identity(id), Set.of("check"), () -> {});
                var job = graph.submit(fixture.member.getJet(), () -> {});
                try {
                    assertThatThrownBy(() -> job.getFuture().get(10, TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class)
                            .hasMessageContaining(ValidationError.INVALID_CONTRACT.code());
                    assertThat(probe.processorsStarted).hasValue(0); assertThat(current.processorsExited()).isFalse();
                    current.seal(); current.forget(); old.forget();
                } finally { job.cancel(); }
            } finally { PROBES.remove(id); }
        }
    }

    @Test @Timeout(40) void originalLookupHasAUniqueStableNameAndNoSecondSubmissionOrBorrowedJob() throws Exception {
        try (var fixture = new Fixture()) {
            String id = UUID.randomUUID().toString(); var probe = new Probe(false); probe.finish.set(true); PROBES.put(id, probe);
            var run = fixture.registry.register(identity(id), Set.of("check"), () -> {}); var graph = fixture.graph(id, run);
            assertThat(graph.handedToJet()).isFalse(); assertThat(graph.reconcile(fixture.member.getJet(), () -> {})).isEmpty();
            var other = new ValidationJobGraph(identity(id), fixture.member.getCluster().getLocalMember().getAddress(), fixture.member.getCluster().getLocalMember().getUuid(), new ValidationJobGraph.Limits(1, 0, 1));
            assertThat(other.jobName()).isNotEqualTo(graph.jobName());
            var job = graph.submit(fixture.member.getJet(), () -> {});
            try {
                job.getFuture().get(10, TimeUnit.SECONDS); assertThat(graph.handedToJet()).isTrue();
                assertThat(graph.reconcile(fixture.member.getJet(), () -> {}).orElseThrow().getId()).isEqualTo(job.getId());
                assertThatThrownBy(() -> graph.submit(fixture.member.getJet(), () -> {})).isInstanceOf(IllegalStateException.class);
                run.seal(); await(run::processorsExited); run.forget();
            } finally { job.cancel(); PROBES.remove(id); }
        }
    }

    @Test @Timeout(40) void nativeFactoriesStayLocalWhileOnlyOriginalCoordinatesTravelWithTheJob() throws Exception {
        try (var fixture = new Fixture()) {
            String id = UUID.randomUUID().toString(); var probe = new Probe(false); probe.finish.set(true); probe.nativeHandle = new Object(); PROBES.put(id, probe);
            var run = fixture.registry.register(identity(id), Set.of("check"), () -> {}); var local = fixture.member.getCluster().getLocalMember();
            var graph = new ValidationJobGraph(identity(id), local.getAddress(), local.getUuid(), new ValidationJobGraph.Limits(1, 0, 1), run);
            assertThatCode(() -> graph.vertex("check", new Supplier(id, probe.nativeHandle))).doesNotThrowAnyException(); var submitted = new AtomicReference<Job>();
            try {
                assertThatCode(() -> submitted.set(graph.submit(fixture.member.getJet(), () -> {}))).doesNotThrowAnyException();
                submitted.get().getFuture().get(10, TimeUnit.SECONDS); run.seal(); await(run::processorsExited);
                assertThat(probe.processorsStarted).hasValue(1); run.forget();
            } finally { if (submitted.get() != null) submitted.get().cancel(); PROBES.remove(id); }
        }
    }

    @Test @Timeout(40) void failedSupplierInitializationGetAndCloseStillProduceActualExitReceipts() throws Exception {
        try (var fixture = new Fixture()) {
            for (Failure phase : List.of(Failure.INIT, Failure.GET, Failure.CLOSE)) {
                String id = UUID.randomUUID().toString(); var probe = new Probe(false); probe.finish.set(true); probe.failure = phase; PROBES.put(id, probe);
                var run = fixture.registry.register(identity(id), Set.of("check"), () -> {}); var job = fixture.graph(id, run).submit(fixture.member.getJet(), () -> {});
                try {
                    if (phase == Failure.CLOSE) { try { job.getFuture().get(10, TimeUnit.SECONDS); } catch (ExecutionException closed) { assertThat(closed).hasMessageContaining(ValidationError.INVALID_CONTRACT.code()); } }
                    else assertThatThrownBy(() -> job.getFuture().get(10, TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class).hasMessageContaining(ValidationError.INVALID_CONTRACT.code());
                    run.seal(); await(run::processorsExited); assertThat(probe.suppliersClosed).hasValue(1);
                    assertThat(probe.processorsClosed).hasValue(phase == Failure.CLOSE ? 1 : 0); run.forget();
                } finally { job.cancel(); PROBES.remove(id); }
            }
        }
    }

    @Test @Timeout(40) void blockedAndThenUnknownSubmissionCannotBlockOriginalLookupOrCleanup() throws Exception {
        var accepted = new CountDownLatch(1); var release = new CountDownLatch(1);
        try (var fixture = new Fixture(); var callers = Executors.newFixedThreadPool(2)) {
            String id = UUID.randomUUID().toString(); var probe = new Probe(false); PROBES.put(id, probe);
            var run = fixture.registry.register(identity(id), Set.of("check"), () -> {}); var graph = fixture.graph(id, run);
            var actual = fixture.member.getJet(); var submitted = new AtomicReference<Job>(); var attempts = new AtomicInteger();
            JetService transport = (JetService) Proxy.newProxyInstance(JetService.class.getClassLoader(), new Class<?>[]{JetService.class}, (proxy, method, args) -> {
                if (method.getName().equals("newJob")) {
                    attempts.incrementAndGet(); submitted.set(actual.newJob((DAG) args[0], (JobConfig) args[1])); accepted.countDown();
                    if (!release.await(10, TimeUnit.SECONDS)) throw new AssertionError("submission response gate timeout"); throw invalid();
                }
                try { return method.invoke(actual, args); } catch (InvocationTargetException failed) { throw failed.getCause(); }
            });
            var waiting = callers.submit(() -> graph.submit(transport, () -> {}));
            try {
                assertThat(accepted.await(10, TimeUnit.SECONDS)).isTrue(); assertThat(probe.started.await(10, TimeUnit.SECONDS)).isTrue();
                var lookup = callers.submit(() -> graph.reconcile(transport, () -> {}).orElseThrow());
                assertThat(lookup.get(2, TimeUnit.SECONDS).getId()).isEqualTo(submitted.get().getId());
                run.seal(); submitted.get().cancel(); await(run::processorsExited); run.forget();
                release.countDown(); assertThatThrownBy(() -> waiting.get(5, TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class);
                assertThat(graph.reconcile(actual, () -> {}).orElseThrow().getId()).isEqualTo(submitted.get().getId());
                assertThatThrownBy(() -> graph.submit(actual, () -> {})).isInstanceOf(IllegalStateException.class); assertThat(attempts).hasValue(1);
            } finally {
                release.countDown(); if (submitted.get() != null) submitted.get().cancel();
                try { waiting.get(5, TimeUnit.SECONDS); } catch (ExecutionException expected) { /* The response was deliberately lost. */ }
                PROBES.remove(id);
            }
        } finally { release.countDown(); }
    }

    @Test @Timeout(40) void originalLookupRejectsASameNameJobWithForeignFixedCoordinates() throws Exception {
        try (var fixture = new Fixture()) {
            String id = UUID.randomUUID().toString(); var probe = new Probe(false); PROBES.put(id, probe);
            var run = fixture.registry.register(identity(id), Set.of("check"), () -> {}); var graph = fixture.graph(id, run);
            JetService transport = (JetService) Proxy.newProxyInstance(JetService.class.getClassLoader(), new Class<?>[]{JetService.class}, (proxy, method, args) -> {
                if (method.getName().equals("newJob")) throw invalid();
                if (method.getName().equals("getJob")) return Proxy.newProxyInstance(Job.class.getClassLoader(), new Class<?>[]{Job.class}, (job, call, values) -> switch (call.getName()) {
                    case "getName" -> graph.jobName();
                    case "getConfig" -> new JobConfig().setName(graph.jobName()).setProcessingGuarantee(ProcessingGuarantee.NONE).setAutoScaling(false).setSuspendOnFailure(false);
                    default -> throw new AssertionError("unexpected Job method " + call.getName());
                });
                throw new AssertionError("unexpected Jet method " + method.getName());
            });
            try {
                assertThatThrownBy(() -> graph.submit(transport, () -> {})).isInstanceOf(io.tapstate.core.common.TapstateException.class);
                assertThatThrownBy(() -> graph.reconcile(transport, () -> {})).isInstanceOf(io.tapstate.core.common.TapstateException.class).hasMessageContaining("job.originalSubmission");
                run.seal(); assertThat(run.processorsExited()).isTrue(); run.forget(); assertThat(probe.processorsStarted).hasValue(0);
            } finally { PROBES.remove(id); }
        }
    }

    private static final class Probe {
        final AtomicBoolean finish = new AtomicBoolean();
        final AtomicInteger processorsStarted = new AtomicInteger(), processorsClosed = new AtomicInteger(), suppliersClosed = new AtomicInteger();
        final CountDownLatch started = new CountDownLatch(1), closing = new CountDownLatch(1), allowClose;
        Object nativeHandle;
        Failure failure;
        Probe(boolean blockClose) { allowClose = new CountDownLatch(blockClose ? 1 : 0); }
    }
    private enum Failure { INIT, GET, CLOSE }
    private static final class Supplier implements ProcessorSupplier {
        private static final long serialVersionUID = 1L;
        private final String id;
        private final Object nativeHandle;
        Supplier(String id) { this(id, null); }
        Supplier(String id, Object nativeHandle) { this.id = id; this.nativeHandle = nativeHandle; }
        @Override public void init(Context context) { if (PROBES.get(id).failure == Failure.INIT) throw invalid(); }
        @Override public Collection<? extends Processor> get(int count) { assertThat(count).isEqualTo(1); assertThat(nativeHandle).isSameAs(PROBES.get(id).nativeHandle); if (PROBES.get(id).failure == Failure.GET) throw invalid(); return List.of(new Work(id)); }
        @Override public void close(Throwable error) { var probe = PROBES.get(id); probe.suppliersClosed.incrementAndGet(); if (probe.failure == Failure.CLOSE) throw invalid(); }
    }
    private static final class Work extends AbstractProcessor {
        private final String id;
        Work(String id) { this.id = id; }
        @Override protected void init(Context context) { var probe = PROBES.get(id); probe.processorsStarted.incrementAndGet(); probe.started.countDown(); }
        @Override public boolean complete() { return PROBES.get(id).finish.get(); }
        @Override public boolean closeIsCooperative() { return false; }
        @Override public void close() throws Exception {
            var probe = PROBES.get(id); probe.closing.countDown();
            if (!probe.allowClose.await(10, TimeUnit.SECONDS)) throw new AssertionError("processor close gate timeout");
            probe.processorsClosed.incrementAndGet();
        }
    }
    private static final class Fixture implements AutoCloseable {
        final HazelcastInstance member;
        final ValidationProcessorRegistry registry;
        Fixture() {
            var config = new Config().setClusterName("validation-processors-" + UUID.randomUUID());
            config.getJetConfig().setEnabled(true).setCooperativeThreadCount(2);
            config.setProperty("hazelcast.logging.type", "none").setProperty("hazelcast.phone.home.enabled", "false").setProperty("hazelcast.shutdownhook.enabled", "false")
                    .setProperty("hazelcast.operation.thread.count", "2").setProperty("hazelcast.operation.generic.thread.count", "2")
                    .setProperty("hazelcast.io.input.thread.count", "1").setProperty("hazelcast.io.output.thread.count", "1").setProperty("hazelcast.partition.count", "17");
            config.getNetworkConfig().setPort(0).setPortAutoIncrement(false).getInterfaces().setEnabled(true).addInterface("127.0.0.1");
            var join = config.getNetworkConfig().getJoin(); join.getMulticastConfig().setEnabled(false); join.getTcpIpConfig().setEnabled(false); join.getAutoDetectionConfig().setEnabled(false);
            member = Hazelcast.newHazelcastInstance(config); registry = new ValidationProcessorRegistry(member, () -> {});
        }
        ValidationJobGraph graph(String id, ValidationProcessorRegistry.Run run) {
            var local = member.getCluster().getLocalMember();
            var graph = new ValidationJobGraph(identity(id), local.getAddress(), local.getUuid(), new ValidationJobGraph.Limits(1, 0, 1), run);
            graph.vertex("check", new Supplier(id)); return graph;
        }
        @Override public void close() { registry.close(); member.shutdown(); }
    }
    private static ValidationRunIdentity identity(String id) { return new ValidationRunIdentity(new ValidationOwner("cluster", "flow", "node", "boot", 1, 1, 1), "task", id, ValidationMode.FULL, "spec", 1, "rules"); }
    private static io.tapstate.core.common.TapstateException invalid() { return new io.tapstate.core.common.TapstateException(ValidationError.INVALID_CONTRACT, Map.of("field", "test.ownerLost"), null); }
    private static void await(BooleanSupplier ready) throws Exception {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!ready.getAsBoolean() && System.nanoTime() - until < 0) Thread.sleep(10);
        assertThat(ready.getAsBoolean()).isTrue();
    }
}
