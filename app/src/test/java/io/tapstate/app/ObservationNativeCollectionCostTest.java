package io.tapstate.app;

import com.hazelcast.cluster.Cluster;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.core.metrics.JobMetrics;
import io.tapstate.core.lifecycle.CaptureReading;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.SnapshotReading;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.runtime.scheduler.ObservationPublisher;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.StateStore;
import io.tapstate.spi.store.StorePort;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Counts actual native collection requests through the assembled publisher without timing a mock. */
class ObservationNativeCollectionCostTest {
    @Test
    void onePreparedObservationCollectsItsNativeJobMetricsOnce() {
        Fixture fixture = new Fixture("orders");
        assertThat(fixture.publisher.prepare(fixture.pipeline, null)).isPresent();
        assertThat(fixture.collections.get()).as("one preparation shares one exact native collection").isEqualTo(1);
        assertThat(fixture.publisher.prepare(fixture.pipeline, null)).isPresent();
        assertThat(fixture.collections.get()).as("the following preparation takes a fresh collection").isEqualTo(2);
    }

    @Test
    void cachedProjectionsAddOnlyBoundedNativeIdentityLookups() {
        Fixture fixture = new Fixture("orders");
        int projections = 100;
        try (Engine.ObservationMetricsSession session = fixture.engine.openObservationMetrics(fixture.pipeline)) {
            for (int i = 0; i < projections; i++) {
                assertThat(fixture.engine.recordCount(fixture.pipeline)).hasValue(0);
            }
            assertThat(session.current()).isTrue();
        }
        assertThat(fixture.collections.get()).isEqualTo(1);
        assertThat(fixture.jobLookups.get())
                .as("retain each getter's live lookup plus bounded session boundary checks")
                .isLessThanOrEqualTo(projections + 4);
    }

    @Test
    void assembledPreparationDoesNotIncreaseTotalNativeRequests() {
        Fixture unshared = new Fixture("orders", false);
        Fixture shared = new Fixture("orders");
        assertThat(unshared.publisher.prepare(unshared.pipeline, null)).isPresent();
        assertThat(shared.publisher.prepare(shared.pipeline, null)).isPresent();
        assertThat(unshared.collections.get()).isEqualTo(12);
        assertThat(shared.collections.get()).isEqualTo(1);
        assertThat(shared.nativeRequests())
                .as("native API calls: unshared %s, shared %s", unshared.requestCounts(), shared.requestCounts())
                .isLessThanOrEqualTo(unshared.nativeRequests());
        System.out.printf("observation-native-api-counts unshared=%s shared=%s%n",
                unshared.requestCounts(), shared.requestCounts());
    }

    @Test
    void identityChangesAfterTheSampleDiscardTheWholePreparedFrame() {
        Fixture replacement = new Fixture("orders");
        replacement.onSnapshot.set(() -> replacement.currentJob.set(replacement.job(2)));
        assertThat(replacement.publisher.prepare(replacement.pipeline, null)).isEmpty();
        assertThat(replacement.collections.get()).isEqualTo(1);

        Fixture membership = new Fixture("orders");
        membership.onSnapshot.set(() -> membership.members.set(Set.of()));
        assertThat(membership.publisher.prepare(membership.pipeline, null)).isEmpty();
        assertThat(membership.collections.get()).isEqualTo(1);

        Fixture status = new Fixture("orders");
        status.onSnapshot.set(() -> status.status.set(JobStatus.SUSPENDED));
        assertThat(status.publisher.prepare(status.pipeline, null)).isEmpty();
        assertThat(status.collections.get()).isEqualTo(1);
    }

    @Test
    void replacementJobAndChangedMembersDiscardTheWholePreparedFrame() {
        Fixture replacement = new Fixture("orders");
        replacement.onMetrics.set(() -> replacement.currentJob.set(replacement.job(2)));
        assertThat(replacement.publisher.prepare(replacement.pipeline, null)).isEmpty();
        assertThat(replacement.collections.get()).isEqualTo(1);
        replacement.onMetrics.set(() -> { });
        assertThat(replacement.publisher.prepare(replacement.pipeline, null)).isPresent();
        assertThat(replacement.collections.get()).isEqualTo(2);

        Fixture membership = new Fixture("orders");
        membership.onMetrics.set(() -> membership.members.set(Set.of()));
        assertThat(membership.publisher.prepare(membership.pipeline, null)).isEmpty();
        assertThat(membership.collections.get()).isEqualTo(1);

        Fixture status = new Fixture("orders");
        status.onMetrics.set(() -> status.status.set(JobStatus.SUSPENDED));
        assertThat(status.publisher.prepare(status.pipeline, null)).isEmpty();
        assertThat(status.collections.get()).isEqualTo(1);
    }

    @Test
    void collectionFailureClosesTheFrameAndAStableAbsentJobStillPrepares() {
        Fixture fixture = new Fixture("orders");
        fixture.rejectMetrics.set(true);
        assertThatThrownBy(() -> fixture.publisher.prepare(fixture.pipeline, null))
                .isInstanceOf(IllegalStateException.class).hasMessage("native collection refused");
        fixture.rejectMetrics.set(false);
        assertThat(fixture.publisher.prepare(fixture.pipeline, null)).isPresent();
        assertThat(fixture.collections.get()).isEqualTo(2);
        fixture.currentJob.set(null);
        assertThat(fixture.publisher.prepare(fixture.pipeline, null)).isPresent();
        assertThat(fixture.collections.get()).isEqualTo(2);
        fixture.currentJob.set(fixture.job(3));
        fixture.status.set(JobStatus.FAILED);
        assertThat(fixture.publisher.prepare(fixture.pipeline, null)).isPresent();
        assertThat(fixture.collections.get()).isEqualTo(2);
    }

    @Test
    void nestedAndCrossThreadUseCannotRemoveTheOwningFrame() throws Exception {
        Fixture fixture = new Fixture("orders");
        try (Engine.ObservationMetricsSession session = fixture.engine.openObservationMetrics(fixture.pipeline)) {
            assertThatThrownBy(() -> fixture.engine.openObservationMetrics(fixture.pipeline))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("nested");
            try (var executor = Executors.newSingleThreadExecutor()) {
                var result = executor.submit(() -> {
                    assertThatThrownBy(session::close).isInstanceOf(IllegalStateException.class)
                            .hasMessageContaining("owning thread");
                });
                result.get(5, TimeUnit.SECONDS);
            }
            assertThat(session.current()).isTrue();
            fixture.engine.recordCount(fixture.pipeline);
            fixture.engine.frontierGaps(fixture.pipeline);
            assertThat(fixture.collections.get()).isEqualTo(1);
        }
        assertThat(fixture.publisher.prepare(fixture.pipeline, null)).isPresent();
        assertThat(fixture.collections.get()).isEqualTo(2);
    }

    @Test
    void parallelPreparationsUseIndependentFrames() throws Exception {
        Fixture fixture = new Fixture("orders");
        CountDownLatch entered = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        Runnable held = () -> {
            entered.countDown();
            try {
                if (!release.await(5, TimeUnit.SECONDS)) { throw new AssertionError("both preparations did not enter"); }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new AssertionError(interrupted);
            }
        };
        fixture.onMetrics.set(held);
        try (var executor = Executors.newFixedThreadPool(2)) {
            var first = executor.submit(() -> fixture.publisher.prepare(fixture.pipeline, null));
            var second = executor.submit(() -> fixture.publisher.prepare(fixture.pipeline, null));
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            release.countDown();
            assertThat(first.get(5, TimeUnit.SECONDS)).isPresent();
            assertThat(second.get(5, TimeUnit.SECONDS)).isPresent();
        } finally {
            release.countDown();
        }
        assertThat(fixture.collections.get()).as("parallel preparations on the same engine each sample once").isEqualTo(2);
    }

    private static final class Fixture {
        private final String pipeline;
        private final AtomicInteger collections = new AtomicInteger();
        private final AtomicInteger jobLookups = new AtomicInteger();
        private final AtomicInteger statusReads = new AtomicInteger();
        private final AtomicInteger configurationReads = new AtomicInteger();
        private final AtomicReference<Job> currentJob = new AtomicReference<>();
        private final AtomicReference<Runnable> onMetrics = new AtomicReference<>(() -> { });
        private final AtomicReference<Runnable> onSnapshot = new AtomicReference<>(() -> { });
        private final AtomicBoolean rejectMetrics = new AtomicBoolean();
        private final AtomicReference<JobStatus> status = new AtomicReference<>(JobStatus.RUNNING);
        private final AtomicReference<Set<Member>> members = new AtomicReference<>();
        private final Engine engine;
        private final io.tapstate.runtime.scheduler.ObservationPublisher publisher;

        private Job job(long id) {
            return port(Job.class, (method, args) -> switch (method.getName()) {
                case "getStatus" -> {
                    statusReads.incrementAndGet();
                    yield status.get();
                }
                case "getId" -> id;
                case "getIdString" -> "0000-0000-0000-0001";
                case "getConfig" -> {
                    configurationReads.incrementAndGet();
                    yield new JobConfig();
                }
                case "getMetrics" -> {
                    collections.incrementAndGet();
                    if (rejectMetrics.get()) { throw new IllegalStateException("native collection refused"); }
                    onMetrics.get().run();
                    yield JobMetrics.of(Map.of());
                }
                default -> throw new AssertionError("unexpected job call: " + method.getName());
            });
        }

        private Fixture(String pipeline) {
            this(pipeline, true);
        }

        private Fixture(String pipeline, boolean shared) {
            this.pipeline = pipeline;
            currentJob.set(job(1));
            Member local = port(Member.class, (method, args) -> {
                if (method.getName().equals("getUuid")) {
                    return UUID.fromString("00000000-0000-0000-0000-000000000001");
                }
                throw new AssertionError("unexpected member call: " + method.getName());
            });
            members.set(Set.of(local));
            Cluster cluster = port(Cluster.class, (method, args) -> {
                if (method.getName().equals("getMembers")) { return members.get(); }
                throw new AssertionError("unexpected cluster call: " + method.getName());
            });
            JetService jet = port(JetService.class, (method, args) -> {
                if (method.getName().equals("getJob") && pipeline.equals(args[0])) {
                    jobLookups.incrementAndGet();
                    return currentJob.get();
                }
                throw new AssertionError("unexpected job lookup: " + method.getName());
            });
            HazelcastInstance member = port(HazelcastInstance.class, (method, args) -> switch (method.getName()) {
                case "getJet" -> jet;
                case "getCluster" -> cluster;
                default -> throw new AssertionError("unexpected instance call: " + method.getName());
            });
            StateStore state = port(StateStore.class, (method, args) -> {
                if (method.getName().equals("read") && pipeline.equals(args[0])) {
                    return Optional.of(CheckpointDoc.initial(pipeline, StateJson.of(PipelineState.RUNNING),
                            Instant.parse("2026-10-08T00:00:00Z")));
                }
                throw new AssertionError("unexpected state call: " + method.getName());
            });
            ObservationStore observations = port(ObservationStore.class, (method, args) -> {
                throw new AssertionError("preparation must not write an observation");
            });
            ArtifactStore artifacts = port(ArtifactStore.class, (method, args) -> {
                if (method.getName().equals("get") && pipeline.equals(args[0])) { return Optional.empty(); }
                throw new AssertionError("unexpected artifact read: " + method.getName());
            });
            StorePort stores = port(StorePort.class, (method, args) -> switch (method.getName()) {
                case "state" -> state;
                case "observations" -> observations;
                case "artifacts" -> artifacts;
                default -> throw new AssertionError("unexpected store binding: " + method.getName());
            });
            PipelineCaptureCoordinator captures = port(PipelineCaptureCoordinator.class, (method, args) -> switch (method.getName()) {
                case "snapshotProgress" -> {
                    onSnapshot.get().run();
                    yield SnapshotReading.NONE;
                }
                case "runSnapshotProgress" -> SnapshotReading.NONE;
                case "capturedRows" -> CaptureReading.NONE;
                default -> throw new AssertionError("unexpected capture call: " + method.getName());
            });
            engine = new Engine(member);
            publisher = shared ? new RuntimeConvergenceConfiguration().observationPublisher(stores, engine, captures)
                    : RuntimeConvergenceConfiguration.observationPublisherFor(stores, engine, captures,
                            id -> new ObservationPublisher.PreparationSession() {
                                @Override public boolean current() { return true; }
                                @Override public void close() { }
                            });
        }

        private int nativeRequests() {
            return collections.get() + jobLookups.get() + statusReads.get() + configurationReads.get();
        }

        private Map<String, Integer> requestCounts() {
            return Map.of("metrics", collections.get(), "jobLookups", jobLookups.get(), "status", statusReads.get(),
                    "config", configurationReads.get());
        }
    }

    private static <T> T port(Class<T> type, BiFunction<Method, Object[], Object> calls) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type},
                (proxy, method, arguments) -> {
                    if (method.getDeclaringClass() == Object.class) {
                        return switch (method.getName()) {
                            case "hashCode" -> System.identityHashCode(proxy);
                            case "equals" -> proxy == arguments[0];
                            case "toString" -> type.getSimpleName() + " test double";
                            default -> throw new AssertionError(method.getName());
                        };
                    }
                    return calls.apply(method, arguments == null ? new Object[0] : arguments);
                }));
    }
}
