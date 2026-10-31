package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.RingbufferStoreConfig;
import com.hazelcast.ringbuffer.Ringbuffer;
import io.tapstate.adapters.pdk.ConnectorStateNamespace;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.PipelineStateHolding;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.dsl.Workspace;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ReadMode;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.Settings;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TableRef;
import io.tapstate.runtime.srs.CaptureError;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.runtime.scheduler.ObservationPublisher;
import io.tapstate.runtime.scheduler.PipelineConverger;
import io.tapstate.runtime.srs.CaptureHandoff;
import io.tapstate.runtime.srs.CaptureHealth;
import io.tapstate.runtime.srs.CaptureRun;
import io.tapstate.runtime.srs.CaptureRunSpec;
import io.tapstate.runtime.srs.CaptureRunUnit;
import io.tapstate.runtime.srs.MiningChainId;
import io.tapstate.runtime.srs.SnapshotBuffer;
import io.tapstate.runtime.srs.SrsCoordinator;
import io.tapstate.runtime.srs.SrsLogRingbufferStoreFactory;
import io.tapstate.spi.capture.CaptureBatch;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.capture.CaptureListener;
import io.tapstate.spi.capture.CapturePort;
import io.tapstate.spi.capture.CaptureStart;
import io.tapstate.spi.capture.ConnectionReport;
import io.tapstate.spi.capture.DiscoveredSchema;
import io.tapstate.spi.capture.SourcePosition;
import io.tapstate.spi.capture.Subscription;
import io.tapstate.spi.store.ClusterMembership;
import io.tapstate.spi.store.ConsumerOffset;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimAttempt;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimReading;
import io.tapstate.spi.store.WorkloadClaimStore;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

class CaptureOwnershipScheduleWitnessTest {
    private static final Duration TTL = Duration.ofSeconds(30);
    private static final SourceResource SOURCE = new SourceResource(
            "orders-source", null, "mysql", Map.of("host", "db.internal"), SourceMode.CDC,
            List.of(TableRef.literal("orders")), null, null);

    @Test
    @org.junit.jupiter.api.Timeout(15)
    void callbackFirst() throws Exception { witness("callback"); }

    @Test
    @org.junit.jupiter.api.Timeout(15)
    void deleteFirst() throws Exception { witness("delete"); }

    @Test
    @org.junit.jupiter.api.Timeout(15)
    void replaceFirst() throws Exception { witness("replace"); }

    private static void witness(String schedule) throws Exception {
        ReleaseBarrier claims = new ReleaseBarrier();
        InMemoryStorePort store = new InMemoryStorePort(artifactsWith("p"));
        AtomicBoolean driving = new AtomicBoolean(true);
        AtomicInteger opened = new AtomicInteger();
        AtomicInteger closed = new AtomicInteger();
        SrsCoordinator chains = new SrsCoordinator(store.meta());
        ClusterMembershipGate gate = eligibleGate();
        CaptureOwnership ownership = new CaptureOwnership("cluster-a",
                new WorkloadOwner("node-a", "boot-node-a"), gate,
                new ClusterWorkloadClaims(claims, gate), TTL);
        StoreBackedPipelineCaptureCoordinator captures = new StoreBackedPipelineCaptureCoordinator(
                store, (spec, passthrough, startTail) -> {
                    assertThat(startTail).isTrue();
                    opened.incrementAndGet();
                    MiningChainId chain = spec.miningChainId();
                    long epoch = chains.provisionSource(spec.sourceId(), chain,
                            spec.config().streams(), spec.retention()).epoch();
                    store.meta().publishCaptureTables(chain.value(), epoch, store.meta().captureTables(chain.value()));
                    chains.attachConsumer(chain, spec.consumerId());
                    AtomicBoolean subscriptionClosed = new AtomicBoolean();
                    return new CaptureRun(Optional.of(chain), false, 0, Optional.empty(),
                            Optional.of((Subscription) () -> {
                                if (subscriptionClosed.compareAndSet(false, true)) {
                                    closed.incrementAndGet();
                                }
                            }), new CaptureHealth());
                }, chains, new SnapshotBuffer(), ownership, Duration.ofMillis(20));
        try {
            captures.runOnlyWhileDriving(ignored -> driving.get());
            captures.startCapture("p");
            assertThat(opened.get()).isEqualTo(1);
            claims.armed.set(true);
            driving.set(false);
            assertThat(claims.released.await(5, TimeUnit.SECONDS)).isTrue();
            Thread old = claims.retired.get();
            assertThat(old).isNotNull().isNotSameAs(Thread.currentThread());
            WorkloadClaimKey key = claims.releasedClaim.get().key();
            assertThat(claims.delegate.read(key).orElseThrow().leased()).isFalse();
            if (schedule.equals("callback")) {
                claims.proceed.countDown();
                join(old);
                assertThat(captures.captureFailure("p")).isPresent();
                assertThat(closed.get()).isEqualTo(1);
                captures.stopCapture("p", false);
            } else {
                captures.stopCapture("p", false);
                assertThat(closed.get()).isEqualTo(1);
                if (schedule.equals("replace")) {
                    driving.set(true);
                    captures.startCapture("p");
                    assertThat(opened.get()).isEqualTo(2);
                    assertThat(captures.isCapturing("p")).isTrue();
                    assertThat(captures.captureFailure("p")).isEmpty();
                }
                claims.proceed.countDown();
                join(old);
                assertThat(closed.get()).isEqualTo(1);
                if (schedule.equals("replace")) {
                    assertThat(captures.isCapturing("p")).isTrue();
                    assertThat(captures.captureFailure("p")).isEmpty();
                    assertThat(claims.delegate.read(key).orElseThrow().leased()).isTrue();
                }
            }
            assertThat(claims.barrierFailure.get()).isNull();
        } finally {
            claims.proceed.countDown();
            try { captures.stopCapture("p", false); }
            finally { captures.close(); }
            Thread old = claims.retired.get();
            if (old != null) join(old);
            assertThat(closed.get()).isEqualTo(opened.get());
        }
    }

    private static void join(Thread thread) throws InterruptedException {
        thread.join(5000);
        assertThat(thread.isAlive()).as("the exact retired renewer has finished its callback").isFalse();
    }

    private static final class ReleaseBarrier implements WorkloadClaimStore {
        final InMemoryWorkloadClaimStore delegate = new InMemoryWorkloadClaimStore();
        final AtomicBoolean armed = new AtomicBoolean();
        final CountDownLatch released = new CountDownLatch(1);
        final CountDownLatch proceed = new CountDownLatch(1);
        final AtomicReference<Thread> retired = new AtomicReference<>();
        final AtomicReference<WorkloadClaim> releasedClaim = new AtomicReference<>();
        final AtomicReference<Throwable> barrierFailure = new AtomicReference<>();
        public WorkloadClaimAttempt acquire(WorkloadClaimKey key, WorkloadOwner owner,
                long revision, Duration ttl) { return delegate.acquire(key, owner, revision, ttl); }
        public Optional<WorkloadClaim> renew(WorkloadClaim claim, Duration ttl) {
            return delegate.renew(claim, ttl);
        }
        public boolean release(WorkloadClaim claim) {
            boolean result = delegate.release(claim);
            if (!result) barrierFailure.compareAndSet(null, new AssertionError("owned claim release refused"));
            if (armed.compareAndSet(true, false)) {
                retired.set(Thread.currentThread());
                releasedClaim.set(claim);
                released.countDown();
                try {
                    if (!proceed.await(5, TimeUnit.SECONDS)) {
                        barrierFailure.set(new AssertionError("release barrier deadline"));
                    }
                } catch (InterruptedException interrupted) {
                    barrierFailure.set(interrupted);
                    Thread.currentThread().interrupt();
                }
            }
            return result;
        }
        public Optional<WorkloadClaim> advanceExecution(WorkloadClaim claim, long revision,
                Set<String> nodes) { return delegate.advanceExecution(claim, revision, nodes); }
        public Optional<WorkloadClaim> recordExecutionFailure(WorkloadClaim claim, boolean afterLoss) {
            return delegate.recordExecutionFailure(claim, afterLoss);
        }
        public Optional<WorkloadClaimReading> read(WorkloadClaimKey key) { return delegate.read(key); }
    }

    private static ClusterMembershipGate eligibleGate() {
        ClusterProperties properties = new ClusterProperties();
        properties.setProfile(ClusterProperties.Profile.PRODUCTION_HA);
        ClusterMembershipGate gate = new ClusterMembershipGate(properties);
        gate.observeNodeSession(() -> true);
        gate.install(new ClusterMembership("cluster-a", 7, Set.of("node-a", "node-b", "node-c")));
        gate.canCommit(Set.of("node-a", "node-b"));
        return gate;
    }

    private static InMemoryArtifactStore artifactsWith(String... pipelineIds) {
        return artifactsWith(ReadMode.CDC_ONLY, pipelineIds);
    }

    private static InMemoryArtifactStore artifactsWith(ReadMode readMode, String... pipelineIds) {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(SOURCE);
        for (String pipelineId : pipelineIds) {
            artifacts.save(new PipelineResource(
                    pipelineId, null, List.of(SourceRef.spec("orders-source", true)), null, null,
                    new ServeBlock.Inline(
                            null, FromRef.literal("orders-source"),
                            List.of(new SyncElement("sync", "target", null, null, null)), null, null),
                    new Settings(null, null, null, null, readMode, "earliest"), null));
        }
        return artifacts;
    }

}
