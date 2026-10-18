package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.runtime.engine.EngineError;
import io.tapstate.runtime.engine.SinkAck;
import io.tapstate.runtime.engine.SinkAckFactory;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimStore;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Exercises the server serializer configuration, including the scope nested inside a DAG ack factory. */
class ScopedExecutionSerializationTest {
    private static final PipelineIntentScope FIRST = new PipelineIntentScope("cluster", 3, "cache-a", "orders", 1);
    private static final CacheBusinessScope BUSINESS = new CacheBusinessScope("cluster", 3, "cache-a");
    private static final WorkloadOwner OWNER = new WorkloadOwner("node-a", "boot-a");
    private static final WorkloadClaimKey KEY = new WorkloadClaimKey("cluster", WorkloadClaimType.PIPELINE_ACTUATION, "orders");
    private static final String LANDED = "test.scoped-landed";
    private static final String CLAIM = "test.scoped-claim";

    @Test void aScopedFenceUsesTheServersActualSerializerConfiguration() {
        var original = new ExecutionFence(FIRST, new WorkloadClaimFence(KEY, OWNER, 2, 4, 7));
        InternalSerializationService serializer = serializer();
        ExecutionFence received = serializer.toObject(serializer.toData(original));
        assertThat(received).isEqualTo(original);
        assertThat(received.scope()).isEqualTo(FIRST);
        assertThat(received.ownerBootId()).isEqualTo("boot-a"); assertThat(received.topologyRevision()).isEqualTo(7L);
    }

    @Test void aSerializedAckFactoryCarriesTheOriginalScopeToItsReceivingGuard() {
        var raw = new InMemoryWorkloadClaimStore();
        var claimed = raw.acquire(KEY, OWNER, 7, Duration.ofSeconds(30)).claim();
        var run = raw.advanceExecution(claimed, 7, Set.of("node-a")).orElseThrow();
        WorkloadClaimFence actual = WorkloadClaimFence.from(run);
        AtomicReference<PipelineIntentScope> current = new AtomicReference<>(FIRST);
        WorkloadClaimStore store = mock(WorkloadClaimStore.class);
        when(store.readPipeline(any())).thenAnswer(call -> {
            if (!call.getArgument(0).equals(current.get())) throw new TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null);
            return raw.read(KEY);
        });
        AtomicLong nanos = new AtomicLong();
        AtomicInteger landed = new AtomicInteger();
        AtomicReference<WorkloadClaimFence> receivedClaim = new AtomicReference<>();
        try (var guard = ExecutionAuthorization.scoped(BUSINESS, store, Duration.ofSeconds(10), nanos::get, () -> true)) {
            HazelcastInstance receiver = mock(HazelcastInstance.class);
            when(receiver.getUserContext()).thenReturn(new ConcurrentHashMap<>(Map.of(
                    ExecutionAuthorization.USER_CONTEXT_KEY, guard, LANDED, landed, CLAIM, receivedClaim)));
            SinkAckFactory sent = FencedSinkAckFactory.heldTo(new RecordingFactory(), new ExecutionFence(FIRST, actual));
            InternalSerializationService serializer = serializer();
            SinkAckFactory received = serializer.toObject(serializer.toData(sent));
            received.beginRun(receiver, Map.of());
            SinkAck ack = received.resolve(receiver).forWriter("writer-1");
            ack.advance("items", new ChainPosition(new SourceOrder(1, 1), "p1"));
            assertThat(receivedClaim.get()).isEqualTo(actual);
            assertThat(landed).hasValue(1);

            var next = new PipelineIntentScope("cluster", 3, "cache-a", "orders", 2);
            current.set(next);
            assertThat(guard.authorized(new ExecutionFence(next, actual))).isTrue();
            assertThatThrownBy(() -> received.beginRun(receiver, Map.of())).isInstanceOfSatisfying(TapstateException.class,
                    failure -> assertThat(failure.code()).isEqualTo(EngineError.EXECUTION_NOT_AUTHORIZED));
            assertThatThrownBy(() -> ack.advance("items", new ChainPosition(new SourceOrder(1, 2), "p2")))
                    .isInstanceOfSatisfying(TapstateException.class,
                            failure -> assertThat(failure.code()).isEqualTo(EngineError.EXECUTION_NOT_AUTHORIZED));
            assertThat(landed).hasValue(1);
            verify(store, never()).read(any());
        }
    }

    @Test void aSerializedOldAckCannotAdoptASameOwnersNewTopology() {
        var raw = new InMemoryWorkloadClaimStore();
        var claimed = raw.acquire(KEY,OWNER,7,Duration.ofSeconds(30)).claim();
        var run = raw.advanceExecution(claimed,7,Set.of(OWNER.nodeId())).orElseThrow(); var original = WorkloadClaimFence.from(run);
        WorkloadClaimStore store = mock(WorkloadClaimStore.class); when(store.readPipeline(FIRST)).thenAnswer(call -> raw.read(KEY));
        var landed = new AtomicInteger(); var actual = new AtomicReference<WorkloadClaimFence>();
        try (var guard = ExecutionAuthorization.scoped(BUSINESS,store,Duration.ofSeconds(10),System::nanoTime,() -> true)) {
            HazelcastInstance receiver = mock(HazelcastInstance.class);
            when(receiver.getUserContext()).thenReturn(new ConcurrentHashMap<>(Map.of(ExecutionAuthorization.USER_CONTEXT_KEY,guard,LANDED,landed,CLAIM,actual)));
            var serializer = serializer(); SinkAckFactory received = serializer.toObject(serializer.toData(
                    FencedSinkAckFactory.heldTo(new RecordingFactory(),new ExecutionFence(FIRST,original))));
            received.beginRun(receiver,Map.of()); var ack = received.resolve(receiver).forWriter("writer-1");
            ack.advance("items",new ChainPosition(new SourceOrder(1,1),"p1"));
            var later = raw.acquire(KEY,OWNER,8,Duration.ofSeconds(30)).claim();
            assertThat(guard.authorized(new ExecutionFence(FIRST,WorkloadClaimFence.from(later)))).isTrue();
            assertThatThrownBy(() -> received.beginRun(receiver,Map.of())).isInstanceOfSatisfying(TapstateException.class,
                    failure -> assertThat(failure.code()).isEqualTo(EngineError.EXECUTION_NOT_AUTHORIZED));
            assertThatThrownBy(() -> ack.advance("items",new ChainPosition(new SourceOrder(1,2),"p2"))).isInstanceOfSatisfying(TapstateException.class,
                    failure -> assertThat(failure.code()).isEqualTo(EngineError.EXECUTION_NOT_AUTHORIZED));
            assertThat(landed).hasValue(1); assertThat(actual.get()).isEqualTo(original);
        }
    }

    private static InternalSerializationService serializer() {
        return new DefaultSerializationServiceBuilder()
                .setConfig(HazelcastConfiguration.memberConfig(new HazelcastProperties()).getSerializationConfig()).build();
    }

    private static final class RecordingFactory implements SinkAckFactory {
        private static final long serialVersionUID = 1L;
        @Override public SinkAck resolve(HazelcastInstance member) {
            return new SinkAck() {
                @Override public void advance(String chain, ChainPosition position) { throw new AssertionError("claim proof lost"); }
                @Override public void advance(String chain, ChainPosition position, WorkloadClaimFence claim) {
                    received(member).set(claim); ((AtomicInteger) member.getUserContext().get(LANDED)).incrementAndGet();
                }
            };
        }
        @Override public void beginRun(HazelcastInstance member, Map<String, List<String>> writers, WorkloadClaimFence claim) {
            received(member).set(claim);
        }
        @SuppressWarnings("unchecked") private static AtomicReference<WorkloadClaimFence> received(HazelcastInstance member) {
            return (AtomicReference<WorkloadClaimFence>) member.getUserContext().get(CLAIM);
        }
    }
}
