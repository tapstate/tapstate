package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.hazelcast.config.Config;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.DAG;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.lifecycle.NodeParallelism;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TransformBody;
import io.tapstate.runtime.engine.DagBindings;
import io.tapstate.runtime.engine.EnvelopeSerializer;
import io.tapstate.runtime.engine.ExecutionShape;
import io.tapstate.runtime.engine.PipelineDagBuilder;
import io.tapstate.runtime.engine.PreparesTargets;
import io.tapstate.runtime.engine.SinkTarget;
import io.tapstate.runtime.srs.SnapshotBuffer;
import io.tapstate.runtime.srs.SourcePlacement;
import io.tapstate.runtime.srs.SrsSourceProcessor;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.TargetKeyConstraints;
import io.tapstate.spi.sink.WriteResult;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.PipelineKeyMoveWindows;
import io.tapstate.spi.store.SrsChainStore;
import io.tapstate.spi.store.SrsConsumerStore;
import io.tapstate.spi.store.SrsLogStore;
import io.tapstate.spi.store.SrsModelStore;
import io.tapstate.spi.store.SrsStoreViews;
import io.tapstate.spi.store.SrsStores;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class SourceKeyMovePreflightTest {
    private static HazelcastInstance first;
    private static HazelcastInstance second;
    private static final AtomicInteger WRITES = new AtomicInteger();
    private static final AtomicInteger PREPARATIONS = new AtomicInteger();

    @BeforeAll static void start() {
        String name = "source-preflight-" + UUID.randomUUID();
        first = Hazelcast.newHazelcastInstance(config(name));
        second = Hazelcast.newHazelcastInstance(config(name));
        assertThat(first.getCluster().getMembers()).hasSize(2);
    }

    @AfterAll static void stop() {
        if (second != null) second.shutdown();
        if (first != null) first.shutdown();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void aRejectedFirstHopNeverArmsAnIntervalOrPoisonsTheNextExecution(boolean directSink) {
        var pending = new AtomicReference<PipelineKeyMoveWindows.Window>();
        var windows = mock(PipelineKeyMoveWindows.class);
        when(windows.read("flow")).thenAnswer(call -> Optional.ofNullable(pending.get()));
        when(windows.arm(any(), anyLong(), anyString(), any(), any(), any(), any())).thenAnswer(call -> {
            var value = new PipelineKeyMoveWindows.Window(call.getArgument(0), call.getArgument(1),
                    call.getArgument(2), UUID.randomUUID(), Map.of(call.getArgument(3), call.getArgument(4)));
            pending.set(value);
            return value;
        });
        var scope = new PipelineIntentScope("cluster", 1, "cache", "flow", 1);
        var fence = new ExecutionFence("flow", 1, 1, scope, "owner", "boot", 1L);
        var authorization = mock(ExecutionAuthorization.class);
        var views = mock(SrsStoreViews.class);
        var consumers = mock(SrsConsumerStore.class);
        when(views.srs(any(), any())).thenReturn(new SrsStores(mock(SrsChainStore.class),
                mock(SrsModelStore.class), mock(SrsLogStore.class), consumers));
        for (var member : List.of(first, second)) {
            member.getUserContext().put(ScopedKeyMoveGuard.STORE_KEY, windows);
            member.getUserContext().put(ExecutionAuthorization.USER_CONTEXT_KEY, authorization);
            member.getUserContext().put(SrsStoreViews.USER_CONTEXT_KEY, views);
        }
        WRITES.set(0);
        PREPARATIONS.set(0);
        String expected = directSink ? "TS0440017" : "TS0440013";
        for (int attempt = 0; attempt < 2; attempt++) {
            var buffer = new SnapshotBuffer();
            String ring = "source-preflight." + UUID.randomUUID();
            var original = Map.<String, Object>of("id", 1, "owner", "first");
            // The first candidate is acceptable. A later rejected row prevents this entire source batch from leaving.
            buffer.append("flow", ring, Envelope.update(1, "orders", original,
                    Map.of("id", 1, "owner", "second"), null)
                    .withPosition(new ChainPosition(new SourceOrder(1, 1), null)));
            buffer.append("flow", ring, Envelope.update(2, "orders", original,
                    Map.of("id", 2, "owner", "first"), null)
                    .withPosition(new ChainPosition(new SourceOrder(1, 2), null)));
            first.getUserContext().put(SnapshotBuffer.USER_CONTEXT_KEY, buffer);
            var guard = new ScopedKeyMoveGuard(fence, List.of("id", "owner"),
                    new PipelineKeyMoveWindows.Stream("chain", "input", "orders", null));
            var source = SrsSourceProcessor.directMetaSupplier("flow", ring, "orders", 1, null,
                    SourcePlacement.on(first.getCluster().getLocalMember().getAddress()), 32, guard);
            var pipeline = pipeline(directSink);
            var bindings = new DagBindings(key -> source,
                    step -> () -> row -> List.of(row), element -> new TargetFactory(),
                    ref -> List.of(((FromRef.Literal) ref).ref()));
            DAG dag = PipelineDagBuilder.build(pipeline, bindings, null, null, shape(directSink));
            var job = first.getJet().newJob(dag);
            assertThatThrownBy(() -> job.getFuture().get(20, TimeUnit.SECONDS)).hasStackTraceContaining(expected);
            assertThat(pending.get()).as("a first-hop rejection must leave no ambiguous recovery interval").isNull();
            assertThat(WRITES.get()).as("nothing in this source batch may reach a target").isZero();
        }
        assertThat(PREPARATIONS.get()).isEqualTo(directSink ? 2 : 0);
    }

    @Test
    void ordinaryRowsSkipPreflightAndALaterRefusalPreservesAnAlreadyAdmittedInterval() {
        var pending = new AtomicReference<PipelineKeyMoveWindows.Window>();
        var windows = mock(PipelineKeyMoveWindows.class);
        when(windows.read("flow")).thenAnswer(call -> Optional.ofNullable(pending.get()));
        when(windows.arm(any(), anyLong(), anyString(), any(), any(), any(), any())).thenAnswer(call -> {
            var admitted = new PipelineKeyMoveWindows.Window(call.getArgument(0), call.getArgument(1),
                    call.getArgument(2), UUID.randomUUID(), Map.of(call.getArgument(3), call.getArgument(4)));
            pending.set(admitted);
            return admitted;
        });
        var scope = new PipelineIntentScope("cluster", 1, "cache", "flow", 1);
        var fence = new ExecutionFence("flow", 1, 1, scope, "owner", "boot", 1L);
        first.getUserContext().put(ScopedKeyMoveGuard.STORE_KEY, windows);
        first.getUserContext().put(ExecutionAuthorization.USER_CONTEXT_KEY, mock(ExecutionAuthorization.class));
        var context = mock(com.hazelcast.jet.core.Processor.Context.class);
        when(context.hazelcastInstance()).thenReturn(first);
        when(context.executionId()).thenReturn(7L);
        var checks = new AtomicInteger();
        var refused = new io.tapstate.core.common.TapstateException(
                io.tapstate.runtime.engine.EngineError.TARGET_KEY_ORDER_UNKNOWN,
                Map.of("node", "target", "stream", "orders"), null);
        var guard = new ScopedKeyMoveGuard(fence, List.of("id"),
                new PipelineKeyMoveWindows.Stream("chain", "input", "orders", null)).open(context, row -> {
                    checks.incrementAndGet();
                    if (row.after().get("id").equals(3)) throw refused;
                });
        guard.beforeBatch(List.of(Envelope.update(1, "orders", Map.of("id", 1, "value", "old"),
                Map.of("id", 1, "value", "new"), null)));
        assertThat(checks.get()).isZero();
        assertThat(pending.get()).isNull();
        var admitted = Envelope.update(2, "orders", Map.of("id", 1), Map.of("id", 2), null)
                .withPosition(new ChainPosition(new SourceOrder(1, 2), null));
        guard.beforeBatch(List.of(admitted));
        var original = pending.get();
        assertThat(original).isNotNull();
        var later = Envelope.update(3, "orders", Map.of("id", 2), Map.of("id", 3), null)
                .withPosition(new ChainPosition(new SourceOrder(1, 3), null));
        assertThatThrownBy(() -> guard.beforeBatch(List.of(later))).isSameAs(refused);
        assertThat(pending.get()).isSameAs(original);
        assertThat(checks.get()).isEqualTo(2);
    }

    private static PipelineResource pipeline(boolean direct) {
        return new PipelineResource("flow", null, List.of(SourceRef.bare("input")),
                direct ? null : List.of(Step.inline("map", FromClause.list(FromRef.literal("input")),
                        new TransformBody.Js("row"), null)), null,
                new ServeBlock.Inline(null, FromClause.list(FromRef.literal(direct ? "input" : "map")),
                        List.of(new SyncElement("s", "target", null, null, null)), null, null), null, null);
    }

    private static ExecutionShape shape(boolean direct) {
        String node = direct ? "serve.s" : "map";
        var width = new NodeParallelism(node, 2, NodeParallelism.Origin.EXPLICIT,
                NodeParallelism.Scope.NATIVE, 2, 1, 2, List.of());
        return new ExecutionShape(2, Map.of(node, width),
                direct ? Map.of() : Map.of(node, Map.of("orders", List.of("id"))),
                direct ? Map.of(node, Map.of("orders", new SinkTarget("orders", List.of("id")))) : Map.of());
    }

    private static final class TargetFactory implements SupplierEx<SinkWriter>, PreparesTargets {
        @Override public SinkWriter getEx() {
            return new SinkWriter() {
                @Override public java.util.concurrent.CompletionStage<WriteResult> write(List<Envelope> rows) {
                    WRITES.addAndGet(rows.size());
                    return CompletableFuture.completedFuture(new WriteResult(rows.size()));
                }
                @Override public void close() {}
            };
        }
        @Override public void prepareTargets(HazelcastInstance member) {}
        @Override public boolean supportsParallelKeyMoves() { return true; }
        @Override public Map<String, TargetKeyConstraints> prepareTargetConstraints(HazelcastInstance member) {
            PREPARATIONS.incrementAndGet();
            return Map.of("orders", TargetKeyConstraints.unknown());
        }
    }

    private static Config config(String name) {
        var c = new Config().setClusterName(name);
        c.getJetConfig().setEnabled(true).setCooperativeThreadCount(2);
        c.setProperty("hazelcast.phone.home.enabled", "false");
        c.setProperty("hazelcast.shutdownhook.enabled", "false");
        c.setProperty("hazelcast.logging.type", "none");
        c.getNetworkConfig().setPort(15895).setPortAutoIncrement(true).setPortCount(2);
        var join = c.getNetworkConfig().getJoin();
        join.getAutoDetectionConfig().setEnabled(false);
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(true).setMembers(List.of("127.0.0.1:15895", "127.0.0.1:15896"));
        c.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        c.getSerializationConfig().addSerializerConfig(new SerializerConfig().setTypeClass(Envelope.class)
                .setImplementation(new EnvelopeSerializer()));
        return c;
    }
}
