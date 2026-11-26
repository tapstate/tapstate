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
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.lifecycle.NodeParallelism;
import io.tapstate.core.model.FieldRule;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class SourceKeyMovePreflightTest {
    private static HazelcastInstance first;
    private static HazelcastInstance second;
    private static final AtomicInteger WRITES = new AtomicInteger();
    private static final AtomicInteger PREPARATIONS = new AtomicInteger();
    private static final AtomicInteger TRANSFORMS = new AtomicInteger();
    private static final AtomicInteger SECOND_TRANSFORMS = new AtomicInteger();
    private static final ConcurrentLinkedQueue<Envelope> TRANSFORMED_WRITES = new ConcurrentLinkedQueue<>();
    private static final ConcurrentLinkedQueue<PipelineKeyMoveWindows.Window> INTERVALS_AT_WRITE =
            new ConcurrentLinkedQueue<>();
    private static AtomicReference<PipelineKeyMoveWindows.Window> currentInterval;
    private static CountDownLatch transformedWrite;
    private static CompletableFuture<Void> expansionObserved;
    private static CompletableFuture<WriteResult> expansionAcknowledged;

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
    @CsvSource({"false,false", "true,false", "false,true"})
    void aRejectedRouteNeverArmsAnIntervalOrPoisonsTheNextExecution(boolean directSink, boolean serialMap) {
        var fixture = new Fixture();
        var pending = fixture.pending;
        WRITES.set(0);
        PREPARATIONS.set(0);
        String expected = directSink ? "TS0440017" : "TS0440013";
        var checks = new SoftAssertions();
        for (int attempt = 0; attempt < 2; attempt++) {
            var rows = new ArrayList<Envelope>();
            var original = Map.<String, Object>of("id", 1, "owner", "first");
            if (!serialMap) {
                // A rejected row must prevent admission of earlier candidates from the same source batch.
                rows.add(Envelope.update(1, "orders", original, Map.of("id", 1, "owner", "second"), null)
                        .withPosition(new ChainPosition(new SourceOrder(1, 1), null)));
            }
            rows.add(Envelope.update(2, "orders", original, Map.of("id", 2, "owner", "first"), null)
                    .withPosition(new ChainPosition(new SourceOrder(1, 2), null)));
            var source = fixture.source(List.of("id", "owner"), rows);
            var pipeline = pipeline(directSink);
            var bindings = new DagBindings(key -> source,
                    step -> () -> row -> List.of(row), element -> new TargetFactory(),
                    ref -> List.of(((FromRef.Literal) ref).ref()));
            DAG dag = PipelineDagBuilder.build(pipeline, bindings, null, null, shape(directSink, serialMap));
            var job = first.getJet().newJob(dag);
            checks.assertThatThrownBy(() -> job.getFuture().get(20, TimeUnit.SECONDS)).hasStackTraceContaining(expected);
            checks.assertThat(WRITES.get()).as("nothing in this source batch may reach a target").isZero();
            checks.assertThat(pending.get()).as("a routing rejection must leave no ambiguous recovery interval").isNull();
        }
        checks.assertThat(PREPARATIONS.get()).isEqualTo(directSink ? 2 : 0);
        checks.assertAll();
    }

    @ParameterizedTest
    @CsvSource({"false,false", "true,false", "false,true"})
    void aSerialMapChecksEveryActualOutputOnceBeforeAdmittingTheOriginalPosition(
            boolean rejectedOutput, boolean multipleTargets)
            throws Exception {
        var fixture = new Fixture();
        TRANSFORMS.set(0);
        SECOND_TRANSFORMS.set(0);
        TRANSFORMED_WRITES.clear();
        INTERVALS_AT_WRITE.clear();
        int expectedWrites = (multipleTargets ? 2 : 1) * 2;
        transformedWrite = new CountDownLatch(expectedWrites);
        var originalPosition = new ChainPosition(new SourceOrder(1, 7), null);
        var rows = new ArrayList<Envelope>();
        rows.add(Envelope.update(7, "orders", Map.of("id", 1), Map.of("id", 2), null)
                .withPosition(originalPosition));
        if (!rejectedOutput) {
            rows.add(Envelope.update(8, "orders", Map.of("id", 2), Map.of("id", 2), null)
                    .withPosition(new ChainPosition(new SourceOrder(1, 8), null)));
        }
        var source = fixture.source(List.of("id"), rows);
        var mapped = new TransformBody.MapProjection(Map.of("id", FieldRule.literal(9)));
        var targets = multipleTargets
                ? List.of(new SyncElement("s", "target", null, null, null),
                        new SyncElement("other", "target", null, null, null))
                : List.of(new SyncElement("s", "target", null, null, null));
        var pipeline = new PipelineResource("flow", null, List.of(SourceRef.bare("input")),
                List.of(Step.inline("map", FromClause.list(FromRef.literal("input")), mapped, null),
                        Step.inline("second", FromClause.list(FromRef.literal("map")), mapped, null)), null,
                new ServeBlock.Inline(null, FromClause.list(FromRef.literal("second")),
                        targets, null, null), null, null);
        var bindings = new DagBindings(key -> source, step -> {
            if (step.id().equals("second")) return () -> event -> {
                SECOND_TRANSFORMS.incrementAndGet();
                return List.of(event);
            };
            return () -> event -> {
                TRANSFORMS.incrementAndGet();
                var stable = Envelope.update(event.ts(), event.src(), Map.of("id", 9), Map.of("id", 9), null);
                return rejectedOutput ? List.of(stable, event) : List.of(stable);
            };
        }, element -> new ObservingTargetFactory(), ref -> List.of(((FromRef.Literal) ref).ref()));
        var job = first.getJet().newJob(PipelineDagBuilder.build(pipeline, bindings, null, null, shape(false, true)));
        try {
            if (rejectedOutput) {
                assertThatThrownBy(() -> job.getFuture().get(20, TimeUnit.SECONDS))
                        .hasStackTraceContaining("TS0440013");
                assertThat(currentInterval.get()).isNull();
                assertThat(TRANSFORMED_WRITES).isEmpty();
            } else {
                assertThat(transformedWrite.await(20, TimeUnit.SECONDS)).as("the mapped row must reach its writer").isTrue();
                assertThat(TRANSFORMED_WRITES).hasSize(expectedWrites).allSatisfy(row -> {
                    assertThat(row.before()).containsEntry("id", 9);
                    assertThat(row.after()).containsEntry("id", 9);
                    assertThat(row.position().order()).isIn(new SourceOrder(1, 7), new SourceOrder(1, 8));
                });
                assertThat(TRANSFORMED_WRITES).filteredOn(row -> row.position().equals(originalPosition))
                        .hasSize(multipleTargets ? 2 : 1);
                assertThat(INTERVALS_AT_WRITE).isNotEmpty().allSatisfy(window -> {
                    assertThat(window.scope()).isEqualTo(fixture.scope);
                    assertThat(window.ranges()).containsValue(new PipelineKeyMoveWindows.Range(1, 7, 7));
                });
            }
            assertThat(TRANSFORMS.get()).as("each source row passes the first Map once")
                    .isEqualTo(rejectedOutput ? 1 : 2);
            assertThat(SECOND_TRANSFORMS.get()).as("each actual first-Map output passes the second Map once")
                    .isEqualTo(2);
        } finally {
            if (!job.getFuture().isDone()) job.cancel();
        }
    }

    @Test
    void aSecondSerialExpansionStartsOnlyAfterTheOriginalIntervalIsArmed() throws Exception {
        var fixture = new Fixture();
        TRANSFORMS.set(0);
        SECOND_TRANSFORMS.set(0);
        TRANSFORMED_WRITES.clear();
        expansionObserved = new CompletableFuture<>();
        expansionAcknowledged = new CompletableFuture<>();
        var position = new ChainPosition(new SourceOrder(1, 7), null);
        var original = Envelope.update(7, "orders", Map.of("id", 1), Map.of("id", 2), null)
                .withPosition(position);
        var source = fixture.source(List.of("id"), List.of(original));
        var script = new TransformBody.Js("row");
        var pipeline = new PipelineResource("flow", null, List.of(SourceRef.bare("input")),
                List.of(Step.inline("map", FromClause.list(FromRef.literal("input")), script, null),
                        Step.inline("second", FromClause.list(FromRef.literal("map")), script, null)), null,
                new ServeBlock.Inline(null, FromClause.list(FromRef.literal("second")),
                        List.of(new SyncElement("s", "target", null, null, null)), null, null), null, null);
        var bindings = new DagBindings(key -> source, step -> {
            if (step.id().equals("second")) return () -> event -> {
                assertThat(currentInterval.get())
                        .as("a second expansion must start after admission, before allocating its outputs").isNotNull();
                SECOND_TRANSFORMS.incrementAndGet();
                return Collections.nCopies(1_000, event);
            };
            return () -> event -> {
                TRANSFORMS.incrementAndGet();
                var stable = Envelope.update(event.ts(), event.src(), Map.of("id", 9), Map.of("id", 9), null);
                return Collections.nCopies(1_000, stable);
            };
        }, element -> new ExpansionTargetFactory(), ref -> List.of(((FromRef.Literal) ref).ref()));
        var job = first.getJet().newJob(PipelineDagBuilder.build(pipeline, bindings, null, null, shape(false, true)));
        try {
            CompletableFuture.anyOf(job.getFuture(), expansionObserved).get(20, TimeUnit.SECONDS);
            assertThat(expansionObserved.isDone()).as("at least one expanded batch must reach the held writer").isTrue();
            assertThat(expansionAcknowledged.isDone()).as("the writer holds backpressure during observation").isFalse();
            assertThat(TRANSFORMS.get()).as("preparation and delivery share the first expansion").isEqualTo(1);
            assertThat(SECOND_TRANSFORMS.get()).isPositive();
            assertThat(TRANSFORMED_WRITES).isNotEmpty().allSatisfy(row -> {
                assertThat(row.position()).isEqualTo(position);
                assertThat(row.before()).containsEntry("id", 9);
                assertThat(row.after()).containsEntry("id", 9);
            });
            assertThat(currentInterval.get().ranges()).containsValue(new PipelineKeyMoveWindows.Range(1, 7, 7));
        } finally {
            if (!job.getFuture().isDone()) job.cancel();
            expansionAcknowledged.completeExceptionally(new java.util.concurrent.CancellationException("test stopped"));
        }
    }

    @Test
    void ordinaryRowsSkipPreflightAndALaterRefusalPreservesAnAlreadyAdmittedInterval() {
        var fixture = new Fixture();
        var pending = fixture.pending;
        var context = mock(com.hazelcast.jet.core.Processor.Context.class);
        when(context.hazelcastInstance()).thenReturn(first);
        when(context.executionId()).thenReturn(7L);
        var checks = new AtomicInteger();
        var refused = new io.tapstate.core.common.TapstateException(
                io.tapstate.runtime.engine.EngineError.TARGET_KEY_ORDER_UNKNOWN,
                Map.of("node", "target", "stream", "orders"), null);
        var guard = fixture.guard(List.of("id")).open(context, row -> {
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

    private static final class Fixture {
        private final AtomicReference<PipelineKeyMoveWindows.Window> pending = new AtomicReference<>();
        private final PipelineIntentScope scope = new PipelineIntentScope("cluster", 1, "cache", "flow", 1);
        private final ExecutionFence fence = new ExecutionFence("flow", 1, 1, scope, "owner", "boot", 1L);

        Fixture() {
            currentInterval = pending;
            var windows = mock(PipelineKeyMoveWindows.class);
            when(windows.read("flow")).thenAnswer(call -> Optional.ofNullable(pending.get()));
            when(windows.arm(any(), anyLong(), anyString(), any(), any(), any(), any())).thenAnswer(call -> {
                var window = new PipelineKeyMoveWindows.Window(call.getArgument(0), call.getArgument(1),
                        call.getArgument(2), UUID.randomUUID(), Map.of(call.getArgument(3), call.getArgument(4)));
                pending.set(window);
                return window;
            });
            var views = mock(SrsStoreViews.class);
            when(views.srs(any(), any())).thenReturn(new SrsStores(mock(SrsChainStore.class),
                    mock(SrsModelStore.class), mock(SrsLogStore.class), mock(SrsConsumerStore.class)));
            var authorization = mock(ExecutionAuthorization.class);
            for (var member : List.of(first, second)) {
                member.getUserContext().put(ScopedKeyMoveGuard.STORE_KEY, windows);
                member.getUserContext().put(ExecutionAuthorization.USER_CONTEXT_KEY, authorization);
                member.getUserContext().put(SrsStoreViews.USER_CONTEXT_KEY, views);
            }
        }

        ScopedKeyMoveGuard guard(List<String> keys) {
            return new ScopedKeyMoveGuard(fence, keys,
                    new PipelineKeyMoveWindows.Stream("chain", "input", "orders", null));
        }

        ProcessorMetaSupplier source(List<String> keys, List<Envelope> rows) {
            var buffer = new SnapshotBuffer();
            String ring = "source-preflight." + UUID.randomUUID();
            for (var row : rows) buffer.append("flow", ring, row);
            first.getUserContext().put(SnapshotBuffer.USER_CONTEXT_KEY, buffer);
            return SrsSourceProcessor.directMetaSupplier("flow", ring, "orders", 1, null,
                    SourcePlacement.on(first.getCluster().getLocalMember().getAddress()), 32, guard(keys));
        }
    }

    private static PipelineResource pipeline(boolean direct) {
        return new PipelineResource("flow", null, List.of(SourceRef.bare("input")),
                direct ? null : List.of(Step.inline("map", FromClause.list(FromRef.literal("input")),
                        new TransformBody.Js("row"), null)), null,
                new ServeBlock.Inline(null, FromClause.list(FromRef.literal(direct ? "input" : "map")),
                        List.of(new SyncElement("s", "target", null, null, null)), null, null), null, null);
    }

    private static ExecutionShape shape(boolean direct, boolean serialMap) {
        String node = direct || serialMap ? "serve.s" : "map";
        var width = new NodeParallelism(node, 2, NodeParallelism.Origin.EXPLICIT,
                NodeParallelism.Scope.NATIVE, 2, 1, 2, List.of());
        if (serialMap) {
            var serial = new NodeParallelism("map", 1, NodeParallelism.Origin.EXPLICIT,
                    NodeParallelism.Scope.TOTAL_ONE, 2, null, 1, List.of());
            return new ExecutionShape(2, Map.of(node, width, "map", serial), Map.of(),
                    Map.of(node, Map.of("orders", new SinkTarget("orders", List.of("id")))));
        }
        return new ExecutionShape(2, Map.of(node, width),
                direct ? Map.of() : Map.of(node, Map.of("orders", List.of("id"))),
                direct ? Map.of(node, Map.of("orders", new SinkTarget("orders", List.of("id")))) : Map.of());
    }

    private static final class ExpansionTargetFactory implements SupplierEx<SinkWriter> {
        @Override public SinkWriter getEx() {
            return new SinkWriter() {
                @Override public java.util.concurrent.CompletionStage<WriteResult> write(List<Envelope> rows) {
                    assertThat(currentInterval.get()).as("expanded writes remain protected").isNotNull();
                    TRANSFORMED_WRITES.addAll(rows);
                    expansionObserved.complete(null);
                    return expansionAcknowledged;
                }
                @Override public void close() {}
            };
        }
    }

    private static final class ObservingTargetFactory implements SupplierEx<SinkWriter> {
        @Override public SinkWriter getEx() {
            return new SinkWriter() {
                @Override public java.util.concurrent.CompletionStage<WriteResult> write(List<Envelope> rows) {
                    var window = currentInterval.get();
                    assertThat(window).as("the original source interval must be armed before any target write").isNotNull();
                    INTERVALS_AT_WRITE.add(window);
                    TRANSFORMED_WRITES.addAll(rows);
                    rows.forEach(row -> transformedWrite.countDown());
                    return CompletableFuture.completedFuture(new WriteResult(rows.size()));
                }
                @Override public void close() {}
            };
        }
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
