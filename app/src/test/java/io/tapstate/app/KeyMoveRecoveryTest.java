package io.tapstate.app;

import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.store.*;
import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class KeyMoveRecoveryTest {
    @Test
    void aRetainedRingSequenceCannotBorrowTheCheckpointsCaptureEpoch() {
        var scope = new PipelineIntentScope("cluster", 1, "incarnation", "flow", 1);
        var claim = new WorkloadClaimFence(new WorkloadClaimKey("cluster", WorkloadClaimType.PIPELINE_ACTUATION, "flow"),
                new WorkloadOwner("node", "boot"), 1, 1, 0);
        var stream = new PipelineKeyMoveWindows.Stream("channel", "input", "orders", null);
        var range = new PipelineKeyMoveWindows.Range(3, 7, 12);
        var observed = new PipelineKeyMoveWindows.Window(scope, 98, new ExecutionFence(scope, claim).operatorExecutionKey(),
                UUID.randomUUID(), Map.of(stream, range));
        var through = new SourceOrder(3, 12);
        var writers = new SrsConsumerWriterRun(1, claim, "run", Map.of("orders", List.of("a")),
                Map.of("orders", Map.of("a", new WriterProgress(through, null))));
        var consumer = mock(SrsConsumerProgress.class);
        var cursor = mock(ConsumerOffset.class);
        when(consumer.cursor()).thenReturn(cursor);
        when(consumer.writerRun()).thenReturn(writers);
        when(consumer.membershipRevision()).thenReturn(1L);
        when(consumer.ringDoneThrough()).thenReturn(Map.of("orders", 100L));
        var views = mock(SrsStoreViews.class);
        var chains = mock(SrsChainStore.class);
        var consumers = mock(SrsConsumerStore.class);
        var chain = mock(SrsChainState.class);
        var table = new SrsTableCheckpoint(new SrsModelKey("channel", "orders", "incarnation", 3, 1), 12);
        when(chain.checkpoint()).thenReturn(new SrsCaptureCheckpoint(new ChainPosition(through, "capture"), Map.of("orders", table)));
        when(chains.load("channel")).thenReturn(java.util.Optional.of(chain));
        when(consumers.progress(any())).thenReturn(java.util.Optional.of(consumer));
        when(views.srs(any(), any())).thenReturn(new SrsStores(chains, mock(SrsModelStore.class), mock(SrsLogStore.class), consumers));
        var windows = mock(PipelineKeyMoveWindows.class);
        when(windows.retire(any(), anyMap(), anyMap())).thenAnswer(call ->
                range.coveredBy(call.<Map<PipelineKeyMoveWindows.Stream, SourceOrder>>getArgument(1).get(stream)) &&
                range.coveredBy(call.<Map<PipelineKeyMoveWindows.Stream, SourceOrder>>getArgument(2).get(stream)));
        when(cursor.sinkAckedByTable()).thenReturn(Map.of("orders", new ChainPosition(new SourceOrder(2, 100), null)));
        assertThat(KeyMoveRecovery.retire(windows, observed, views, () -> {})).isFalse();
        when(consumer.ringDoneThrough()).thenReturn(Map.of("orders", 12L));
        assertThat(KeyMoveRecovery.retire(windows, observed, views, () -> {})).isFalse();
        when(cursor.sinkAckedByTable()).thenReturn(Map.of("orders", new ChainPosition(through, null)));
        assertThat(KeyMoveRecovery.retire(windows, observed, views, () -> {})).isTrue();
    }

    @Test
    void restartCanRetireACoveredPrivateIntervalOnlyWithTheOriginalCompleteRosterAndToken() {
        var scope = new PipelineIntentScope("cluster", 1, "incarnation", "flow", 1);
        var claim = new WorkloadClaimFence(new WorkloadClaimKey("cluster", WorkloadClaimType.PIPELINE_ACTUATION, "flow"),
                new WorkloadOwner("node", "boot"), 1, 1, 0);
        var stream = new PipelineKeyMoveWindows.Stream("channel", "input", "orders", "producer");
        var through = new SourceOrder(2, 12);
        var range = new PipelineKeyMoveWindows.Range(2, 7, 12);
        var observed = new PipelineKeyMoveWindows.Window(scope, 98, new ExecutionFence(scope, claim).operatorExecutionKey(),
                UUID.randomUUID(), Map.of(stream, range));
        var state = mock(DirectChannelProgress.class);
        var views = mock(SrsStoreViews.class);
        var privateChannels = mock(DirectChannelStore.class);
        var cursor = mock(ConsumerOffset.class);
        when(views.directChannels(eq(new CacheBusinessScope("cluster", 1, "incarnation")), any())).thenReturn(privateChannels);
        when(privateChannels.load(new DirectChannelContext(scope, "channel", "input", "producer"))).thenReturn(state);
        when(state.cursor()).thenReturn(cursor);
        var receipt = new WriterProgress(through, null);
        var roster = Map.of("orders", List.of("a", "b"));
        var windows = mock(PipelineKeyMoveWindows.class);
        when(windows.retire(any(), anyMap(), anyMap())).thenAnswer(call -> {
            Map<PipelineKeyMoveWindows.Stream, SourceOrder> resume = call.getArgument(1);
            Map<PipelineKeyMoveWindows.Stream, SourceOrder> settled = call.getArgument(2);
            return range.coveredBy(resume.get(stream)) && range.coveredBy(settled.get(stream));
        });
        when(state.writerRun()).thenReturn(new DirectWriterRun(2, claim, new WriterRun("g1", roster,
                Map.of("orders", Map.of("a", receipt, "b", receipt)), null)));
        when(cursor.sinkAcked()).thenReturn(new ChainPosition(new SourceOrder(2, 6), "batch-start"));
        assertThat(KeyMoveRecovery.retire(windows, observed, views, () -> {})).isFalse();
        when(cursor.sinkAcked()).thenReturn(new ChainPosition(through, "batch-end"));
        when(state.writerRun()).thenReturn(new DirectWriterRun(2, claim, new WriterRun("g1", roster,
                Map.of("orders", Map.of("a", receipt)), null)));
        assertThat(KeyMoveRecovery.retire(windows, observed, views, () -> {})).isFalse();
        when(state.writerRun()).thenReturn(new DirectWriterRun(2, claim, new WriterRun("g1", roster,
                Map.of("orders", Map.of("a", receipt, "b", receipt)), null)));
        assertThat(KeyMoveRecovery.retire(windows, observed, views, () -> {})).isTrue();
    }
}
