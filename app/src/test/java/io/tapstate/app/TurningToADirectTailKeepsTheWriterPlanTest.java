package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.model.ReadMode;
import io.tapstate.runtime.srs.CaptureRun;
import io.tapstate.runtime.srs.CaptureRunSpec;
import io.tapstate.runtime.srs.CaptureRunUnit;
import io.tapstate.runtime.srs.MiningChainId;
import io.tapstate.runtime.srs.SrsCoordinator;
import io.tapstate.runtime.srs.StartFrom;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.capture.CapturePort;
import io.tapstate.spi.capture.Subscription;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * A pipeline writing to two targets that loaded its table through the shared ring, and then turns the buffering
 * off, still prepares its two writers when its next job is assembled.
 *
 * <p>Turning to a direct tail moves where the pipeline's own run begins. Done by rewriting its consumer record
 * whole, the move dropped the plan its writers were recorded under; with two writers, a finished load and no plan
 * the writers cannot be told apart, and every start of the pipeline was refused from then on.
 */
class TurningToADirectTailKeepsTheWriterPlanTest {

    @Test
    void aTwoTargetPipelineTurningToADirectTailStillPreparesItsWriters() {
        InMemorySrsMetaStore store = new InMemorySrsMetaStore();
        CaptureConfig config = new CaptureConfig("mysql", Map.of("host", "h"), List.of("orders"));
        String chain = MiningChainId.resolve(config, "k-two-targets").value();
        store.create(chain, null);
        long epoch = store.openEpoch(chain);
        store.selectConsumerTables(chain, "pipe-1", List.of("orders"), epoch);
        Map<String, List<String>> plan = Map.of("orders", List.of("target-a", "target-b"));
        new StoreBackedSinkAckFactory(Map.of("orders", chain), "pipe-1", store).prepareWriterPlan(plan);
        store.markSinkWriterSnapshotComplete(chain, "pipe-1", "target-a", "orders");
        store.markSinkWriterSnapshotComplete(chain, "pipe-1", "target-b", "orders");
        store.advancePhysicalSourceReadOffset(
                chain, epoch, new ChainPosition(new SourceOrder(epoch, 5), "where-the-chain-stands"), true);

        CapturePort port = mock(CapturePort.class);
        when(port.cdc(any(), any(), any())).thenReturn(mock(Subscription.class));
        CaptureRunUnit unit = new CaptureRunUnit(port, new SrsCoordinator(store), store, mock(HazelcastInstance.class));
        CaptureRun run = unit.start(new CaptureRunSpec(config, ReadMode.CDC_ONLY, "k-two-targets", false, "src-1",
                "pipe-1", StartFrom.latest(), null, 0L), e -> { });
        try {
            assertThat(store.read(chain).orElseThrow().consumerOffset("pipe-1").orElseThrow().cdcStartPosition())
                    .as("where the chain stood for it, now its own start").isEqualTo("where-the-chain-stands");
            assertThatCode(() -> new StoreBackedSinkAckFactory(Map.of("orders", chain), "pipe-1", store)
                    .prepareWriterPlan(plan))
                    .as("the direct run's job prepares the same two writers")
                    .doesNotThrowAnyException();
        } finally {
            run.close();
        }
    }
}
