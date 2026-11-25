package io.tapstate.spi.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class SrsFreshFullReceiptTest {
    private static final PipelineIntentScope SCOPE = new PipelineIntentScope("cluster", 1, "cache", "pipeline", 2);
    private static final SrsFreshFull FULL = new SrsFreshFull(new SrsFreshFullOrigin("operation", SCOPE,
            "preparation", "a".repeat(64), "source"), 2,
            Map.of("orders", new SrsModelKey("chain", "orders", "cache", 2, 0)));

    @Test void theExactOriginalConsumerAndCompleteInitialFloorFormAReceipt() {
        var progress = progress("pipeline", "source", 2, Map.of("orders", 4L), Map.of("orders", 4L));
        assertThat(new SrsFreshFullReceipt(chain(), progress).progress()).isEqualTo(progress);
    }

    @Test void aPendingRequestCannotInterpretALostRealCheckpointAsUnusedHistory() {
        assertThatThrownBy(() -> new SrsChainState("chain", "capture", null, 2, Set.of("orders"), Set.of("orders"),
                0, null, null, 1, 1, new SourceResumeOverride("fixed-cut", 1, Instant.EPOCH), FULL))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"pipeline", "source", "generation", "missing-floor", "advanced-read"})
    void aDifferentConsumerOrIncompleteInitialFloorCannotSubstituteForTheOriginalPublication(String damage) {
        var progress = progress(damage.equals("pipeline") ? "different" : "pipeline",
                damage.equals("source") ? "different" : "source", damage.equals("generation") ? 3 : 2,
                Map.of("orders", damage.equals("advanced-read") ? 5L : 4L),
                damage.equals("missing-floor") ? Map.of() : Map.of("orders", 4L));
        assertThatThrownBy(() -> new SrsFreshFullReceipt(chain(), progress)).isInstanceOf(IllegalArgumentException.class);
    }

    private static SrsChainState chain() {
        var checkpoint = new SrsCaptureCheckpoint(new ChainPosition(new SourceOrder(1, 1), "old-cut"),
                Map.of("orders", new SrsTableCheckpoint(new SrsModelKey("chain", "orders", "cache", 1, 0), -1)));
        return new SrsChainState("chain", "capture", null, 2, Set.of("orders"), Set.of("orders"), 1,
                checkpoint, Instant.EPOCH, 1, 1, new SourceResumeOverride("fixed-cut", 1, Instant.EPOCH), FULL);
    }

    private static SrsConsumerProgress progress(String pipeline, String source, long generation,
                                               Map<String, Long> read, Map<String, Long> floor) {
        var context = new SrsConsumerContext(new SrsConsumerKey("chain", pipeline, source), generation);
        var snapshot = new SrsSnapshotPin(2, "fixed-cut", FULL.models());
        var cursor = new ConsumerOffset(context.key().consumerId(), read, null, List.of(), "fixed-cut", 2,
                Map.of(), ConsumerProgressKind.SRS);
        return new SrsConsumerProgress(context, 1, 1, cursor, snapshot, null, floor);
    }
}
