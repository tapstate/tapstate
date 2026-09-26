package io.tapstate.core.lifecycle;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PipelineEventTest {

    @Test
    void oneLostIntervalHasOneStableIdentityWithinItsExecution() {
        Instant from = Instant.parse("2026-09-27T10:00:00.123456Z");
        String marker = PipelineEvent.gapId("orders", "inc-a", 41, from);

        assertThat(marker).startsWith("gap-")
                .isEqualTo(PipelineEvent.gapId("orders", "inc-a", 41,
                        Instant.parse("2026-09-27T10:00:00.123Z")));
        assertThat(PipelineEvent.gapId("orders", "inc-a", 42, from)).isNotEqualTo(marker);
        assertThat(PipelineEvent.gapId("orders", "inc-b", 41, from)).isNotEqualTo(marker);
        assertThat(PipelineEvent.gapId("other", "inc-a", 41, from)).isNotEqualTo(marker);
        assertThat(PipelineEvent.gapId("orders", "inc-a", 41, from.plusMillis(1))).isNotEqualTo(marker);
    }

    @Test
    void markerReasonsHaveAStableClosedOrderAndCannotBeAttachedToAnOrdinaryEvent() {
        Instant from = Instant.parse("2026-09-27T10:00:00Z");
        PipelineEvent.Gap gap = new PipelineEvent.Gap(from, from.plusSeconds(2), List.of(
                PipelineEvent.GapReason.SHUTDOWN,
                PipelineEvent.GapReason.QUEUE_FULL,
                PipelineEvent.GapReason.SHUTDOWN));

        assertThat(gap.reasons()).containsExactly(
                PipelineEvent.GapReason.QUEUE_FULL, PipelineEvent.GapReason.SHUTDOWN);
        assertThatThrownBy(() -> new PipelineEvent("ev-1", "orders", "inc-a", 41,
                PipelineEvent.Kind.STATE_CHANGED, from, PipelineState.NEW, PipelineState.RUNNING,
                null, null, gap)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new PipelineEvent("ev-2", "orders", "inc-a", 41,
                PipelineEvent.Kind.TELEMETRY_GAP, from, null, null, null, null, null))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
