package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.ObservationFailure;
import io.tapstate.core.lifecycle.PipelineEvent;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.IoError;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MongoPipelineEventStoreTest {

    private static final Instant AT = Instant.parse("2026-09-27T10:00:00Z");

    @Test
    void eventEnvelopeAndCodedFailureRoundTripWithoutPublicMessage() {
        PipelineEvent event = new PipelineEvent("ev-failure", "orders", "inc-a", 41,
                PipelineEvent.Kind.FAILURE, AT, PipelineState.RUNNING, PipelineState.FAILED,
                new ObservationFailure("engine.job-failed", Map.of("pipeline", "orders")),
                "sink refused the batch", null);

        Document document = MongoPipelineEventStore.toDocument(event);

        assertThat(document.get(MongoPipelineEventStore.OCCURRED_AT)).isEqualTo(Date.from(AT));
        assertThat(document.getString(MongoPipelineEventStore.INCARNATION_ID)).isEqualTo("inc-a");
        assertThat(document.getLong(MongoPipelineEventStore.GENERATION)).isEqualTo(41L);
        assertThat(document).doesNotContainKey("message");
        assertThat(MongoPipelineEventStore.toEvent(document)).isEqualTo(event);
    }

    @Test
    void gapBoundsAndClosedReasonsSurviveTheSameCollectionCodec() {
        PipelineEvent.Gap gap = new PipelineEvent.Gap(AT.minusSeconds(20), AT.minusSeconds(1),
                List.of(PipelineEvent.GapReason.WRITE_FAILURE, PipelineEvent.GapReason.QUEUE_FULL));
        PipelineEvent event = new PipelineEvent(PipelineEvent.gapId("orders", "inc-a", 41, gap.from()),
                "orders", "inc-a", 41, PipelineEvent.Kind.TELEMETRY_GAP,
                AT, null, null, null, null, gap);

        Document document = MongoPipelineEventStore.toDocument(event);

        assertThat(document.get(MongoPipelineEventStore.GAP_FROM)).isEqualTo(Date.from(gap.from()));
        assertThat(document.get(MongoPipelineEventStore.GAP_TO)).isEqualTo(Date.from(gap.to()));
        assertThat(document.getList(MongoPipelineEventStore.GAP_REASONS, String.class))
                .containsExactly("QUEUE_FULL", "WRITE_FAILURE");
        assertThat(MongoPipelineEventStore.toEvent(document)).isEqualTo(event);
    }

    @Test
    void aStringTimestampCannotMasqueradeAsAnExpiringDate() {
        Document document = MongoPipelineEventStore.toDocument(new PipelineEvent("ev-a", "orders", "inc-a", 41,
                PipelineEvent.Kind.STATE_CHANGED, AT, PipelineState.NEW, PipelineState.RUNNING,
                null, null, null));
        document.put(MongoPipelineEventStore.OCCURRED_AT, AT.toString());

        assertThatThrownBy(() -> MongoPipelineEventStore.toEvent(document))
                .isInstanceOfSatisfying(TapstateException.class, failure -> {
                    assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);
                    assertThat(failure.args()).containsEntry("field", MongoPipelineEventStore.OCCURRED_AT);
                });
    }
}
