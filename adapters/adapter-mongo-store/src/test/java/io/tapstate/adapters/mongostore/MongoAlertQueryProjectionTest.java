package io.tapstate.adapters.mongostore;

import io.tapstate.core.alerting.*;
import io.tapstate.core.alerting.AlertIntent.*;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import static io.tapstate.adapters.mongostore.AlertDeliveryFixtures.*;
import static org.assertj.core.api.Assertions.*;

class MongoAlertQueryProjectionTest {
    @Test void severityAndNegativeTimeTuplePreservePriorityAndAdjacentNanosecondsWithinTheDeclaredBudget() {
        Document first = MongoAlertQueryProjection.recent(AlarmSeverity.CRITICAL, AT);
        Document next = MongoAlertQueryProjection.recent(AlarmSeverity.CRITICAL, AT.plusNanos(1));
        assertThat(first.getInteger("severityRank")).isZero();
        assertThat(first.get("recentSecondOrder")).isInstanceOf(Long.class);
        assertThat(first.get("recentNanoOrder")).isInstanceOf(Integer.class);
        assertThat(next.getInteger("recentNanoOrder")).isLessThan(first.getInteger("recentNanoOrder"));
        assertThat(MongoAlertConfigurationCodec.size(first)).isLessThanOrEqualTo(192);
        assertThat(MongoAlertConfigurationCodec.size(MongoAlertQueryProjection.ended(AT))).isLessThanOrEqualTo(64);
        assertThat(MongoAlertQueryProjection.ended(null)).isEqualTo(new Document("endedSecondOrder", null).append("endedNanoOrder", null));
        assertThat(MongoAlertQueryProjection.recent(AlarmSeverity.WARNING, Instant.EPOCH.minusNanos(1)).getLong("recentSecondOrder")).isEqualTo(1L);
    }

    @Test void originalPendingDeadlineAndOwnerEligibilityAreCandidatesWithoutExtendingTheOriginalAge() {
        var original = business();
        assertThat(MongoAlertQueryProjection.intentCheck(original).toInstant()).isAfterOrEqualTo(AT);
        var permitted = pending(original, "owner-a", "attempt-a");
        assertThat(MongoAlertQueryProjection.intentCheck(permitted).toInstant()).isAfterOrEqualTo(AT.plusSeconds(60))
                .isBefore(AT.plusSeconds(60).plusMillis(1));
        var s = original.state();
        var leased = new AlertIntent.Record(original.frozen(), new State(new AlertOwnership(1, "owner-a", AT.plusSeconds(120)),
                0, false, null, AT, 0, List.of(), s.summary(), s.retained(), s.payload(), null, null, null, null));
        assertThat(MongoAlertQueryProjection.intentCheck(leased).toInstant()).isAfterOrEqualTo(AT.plusSeconds(120));
        assertThat(MongoAlertConfigurationCodec.size(new Document("nextCheckAt", MongoAlertQueryProjection.intentCheck(leased)))).isLessThanOrEqualTo(32);
    }

    @Test void cleanupTracksOriginalControlThenPayloadThenResultRatherThanWaitingForOnlyFinalRowExpiry() {
        var original = business();
        assertThat(MongoAlertQueryProjection.intentCleanup(original).toInstant()).isAfterOrEqualTo(original.frozen().deadline());
        var received = accepted(pending(original, "owner-a", "attempt-a"), AT.plusSeconds(1));
        assertThat(MongoAlertQueryProjection.intentCheck(received)).isNull();
        assertThat(MongoAlertQueryProjection.intentCleanup(received).toInstant()).isAfterOrEqualTo(received.state().retained().payloadUntil())
                .isBefore(received.state().retained().payloadUntil().plusMillis(1));
        var s = received.state();
        var pruned = new AlertIntent.Record(received.frozen(), new State(s.owner(), s.manualCycles(), true, s.stopReason(), null,
                s.attemptsConsumed(), s.attempts(), s.summary(), s.retained(), null, null, s.retained().payloadUntil(), null, null,
                s.permitsStoppedAt(), s.lastAttemptEnd(), s.cycleStarts(), s.acceptedReceipt()));
        assertThat(MongoAlertQueryProjection.intentCleanup(pruned).toInstant()).isAfterOrEqualTo(s.retained().resultUntil());
    }

    @Test void uniqueSdkPermitSchedulesOriginalDeadlineSettlementEvenWhenItsBudgetAlreadyStopped() {
        var retry = new ChannelRetryPolicy(1, 0, List.of(), Duration.ofDays(1), Duration.ofSeconds(30));
        var permitted = pending(business(true, PAYLOAD, retry), "owner-a", "attempt-a");
        assertThat(permitted.state().permitsStopped()).isTrue();
        assertThat(MongoAlertQueryProjection.intentCleanup(permitted).toInstant()).isAfterOrEqualTo(AT.plusSeconds(30))
                .isBefore(AT.plusSeconds(30).plusMillis(1));
        assertThat(MongoAlertQueryProjection.intentCheck(permitted).toInstant()).isBefore(permitted.frozen().deadline());
    }
}
