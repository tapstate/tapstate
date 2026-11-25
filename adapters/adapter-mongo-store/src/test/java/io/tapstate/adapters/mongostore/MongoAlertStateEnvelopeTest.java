package io.tapstate.adapters.mongostore;

import io.tapstate.core.alerting.*;
import io.tapstate.core.alerting.AlertStateRecord.*;
import io.tapstate.core.common.TapstateException;
import java.time.*;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import static io.tapstate.adapters.mongostore.AlertStateEnvelopeFixtures.*;
import static org.assertj.core.api.Assertions.*;

class MongoAlertStateEnvelopeTest {
    @Test void operationFormatRetainsFailedCapabilitiesWithinTheOriginalRowBudgetAndKeepsV2Readable(){
        var old=fired();var value=new Head(old.snapshot(),old.deployment(),old.owner(),old.retiredAt(),old.summariesHandedOff(),old.episode(),old.binding(),new OperationalEvidence("a".repeat(64),255));
        var row=MongoAlertStateEnvelope.head(value,4);var legacy=MongoAlertStateEnvelope.head(old,4);
        assertThat(row.getInteger("formatVersion")).isEqualTo(3);
        assertThat(MongoAlertConfigurationCodec.size(row)-MongoAlertConfigurationCodec.size(legacy)).isLessThanOrEqualTo(192);
        assertThat(MongoAlertStateEnvelope.read(KEY,row,MongoAlertStateEnvelope.episode(value.episode())).value()).isEqualTo(value);
        assertThat(MongoAlertStateEnvelope.read(KEY,legacy,MongoAlertStateEnvelope.episode(old.episode())).value().operationalEvidence()).isNull();
        row.get("operationalEvidence",Document.class).put("failedCapabilities",256);
        assertThatThrownBy(()->MongoAlertStateEnvelope.read(KEY,row,MongoAlertStateEnvelope.episode(value.episode()))).isInstanceOf(TapstateException.class);
    }

    @Test void fullMetadataGoldenRetainsOriginalBindingAndOnlyOneCopyForTheSameCapture() throws Exception {
        var value = fired(); var row = MongoAlertStateEnvelope.head(value, 3); var ep = MongoAlertStateEnvelope.episode(value.episode());
        assertThat(MongoAlertStateEnvelope.read(KEY, row, ep)).isEqualTo(new MongoAlertStateEnvelope.Stored(3, value));
        assertThat(ep.get("diagnostics", Document.class).get("latestTrace")).isNull();
        var actual = new Document("head", row).append("episode", ep);
        try (var input = getClass().getResourceAsStream("/alert-state-v2.golden.json")) {
            assertThat(input).isNotNull(); assertThat(actual).isEqualTo(Document.parse(new String(input.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8)));
        }
    }
    @Test void replacementKeepsOriginalEpisodeBindingAndAdministrativeReasonWithoutInventingRecovery() {
        var old = fired(); var replacement = new AlertRule(KEY, 2, 3, "producer-2", Duration.ZERO, Duration.ZERO, Duration.ofSeconds(15), Duration.ofSeconds(10));
        var u = AlertLifecycle.replace(old.snapshot(), replacement, T.plusSeconds(1));
        var e = new Episode(RULE, u.snapshot().episode(), POLICY, old.episode().diagnostics(), false);
        var h = head(u.snapshot(), e, old.owner());
        assertThat(MongoAlertStateEnvelope.read(KEY, MongoAlertStateEnvelope.head(h, 4), MongoAlertStateEnvelope.episode(e)).value()).isEqualTo(h);
        var change = new Transition(u.transitions().getFirst(), AlertSnapshot.EndReason.SUPERSEDED, DiagnosticStatus.CAPTURED, e.diagnostics().latest(), POLICY);
        assertThat(MongoAlertStateEnvelope.readTransition(KEY, 4, 0, MongoAlertStateEnvelope.transition(KEY, 4, 0, change))).isEqualTo(change);
        assertThat(e.value().recoveredAt()).isNull(); assertThat(e.historyUntil()).isEqualTo(T.plusSeconds(1).plus(Duration.ofDays(30)));
    }
    @Test void expiredTraceKeepsOccurrenceReferencesAndCannotBeRestoredAsCapturedText() {
        var old = fired(); var u = AlertLifecycle.terminate(old.snapshot(), AlertSnapshot.EndReason.DISABLED, T.plusSeconds(1));
        var e = new Episode(RULE, u.snapshot().episode(), POLICY, old.episode().diagnostics(), false);
        assertThatThrownBy(() -> new Episode(RULE, e.value(), POLICY, e.diagnostics().prune(e.diagnosticUntil().minusNanos(1)), false)).isInstanceOf(TapstateException.class);
        var pruned = new Episode(RULE, e.value(), POLICY, e.diagnostics().prune(e.diagnosticUntil()), false);
        var row = MongoAlertStateEnvelope.episode(pruned);
        assertThat(MongoAlertStateEnvelope.readEpisode(KEY, 1, row)).isEqualTo(pruned);
        assertThat(row.get("diagnostics", Document.class)).containsEntry("firstTrace", null).containsEntry("status", "PRUNED");
    }
    @Test void unknownFieldsTypesVersionsProjectionTamperingAndForeignOccurrenceFailClosed() {
        var value = fired(); var ep = MongoAlertStateEnvelope.episode(value.episode());
        for (var mutation : java.util.List.<java.util.function.Consumer<Document>>of(d -> d.put("formatVersion", 1), d -> d.remove("deployment"),
                d -> d.put("recentNanoOrder", 0), d -> d.put("severityRank", 1L), d -> d.put("extra", true))) {
            var row = MongoAlertStateEnvelope.head(value, 3); mutation.accept(row);
            assertThatThrownBy(() -> MongoAlertStateEnvelope.read(KEY, row, ep)).isInstanceOf(TapstateException.class);
        }
        ep.get("diagnostics", Document.class).get("first", Document.class).put("sequence", 1);
        assertThatThrownBy(() -> MongoAlertStateEnvelope.readEpisode(KEY, 1, ep)).isInstanceOf(TapstateException.class);
    }
    @Test void legacyFactsRemainReadableOnlyThroughTheirOriginalCodec() {
        var s = fired().snapshot(); var h = MongoAlertStateCodec.head(s, 3); var e = MongoAlertStateCodec.episode(KEY, s.episode());
        assertThat(MongoAlertStateCodec.read(KEY, h, e).snapshot()).isEqualTo(s);
        assertThatThrownBy(() -> MongoAlertStateEnvelope.read(KEY, h, e)).isInstanceOf(TapstateException.class);
    }
    @Test void cloudCutoffsAndPairBudgetUseTheFrozenDeployment() {
        var old = fired(); var ended = AlertLifecycle.terminate(old.snapshot(), AlertSnapshot.EndReason.RESOURCE_REMOVED, T.plusSeconds(1)).snapshot().episode();
        var cloud = AlertIntent.Policy.capture(AlertRetentionPolicy.defaults(AlertChannelPolicy.Deployment.CLOUD_MANAGED));
        var e = new Episode(RULE, ended, cloud, old.episode().diagnostics(), false);
        assertThat(e.diagnosticUntil()).isEqualTo(ended.endedAt().plus(Duration.ofDays(1)));
        assertThat(e.historyUntil()).isEqualTo(ended.endedAt().plus(Duration.ofDays(7)));
        var a = old.episode().diagnostics().first(); var b = new AlarmDiagnostics.Occurrence(a.bindingDigest(), 1, 2, T.plusNanos(1));
        var text = new AlarmDiagnostics.SafeTrace("x".repeat(16384), 8, true, false);
        var max = new Episode(RULE, ended, POLICY, Diagnostics.captured(new AlarmDiagnostics.History(new AlarmDiagnostics.Capture(a, text), new AlarmDiagnostics.Capture(b, text))), false);
        assertThat(MongoAlertConfigurationCodec.size(MongoAlertStateEnvelope.episode(max).get("diagnostics", Document.class))).isLessThanOrEqualTo(36 * 1024);
    }
}
