package io.tapstate.adapters.mongostore;

import io.tapstate.core.alerting.AlarmKey;
import io.tapstate.core.alerting.AlarmSeverity;
import io.tapstate.core.alerting.AlertLifecycle;
import io.tapstate.core.alerting.AlertRule;
import io.tapstate.core.alerting.AlertSignal;
import io.tapstate.core.alerting.AlertSnapshot;
import io.tapstate.core.alerting.AlertUpdate;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.function.Consumer;
import org.bson.BsonBinaryReader;
import org.bson.BsonBinaryWriter;
import org.bson.Document;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
import org.junit.jupiter.api.Test;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import static org.assertj.core.api.Assertions.*;

class MongoAlertStateCodecTest {
    private static final Instant T = Instant.parse("2026-10-27T00:00:00.123456789Z");
    private static final AlarmKey KEY = new AlarmKey(new AlarmKey.Scope("org-a", "cluster-a"),
            new AlarmKey.Resource("pipeline", "orders", "resource-1"), "pipeline.failed", "rule-a");
    private static final AlertRule RULE = new AlertRule(KEY, 1, 2, "producer-1", Duration.ZERO,
            Duration.ZERO, Duration.ofSeconds(15), Duration.ofSeconds(10));

    @Test void exactSeparatedShapeMatchesTheAuthoredGolden() throws Exception {
        AlertUpdate fired = signal(AlertSnapshot.initial(RULE), 1, T, AlertSignal.Condition.BAD);
        Document actual = new Document("head", MongoAlertStateCodec.head(fired.snapshot(), 3))
                .append("episode", MongoAlertStateCodec.episode(KEY, fired.snapshot().episode()))
                .append("transition", MongoAlertStateCodec.transition(KEY, 3, 0, fired.transitions().getLast()));
        try (var input = getClass().getResourceAsStream("/alert-state-v1.golden.json")) {
            assertThat(input).isNotNull();
            assertThat(actual).isEqualTo(Document.parse(new String(input.readAllBytes(), StandardCharsets.UTF_8)));
        }
        assertThat(actual.get("head", Document.class)).doesNotContainKeys("rule", "episode", "firedAt", "severity");
    }

    @Test void realBsonRoundTripPreservesNanosecondsFrontiersAndAdministrativeEnds() {
        AlertSnapshot initial = AlertSnapshot.initial(RULE);
        assertRoundTrip(initial, 1);
        AlertSnapshot fired = signal(initial, 1, T, AlertSignal.Condition.BAD).snapshot();
        assertRoundTrip(fired, 2);
        AlertSnapshot stale = AlertLifecycle.tick(fired, T.plusSeconds(16)).snapshot();
        assertRoundTrip(stale, 3);
        assertThat(stale.status()).isEqualTo(AlertSnapshot.Status.FIRING);
        AlertSnapshot recovered = signal(fired, 2, T.plusNanos(1), AlertSignal.Condition.GOOD).snapshot();
        assertRoundTrip(recovered, 4);
        AlertSnapshot next = signal(recovered, 3, T.plusNanos(2), AlertSignal.Condition.BAD).snapshot();
        assertRoundTrip(next, 5);
        assertThat(next.episode().number()).isEqualTo(2);
        AlertSnapshot replaced = AlertLifecycle.replace(next, new AlertRule(KEY, 2, 3, "producer-2",
                Duration.ZERO, Duration.ZERO, Duration.ofSeconds(15), Duration.ofSeconds(10)), T.plusNanos(3)).snapshot();
        assertRoundTrip(replaced, 6);
        assertThat(replaced.episode().endReason()).isEqualTo(AlertSnapshot.EndReason.SUPERSEDED);
        assertThat(replaced.episode().recoveredAt()).isNull();
    }

    @Test void unknownVersionsFieldsEnumsAndWrongBsonNumericTypesFailClosed() {
        List<Consumer<Document>> corruptions = List.of(
                d -> d.put("formatVersion", 2), d -> d.remove("formatVersion"),
                d -> d.put("formatVersion", 1L), d -> d.put("recordRevision", 3),
                d -> d.put("recordRevision", Long.MAX_VALUE), d -> d.put("lastSequence", 1.0),
                d -> d.put("status", "HEALTHY"), d -> d.put("quality", "NORMAL"),
                d -> d.put("enabled", "true"), d -> d.put("unknown", "never-drop"),
                d -> d.put("lastObservedAt", "invalid"),
                d -> d.get("evaluationBinding", Document.class).put("ruleRevision", 0L),
                d -> d.get("evaluationBinding", Document.class).put("generation", 0L),
                d -> d.put("episodeSequence", 2L));
        AlertSnapshot fired = signal(AlertSnapshot.initial(RULE), 1, T, AlertSignal.Condition.BAD).snapshot();
        for (Consumer<Document> corrupt : corruptions) {
            Document head = bson(MongoAlertStateCodec.head(fired, 3)); corrupt.accept(head);
            assertUnreadable(() -> MongoAlertStateCodec.read(KEY, head,
                    MongoAlertStateCodec.episode(KEY, fired.episode())));
        }
    }

    @Test void missingOrMismatchedOriginalEpisodeIsNeverRecreatedFromTheCurrentState() {
        AlertSnapshot fired = signal(AlertSnapshot.initial(RULE), 1, T, AlertSignal.Condition.BAD).snapshot();
        Document head = MongoAlertStateCodec.head(fired, 3);
        assertUnreadable(() -> MongoAlertStateCodec.read(KEY, head, null));
        Document episode = MongoAlertStateCodec.episode(KEY, fired.episode());
        episode.get("_id", Document.class).put("number", 2L);
        assertUnreadable(() -> MongoAlertStateCodec.read(KEY, head, episode));
        Document foreign = MongoAlertStateCodec.episode(new AlarmKey(new AlarmKey.Scope("org-b", "cluster-a"),
                KEY.resource(), KEY.typeId(), KEY.qualifier()), fired.episode());
        assertUnreadable(() -> MongoAlertStateCodec.read(KEY, head, foreign));
        assertUnreadable(() -> MongoAlertStateCodec.read(KEY, MongoAlertStateCodec.head(AlertSnapshot.initial(RULE), 1),
                MongoAlertStateCodec.episode(KEY, fired.episode())));
    }

    @Test void enumAndOccurrenceIdentityInTransitionsRemainExplicit() {
        for (AlertUpdate.Kind kind : AlertUpdate.Kind.values()) {
            for (AlarmSeverity severity : AlarmSeverity.values()) {
                var transition = new AlertUpdate.Transition(kind, T, 4, severity);
                Document encoded = bson(MongoAlertStateCodec.transition(KEY, 7, 2, transition));
                assertThat(MongoAlertStateCodec.readTransition(KEY, 7, 2, encoded)).isEqualTo(transition);
                assertUnreadable(() -> MongoAlertStateCodec.readTransition(KEY, 8, 2, encoded));
                assertUnreadable(() -> MongoAlertStateCodec.readTransition(KEY, 7, 3, encoded));
            }
        }
    }

    @Test void inconsistentLifecycleAndRecoveryTimesAreNotTrustedAsStoredFacts() {
        AlertSnapshot fired = signal(AlertSnapshot.initial(RULE), 1, T, AlertSignal.Condition.BAD).snapshot();
        AlertSnapshot recovered = signal(fired, 2, T.plusSeconds(1), AlertSignal.Condition.GOOD).snapshot();
        List<Consumer<Document>> corruptions = List.of(
                d -> d.put("firedAt", T.plusSeconds(2).toString()),
                d -> d.put("recoveredAt", T.toString()), d -> d.put("endReason", "DISABLED"),
                d -> d.put("severity", "NORMAL"), d -> d.put("formatVersion", 2));
        for (Consumer<Document> corrupt : corruptions) {
            Document episode = bson(MongoAlertStateCodec.episode(KEY, recovered.episode())); corrupt.accept(episode);
            assertUnreadable(() -> MongoAlertStateCodec.read(KEY, MongoAlertStateCodec.head(recovered, 3), episode));
        }
        Document impossible = MongoAlertStateCodec.head(fired, 2);
        impossible.put("status", "CLEAR");
        assertUnreadable(() -> MongoAlertStateCodec.read(KEY, impossible, MongoAlertStateCodec.episode(KEY, fired.episode())));
    }

    @Test void activeEpisodeRequiresAnObservedFrontierAndRecoveryWindowAtOrAfterItsOccurrence() {
        AlertSnapshot fired = signal(AlertSnapshot.initial(RULE), 1, T, AlertSignal.Condition.BAD).snapshot();
        List<Consumer<Document>> corruptions = List.of(
                d -> { d.put("lastSequence", -1L); d.put("lastObservedAt", null); },
                d -> d.put("lastObservedAt", T.minusNanos(1).toString()),
                d -> d.put("recoverySince", T.minusNanos(1).toString()));
        for (Consumer<Document> corrupt : corruptions) {
            Document head = MongoAlertStateCodec.head(fired, 2); corrupt.accept(head);
            assertUnreadable(() -> MongoAlertStateCodec.read(KEY, head, MongoAlertStateCodec.episode(KEY, fired.episode())));
        }
    }

    @Test void resolvedEpisodeCannotHaveAFrontierBeforeItsConfirmedRecovery() {
        AlertSnapshot fired = signal(AlertSnapshot.initial(RULE), 1, T, AlertSignal.Condition.BAD).snapshot();
        AlertSnapshot recovered = signal(fired, 2, T.plusSeconds(1), AlertSignal.Condition.GOOD).snapshot();
        Document head = MongoAlertStateCodec.head(recovered, 3);
        head.put("lastObservedAt", T.plusMillis(500).toString());
        assertUnreadable(() -> MongoAlertStateCodec.read(KEY, head, MongoAlertStateCodec.episode(KEY, recovered.episode())));
    }

    @Test void releasedExpiredHistoryKeepsTheEpisodeSequenceWithoutRecreatingTheOldEpisode() {
        AlertSnapshot fired = signal(AlertSnapshot.initial(RULE), 1, T, AlertSignal.Condition.BAD).snapshot();
        AlertSnapshot recovered = signal(fired, 2, T.plusSeconds(1), AlertSignal.Condition.GOOD).snapshot();
        AlertSnapshot released = new AlertSnapshot(recovered.rule(), recovered.enabled(), recovered.status(),
                recovered.quality(), recovered.lastSequence(), recovered.lastObservedAt(), recovered.pendingSince(),
                recovered.recoverySince(), recovered.episodeSequence(), null);
        AlertSnapshot reopened = MongoAlertStateCodec.read(KEY, bson(MongoAlertStateCodec.head(released, 4)), null).snapshot();
        assertThat(reopened).isEqualTo(released);
        assertThat(signal(reopened, 3, T.plusSeconds(2), AlertSignal.Condition.BAD).snapshot().episode().number()).isEqualTo(2);
        assertUnreadable(() -> MongoAlertStateCodec.read(KEY, MongoAlertStateCodec.head(fired, 2), null));
    }

    @Test void fullUnicodeBindingsRoundTripAndNoncanonicalEmbeddedIdsAreRefused() {
        String producer = new String(Character.toChars(0x1f680)).repeat(256);
        AlertRule rule = new AlertRule(KEY, 1, 2, producer, Duration.ZERO, Duration.ZERO,
                Duration.ofSeconds(15), Duration.ofSeconds(10));
        assertRoundTrip(AlertSnapshot.initial(rule), 1);
        Document row = MongoAlertStateCodec.head(AlertSnapshot.initial(RULE), 1);
        Document reversed = new Document();
        List<String> fields = row.get("_id", Document.class).keySet().stream().toList();
        for (String field : fields.reversed()) { reversed.put(field, row.get("_id", Document.class).get(field)); }
        row.put("_id", reversed);
        assertUnreadable(() -> MongoAlertStateCodec.read(KEY, row, null));
    }

    private static AlertUpdate signal(AlertSnapshot old, long sequence, Instant at, AlertSignal.Condition condition) {
        AlertRule rule = old.rule();
        return AlertLifecycle.evaluate(old, new AlertSignal(rule.key(), rule.revision(), rule.generation(),
                rule.producerIncarnation(), sequence, at, condition, AlarmSeverity.MAJOR), at);
    }
    private static void assertRoundTrip(AlertSnapshot snapshot, long revision) {
        Document head = bson(MongoAlertStateCodec.head(snapshot, revision));
        Document episode = snapshot.episode() == null ? null : bson(MongoAlertStateCodec.episode(KEY, snapshot.episode()));
        assertThat(MongoAlertStateCodec.read(KEY, head, episode))
                .isEqualTo(new MongoAlertStateCodec.State(revision, snapshot));
    }
    private static Document bson(Document value) {
        try (var output = new BasicOutputBuffer(); var writer = new BsonBinaryWriter(output)) {
            new DocumentCodec().encode(writer, value, EncoderContext.builder().build());
            try (var reader = new BsonBinaryReader(ByteBuffer.wrap(output.toByteArray()))) {
                return new DocumentCodec().decode(reader, DecoderContext.builder().build());
            }
        }
    }
    private static void assertUnreadable(ThrowingCallable action) {
        assertThatThrownBy(action).isInstanceOfSatisfying(TapstateException.class,
                failure -> assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
    }
}
