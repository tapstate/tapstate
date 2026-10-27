package io.tapstate.adapters.mongostore;

import io.tapstate.core.alerting.*;
import io.tapstate.core.alerting.AlertIntent.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
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
import static io.tapstate.adapters.mongostore.AlertDeliveryFixtures.*;
import static org.assertj.core.api.Assertions.*;

class MongoAlertDeliveryCodecTest {
    @Test void originalBusinessEnvelopeMatchesItsAuthoredGoldenAndPreservesBsonTypesAndNanos() throws Exception {
        var original = business(); Document actual = MongoAlertDeliveryCodec.write(original, 1);
        try (var input = getClass().getResourceAsStream("/alert-delivery-v1.golden.json")) {
            assertThat(input).isNotNull();
            assertThat(actual).isEqualTo(Document.parse(new String(input.readAllBytes(), StandardCharsets.UTF_8)));
        }
        assertThat(MongoAlertDeliveryCodec.read(original.frozen().identity(), bson(actual)))
                .isEqualTo(new MongoAlertDeliveryCodec.Stored(1, original));
        assertThat(actual.getLong("recordRevision")).isOne();
        assertThat(actual.get("frozen", Document.class).getString("createdAt")).isEqualTo(AT.toString());
        assertThat(actual.getDate("cleanupAt").toInstant()).isEqualTo(AT.plusSeconds(86400).plusNanos(543211).truncatedTo(java.time.temporal.ChronoUnit.MILLIS));
    }

    @Test void pendingLateAcceptedAndSdkReceiptRoundTripWithoutResettingTheOriginalDeadline() {
        var sdkRetry = new ChannelRetryPolicy(1, 0, List.of(), Duration.ofDays(1), Duration.ofSeconds(30));
        for (var original : List.of(business(), business(true, PAYLOAD, sdkRetry))) {
            var pending = pending(original, "owner-a", "attempt-a");
            var accepted = accepted(pending, AT.plusSeconds(120));
            for (var value : List.of(pending, accepted)) {
                Document row = bson(MongoAlertDeliveryCodec.write(value, 7));
                assertThat(MongoAlertDeliveryCodec.read(value.frozen().identity(), row).value()).isEqualTo(value);
                assertThat(row.getString("deadline")).isEqualTo(original.frozen().deadline().toString());
            }
            assertThat(accepted.state().attempts().getFirst().endedAt()).isEqualTo(pending.state().attempts().getFirst().permit().deadline());
        }
    }

    @Test void commandLookupAfterAdmissionExpiryReturnsTheOriginalCandidateAndTimeWithoutAFabricatedNewAcceptance() {
        var original = test();
        var command = ((AlertIntent.Test) original.frozen().identity()).command();
        Document row = bson(MongoAlertDeliveryCodec.write(original, 1));
        var stored = MongoAlertDeliveryCodec.readTest(command.key(), row).value();
        assertThat(stored).isEqualTo(original);
        var replay = AlertTestCommand.admit(AlertChannelPolicy.Deployment.ON_PREM, original.frozen().channel(), command.key(), command.parameters(),
                ((AlertIntent.Test) stored.frozen().identity()).command(), AT.plus(Duration.ofDays(1)));
        assertThat(replay.replay()).isTrue();
        assertThat(replay.command().deadline()).isEqualTo(AT.plusSeconds(300));
        assertThat(row.getString("quotaMinute")).isEqualTo("2026-10-27T00:00:00Z");
    }

    @Test void unknownOrMalformedFactsCannotBeReinterpretedAsFreshWork() {
        List<Consumer<Document>> changes = List.of(row -> row.put("formatVersion", 2), row -> row.put("formatVersion", 1L),
                row -> row.put("recordRevision", 1), row -> row.put("channelVersion", 9), row -> row.put("deadline", AT.toString()),
                row -> row.put("extra", "unknown"), row -> row.get("_id", Document.class).put("authorityId", "foreign"),
                row -> row.get("frozen", Document.class).get("retry", Document.class).put("maximumAgeNanos", 1),
                row -> row.get("state", Document.class).put("attemptsConsumed", 1), row -> row.get("state", Document.class).put("payload", null),
                row -> row.get("state", Document.class).get("retained", Document.class).put("resultUntil", AT.plus(Duration.ofDays(99)).toString()));
        for (var change : changes) {
            Document row = bson(MongoAlertDeliveryCodec.write(business(), 1)); change.accept(row);
            assertUnreadable(() -> MongoAlertDeliveryCodec.read(business().frozen().identity(), row));
        }
        Document future = MongoAlertDeliveryCodec.write(business(), 1); future.put("formatVersion", 77);
        var header = MongoAlertDeliveryCodec.header(future);
        assertThat(header.recordVersion()).isEqualTo(77);
        assertThat(header.deadline()).isEqualTo(business().frozen().deadline());
        future.remove("deadline"); assertThat(MongoAlertDeliveryCodec.header(future).deadline()).isNull();
    }

    @Test void originalPermitAndReceiptReservationsFitEvenWithMaximumLegalIdentityText() {
        String owner = new String(Character.toChars(0x1f680)).repeat(256);
        var original = pending(business(), owner, "a".repeat(256));
        Document row = MongoAlertDeliveryCodec.write(original, 1);
        assertThat(MongoAlertDeliveryCodec.read(original.frozen().identity(), bson(row)).value()).isEqualTo(original);
        var attempt = row.get("state", Document.class).getList("attempts", Document.class).getFirst();
        assertThat(MongoAlertConfigurationCodec.size(attempt)).isLessThanOrEqualTo(AlertIntent.MAX_ATTEMPT_BYTES);
    }

    @Test void sdkEventEvidenceAndConsumedBudgetSurviveDetailPruningWithoutRestoringPayload() {
        var retry = new ChannelRetryPolicy(1, 0, List.of(), Duration.ofDays(1), Duration.ofSeconds(30));
        var original = accepted(pending(business(true, PAYLOAD, retry), "owner-a", "attempt-a"), AT.plusSeconds(120));
        var s = original.state();
        var pruned = new AlertIntent.Record(original.frozen(), new State(s.owner(), 0, true, s.stopReason(), null, s.attemptsConsumed(), List.of(),
                s.summary(), s.retained(), null, null, s.retained().payloadUntil(), null, s.retained().resultUntil(),
                s.permitsStoppedAt(), s.lastAttemptEnd(), s.cycleStarts(), s.acceptedReceipt()));
        Document row = bson(MongoAlertDeliveryCodec.write(pruned, 9));
        var reopened = MongoAlertDeliveryCodec.read(pruned.frozen().identity(), row).value();
        assertThat(reopened.state().acceptedReceipt().source()).isEqualTo("sdk-event-center");
        assertThat(reopened.state().acceptedReceipt().remoteId()).isEqualTo("event-a");
        assertThat(reopened.state().attemptsConsumed()).isOne();
        assertThat(reopened.state().attempts()).isEmpty();
        assertThat(reopened.state().payload()).isNull();
        row.get("state", Document.class).put("acceptedReceipt", null);
        assertUnreadable(() -> MongoAlertDeliveryCodec.read(pruned.frozen().identity(), row));
    }

    @Test void initialAdmissionReservesAllThirtyResultSlotsWithinTheOriginalIntentByteBound() {
        var retry = new ChannelRetryPolicy(10, 2, java.util.Collections.nCopies(9, Duration.ofSeconds(1)), Duration.ofDays(1), Duration.ofSeconds(60));
        var value = business(false, new Payload(new byte[256 * 1024]), retry);
        Document row = MongoAlertDeliveryCodec.write(value, 1);
        assertThat(MongoAlertConfigurationCodec.size(row)).isLessThanOrEqualTo(AlertIntent.MAX_BYTES);
        assertThat(MongoAlertDeliveryCodec.read(value.frozen().identity(), bson(row)).value()).isEqualTo(value);
        var frozen = value.frozen(); var members = new java.util.ArrayList<Member>();
        String largeText = new String(Character.toChars(0x1f680)).repeat(250);
        for (int index = 0; index < 100; index++) {
            var template = frozen.members().getFirst(); var old = template.evaluation();
            var alarm = new AlarmKey(SCOPE, new AlarmKey.Resource("pipeline", largeText + index, largeText), "pipeline.failed", largeText);
            members.add(new Member(new AlertRule(alarm, 1, 1, largeText, old.triggerFor(), old.resolveFor(), old.maxAge(), old.maxGap()),
                    1, template.ruleIdentity(), 0));
        }
        var large = new Frozen(frozen.identity(), frozen.channel(), frozen.route(), frozen.target(), frozen.content(), members, retry,
                frozen.retention(), frozen.createdAt(), frozen.deadline());
        // Each bounded field is legal, but the immutable aggregate must still leave room for every permitted result.
        var enlarged = initial(large, value.state().payload(), null);
        assertThatThrownBy(() -> MongoAlertDeliveryCodec.write(enlarged, 1)).isInstanceOfSatisfying(TapstateException.class,
                failure -> assertThat(failure.code()).isEqualTo(AlertError.INVALID_CONFIGURATION));
    }

    private static Document bson(Document document) {
        try (BasicOutputBuffer out = new BasicOutputBuffer(); BsonBinaryWriter writer = new BsonBinaryWriter(out)) {
            new DocumentCodec().encode(writer, document, EncoderContext.builder().build());
            try (BsonBinaryReader reader = new BsonBinaryReader(ByteBuffer.wrap(out.toByteArray()))) {
                return new DocumentCodec().decode(reader, DecoderContext.builder().build());
            }
        }
    }
    private static void assertUnreadable(org.assertj.core.api.ThrowableAssert.ThrowingCallable action) {
        assertThatThrownBy(action).isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
    }
}
