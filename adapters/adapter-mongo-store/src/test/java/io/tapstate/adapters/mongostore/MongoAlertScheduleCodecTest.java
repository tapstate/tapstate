package io.tapstate.adapters.mongostore;

import io.tapstate.core.alerting.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.function.Consumer;
import org.bson.*;
import org.bson.codecs.*;
import org.bson.io.BasicOutputBuffer;
import org.junit.jupiter.api.Test;
import static io.tapstate.adapters.mongostore.AlertDeliveryFixtures.*;
import static io.tapstate.adapters.mongostore.AlertScheduleFixtures.*;
import static org.assertj.core.api.Assertions.*;

class MongoAlertScheduleCodecTest {
    @Test void initialScopedScheduleMatchesTheAuthoredGolden() throws Exception {
        try (var input = getClass().getResourceAsStream("/alert-schedule-v1.golden.json")) {
            assertThat(input).isNotNull();
            assertThat(MongoAlertScheduleCodec.write(root(), 1)).isEqualTo(Document.parse(new String(input.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8)));
        }
    }
    @Test void currentMembersClocksAndOriginalBatchRoundTripThroughRealBsonWithoutReset() {
        var intent = groupedIntent();
        for (var value : List.of(root(), planned(root(), ((AlertIntent.Business) business().frozen().identity()).key()), groupBatch(intent))) {
            Document row = bson(MongoAlertScheduleCodec.write(value, 7));
            assertThat(MongoAlertScheduleCodec.read(value.key(), row)).isEqualTo(new MongoAlertScheduleCodec.Stored(7, value));
            assertThat(row.get("recordRevision")).isInstanceOf(Long.class);
            assertThat(row.get("clock", Document.class).get("round")).isInstanceOf(Long.class);
            assertThat(row.getInteger("formatVersion")).isEqualTo(1);
        }
        assertThat(MongoAlertQueryProjection.scheduleCheck(planned(root(), ((AlertIntent.Business) business().frozen().identity()).key()))).isNotNull();
    }

    @Test void partialDispositionKeepsOriginalFrozenMembersAndOneScopedSuccessor() {
        var intent = groupedIntent(); var batch = groupBatch(intent); var key = ((AlertIntent.Business) intent.frozen().identity()).key();
        var n = key.notification();
        var successor = new DeliveryKey(new DeliveryKey.NotificationRound(n.scope(), n.subjectId(), "2", n.routeId(), n.routeIncarnation(), n.kind(), 2),
                key.channelType(), key.receiverIncarnation(), key.targetId());
        var survivors = List.of(batch.members().getLast());
        var disposition = new AlertSchedule.Disposition(key, 1, FrozenGroupPolicy.Action.STOP_PENDING, survivors, successor, null, DeliveryEvidence.Outcome.UNKNOWN);
        var value = new AlertSchedule.Record(batch.key(), batch.route(), batch.retention(), AT, batch.owner(), batch.grouping(), batch.clock(), 1,
                List.of(), batch.batch(), disposition, List.of(), List.of(key, successor), null, false, survivors);
        assertThat(MongoAlertScheduleCodec.read(value.key(), bson(MongoAlertScheduleCodec.write(value, 2))).value()).isEqualTo(value);
        assertThat(value.batch().members()).isEqualTo(intent.frozen().members());
        assertThat(value.disposition().successor()).isEqualTo(successor);
        assertThatThrownBy(() -> new AlertSchedule.Disposition(key, 1, FrozenGroupPolicy.Action.STOP_PENDING, batch.members(), successor, null,
                DeliveryEvidence.Outcome.ACCEPTED)).isInstanceOf(TapstateException.class);
    }

    @Test void unknownMalformedAndCrossScopeRecordsNeverBecomeAnEmptySchedule() {
        List<Consumer<Document>> changes = List.of(row -> row.put("formatVersion", 2), row -> row.put("recordRevision", 1),
                row -> row.get("_id", Document.class).put("authorityId", "foreign"), row -> row.put("batchSequence", 0), row -> row.put("extra", true),
                row -> row.get("clock", Document.class).put("round", -1L), row -> row.get("grouping", Document.class).put("initialNanos", 0),
                row -> row.put("nextCheckAt", null), row -> row.put("members", List.of()));
        for (var change : changes) {
            Document row = bson(MongoAlertScheduleCodec.write(root(), 1)); change.accept(row);
            assertThatThrownBy(() -> MongoAlertScheduleCodec.read(root().key(), row)).isInstanceOfSatisfying(TapstateException.class,
                    failure -> assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
        }
    }

    @Test void retirementUsesFrozenDeploymentAndKeepsSummariesUntilTheirHandoff() {
        var before = root();
        var retired = new AlertSchedule.Record(before.key(), before.route(), before.retention(), AT, before.owner(), before.grouping(), before.clock(),
                0, List.of(), null, null, List.of(), List.of(), AT.plusSeconds(1), true, List.of());
        assertThat(retired.retiredCleanupAt()).isEqualTo(AT.plusSeconds(1).plus(Duration.ofDays(7)));
        assertThat(MongoAlertScheduleCodec.read(retired.key(), bson(MongoAlertScheduleCodec.write(retired, 2))).value()).isEqualTo(retired);
    }
    private static Document bson(Document value) {
        try (BasicOutputBuffer out = new BasicOutputBuffer(); BsonBinaryWriter writer = new BsonBinaryWriter(out)) {
            new DocumentCodec().encode(writer, value, EncoderContext.builder().build());
            try (BsonBinaryReader reader = new BsonBinaryReader(ByteBuffer.wrap(out.toByteArray()))) { return new DocumentCodec().decode(reader, DecoderContext.builder().build()); }
        }
    }
}
