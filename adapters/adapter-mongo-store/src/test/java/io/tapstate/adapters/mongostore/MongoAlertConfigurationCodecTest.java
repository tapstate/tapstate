package io.tapstate.adapters.mongostore;

import io.tapstate.core.alerting.AlertConfiguration.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
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
import static io.tapstate.adapters.mongostore.AlertConfigurationFixtures.*;
import static org.assertj.core.api.Assertions.*;

class MongoAlertConfigurationCodecTest {
    @Test void explicitShapesMatchTheAuthoredGoldenAndRetainBsonTypesAndNanoseconds() throws Exception {
        Document actual = new Document("rule", MongoAlertConfigurationCodec.write(rule(), 0))
                .append("subscription", MongoAlertConfigurationCodec.write(legacyRoute(), 0))
                .append("silence", MongoAlertConfigurationCodec.write(silence(), 0));
        try (var input = getClass().getResourceAsStream("/alert-configuration-v1.golden.json")) {
            assertThat(input).isNotNull();
            assertThat(actual).isEqualTo(Document.parse(new String(input.readAllBytes(), StandardCharsets.UTF_8)));
        }
        for (Definition definition : List.of(rule(), route(), legacyRoute(), silence(), retire(rule()))) {
            Document encoded = bson(MongoAlertConfigurationCodec.write(definition, 7));
            var identity = definition.header().identity();
            assertThat(MongoAlertConfigurationCodec.read(identity.kind(), identity.scope(), identity.id(), encoded))
                    .isEqualTo(new MongoAlertConfigurationCodec.Stored(definition, 7));
            assertThat(encoded.get("recordRevision")).isInstanceOf(Long.class);
            assertThat(encoded.get("formatVersion")).isInstanceOf(Integer.class);
        }
    }

    @Test void subscriptionVersionTwoMatchesItsSeparateGoldenAndRejectsMissingOrMalformedSemanticFacts() throws Exception {
        Document actual = bson(MongoAlertConfigurationCodec.write(route(), 0));
        try (var input = getClass().getResourceAsStream("/alert-subscription-v2.golden.json")) {
            assertThat(input).isNotNull();
            assertThat(actual).isEqualTo(Document.parse(new String(input.readAllBytes(), StandardCharsets.UTF_8)));
        }
        assertThat(actual.getInteger("formatVersion")).isEqualTo(2);
        assertThat(actual.get("body", Document.class).get("bindingRevision")).isInstanceOf(Long.class);
        assertThat(MongoAlertConfigurationCodec.size(actual)
                - MongoAlertConfigurationCodec.size(MongoAlertConfigurationCodec.write(legacyRoute(), 0))).isEqualTo(25);
        for (Object bad : java.util.Arrays.asList(null, 1, 0L, -1L, 2L, Double.POSITIVE_INFINITY)) {
            Document row = bson(actual);
            if (bad == null) { row.get("body", Document.class).remove("bindingRevision"); }
            else { row.get("body", Document.class).put("bindingRevision", bad); }
            assertUnreadable(() -> MongoAlertConfigurationCodec.read(Kind.SUBSCRIPTION, SCOPE, "route-a", row));
        }
        Document legacyWithExtra = MongoAlertConfigurationCodec.write(legacyRoute(), 0);
        legacyWithExtra.get("body", Document.class).append("bindingRevision", 1L);
        assertUnreadable(() -> MongoAlertConfigurationCodec.read(Kind.SUBSCRIPTION, SCOPE, "route-a", legacyWithExtra));
    }

    @Test void malformedOrFutureEnvelopeCannotMasqueradeAsAnEmptyDefinition() {
        List<Consumer<Document>> corruptions = List.of(row -> row.put("formatVersion", 2),
                row -> row.put("formatVersion", 1L), row -> row.remove("formatVersion"),
                row -> row.put("recordRevision", 1), row -> row.put("writeFence", -1L),
                row -> row.put("deployment", "UNRESOLVED"), row -> row.put("createdAt", "not-time"),
                row -> row.put("updatedAt", "2026-10-26T00:00:00Z"), row -> row.put("extra", "unknown"),
                row -> row.get("_id", Document.class).put("authorityId", "foreign"),
                row -> row.get("body", Document.class).put("enabled", "true"),
                row -> row.get("body", Document.class).put("severityOverride", "INDETERMINATE"),
                row -> row.get("body", Document.class).get("parameters", Document.class).put("trigger-for", "1E+1000000"));
        for (Consumer<Document> corrupt : corruptions) {
            Document row = MongoAlertConfigurationCodec.write(rule(), 0); corrupt.accept(row);
            assertUnreadable(() -> MongoAlertConfigurationCodec.read(Kind.RULE, SCOPE, "rule-a", row));
        }
    }

    @Test void unknownTypeIsPreservedForCapabilityDiagnosticsButNotReinterpreted() {
        Document row = MongoAlertConfigurationCodec.write(rule(), 0);
        row.get("body", Document.class).put("typeId", "future.type");
        row.get("body", Document.class).put("typeVersion", 9);
        Rule decoded = (Rule) MongoAlertConfigurationCodec.read(Kind.RULE, SCOPE, "rule-a", row).definition();
        assertThat(decoded.typeId()).isEqualTo("future.type");
        assertThat(decoded.typeVersion()).isEqualTo(9);
        assertThatThrownBy(() -> decoded.validated(io.tapstate.core.alerting.BuiltinAlarmTypes.catalog()))
                .isInstanceOf(TapstateException.class);
    }

    @Test void duplicateAndCrossScopeReferencesOrUnknownNestedFieldsRefuseDecoding() {
        Document row = MongoAlertConfigurationCodec.write(route(), 0);
        Document channel = row.get("body", Document.class).getList("channels", Document.class).getFirst();
        channel.getList("receivers", Document.class).getFirst().put("authorityId", "foreign");
        assertUnreadable(() -> MongoAlertConfigurationCodec.read(Kind.SUBSCRIPTION, SCOPE, "route-a", row));
        Document badWindow = MongoAlertConfigurationCodec.write(silence(), 0);
        badWindow.get("body", Document.class).get("window", Document.class).put("durationSeconds", 600);
        assertUnreadable(() -> MongoAlertConfigurationCodec.read(Kind.SILENCE, SCOPE, "silence-a", badWindow));
        Document large = MongoAlertConfigurationCodec.write(rule(), 0);
        large.get("body", Document.class).put("name", "x".repeat(MongoAlertConfigurationCodec.MAX_BYTES));
        assertUnreadable(() -> MongoAlertConfigurationCodec.read(Kind.RULE, SCOPE, "rule-a", large));
    }

    @Test void boundedMembersStillRejectAnOversizedEncodedRowWithACodedConfigurationError() {
        var scope = new io.tapstate.core.alerting.AlarmKey.Scope("a".repeat(256), "c".repeat(256));
        var identity = Identity.create(Kind.SUBSCRIPTION, scope, "route");
        var header = Header.create(identity, io.tapstate.core.alerting.AlertChannelPolicy.Deployment.ON_PREM, AT, "actor");
        var receivers = java.util.stream.IntStream.range(0, 32)
                .mapToObj(i -> Identity.create(Kind.RECEIVER, scope, "r".repeat(250) + i)).toList();
        var channels = java.util.stream.IntStream.range(0, 16)
                .mapToObj(i -> new RouteChannel("channel-" + i, receivers, io.tapstate.core.alerting.MessageSettings.empty())).toList();
        var route = new Subscription(header, "Route", route().match(), true, channels, java.util.Set.of(), route().grouping(), List.of());
        assertThatThrownBy(() -> MongoAlertConfigurationCodec.write(route, 0)).isInstanceOfSatisfying(TapstateException.class,
                failure -> assertThat(failure.code()).isEqualTo(io.tapstate.core.alerting.AlertError.INVALID_CONFIGURATION));
    }

    private static Document bson(Document document) {
        try (BasicOutputBuffer output = new BasicOutputBuffer(); BsonBinaryWriter writer = new BsonBinaryWriter(output)) {
            new DocumentCodec().encode(writer, document, EncoderContext.builder().build());
            try (BsonBinaryReader reader = new BsonBinaryReader(ByteBuffer.wrap(output.toByteArray()))) {
                return new DocumentCodec().decode(reader, DecoderContext.builder().build());
            }
        }
    }
    private static void assertUnreadable(org.assertj.core.api.ThrowableAssert.ThrowingCallable action) {
        assertThatThrownBy(action).isInstanceOfSatisfying(TapstateException.class,
                failure -> assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
    }
}
