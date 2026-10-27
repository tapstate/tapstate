package io.tapstate.adapters.mongostore;

import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.CapturePreparation;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MongoCapturePreparationFactsTest {
    private static final CapturePreparation BIRTH = new CapturePreparation(
            new CacheBusinessScope("cluster-a", 1, "cache-a"), CapturePreparation.Mode.SHARED_SRS,
            "canonical-chain", "capture-a", "mc-" + "a".repeat(64), "11111111-1111-4111-8111-111111111111");
    private static final String OPERATION = "22222222-2222-4222-8222-222222222222";
    private static final String PHYSICAL = "srs-recovery-" + CanonicalHash.ofText(JsonWriter.write(List.of(
            "shared-srs-replacement", "cluster-a", 1L, "cache-a", "canonical-chain", OPERATION)));

    @Test void theOriginalBirthKeepsItsExactVersionOneShape() {
        var row = MongoCapturePreparationFacts.row(BIRTH);
        assertThat(row.getInteger("formatVersion")).isEqualTo(1);
        assertThat(MongoCapturePreparationFacts.decode(row)).isEqualTo(BIRTH);
        assertThat(MongoCapturePreparationFacts.row(BIRTH)).isEqualTo(row);
    }

    @Test void theReplacementClaimRetainsTheOriginalBirthAndOnePhysicalIdentity() {
        var original = MongoCapturePreparationFacts.row(BIRTH);
        var annex = new Document("formatVersion", 2).append("original", original)
                .append("physicalChain", PHYSICAL).append("preparationId", OPERATION);
        var effective = MongoCapturePreparationFacts.decode(annex);
        assertThat(effective.scope()).isEqualTo(BIRTH.scope());
        assertThat(effective.captureId()).isEqualTo(BIRTH.captureId());
        assertThat(effective.sourceIdentityHash()).isEqualTo(BIRTH.sourceIdentityHash());
        assertThat(effective.chainId()).isEqualTo(PHYSICAL);
        assertThat(effective.preparationId()).isEqualTo(OPERATION);
        assertThat(MongoCapturePreparationFacts.row(effective)).isEqualTo(annex);
        assertThat(original).isEqualTo(MongoCapturePreparationFacts.row(BIRTH));
    }

    @Test void aReservedMappingHasNoOrdinaryLoadAuthorityAndReadyNeverReissuesTheBirth() {
        var original = MongoCapturePreparationFacts.row(BIRTH);
        var replacement = MongoCapturePreparationFacts.decode(new Document("formatVersion", 2).append("original", original)
                .append("physicalChain", PHYSICAL).append("preparationId", OPERATION));
        var reserved = new MongoCapturePreparationFacts.Recovery(BIRTH, replacement, "b".repeat(64), MongoCapturePreparationFacts.RecoveryPhase.RESERVED);
        var row = MongoCapturePreparationFacts.configurationRow(reserved);
        assertThat(MongoCapturePreparationFacts.decode(row)).isEqualTo(replacement);
        assertThatThrownBy(() -> MongoCapturePreparationFacts.available(row)).isInstanceOfSatisfying(TapstateException.class,
                error -> assertThat(error.code()).isEqualTo(IoError.CAPTURE_STATE_REBUILD_REQUIRED));
        var ready = new MongoCapturePreparationFacts.Recovery(BIRTH, replacement, reserved.planHash(), MongoCapturePreparationFacts.RecoveryPhase.READY);
        assertThat(MongoCapturePreparationFacts.available(MongoCapturePreparationFacts.configurationRow(ready))).isEqualTo(replacement);
        assertThat(replacement.birth()).isEqualTo(BIRTH);
        assertThat(MongoCapturePreparationFacts.row(replacement).get("original")).isEqualTo(original);
        assertThatThrownBy(() -> MongoCapturePreparationFacts.annex(row)).isInstanceOf(TapstateException.class);
    }

    @Test void aSecondReplacementKeepsOnlyOneFlatPredecessorAndTheSameOriginalBirth() {
        var first = MongoCapturePreparationFacts.decode(new Document("formatVersion", 2).append("original", MongoCapturePreparationFacts.row(BIRTH))
                .append("physicalChain", PHYSICAL).append("preparationId", OPERATION));
        String secondId = "33333333-3333-4333-8333-333333333333";
        var second = MongoCapturePreparationFacts.decode(new Document("formatVersion", 2).append("original", MongoCapturePreparationFacts.row(BIRTH))
                .append("physicalChain", MongoCapturePreparationFacts.replacementChain(BIRTH, secondId)).append("preparationId", secondId));
        var row = MongoCapturePreparationFacts.configurationRow(new MongoCapturePreparationFacts.Recovery(first, second,
                "c".repeat(64), MongoCapturePreparationFacts.RecoveryPhase.READY));
        assertThat(row.get("chain")).isEqualTo(MongoCapturePreparationFacts.row(BIRTH).get("chain"));
        assertThat(row.getString("preparationId")).isEqualTo(BIRTH.preparationId());
        assertThat(MongoCapturePreparationFacts.recovery(row).orElseThrow().previous()).isEqualTo(first);
        assertThat(MongoCapturePreparationFacts.available(row)).isEqualTo(second);
        assertThat(MongoCapturePreparationFacts.row(second).get("original")).isEqualTo(MongoCapturePreparationFacts.row(BIRTH));
        assertThat(row.get("currentRecovery", Document.class).keySet()).isEqualTo(Set.of(
                "formatVersion", "operationId", "planHash", "previousPhysicalChain", "previousPreparationId", "physicalChain", "phase"));
    }

    static Stream<Document> malformedAnnexes() {
        var original = MongoCapturePreparationFacts.row(BIRTH);
        var valid = new Document("formatVersion", 2).append("original", original).append("physicalChain", PHYSICAL).append("preparationId", OPERATION);
        var missing = new Document(valid); missing.remove("original");
        var extra = new Document(valid).append("extra", true);
        var nested = new Document(valid).append("original", valid);
        var mode = new Document(original).append("mode", "DIRECT");
        var wrongBirth = new Document(original).append("preparationId", OPERATION);
        return Stream.of(missing, extra, nested, new Document(valid).append("formatVersion", 2L),
                new Document(valid).append("physicalChain", "a-different-physical-domain"),
                new Document(valid).append("original", mode), new Document(valid).append("original", wrongBirth),
                new Document(valid).append("original", new Document(original).append("extra", true)));
    }
    @ParameterizedTest @MethodSource("malformedAnnexes")
    void malformedReplacementFactsNeverFallBackToTheOriginalBirth(Document row) {
        assertThatThrownBy(() -> MongoCapturePreparationFacts.decode(row)).isInstanceOfSatisfying(TapstateException.class,
                error -> assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
    }
}
