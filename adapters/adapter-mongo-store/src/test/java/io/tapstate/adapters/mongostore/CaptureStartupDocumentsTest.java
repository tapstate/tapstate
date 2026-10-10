package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.model.ReadMode;
import io.tapstate.spi.store.CaptureResumeWitness;
import io.tapstate.spi.store.ConsumerProgressKind;
import io.tapstate.spi.store.IoError;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CaptureStartupDocumentsTest {
    @Test
    void frozenSnapshotAndConsumerCoordinatesRoundTripWithoutReadingNewerTruth() {
        var witness = new CaptureResumeWitness("source", "mongo", "chain", "consumer", ReadMode.SNAPSHOT_AND_CDC,
                true, List.of("orders", "customers"), true, 4,
                new ChainPosition(new SourceOrder(4, 200), "producer-ahead"), true, true,
                List.of("orders"), "original-own-seam", 2, ConsumerProgressKind.SRS,
                new ChainPosition(new SourceOrder(2, 30), "confirmed-consumer"),
                Map.of("orders", new ChainPosition(new SourceOrder(2, 30), "confirmed-consumer")));
        assertThat(CaptureStartupDocuments.witness(CaptureStartupDocuments.witness(witness))).isEqualTo(witness);
        assertThat(CaptureStartupDocuments.requested(CaptureStartupDocuments.requested(
                witness.requestedPosition("actual-capture").orElseThrow())))
                .isEqualTo(witness.requestedPosition("actual-capture").orElseThrow());
    }

    @Test
    void missingRequiredSourceDeclarationIsUnreadableRatherThanAnEmptyCompleteSet() {
        assertThatThrownBy(() -> CaptureStartupDocuments.requiredSources(new Document()))
                .isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
    }

    @Test
    void malformedNewReaderMarkerCannotMasqueradeAsLegacyAbsence() {
        assertThatThrownBy(() -> CaptureStartupDocuments.reader(new Document("schemaVersion", 1)))
                .isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
    }
    @Test
    void aCompleteLookingMarkerWithAnUnknownSchemaCannotAuthorizeReaderAcceptance() {
        Document stored = acceptedReader();
        stored.put("schemaVersion", 2);
        assertThatThrownBy(() -> CaptureStartupDocuments.reader(stored))
                .isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
    }

    @Test
    void aMissingFailureFlagCannotBeInterpretedAsAHealthyAcceptedReader() {
        Document stored = acceptedReader();
        stored.remove("failed");
        assertThatThrownBy(() -> CaptureStartupDocuments.reader(stored))
                .isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
    }

    @Test
    void namedFailureMapsUseCanonicalBsonOrderAcrossEndpoints() {
        java.util.Map<String, Object> first = new java.util.LinkedHashMap<>();
        first.put("retention", "2h"); first.put("requested", "old");
        java.util.Map<String, Object> second = new java.util.LinkedHashMap<>();
        second.put("requested", "old"); second.put("retention", "2h");
        assertThat(CaptureStartupDocuments.namedParams(first).toJson())
                .isEqualTo(CaptureStartupDocuments.namedParams(second).toJson());
    }

    private static Document acceptedReader() {
        var fence = new io.tapstate.spi.store.WorkloadClaimFence(new io.tapstate.spi.store.WorkloadClaimKey("east",
                io.tapstate.spi.store.WorkloadClaimType.CAPTURE, "capture"),
                new io.tapstate.spi.store.WorkloadOwner("node", "boot"), 1, 0, 1, 1);
        return new Document("schemaVersion", 1).append("miningChainId", "chain").append("chainEpoch", 1L)
                .append("version", 1L).append("captureClaim", WorkloadClaimDocuments.stored(fence))
                .append("tables", List.of("orders")).append("requestedKind", "PRESENT")
                .append("requestedToken", null).append("requestedInstant", null)
                .append("allocatedAt", new java.util.Date(0)).append("resolvedAnchor", "real-anchor")
                .append("anchorResolvedAt", new java.util.Date(0)).append("firstDeliveredAt", new java.util.Date(1))
                .append("failed", false).append("failureCode", null);
    }

}
