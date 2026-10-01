package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.StopAuthority;
import io.tapstate.spi.store.StopReservation;
import org.bson.BsonBinaryWriter;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Strict internal wire shapes remain bounded through legacy conversion and every successor phase. */
class StopReservationDocumentTest {
    @Test void aNearLimitLegacyMarkerConvertsAndStillFitsItsAdmittedAndBoundForms() {
        StopReservation small = legacy("r");
        int revisionLength = 4090 - bytes(StopReservationDocument.write(small)) + 1;
        StopReservation old = legacy("r".repeat(revisionLength));
        Document wire = StopReservationDocument.write(old);
        assertThat(bytes(wire)).isBetween(4080, 4096);
        assertThat(StopReservationDocument.read("orders", 1, wire)).isEqualTo(old);
        StopReservation pending = new StopReservation("orders", old.token(), 0, 2, old.originalDesired(),
                old.source(), StopReservation.Phase.REPLACEMENT_PENDING, StopReservation.CounterPolicy.CONTINUE,
                old.writerAuthority(), null, 16);
        ObservationStore.Scope target = new ObservationStore.Scope("inc-a", 2);
        StopReservation admitted = new StopReservation("orders", old.token(), 0, 3, old.originalDesired(),
                old.source(), StopReservation.Phase.SUCCESSOR_ADMITTED, StopReservation.CounterPolicy.CONTINUE,
                StopAuthority.standalone("cluster-a", 2), new StopReservation.Successor(target, "target-boot", null), 16);
        StopReservation bound = new StopReservation("orders", old.token(), 0, 4, old.originalDesired(),
                old.source(), StopReservation.Phase.SUCCESSOR_BOUND, StopReservation.CounterPolicy.CONTINUE,
                admitted.writerAuthority(), new StopReservation.Successor(target, "target-boot",
                        new StopReservation.JobIdentity("cluster-a", 42, "target-boot")), 16);
        for (StopReservation marker : new StopReservation[]{pending, admitted, bound}) {
            Document encoded = StopReservationDocument.write(marker);
            assertThat(bytes(encoded)).isLessThanOrEqualTo(4096);
            assertThat(StopReservationDocument.read("orders", marker.reservedEpoch(), encoded)).isEqualTo(marker);
            assertThat(marker.source()).isEqualTo(old.source());
        }
    }

    @Test void compactSlotCannotPretendItsGenerationIsTheOldWriterGeneration() {
        StopReservation old = legacy("r");
        ObservationStore.Scope target = new ObservationStore.Scope("inc-a", 2);
        StopReservation marker = new StopReservation("orders", "work", 0, 2, old.originalDesired(), old.source(),
                StopReservation.Phase.SUCCESSOR_ADMITTED, StopReservation.CounterPolicy.CONTINUE,
                StopAuthority.standalone("cluster-a", 2), new StopReservation.Successor(target, "target-boot", null), 16);
        Document encoded = StopReservationDocument.write(marker);
        encoded.get("w", Document.class).put("g", 1L);
        assertThatThrownBy(() -> StopReservationDocument.read("orders", 2, encoded))
                .isInstanceOfSatisfying(TapstateException.class, e -> assertThat(e.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
    }

    @Test void anUnknownLegacyPolicyIsNotRewrittenAsReset() {
        StopReservation old = legacy("r");
        StopReservation decoded = StopReservationDocument.read("orders", 1, StopReservationDocument.write(old));
        assertThat(decoded.legacy()).isTrue();
        assertThat(decoded.counterPolicy()).isNull();
        assertThat(decoded.source()).isEqualTo(old.source());
        assertThat(decoded.writerAuthority()).isEqualTo(old.writerAuthority());
    }

    @Test void oversizedCompactStringsFailWithACodeInsteadOfTruncation() {
        StopReservation old = legacy("r".repeat(5000));
        StopReservation current = new StopReservation("orders", old.token(), 0, 1, old.originalDesired(), old.source(),
                StopReservation.Phase.STOPPING, StopReservation.CounterPolicy.CONTINUE, old.writerAuthority(), null, 16);
        assertThatThrownBy(() -> StopReservationDocument.write(current))
                .isInstanceOfSatisfying(TapstateException.class, e -> assertThat(e.code()).isEqualTo(IoError.DOCUMENT_TOO_LARGE));
    }

    private static StopReservation legacy(String revision) {
        return new StopReservation("orders", "work", 0, 1,
                new DesiredState("orders", PipelineState.RUNNING, revision, false, "assembly-a", false, null),
                new StopReservation.ExistingJob("inc-a", 1,
                        new StopReservation.JobIdentity("cluster-a", 11, "old-boot"),
                        StopAuthority.standalone("cluster-a", 1)));
    }

    private static int bytes(Document value) {
        try (BasicOutputBuffer output = new BasicOutputBuffer(); BsonBinaryWriter writer = new BsonBinaryWriter(output)) {
            new DocumentCodec().encode(writer, value, EncoderContext.builder().build());
            return output.getSize();
        }
    }
}
