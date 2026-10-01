package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import org.bson.Document;
import org.bson.types.Binary;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ObservationBsonBoundsTest {
    @Test
    void fourMaximumDescriptorSlotsAndTheFixedHeaderStayBelowThePhysicalLimit() {
        Document descriptor = new Document("payload", new Binary(new byte[ObservationBsonBounds.DESCRIPTOR_BYTES - 19]));
        ObservationBsonBounds.requireDescriptor("orders", descriptor);
        assertThat(ObservationBsonBounds.bsonBytes(descriptor)).isEqualTo(ObservationBsonBounds.DESCRIPTOR_BYTES);
        Document envelope = new Document("_id", new Binary(new byte[32])).append("ownerDigest", new Binary(new byte[32]))
                .append("formatVersion", 1).append("legacyFallback", false).append("legacyResidue", false)
                .append("revision", "00000000-0000-0000-0000-000000000000")
                .append("current", descriptor).append("pending", descriptor)
                .append("continuation", descriptor).append("continuationPending", descriptor);
        int actual = ObservationBsonBounds.bsonBytes(envelope);
        assertThat(actual).isLessThan(ObservationBsonBounds.ENVELOPE_BYTES);
        assertThat(actual - 4 * ObservationBsonBounds.DESCRIPTOR_BYTES).isLessThan(1024);
        System.out.printf("observation-companion-bson descriptorBytes=%d fourSlotEnvelopeBytes=%d physicalLimitBytes=%d%n",
                ObservationBsonBounds.DESCRIPTOR_BYTES, actual, ObservationBsonBounds.ENVELOPE_BYTES);
    }

    @Test
    void trueBsonSizeRejectsOversizedBinaryAndMultibyteIdentityBeforePersistence() {
        Document binary = new Document("payload", new Binary(new byte[ObservationBsonBounds.DESCRIPTOR_BYTES]));
        Document unicode = new Document("pipelineIncarnationId", "\uD83D\uDE00".repeat(ObservationBsonBounds.DESCRIPTOR_BYTES / 4));
        for (Document descriptor : java.util.List.of(binary, unicode)) {
            assertThatThrownBy(() -> ObservationBsonBounds.requireDescriptor("orders", descriptor))
                    .isInstanceOfSatisfying(TapstateException.class, failure -> {
                        assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_TOO_LARGE);
                        assertThat(failure.args()).containsEntry("id", "orders");
                    });
        }
    }
}
