package io.tapstate.adapters.mongostore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.*;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import org.bson.BsonBinaryWriter;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
import org.junit.jupiter.api.Test;

class MongoSrsSourceRequestsTest {
    private static final SrsFreshFull FULL = new SrsFreshFull(new SrsFreshFullOrigin("full-operation",
            new PipelineIntentScope("cluster", 1, "cache", "pipeline", 2), "preparation", "a".repeat(64), "source"),
            2, Map.of("orders", new SrsModelKey("chain", "orders", "cache", 2, 0)));

    @Test void ordinaryRequestsRetainTheirOriginalStrictShapeAndDirectDecoder() {
        var request = request("cut-90");
        var original = MongoResumeOverrides.stored(request);
        assertThat(original.keySet()).isEqualTo(Set.of("token", "fromEpoch", "editedAt", "contentHash"));
        assertThat(MongoSrsSourceRequests.stored(request, null)).isEqualTo(original);
        assertThat(MongoSrsSourceRequests.read(original)).isEqualTo(new MongoSrsSourceRequests.Decoded(request, null));
        assertThat(MongoResumeOverrides.read(original)).isEqualTo(request);
    }

    @Test void pendingMetadataRoundTripsButCannotEnterTheOrdinaryDirectCodec() {
        var row = MongoSrsSourceRequests.stored(request("cut-90"), FULL);
        assertThat(MongoSrsSourceRequests.read(row)).isEqualTo(new MongoSrsSourceRequests.Decoded(request("cut-90"), FULL));
        assertThatThrownBy(() -> MongoResumeOverrides.read(row)).isInstanceOf(TapstateException.class);
        var corrupted = new Document(row).append("contentHash", "b".repeat(64));
        assertThatThrownBy(() -> MongoSrsSourceRequests.read(corrupted)).isInstanceOf(TapstateException.class);
        var unknown = new Document(row).append("unrecognized", true);
        assertThatThrownBy(() -> MongoSrsSourceRequests.read(unknown)).isInstanceOf(TapstateException.class);
        var pending = new Document(row.get("pendingFreshFull", Document.class)).append("formatVersion", 2);
        assertThatThrownBy(() -> MongoSrsSourceRequests.read(new Document(row).append("pendingFreshFull", pending)))
                .isInstanceOf(TapstateException.class);
    }

    @Test void theLimitCountsActualBsonIncludingTheTokenOriginModelsAndHash() {
        int fixed = bytes(MongoSrsSourceRequests.stored(request("x"), FULL));
        String fitting = "x".repeat(SrsFreshFull.MAX_ENCODED_REQUEST_BYTES - fixed + 1);
        var boundary = MongoSrsSourceRequests.stored(request(fitting), FULL);
        assertThat(bytes(boundary)).isEqualTo(SrsFreshFull.MAX_ENCODED_REQUEST_BYTES);
        assertThat(MongoSrsSourceRequests.read(boundary).request().token()).isEqualTo(fitting);
        assertThatThrownBy(() -> MongoSrsSourceRequests.stored(request(fitting + "x"), FULL))
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code()).isEqualTo(IoError.SRS_FULL_PREPARATION_CAPACITY));
    }

    private static SourceResumeOverride request(String token) { return new SourceResumeOverride(token, 1, Instant.EPOCH); }
    private static int bytes(Document row) {
        try (var output = new BasicOutputBuffer(); var writer = new BsonBinaryWriter(output)) {
            new DocumentCodec().encode(writer, row, EncoderContext.builder().build()); return output.getSize();
        }
    }
}
