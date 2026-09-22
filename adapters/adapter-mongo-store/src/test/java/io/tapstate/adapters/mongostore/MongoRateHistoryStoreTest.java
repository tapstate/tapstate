package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.RateSample;
import io.tapstate.spi.store.IoError;
import org.bson.Document;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Date;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * The sample-document codec, witnessed without a server. What the store does against a real server —
 * append, the range read, the delete, and above all the expiry — is exercised by the integration cases,
 * which need one.
 */
class MongoRateHistoryStoreTest {

    private static final Instant TAKEN = Instant.parse("2026-09-17T10:05:00Z");
    private static final Instant SINCE = Instant.parse("2026-09-17T02:00:00Z");

    private static RateSample aSample() {
        return new RateSample("orders_sync", TAKEN,
                Map.of("records.in", 128_500L, "records.out", 128_412L, "errors.sink.write-rejected", 1L),
                Map.of("orders", 2L, "order_items", 47L), SINCE);
    }

    @Test
    @DisplayName("the sample timestamp is stored as a date, not a string")
    void theSampleTimestampIsADateNotAString() {
        // An expiring index over a string matches nothing and drops nothing, and the collection grows
        // forever under an index that looks exactly right. The neighbouring position record stores its
        // instant as a string on purpose; this one must not copy it.
        Document document = MongoRateHistoryStore.toDocument(aSample());

        assertThat(document.get("observedAt")).isInstanceOf(Date.class).isEqualTo(Date.from(TAKEN));
        assertThat(document.get("countingSince")).isInstanceOf(Date.class).isEqualTo(Date.from(SINCE));
    }

    @Test
    @DisplayName("the document carries the pipeline, the counters and the per-table delay as sub-documents")
    void theDocumentCarriesCountersAndLagApart() {
        Document document = MongoRateHistoryStore.toDocument(aSample());

        assertThat(document.getString("pipelineId")).isEqualTo("orders_sync");
        assertThat(((Document) document.get("counters")).get("records.out")).isEqualTo(128_412L);
        assertThat(((Document) document.get("lag")).get("order_items")).isEqualTo(47L);
    }

    @Test
    void roundTripReconstructsTheSameSample() {
        RateSample sample = aSample();

        assertThat(MongoRateHistoryStore.toSample(MongoRateHistoryStore.toDocument(sample))).isEqualTo(sample);
    }

    @Test
    void aSampleWithNoStartRoundTripsWithoutOne() {
        RateSample sample = new RateSample("orders_sync", TAKEN, Map.of(), Map.of("orders", 0L), null);

        Document document = MongoRateHistoryStore.toDocument(sample);
        assertThat(document).doesNotContainKey("countingSince");
        assertThat(MongoRateHistoryStore.toSample(document)).isEqualTo(sample);
    }

    @Test
    void aTimestampStoredAsAStringIsDocumentUnreadableNotACastCrash() {
        Document document = new Document("pipelineId", "orders_sync")
                .append("observedAt", "2026-09-17T10:05:00Z")
                .append("counters", new Document()).append("lag", new Document());

        Throwable thrown = catchThrowable(() -> MongoRateHistoryStore.toSample(document));

        assertThat(thrown).isInstanceOf(TapstateException.class);
        assertThat(((TapstateException) thrown).code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);
        assertThat(((TapstateException) thrown).args()).containsEntry("field", "observedAt");
    }

    @Test
    void aCounterCellThatIsNotANumberIsDocumentUnreadable() {
        Document document = new Document("pipelineId", "orders_sync")
                .append("observedAt", Date.from(TAKEN))
                .append("counters", new Document("records.in", "lots")).append("lag", new Document());

        Throwable thrown = catchThrowable(() -> MongoRateHistoryStore.toSample(document));

        assertThat(thrown).isInstanceOf(TapstateException.class);
        assertThat(((TapstateException) thrown).args()).containsEntry("field", "counters");
    }

    @Test
    void theDefaultRetentionIsFifteenDays() {
        // The registry declares the same expiry on the index, so the two cannot drift.
        assertThat(MongoRateHistoryStore.DEFAULT_RETENTION.toDays()).isEqualTo(15L);
        assertThat(SystemCollections.PIPELINE_RATE_HISTORY.indexes().get(0).expireAfterSeconds())
                .isEqualTo(15L * 24 * 3600);
    }
}
