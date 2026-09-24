package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.HistogramBounds;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.MetricType;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.ObservationFailure;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.TableSnapshot;
import io.tapstate.spi.store.IoError;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * The observation-document codec is the mapping core of the observation store: an observation is
 * stored as a structured document — its pipeline id as {@code _id}, its state as a field, its metrics
 * and per-table snapshot progress as sub-documents — and reconstructed from it on read. These witness
 * the mapping deterministically, without a Mongo server. A real Mongo round-trip is exercised by
 * {@code MongoObservationStoreIT} (skipped where Docker is absent).
 */
class MongoObservationStoreTest {

    @Test
    void documentCarriesIdStateMetricsAndSnapshot() {
        Observation obs = new Observation("orders_sync", PipelineState.RUNNING,
                Map.of("recordCount", 128500L, "lag", 42000L),
                Map.of("orders", new TableSnapshot(90000L, 120000L, 75)));

        Document document = MongoObservationStore.toDocument(obs);

        assertThat(document.getString("_id")).isEqualTo("orders_sync");
        assertThat(document.getString("state")).isEqualTo("RUNNING");
        Document metrics = (Document) document.get("metrics");
        assertThat(metrics.get("recordCount")).isEqualTo(128500L);
        assertThat(metrics.get("lag")).isEqualTo(42000L);
        Document orders = (Document) ((Document) document.get("snapshot")).get("orders");
        assertThat(orders.get("rowsDone")).isEqualTo(90000L);
        assertThat(orders.get("rowsTotal")).isEqualTo(120000L);
        assertThat(orders.get("donePct")).isEqualTo(75);
    }

    @Test
    void roundTripPreservesWhenTheObservationWasTaken() {
        Instant observedAt = Instant.parse("2026-07-01T12:34:56.789Z");
        Observation obs = new Observation("orders_sync", PipelineState.RUNNING, Map.of(), Map.of(), Map.of(),
                null, observedAt);

        assertThat(MongoObservationStore.toObservation(MongoObservationStore.toDocument(obs))).isEqualTo(obs);
    }

    @Test
    void theObservationTimeIsStoredAsADateNotAString() {
        Observation obs = new Observation("orders_sync", PipelineState.RUNNING, Map.of(), Map.of(), Map.of(),
                null, Instant.parse("2026-07-01T12:34:56.789Z"));

        Object stored = MongoObservationStore.toDocument(obs).get("observedAt");

        // A stored instant is only useful if the server can order and expire it, and a string sorts
        // lexicographically rather than chronologically. The round trip above passes either way, so this is
        // the assertion that holds the encoding.
        assertThat(stored).isInstanceOf(Date.class);
    }

    @Test
    void toObservationOnADocumentMissingTheObservationTimeReadsNotKnown() {
        Document legacy = new Document("_id", "orders_sync").append("state", "RUNNING");

        // A document written before this version recorded the time is read, not rejected, and the absence
        // stays an absence: filling it in from the reader's own clock would report every stale projection
        // as fresh.
        assertThat(MongoObservationStore.toObservation(legacy).observedAt()).isNull();
    }

    @Test
    void roundTripReconstructsTheSameObservation() {
        Observation obs = new Observation("orders_sync", PipelineState.RUNNING,
                Map.of("recordCount", 5L), Map.of("orders", new TableSnapshot(10L, 20L, 50)));

        assertThat(MongoObservationStore.toObservation(MongoObservationStore.toDocument(obs))).isEqualTo(obs);
    }

    @Test
    void roundTripsAStateOnlyObservationWithEmptyMetricsAndSnapshot() {
        Observation obs = new Observation("p1", PipelineState.NEW, Map.of(), Map.of());

        assertThat(MongoObservationStore.toObservation(MongoObservationStore.toDocument(obs))).isEqualTo(obs);
    }

    @Test
    void roundTripPreservesAnUnavailableSnapshotTotal() {
        // rows_total not yet wired: it is stored absent and reads back as null (unavailable), never faked.
        Observation obs = new Observation("p1", PipelineState.RUNNING, Map.of(),
                Map.of("orders", new TableSnapshot(500L, null, null)));

        Observation back = MongoObservationStore.toObservation(MongoObservationStore.toDocument(obs));

        assertThat(back.snapshot().get("orders").rowsTotal()).isNull();
        assertThat(back.snapshot().get("orders").donePct()).isNull();
        assertThat(back).isEqualTo(obs);
    }

    @Test
    void documentCarriesPerTablePositions() {
        Observation obs = new Observation("orders_sync", PipelineState.RUNNING,
                Map.of(), Map.of(), Map.of("orders", "w7"));

        Document document = MongoObservationStore.toDocument(obs);

        assertThat(document.get("positions")).isInstanceOf(Document.class);
        assertThat(((Document) document.get("positions")).get("orders")).isEqualTo("w7");
    }

    @Test
    void roundTripPreservesPerTablePositions() {
        Observation obs = new Observation("orders_sync", PipelineState.RUNNING,
                Map.of("recordCount", 5L), Map.of(), Map.of("orders", "w7"));

        assertThat(MongoObservationStore.toObservation(MongoObservationStore.toDocument(obs))).isEqualTo(obs);
    }

    @Test
    void toObservationOnADocumentMissingPositionsReadsEmptyPositions() {
        // An observation written before positions existed has no positions field; it reads back empty
        // (unavailable), never a crash.
        Document legacy = new Document("_id", "p1").append("state", "RUNNING");

        assertThat(MongoObservationStore.toObservation(legacy).positions()).isEmpty();
    }

    @Test
    void documentCarriesTheCodedFailure() {
        Observation obs = new Observation("orders_sync", PipelineState.FAILED, Map.of(), Map.of(), Map.of(),
                new ObservationFailure("engine.job-failed", Map.of("pipeline", "orders_sync", "cause", "sink refused")));

        Document document = MongoObservationStore.toDocument(obs);

        Document failure = (Document) document.get("failure");
        assertThat(failure.getString("code")).isEqualTo("engine.job-failed");
        assertThat(((Document) failure.get("params")).get("cause")).isEqualTo("sink refused");
    }

    @Test
    void roundTripPreservesTheCodedFailure() {
        Observation obs = new Observation("orders_sync", PipelineState.FAILED, Map.of("errorCount", 1L), Map.of(),
                Map.of(), new ObservationFailure("engine.job-failed", Map.of("pipeline", "orders_sync")));

        assertThat(MongoObservationStore.toObservation(MongoObservationStore.toDocument(obs))).isEqualTo(obs);
    }

    @Test
    void aHealthyObservationStoresNoFailureFieldAndReadsBackNull() {
        // Absence is the healthy case: a pipeline with nothing wrong must not store an empty-coded failure,
        // and a document written before failures existed reads back null rather than crashing.
        Observation healthy = new Observation("p1", PipelineState.RUNNING, Map.of(), Map.of());

        Document document = MongoObservationStore.toDocument(healthy);

        assertThat(document.get("failure")).isNull();
        assertThat(MongoObservationStore.toObservation(document).failure()).isNull();
    }

    @Test
    void toObservationOnAFailureStoredAsSomethingElseIsDocumentUnreadable() {
        // A failure field stored as anything but a sub-document is store corruption, surfaced as a coded io
        // diagnostic rather than a class-cast crash while reconstructing.
        Document corrupt = new Document("_id", "p1").append("state", "FAILED").append("failure", "not-a-document");

        Throwable thrown = catchThrowable(() -> MongoObservationStore.toObservation(corrupt));

        assertThat(thrown).isInstanceOf(TapstateException.class);
        assertThat(((TapstateException) thrown).code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);
    }

    @Test
    void toObservationOnAFailureMissingItsCodeIsDocumentUnreadable() {
        // A failure sub-document with no code cannot be rendered by any read face; that is corruption, not a
        // failure with a null code silently handed on.
        Document corrupt = new Document("_id", "p1").append("state", "FAILED")
                .append("failure", new Document("params", new Document("pipeline", "p1")));

        Throwable thrown = catchThrowable(() -> MongoObservationStore.toObservation(corrupt));

        assertThat(thrown).isInstanceOf(TapstateException.class);
        assertThat(((TapstateException) thrown).code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);
    }

    @Test
    void toObservationOnAFailureCodeStoredAsANonStringIsDocumentUnreadable() {
        // Document.getString is an unchecked cast (get(key, String.class)): a wrong-typed code must surface
        // as this same coded diagnostic, not escape as a bare ClassCastException while reconstructing.
        Document corrupt = new Document("_id", "p1").append("state", "FAILED")
                .append("failure", new Document("code", 42).append("params", new Document()));

        Throwable thrown = catchThrowable(() -> MongoObservationStore.toObservation(corrupt));

        assertThat(thrown).isInstanceOf(TapstateException.class);
        assertThat(((TapstateException) thrown).code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);
    }

    @Test
    void toObservationOnADocumentMissingStateIsDocumentUnreadable() {
        Document corrupt = new Document("_id", "p1").append("metrics", new Document());

        Throwable thrown = catchThrowable(() -> MongoObservationStore.toObservation(corrupt));

        assertThat(thrown).isInstanceOf(TapstateException.class);
        TapstateException coded = (TapstateException) thrown;
        assertThat(coded.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);
        assertThat(coded.args()).containsEntry("id", "p1");
    }

    @Test
    void toObservationOnAnUnrecognizedStateIsDocumentUnreadable() {
        Document corrupt = new Document("_id", "p1").append("state", "TELEPORTING");

        Throwable thrown = catchThrowable(() -> MongoObservationStore.toObservation(corrupt));

        assertThat(thrown).isInstanceOf(TapstateException.class);
        assertThat(((TapstateException) thrown).code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);
    }

    @Test
    void toObservationOnAWrongTypedMetricValueIsDocumentUnreadable() {
        // A metric value stored as a non-number is store corruption, surfaced as a coded io diagnostic
        // rather than a bare ClassCastException while reconstructing.
        Document corrupt = new Document("_id", "p1")
                .append("state", "RUNNING")
                .append("metrics", new Document("recordCount", "not-a-number"));

        Throwable thrown = catchThrowable(() -> MongoObservationStore.toObservation(corrupt));

        assertThat(thrown).isInstanceOf(TapstateException.class);
        assertThat(((TapstateException) thrown).code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);
    }

    @Test
    void toObservationOnAWrongTypedPositionValueIsDocumentUnreadable() {
        // A per-table position stored as a non-string is store corruption, surfaced as a coded io
        // diagnostic rather than a bare crash while reconstructing.
        Document corrupt = new Document("_id", "p1")
                .append("state", "RUNNING")
                .append("positions", new Document("orders", 42));

        Throwable thrown = catchThrowable(() -> MongoObservationStore.toObservation(corrupt));

        assertThat(thrown).isInstanceOf(TapstateException.class);
        assertThat(((TapstateException) thrown).code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);
    }

    @Test
    void toObservationOnANonDocumentPositionsFieldIsDocumentUnreadable() {
        // A positions field stored as something other than a sub-document is store corruption, surfaced
        // as a coded io diagnostic rather than a bare cast crash while reconstructing.
        Document corrupt = new Document("_id", "p1")
                .append("state", "RUNNING")
                .append("positions", "not-a-document");

        Throwable thrown = catchThrowable(() -> MongoObservationStore.toObservation(corrupt));

        assertThat(thrown).isInstanceOf(TapstateException.class);
        assertThat(((TapstateException) thrown).code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);
    }

    @Test
    void toObservationOnANonDocumentMetricsFieldIsDocumentUnreadable() {
        // A metrics field stored as something other than a sub-document is store corruption, surfaced
        // as a coded io diagnostic rather than a bare cast crash while reconstructing.
        Document corrupt = new Document("_id", "p1")
                .append("state", "RUNNING")
                .append("metrics", "not-a-document");

        Throwable thrown = catchThrowable(() -> MongoObservationStore.toObservation(corrupt));

        assertThat(thrown).isInstanceOf(TapstateException.class);
        assertThat(((TapstateException) thrown).code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);
    }

    @Test
    void toObservationOnANonDocumentSnapshotFieldIsDocumentUnreadable() {
        // A snapshot field stored as something other than a sub-document is store corruption, surfaced
        // as a coded io diagnostic rather than a bare cast crash while reconstructing.
        Document corrupt = new Document("_id", "p1")
                .append("state", "RUNNING")
                .append("snapshot", "not-a-document");

        Throwable thrown = catchThrowable(() -> MongoObservationStore.toObservation(corrupt));

        assertThat(thrown).isInstanceOf(TapstateException.class);
        assertThat(((TapstateException) thrown).code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);
    }

    // ---- the measured facts travel with the document, as the fact type lays them out ----

    private static final Instant TAKEN = Instant.parse("2026-09-17T10:00:00.250Z");
    private static final Instant COUNTING_SINCE = Instant.parse("2026-09-17T09:00:00Z");

    /** One of each kind, with the shapes a reader of the store meets: attributes, a start, a distribution. */
    private static List<MetricFact> threeFacts() {
        MetricFact records = new MetricFact("tapstate.pipeline.records", MetricType.COUNTER, "{record}", List.of(
                MetricPoint.accumulated(Map.of("tapstate.pipeline.id", "orders_sync", "tapstate.table.id", "orders",
                        "direction", "in", "op", "insert"), COUNTING_SINCE, TAKEN, 100L),
                MetricPoint.accumulated(Map.of("tapstate.pipeline.id", "orders_sync", "tapstate.table.id", "orders",
                        "direction", "out", "op", "insert"), COUNTING_SINCE, TAKEN, 98L)));
        MetricFact count = MetricFact.single("recordCount", MetricType.GAUGE, "{record}",
                MetricPoint.reading(Map.of(), TAKEN, 98L));
        HistogramBounds bounds = HistogramBounds.RECORD_DELIVERY_DURATION;
        List<Long> buckets = new ArrayList<>();
        for (int i = 0; i < bounds.buckets(); i++) {
            buckets.add(i == 3 ? 7L : 0L);
        }
        MetricFact delivery = MetricFact.single(bounds.instrument(), MetricType.HISTOGRAM, HistogramBounds.UNIT,
                MetricPoint.distribution(Map.of("tapstate.pipeline.id", "orders_sync", "tapstate.table.id", "orders"),
                        COUNTING_SINCE, TAKEN, bounds.value(7, 0.63, buckets)));
        return List.of(records, count, delivery);
    }

    @Test
    void documentCarriesEachFactWithItsPointsAsTheFactTypeLaysThemOut() {
        Observation obs = new Observation("orders_sync", PipelineState.RUNNING, Map.of("recordCount", 98L),
                Map.of(), Map.of(), null, TAKEN, threeFacts());

        Document document = MongoObservationStore.toDocument(obs);

        List<?> facts = (List<?>) document.get("facts");
        assertThat(facts).hasSize(3);
        Document records = (Document) facts.get(0);
        assertThat(records.getString("name")).isEqualTo("tapstate.pipeline.records");
        assertThat(records.getString("type")).isEqualTo("COUNTER");
        assertThat(records.getString("unit")).isEqualTo("{record}");
        Document inbound = (Document) ((List<?>) records.get("points")).get(0);
        // Attributes sorted by key, instants as BSON dates, a single value and no distribution fields.
        assertThat(((Document) inbound.get("attributes")).keySet())
                .containsExactly("direction", "op", "tapstate.pipeline.id", "tapstate.table.id");
        assertThat(inbound.get("startTime")).isEqualTo(Date.from(COUNTING_SINCE));
        assertThat(inbound.get("observedAt")).isEqualTo(Date.from(TAKEN));
        assertThat(inbound.get("value")).isEqualTo(100L);
        assertThat(inbound).doesNotContainKeys("count", "sum", "bounds", "bucketCounts");
        Document gauge = (Document) ((List<?>) ((Document) facts.get(1)).get("points")).get(0);
        // A reading has no start, and the field is absent rather than null.
        assertThat(gauge).doesNotContainKey("startTime");
        Document distribution = (Document) ((List<?>) ((Document) facts.get(2)).get("points")).get(0);
        assertThat(distribution).doesNotContainKey("value");
        assertThat(distribution.get("count")).isEqualTo(7L);
        assertThat(distribution.get("sum")).isEqualTo(0.63);
        assertThat((List<?>) distribution.get("bounds")).hasSize(15);
        assertThat((List<?>) distribution.get("bucketCounts")).hasSize(16);
    }

    @Test
    void roundTripReconstructsEveryFactAsItWasMeasured() {
        Observation obs = new Observation("orders_sync", PipelineState.RUNNING, Map.of("recordCount", 98L),
                Map.of(), Map.of(), null, TAKEN, threeFacts());

        assertThat(MongoObservationStore.toObservation(MongoObservationStore.toDocument(obs))).isEqualTo(obs);
    }

    @Test
    void toObservationOnADocumentWithoutFactsReadsNone() {
        // Written before facts travelled: none carried, and none invented from the flat map beside it.
        Document document = new Document("_id", "orders_sync").append("state", "RUNNING")
                .append("metrics", new Document("recordCount", 98L));

        Observation obs = MongoObservationStore.toObservation(document);

        assertThat(obs.facts()).isEmpty();
        assertThat(obs.metrics()).containsEntry("recordCount", 98L);
    }

    @Test
    void toObservationOnFactsStoredAsSomethingOtherThanAnArrayIsDocumentUnreadable() {
        Document document = new Document("_id", "orders_sync").append("state", "RUNNING")
                .append("facts", "not-a-list");

        Throwable thrown = catchThrowable(() -> MongoObservationStore.toObservation(document));

        assertThat(thrown).isInstanceOf(TapstateException.class);
        assertThat(((TapstateException) thrown).code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);
        assertThat(((TapstateException) thrown).args()).containsEntry("field", "facts");
    }

    @Test
    void toObservationOnAFactThisVersionRefusesIsDocumentUnreadableNotABareCrash() {
        // A distribution stored over bounds that are not the registered ones: the fact type refuses to
        // build it, and that refusal reaches the reader as corruption of the facts field rather than as a
        // bare IllegalArgumentException from the middle of a document read.
        Document point = new Document("attributes", new Document("tapstate.pipeline.id", "orders_sync"))
                .append("observedAt", Date.from(TAKEN))
                .append("count", 1L).append("sum", 0.5)
                .append("bounds", List.of(1.0)).append("bucketCounts", List.of(1L, 0L));
        Document fact = new Document("name", "tapstate.pipeline.record.delivery.duration")
                .append("type", "HISTOGRAM").append("unit", "s").append("points", List.of(point));
        Document document = new Document("_id", "orders_sync").append("state", "RUNNING")
                .append("facts", List.of(fact));

        Throwable thrown = catchThrowable(() -> MongoObservationStore.toObservation(document));

        assertThat(thrown).isInstanceOf(TapstateException.class);
        assertThat(((TapstateException) thrown).code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);
        assertThat(((TapstateException) thrown).args()).containsEntry("field", "facts");
        assertThat(thrown.getCause()).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("registered bounds");
    }

    @Test
    void toObservationOnAPointStoredWithNeitherAValueNorADistributionIsDocumentUnreadable() {
        Document point = new Document("attributes", new Document()).append("observedAt", Date.from(TAKEN));
        Document fact = new Document("name", "recordCount").append("type", "GAUGE").append("unit", "{record}")
                .append("points", List.of(point));
        Document document = new Document("_id", "orders_sync").append("state", "RUNNING")
                .append("facts", List.of(fact));

        Throwable thrown = catchThrowable(() -> MongoObservationStore.toObservation(document));

        assertThat(thrown).isInstanceOf(TapstateException.class);
        assertThat(((TapstateException) thrown).args()).containsEntry("field", "facts");
    }
}
