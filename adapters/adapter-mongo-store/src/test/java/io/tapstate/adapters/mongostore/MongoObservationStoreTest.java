package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.ObservationFailure;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.TableSnapshot;
import io.tapstate.spi.store.IoError;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Date;
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
}
