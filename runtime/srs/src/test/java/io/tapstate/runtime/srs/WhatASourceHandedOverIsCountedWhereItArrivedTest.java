package io.tapstate.runtime.srs;

import io.tapstate.core.event.Envelope;
import io.tapstate.spi.capture.CaptureListener;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What a run has taken from its source, counted at the one point every change comes through.
 *
 * <p>The boundary being pinned here is the handover: rows are counted as the source gives them up, not once
 * the run has managed to do something with them. The two come apart exactly when something goes wrong,
 * which is when the number is being read, so a case here drives a handler that throws and one that is
 * handed the same rows twice - the two shapes under which a count taken further downstream would quietly
 * report less than arrived.
 *
 * <p>What those rows weighed is pinned in the same cases and against the same boundary, because it is one
 * account taken in one place: every case that says a row arrived says what it weighed, so the two cannot
 * be wired to different sets of arrivals. The figures are the product's own definition of a row's payload
 * -- a field's name plus its value, a whole number at one declared width -- worked out here by hand so
 * that a change to that definition shows up as a number in a test rather than as a shift in a chart.
 */
class WhatASourceHandedOverIsCountedWhereItArrivedTest {

    private static final Map<String, Object> NO_SCHEMA = Map.of();

    @Test
    @DisplayName("a batch is counted by the table it came from and the operation the source performed")
    void everyChangeInABatchIsCountedByTableAndOperation() {
        CaptureHealth health = new CaptureHealth();
        CaptureListener listener = health.recording((events, position) -> { });

        listener.onBatch(List.of(
                Envelope.insert(1L, "orders", Map.of("id", 1), NO_SCHEMA),
                Envelope.insert(2L, "orders", Map.of("id", 2), NO_SCHEMA),
                Envelope.update(3L, "orders", Map.of("id", 1), Map.of("id", 1), NO_SCHEMA),
                Envelope.delete(4L, "items", Map.of("id", 9), NO_SCHEMA),
                Envelope.ddl(5L, "items", Map.of("cols", List.of()))), Optional.empty());

        assertThat(health.receivedRows()).containsOnly(
                Map.entry("orders", Map.of("i", 2L, "u", 1L)),
                Map.entry("items", Map.of("d", 1L, "ddl", 1L)));
        // orders: two inserts of {id: <number>} at ten bytes each -- two for the name, eight for the
        // number -- and an update carrying both halves of one such row, at twenty. items: a delete
        // carrying one, at ten, and a schema change carrying no row at all, at nothing.
        assertThat(health.receivedBytes()).containsOnly(
                Map.entry("orders", 40L), Map.entry("items", 10L));
    }

    @Test
    @DisplayName("a schema change arrives and is counted, and weighs nothing, because it carries no row")
    void aSchemaChangeIsCountedAtNoWeightRatherThanNotCountedAtAll() {
        CaptureHealth health = new CaptureHealth();
        CaptureListener listener = health.recording((events, position) -> { });

        listener.onBatch(List.of(
                Envelope.ddl(1L, "orders", Map.of("cols", List.of("id", "region", "total")))),
                Optional.empty());

        // Nought here is measured and not absent, and the two differ in what they say about the table:
        // something arrived for it, and what arrived was not data. A pipeline doing nothing but schema
        // work therefore shows its rows rising while its payload stays flat, which is the truth about it.
        assertThat(health.receivedRows()).containsEntry("orders", Map.of("ddl", 1L));
        assertThat(health.receivedBytes()).containsEntry("orders", 0L);
    }

    @Test
    @DisplayName("rows the run could not go on to store have still arrived, and are still counted")
    void aBatchThatTheHandlerRejectsIsStillCounted() {
        CaptureHealth health = new CaptureHealth();
        CaptureListener listener = health.recording((events, position) -> {
            throw new IllegalStateException("the ring refused the batch");
        });

        assertThatThrownBy(() -> listener.onBatch(
                List.of(Envelope.insert(1L, "orders", Map.of("id", 1), NO_SCHEMA)), Optional.empty()))
                .isInstanceOf(IllegalStateException.class);

        // The boundary is the handover, and the source handed these over. Counted after the handler
        // instead, a run whose ring is refusing writes would report a source that had gone quiet - which
        // is the reading somebody would be looking at this number to rule out.
        assertThat(health.receivedRows()).containsEntry("orders", Map.of("i", 1L));
        assertThat(health.receivedBytes()).containsEntry("orders", 10L);
    }

    @Test
    @DisplayName("the same row handed over twice crosses the boundary twice")
    void aRedeliveredRowIsCountedAgain() {
        CaptureHealth health = new CaptureHealth();
        CaptureListener listener = health.recording((events, position) -> { });
        Envelope same = Envelope.insert(1L, "orders", Map.of("id", 1), NO_SCHEMA);

        listener.onBatch(List.of(same), Optional.empty());
        listener.onBatch(List.of(same), Optional.empty());

        // A count of arrivals, not of distinct rows. De-duplicating here would make this number disagree
        // with every other account of the same traffic, and would hide a source replaying its log.
        assertThat(health.receivedRows()).containsEntry("orders", Map.of("i", 2L));
        assertThat(health.receivedBytes()).containsEntry("orders", 20L);
    }

    @Test
    @DisplayName("a table nothing arrived for is absent, not present at zero")
    void aSilentTableIsNotReportedAtZero() {
        CaptureHealth health = new CaptureHealth();
        CaptureListener listener = health.recording((events, position) -> { });

        listener.onBatch(List.of(Envelope.insert(1L, "orders", Map.of("id", 1), NO_SCHEMA)),
                Optional.empty());

        // A stream nobody is reading and a stream with nothing to say want opposite responses, and a zero
        // published for both spells them the same way.
        assertThat(health.receivedRows()).containsOnlyKeys("orders");
        assertThat(health.receivedBytes()).containsOnlyKeys("orders");
    }

    @Test
    @DisplayName("an account says since when, before anything has arrived on it")
    void theStartExistsBeforeTheFirstRow() {
        Instant before = Instant.now();

        CaptureHealth health = new CaptureHealth();

        // Taken when the account is opened rather than at the first row: without it, "nothing has arrived"
        // and "nothing is being measured" are the same reading, and a totals stream with no start hands a
        // consumer one in which a restart and a decrease cannot be told apart.
        assertThat(health.receivedRows()).isEmpty();
        assertThat(health.receivedBytes()).isEmpty();
        assertThat(health.countingSince()).isBetween(before, Instant.now());
    }

    @Test
    @DisplayName("a tally already read does not change under the reader when more arrives")
    void aReadingIsASnapshotAndNotALiveView() {
        CaptureHealth health = new CaptureHealth();
        CaptureListener listener = health.recording((events, position) -> { });
        listener.onBatch(List.of(Envelope.insert(1L, "orders", Map.of("id", 1), NO_SCHEMA)),
                Optional.empty());

        Map<String, Map<String, Long>> taken = health.receivedRows();
        listener.onBatch(List.of(Envelope.insert(2L, "orders", Map.of("id", 2), NO_SCHEMA)),
                Optional.empty());

        // The account is written on the connector's thread and read on another. A reading that went on
        // moving would put a projection's own numbers out of step with the moment it stamped them.
        assertThat(taken).containsEntry("orders", Map.of("i", 1L));
        assertThat(health.receivedRows()).containsEntry("orders", Map.of("i", 2L));
    }
}
