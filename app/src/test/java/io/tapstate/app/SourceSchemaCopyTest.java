package io.tapstate.app;

import io.tapstate.core.common.TapstateType;
import io.tapstate.spi.store.DerivedSchema;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceTable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Copying a source table's discovered model into the pipeline's own record of it.
 *
 * <p>A pipeline works from a copy of what discovery found rather than reading the discovery itself, so
 * that a re-discovery cannot change the shape under a run already using it. This is where that copy is
 * taken, and the two things it has to get right are the id the copy is filed under and what it does
 * when there is nothing to copy.
 */
class SourceSchemaCopyTest {

    private static SourceTable orders(TapstateType totalType) {
        return new SourceTable("orders", List.of(
                new SourceField("o_id", "bigint", TapstateType.INT64),
                new SourceField("o_total", "decimal(18,4)", totalType)),
                List.of("o_id"), List.of());
    }

    private final InMemoryDerivedSchemaStore records = new InMemoryDerivedSchemaStore();
    private final SourceSchemaCopy copy = new SourceSchemaCopy(records);

    @Test
    @DisplayName("files the copy under sourceId.table even when the source selects a single table")
    void filesUnderSourceAndTable() {
        copy.copy("flow", "src", "orders", orders(TapstateType.DECIMAL));

        // The whole point of the qualified id: the DAG keys a single-table source by the source id
        // alone and re-keys it the moment a second table is selected. A record filed under that key
        // would go missing on the day the selection widens - and a missing record reads as "never
        // recorded", which is a pass, not an alarm.
        assertThat(records.latest("flow", "src.orders")).isPresent();
        assertThat(records.latest("flow", "src")).isEmpty();
    }

    @Test
    @DisplayName("copies every discovered column, at the tapstate type discovery resolved")
    void copiesTheColumns() {
        copy.copy("flow", "src", "orders", orders(TapstateType.DECIMAL));

        assertThat(records.latest("flow", "src.orders"))
                .get()
                .extracting(DerivedSchema::schema)
                .isEqualTo(Map.of("o_id", "INT64 NULL", "o_total", "DECIMAL NULL"));
    }

    @Test
    @DisplayName("a table nothing discovered records nothing, rather than an empty placeholder")
    void undiscoveredRecordsNothing() {
        copy.copy("flow", "src", "orders", null);

        // An empty-but-present record would be read as the baseline by the next comparison, so the
        // first real discovery would report every column as newly appeared - an alarm manufactured
        // here, indistinguishable from a source that actually moved.
        assertThat(records.latest("flow", "src.orders")).isEmpty();
    }

    @Test
    @DisplayName("copying the same shape again does not spend a version")
    void unchangedShapeDoesNotAppend() {
        copy.copy("flow", "src", "orders", orders(TapstateType.DECIMAL));
        copy.copy("flow", "src", "orders", orders(TapstateType.DECIMAL));

        assertThat(records.latest("flow", "src.orders")).get().extracting(DerivedSchema::version)
                .isEqualTo(0L);
    }

    @Test
    @DisplayName("a column whose type moved appends a version, so the baseline moves with it")
    void changedColumnAppends() {
        copy.copy("flow", "src", "orders", orders(TapstateType.DECIMAL));
        copy.copy("flow", "src", "orders", orders(TapstateType.DOUBLE));

        Optional<DerivedSchema> latest = records.latest("flow", "src.orders");
        assertThat(latest).get().extracting(DerivedSchema::version).isEqualTo(1L);
        assertThat(latest).get().extracting(DerivedSchema::schema)
                .isEqualTo(Map.of("o_id", "INT64 NULL", "o_total", "DOUBLE NULL"));
    }

    @Test
    @DisplayName("a column widened in the source's own spelling moves the provenance, not the shape")
    void widenedColumnMovesProvenanceOnly() {
        SourceTable narrow = new SourceTable("orders", List.of(
                new SourceField("o_total", "decimal(18,4)", TapstateType.DECIMAL)), List.of(), List.of());
        SourceTable wide = new SourceTable("orders", List.of(
                new SourceField("o_total", "decimal(20,4)", TapstateType.DECIMAL)), List.of(), List.of());

        copy.copy("flow", "src", "orders", narrow);
        String was = records.latest("flow", "src.orders").orElseThrow().derivedFrom();
        copy.copy("flow", "src", "orders", wide);
        DerivedSchema now = records.latest("flow", "src.orders").orElseThrow();

        // The recorded shape does not move: both columns are DECIMAL in the shared vocabulary, so no
        // version is spent. The world did move though, and provenance that missed it would leave the
        // next real difference attributed to the derivation rather than to the source.
        assertThat(now.version()).isEqualTo(0L);
        assertThat(now.schema()).isEqualTo(Map.of("o_total", "DECIMAL NULL"));
        assertThat(now.derivedFrom()).isNotEqualTo(was);
    }

    @Test
    @DisplayName("what the author wrote and what the world said are fingerprinted apart")
    void provenanceKeepsAuthorAndWorldApart() {
        copy.copy("flow", "src", "orders", orders(TapstateType.DECIMAL));
        String statement = records.latest("flow", "src.orders").orElseThrow().statement();
        String derivedFrom = records.latest("flow", "src.orders").orElseThrow().derivedFrom();

        copy.copy("flow", "src", "orders", orders(TapstateType.DOUBLE));

        // The reference the author wrote is the same one; the columns the world reported are not.
        // Folded into one fingerprint the two would be indistinguishable, and a source that moved
        // would read as an edit nobody made.
        assertThat(records.latest("flow", "src.orders").orElseThrow().statement()).isEqualTo(statement);
        assertThat(records.latest("flow", "src.orders").orElseThrow().derivedFrom())
                .isNotEqualTo(derivedFrom);
    }
}
