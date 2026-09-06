package io.tapstate.adapters.pdk;

import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.TapDDLUnknownEvent;
import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The TapEvent ↔ tapstate envelope projection: the stable currency every downstream transform sees.
 * The snapshot / change phase is external truth — a batch read yields snapshot rows ({@code op=r}),
 * a stream read yields change events ({@code i}/{@code u}/{@code d}/{@code ddl}) — because a snapshot
 * row and a cdc insert are the same insert-shaped event on the wire, so decode is phase-aware.
 */
class TapEventCodecTest {

    // ---- decode: change stream (i/u/d) ----

    @Test
    void decodesInsertToOpInsertWithAfterRow() {
        TapInsertRecordEvent event = TapInsertRecordEvent.create()
                .table("orders").referenceTime(1000L).after(Map.of("id", 1, "region", "eu"));
        Envelope env = TapEventCodec.decodeChange(event);
        assertThat(env.op()).isEqualTo(Op.INSERT);
        assertThat(env.ts()).isEqualTo(1000L);
        assertThat(env.src()).isEqualTo("orders");
        assertThat(env.after()).isEqualTo(Map.of("id", 1L, "region", "eu"));
        assertThat(env.before()).isNull();
    }

    @Test
    void decodesUpdateToOpUpdateWithBeforeAndAfter() {
        TapUpdateRecordEvent event = TapUpdateRecordEvent.create()
                .table("orders").referenceTime(1000L)
                .before(Map.of("id", 1, "region", "eu")).after(Map.of("id", 1, "region", "us"));
        Envelope env = TapEventCodec.decodeChange(event);
        assertThat(env.op()).isEqualTo(Op.UPDATE);
        assertThat(env.before()).isEqualTo(Map.of("id", 1L, "region", "eu"));
        assertThat(env.after()).isEqualTo(Map.of("id", 1L, "region", "us"));
    }

    @Test
    void decodesDeleteToOpDeleteWithBeforeOnly() {
        TapDeleteRecordEvent event = TapDeleteRecordEvent.create()
                .table("orders").referenceTime(1000L).before(Map.of("id", 1));
        Envelope env = TapEventCodec.decodeChange(event);
        assertThat(env.op()).isEqualTo(Op.DELETE);
        assertThat(env.before()).isEqualTo(Map.of("id", 1L));
        assertThat(env.after()).isNull();
    }

    // ---- decode: ddl is never swallowed (a dropped ddl silently breaks the schema-evolution chain) ----

    @Test
    void decodesDdlToOpDdlAndNeverSwallowsIt() {
        TapNewFieldEvent event = new TapNewFieldEvent();
        event.setTableId("orders");
        event.setReferenceTime(1000L);
        event.setOriginDDL("ALTER TABLE orders ADD note VARCHAR(64)");
        Envelope env = TapEventCodec.decodeChange(event);
        assertThat(env).isNotNull();
        assertThat(env.op()).isEqualTo(Op.DDL);
        assertThat(env.src()).isEqualTo("orders");
        assertThat(env.schema()).isEqualTo(Map.of("origin", "ALTER TABLE orders ADD note VARCHAR(64)"));
        assertThat(env.before()).isNull();
        assertThat(env.after()).isNull();
    }

    @Test
    void decodesDdlWithNoOriginToAnEmptySchema() {
        TapNewFieldEvent event = new TapNewFieldEvent();
        event.setTableId("orders");
        event.setReferenceTime(1000L);
        Envelope env = TapEventCodec.decodeChange(event);
        assertThat(env.op()).isEqualTo(Op.DDL);
        assertThat(env.schema()).isEqualTo(Map.of());
    }

    // ---- decode: snapshot phase (r) ----

    @Test
    void decodesSnapshotRowToOpRead() {
        TapInsertRecordEvent row = TapInsertRecordEvent.create()
                .table("orders").referenceTime(1000L).after(Map.of("id", 7));
        Envelope env = TapEventCodec.decodeSnapshotRow(row);
        assertThat(env.op()).isEqualTo(Op.READ);
        assertThat(env.after()).isEqualTo(Map.of("id", 7L));
        assertThat(env.before()).isNull();
    }

    @Test
    void decodesZonedDateTimesToDatesAtEveryRowDepth() {
        ZonedDateTime topLevel = ZonedDateTime.of(2026, 8, 12, 21, 8, 43, 123_000_000, ZoneId.of("Asia/Shanghai"));
        ZonedDateTime nestedMap = topLevel.plusHours(1);
        ZonedDateTime nestedList = topLevel.plusHours(2);
        TapInsertRecordEvent event = TapInsertRecordEvent.create().table("orders").after(Map.of(
                "top_level", topLevel,
                "nested", Map.of("map_value", nestedMap),
                "items", List.of(nestedList)));

        Envelope env = TapEventCodec.decodeChange(event);

        assertThat(env.after()).isEqualTo(Map.of(
                "top_level", Date.from(topLevel.toInstant()),
                "nested", Map.of("map_value", Date.from(nestedMap.toInstant())),
                "items", List.of(Date.from(nestedList.toInstant()))));
    }

    // ---- ts: source reference time, falling back to event time ----

    @Test
    void tsPrefersReferenceTimeThenFallsBackToEventTime() {
        TapInsertRecordEvent withRef = TapInsertRecordEvent.create().table("t").after(Map.of("a", 1));
        withRef.setReferenceTime(1000L);
        withRef.setTime(500L);
        assertThat(TapEventCodec.decodeChange(withRef).ts()).isEqualTo(1000L);

        TapInsertRecordEvent noRef = TapInsertRecordEvent.create().table("t").after(Map.of("a", 1));
        noRef.setTime(500L);
        assertThat(TapEventCodec.decodeChange(noRef).ts()).isEqualTo(500L);
    }

    // ---- decode rejects a phase/type mismatch (bare crash, not a silent mis-projection) ----

    @Test
    void snapshotRowDecodeRejectsANonInsertEvent() {
        TapDeleteRecordEvent delete = TapDeleteRecordEvent.create().table("t").before(Map.of("id", 1));
        assertThatThrownBy(() -> TapEventCodec.decodeSnapshotRow(delete))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ---- round trip: an envelope survives encode → (phase-appropriate) decode unchanged ----
    //
    // The fixtures hold int64 because that is what an envelope in circulation holds: decode is where
    // a driver's own boxing becomes the value model, so a row that has been through it once is
    // already in it. An envelope hand-built with a narrower box is not a shape the runtime produces,
    // and asking the round trip to preserve it would be asking decode to undo its own job.

    @Test
    void insertRoundTrips() {
        Envelope env = Envelope.insert(1000L, "orders", Map.of("id", 1L, "region", "eu"), null);
        assertThat(TapEventCodec.decodeChange(TapEventCodec.encode(env))).isEqualTo(env);
    }

    @Test
    void updateRoundTrips() {
        Envelope env = Envelope.update(1000L, "orders", Map.of("id", 1L), Map.of("id", 1L, "n", 2L), null);
        assertThat(TapEventCodec.decodeChange(TapEventCodec.encode(env))).isEqualTo(env);
    }

    @Test
    void deleteRoundTrips() {
        Envelope env = Envelope.delete(1000L, "orders", Map.of("id", 1L), null);
        assertThat(TapEventCodec.decodeChange(TapEventCodec.encode(env))).isEqualTo(env);
    }

    @Test
    void readRoundTripsThroughTheSnapshotPhase() {
        Envelope env = Envelope.read(1000L, "orders", Map.of("id", 7L), null);
        assertThat(TapEventCodec.decodeSnapshotRow(TapEventCodec.encode(env))).isEqualTo(env);
    }

    @Test
    void ddlRoundTrips() {
        Envelope env = Envelope.ddl(1000L, "orders", Map.of("origin", "ALTER TABLE orders ADD note INT"));
        TapEvent encoded = TapEventCodec.encode(env);
        assertThat(encoded).isInstanceOf(TapDDLUnknownEvent.class);
        assertThat(TapEventCodec.decodeChange(encoded)).isEqualTo(env);
    }

    /**
     * That a producer saying a field is gone reaches the connector as the removal it is.
     *
     * <p><b>A field dropped from a row and a field the row never had are the same bytes on the wire, and a
     * sink cannot tell them apart from the row alone.</b> Every write into a keyed target applies the row by
     * setting the fields in it, so a field that stops being produced is left standing in the target for ever
     * - the write succeeds, the row that arrives is correct, and nothing anywhere reports a difference. The
     * removal has to travel beside the row or it does not travel at all.
     */
    @Test
    void afieldTheProducerDroppedTravelsToTheConnectorAsARemoval() {
        TapInsertRecordEvent insert = (TapInsertRecordEvent) TapEventCodec.encode(
                Envelope.insert(1L, "t", Map.of("id", 1), null).withRemoved(Set.of("customer")));
        assertThat(insert.getRemovedFields())
                .describedAs("the connector reads this to build the removal; left unset it writes none, and "
                        + "the target keeps the old value with every other assertion still passing")
                .containsExactly("customer");

        TapUpdateRecordEvent update = (TapUpdateRecordEvent) TapEventCodec.encode(
                Envelope.update(1L, "t", Map.of("id", 1), Map.of("id", 1), null)
                        .withRemoved(Set.of("customer")));
        assertThat(update.getRemovedFields()).containsExactly("customer");
    }

    @Test
    void anEventThatDroppedNothingCarriesNoRemoval() {
        // Distinct from an empty list on purpose: the connector tests it for emptiness either way, and a
        // producer that drops nothing should be indistinguishable from one built before this existed.
        TapInsertRecordEvent insert = (TapInsertRecordEvent)
                TapEventCodec.encode(Envelope.insert(1L, "t", Map.of("id", 1), null));
        assertThat(insert.getRemovedFields()).isNullOrEmpty();
    }

    @Test
    void encodedRowMapsAreMutableForThePdkWritePath() {
        // The PDK sink value-conversion mutates an event's row map in place (Entry.setValue during
        // type coercion), so encode must hand PDK a mutable map, not the envelope's unmodifiable view.
        TapInsertRecordEvent insert = (TapInsertRecordEvent)
                TapEventCodec.encode(Envelope.insert(1L, "t", Map.of("id", 1), null));
        assertThatCode(() -> insert.getAfter().put("id", 2)).doesNotThrowAnyException();

        TapUpdateRecordEvent update = (TapUpdateRecordEvent)
                TapEventCodec.encode(Envelope.update(1L, "t", Map.of("id", 1), Map.of("id", 2), null));
        assertThatCode(() -> {
            update.getBefore().put("id", 9);
            update.getAfter().put("id", 9);
        }).doesNotThrowAnyException();

        TapDeleteRecordEvent delete = (TapDeleteRecordEvent)
                TapEventCodec.encode(Envelope.delete(1L, "t", Map.of("id", 1), null));
        assertThatCode(() -> delete.getBefore().put("id", 9)).doesNotThrowAnyException();
    }

    @Test
    void encodeProducesTheExpectedConcreteEventTypes() {
        assertThat(TapEventCodec.encode(Envelope.insert(1L, "t", Map.of("a", 1), null)))
                .isInstanceOf(TapInsertRecordEvent.class);
        assertThat(TapEventCodec.encode(Envelope.read(1L, "t", Map.of("a", 1), null)))
                .isInstanceOf(TapInsertRecordEvent.class);
        assertThat(TapEventCodec.encode(Envelope.update(1L, "t", Map.of("a", 1), Map.of("a", 2), null)))
                .isInstanceOf(TapUpdateRecordEvent.class);
        assertThat(TapEventCodec.encode(Envelope.delete(1L, "t", Map.of("a", 1), null)))
                .isInstanceOf(TapDeleteRecordEvent.class);
    }
}
