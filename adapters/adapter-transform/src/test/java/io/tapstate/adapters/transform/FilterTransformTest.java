package io.tapstate.adapters.transform;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.transform.TransformPort;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Row membership changes reach downstream sinks while explicit operation predicates still apply.
 * Non-row {@code ddl} events bypass the predicate to preserve schema evolution.
 */
class FilterTransformTest {

    @Test
    @DisplayName("keeps a row whose predicate is true")
    void keepsRowWhenPredicateTrue() {
        TransformPort filter = StatelessTransforms.filter("after.deleted == 0");
        Envelope row = Envelope.insert(1L, "orders", Map.of("id", 1, "deleted", 0), null);

        assertThat(filter.transform(row)).containsExactly(row);
    }

    @Test
    @DisplayName("drops a row whose predicate is false")
    void dropsRowWhenPredicateFalse() {
        TransformPort filter = StatelessTransforms.filter("after.deleted == 0");
        Envelope row = Envelope.insert(1L, "orders", Map.of("id", 1, "deleted", 1), null);

        assertThat(filter.transform(row)).isEmpty();
    }

    @Test
    @DisplayName("removes a previously matching shipment from the view when its status changes")
    void emitsADeleteWhenAShipmentStopsMatchingTheViewFilter() {
        TransformPort filter = StatelessTransforms.filter("after.status == 'in_transit'");
        Map<String, Object> inTransit = Map.of("id", 2, "status", "in_transit");
        Envelope shipment2 = Envelope.read(1L, "shipments", inTransit, null);
        Envelope shipment6 = Envelope.read(2L, "shipments", Map.of("id", 6, "status", "in_transit"), null);
        Envelope newShipment = Envelope.insert(3L, "shipments", Map.of("id", 7, "status", "in_transit"), null);

        assertThat(filter.transform(shipment2)).containsExactly(shipment2);
        assertThat(filter.transform(shipment6)).containsExactly(shipment6);
        assertThat(filter.transform(newShipment)).containsExactly(newShipment);

        Envelope delivered = Envelope.update(4L, "shipments", inTransit,
                Map.of("id", 2, "status", "delivered"), null);

        assertThat(filter.transform(delivered))
                .as("the view needs a delete for shipment 2 instead of retaining its old in_transit row")
                .singleElement().satisfies(removal -> {
                    assertThat(removal.op()).isEqualTo(Op.DELETE);
                    assertThat(removal.before()).containsEntry("id", 2);
                    assertThat(removal.after()).isNull();
                });
    }

    @ParameterizedTest
    @CsvSource({"in_transit,in_transit,true", "delivered,in_transit,true", "delivered,delivered,false"})
    void keepsMatchingUpdatesAndDropsUpdatesThatNeverMatched(String before, String after, boolean kept) {
        TransformPort filter = StatelessTransforms.filter("after.status == 'in_transit'");
        Envelope update = Envelope.update(1L, "shipments", Map.of("id", 2, "status", before),
                Map.of("id", 2, "status", after), null);

        assertThat(filter.transform(update)).containsExactly(kept ? new Envelope[] {update} : new Envelope[0]);
    }

    @Test
    void aRetractionKeepsTheOldKeyAndEventMetadataAndUsesTheOriginalOperationForEvaluation() {
        TransformPort filter = StatelessTransforms.filter("op == 'u' && after.status == 'in_transit'");
        Envelope update = Envelope.update(42L, "shipments", Map.of("id", 2, "status", "in_transit"),
                        Map.of("id", 20, "status", "delivered"), Map.of("id", "int"))
                .withPositions(Map.of("shipments", new ChainPosition(new SourceOrder(1, 5), "offset-5")))
                .withRemoved(Set.of("obsolete"));

        assertThat(filter.transform(update)).containsExactly(new Envelope(
                Op.DELETE, update.ts(), update.src(), update.before(), null,
                update.schema(), update.positions(), update.removed()));
    }

    @Test
    void sourceDeletesKeepTheirAbsentAfterImageDuringPredicateEvaluation() {
        TransformPort filter = StatelessTransforms.filter("op != 'd' || !has(after.id)");
        Envelope insert = Envelope.insert(1L, "shipments", Map.of("id", 2, "status", "in_transit"), null);
        Envelope delete = Envelope.delete(2L, "shipments", insert.after(), null);

        assertThat(filter.transform(insert)).containsExactly(insert);
        assertThat(filter.transform(delete)).containsExactly(delete);
    }

    @Test
    void operationPredicatesStillExcludeUpdatesAndDeletesWhenTheyReadRowFields() {
        TransformPort filter = StatelessTransforms.filter("op == 'i' && after.status == 'in_transit'");
        Map<String, Object> matching = Map.of("id", 2, "status", "in_transit");
        Envelope update = Envelope.update(1L, "shipments", matching, Map.of("id", 2, "status", "delivered"), null);

        assertThat(filter.transform(update)).isEmpty();
        assertThat(filter.transform(Envelope.delete(2L, "shipments", matching, null))).isEmpty();
    }

    @Test
    void aMissingEarlierPredicateFieldStillSurfacesTheExistingCodedEvaluationFailure() {
        TransformPort filter = StatelessTransforms.filter("after.status == 'in_transit'");
        Envelope update = Envelope.update(1L, "shipments", Map.of("id", 2),
                Map.of("id", 2, "status", "delivered"), null);

        assertThatThrownBy(() -> filter.transform(update))
                .isInstanceOf(TapstateException.class)
                .satisfies(failure -> assertThat(((TapstateException) failure).code().code())
                        .isEqualTo("transform.expression-failed"));
    }

    @Test
    @DisplayName("binds op as its wire symbol so a predicate can select by change kind")
    void bindsOpAsWireSymbol() {
        TransformPort insertsOnly = StatelessTransforms.filter("op == 'i'");
        Envelope insert = Envelope.insert(1L, "t", Map.of("id", 1), null);
        Envelope update = Envelope.update(1L, "t", Map.of("id", 1), Map.of("id", 2), null);

        assertThat(insertsOnly.transform(insert)).containsExactly(insert);
        assertThat(insertsOnly.transform(update)).isEmpty();
    }

    @Test
    @DisplayName("evaluates a delete like any other row event (before is bound)")
    void evaluatesDeleteAsRow() {
        TransformPort dropDeletes = StatelessTransforms.filter("op != 'd'");
        Envelope delete = Envelope.delete(1L, "t", Map.of("id", 1), null);
        Envelope insert = Envelope.insert(1L, "t", Map.of("id", 1), null);

        assertThat(dropDeletes.transform(delete)).isEmpty();
        assertThat(dropDeletes.transform(insert)).containsExactly(insert);
    }

    @Test
    @DisplayName("bypasses a ddl event without evaluating the predicate")
    void bypassesDdlEvent() {
        TransformPort filter = StatelessTransforms.filter("after.deleted == 0");
        Envelope ddl = Envelope.ddl(1L, "orders", Map.of("kind", "add-column"));

        assertThat(filter.transform(ddl)).containsExactly(ddl);
    }

    @Test
    @DisplayName("a row expression that fails to evaluate surfaces a coded transform.expression-failed")
    void codesRowExpressionEvaluationFailure() {
        // now() type-checks (it is declared in the compile environment) but is unbound in the runtime,
        // so the predicate compiles and then fails at eval — the operator must see a coded diagnostic
        // naming the expression, not a bare crash.
        TransformPort filter = StatelessTransforms.filter("now() == now()");
        Envelope row = Envelope.insert(1L, "orders", Map.of("id", 1), null);

        assertThatThrownBy(() -> filter.transform(row))
                .isInstanceOf(TapstateException.class)
                .satisfies(thrown -> {
                    TapstateException e = (TapstateException) thrown;
                    assertThat(e.code().code()).isEqualTo("transform.expression-failed");
                    assertThat(e.args().get("expr")).isEqualTo("now() == now()");
                    assertThat(e.args()).containsKey("detail");
                });
    }
}
