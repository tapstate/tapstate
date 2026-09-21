package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import io.tapstate.adapters.transform.MapSpec;
import io.tapstate.adapters.transform.StatelessTransforms;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.common.TapstateType;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.model.FieldRule;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.sql.JoinPlan;
import io.tapstate.core.sql.SourceColumn;
import io.tapstate.core.sql.SourceTable;
import io.tapstate.core.sql.SqlFrontEnd;
import io.tapstate.runtime.engine.join.JoinDriver;
import io.tapstate.runtime.engine.join.MapJoinStores;
import io.tapstate.runtime.engine.join.SourceChange;
import io.tapstate.spi.transform.TransformPort;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class AComputedFieldReadingAJoinDimensionColumnTest {

    @Test
    @DisplayName("a computed field cannot carry CEL's unknown value out of an unmatched dimension")
    void aComputedFieldCannotCarryAnInternalCelValueOutOfAJoin() {
        JoinPlan plan = SqlFrontEnd.derive(
                "SELECT s.id AS shipment_id, s.order_id AS order_id, s.carrier AS carrier, "
                        + "s.status AS status, o.customer AS customer "
                        + "FROM shipments s LEFT JOIN orders o ON s.order_id = o.id",
                List.of(
                        new SourceTable("shipments", List.of(
                                new SourceColumn("id", TapstateType.INT64, false),
                                new SourceColumn("order_id", TapstateType.INT64, false),
                                new SourceColumn("carrier", TapstateType.STRING, false),
                                new SourceColumn("status", TapstateType.STRING, false))),
                        new SourceTable("orders", List.of(
                                new SourceColumn("id", TapstateType.INT64, false),
                                new SourceColumn("customer", TapstateType.STRING, false)))));
        JoinDriver join = new JoinDriver(plan, List.of("id"), "j", new MapJoinStores());
        List<Envelope> joined = new ArrayList<>();

        assertThat(join.apply(List.of(new SourceChange("s", Envelope.read(1L, "shipments",
                Map.of("id", 6L, "order_id", 4L, "carrier", "fedex",
                        "status", "in_transit"), null))), joined::add)).isTrue();
        Envelope beforeDimension = joined.getFirst();
        assertThat(beforeDimension.after()).containsEntry("customer", null);

        assertThat(join.apply(List.of(new SourceChange("o", Envelope.read(2L, "orders",
                Map.of("id", 4L, "customer", "dave"), null))), joined::add)).isTrue();
        assertThat(joined.getLast().after()).containsEntry("customer", "dave");

        TransformPort map = StatelessTransforms.map(MapSpec.from(new TransformBody.MapProjection(
                Map.of("probe", FieldRule.computed("after.customer + '!'")))));

        try {
            Object leaked = map.transform(beforeDimension).getFirst().after().get("probe");
            fail("expected transform.expression-failed, but the computed row carried %s as %s",
                    leaked, leaked == null ? "null" : leaked.getClass().getName());
        } catch (TapstateException failure) {
            assertThat(failure.code().code()).isEqualTo("transform.expression-failed");
            assertThat(failure.args()).containsEntry("expr", "after.customer + '!'");
        }
    }
}
