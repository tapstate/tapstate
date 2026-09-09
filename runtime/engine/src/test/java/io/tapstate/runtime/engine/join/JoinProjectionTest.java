package io.tapstate.runtime.engine.join;

import io.tapstate.core.common.TapstateType;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.core.sql.Expr;
import io.tapstate.core.sql.JoinKey;
import io.tapstate.core.sql.JoinKind;
import io.tapstate.core.sql.JoinPlan;
import io.tapstate.core.sql.JoinTree;
import io.tapstate.core.sql.OutputField;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class JoinProjectionTest {
    private final MapJoinStores stores = new MapJoinStores();
    private final JoinPlan plan = plan();
    private final JoinProjection projection = new JoinProjection(plan, List.of("id"), "joined", stores);

    @Test
    void aLateOutboxImageCannotOverwriteTheMatchThatAlreadyArrived() {
        JoinDriver facts = new JoinDriver(plan, List.of("id"), "joined", stores);
        JoinDriver dimensions = new JoinDriver(plan, List.of("id"), "joined", stores);
        List<Envelope> delayed = new ArrayList<>();
        facts.apply(List.of(new SourceChange("o", Envelope.insert(1, "orders",
                Map.of("id", 10L, "customer_id", 1L), null))), event -> delayed.add(event));
        List<Envelope> newer = new ArrayList<>();
        dimensions.apply(List.of(new SourceChange("c", Envelope.insert(1, "customers",
                Map.of("id", 1L, "name", "Ada"), null))), event -> newer.add(event));

        // Both drivers have already emitted into separate outboxes. Deliver the newer match first,
        // then the older null image, exactly the cross-partition order that used to lose the match.
        assertThat(delayed.getFirst().after()).containsEntry("customer_name", null);
        assertThat(projection.refresh(newer).getLast().after()).containsEntry("customer_name", "Ada");
        assertThat(projection.refresh(delayed).getLast().after()).containsEntry("customer_name", "Ada");
    }

    @Test
    void aDelayedInsertCannotResurrectADeletedFact() {
        Envelope stale = Envelope.insert(1, "joined", Map.of("order_id", 10L), null);
        assertThat(projection.refresh(List.of(stale))).singleElement()
                .satisfies(event -> {
                    assertThat(event.op()).isEqualTo(Op.DELETE);
                    assertThat(event.before()).containsEntry("order_id", 10L);
                });
    }

    @Test
    void aDelayedDeleteCannotEraseAReinsertedFact() {
        stores.putFact(JoinKey.of(List.of(10L)).name(), Map.of("id", 10L, "customer_id", 1L));
        stores.putDimensionRow("c", JoinKey.of(List.of(1L)).name(), Map.of("id", 1L, "name", "New"));
        Envelope stale = Envelope.delete(1, "joined", Map.of("order_id", 10L), null);
        assertThat(projection.refresh(List.of(stale))).singleElement()
                .satisfies(event -> {
                    assertThat(event.op()).isEqualTo(Op.INSERT);
                    assertThat(event.after()).containsEntry("customer_name", "New");
                });
    }

    static JoinPlan plan() {
        JoinTree from = new JoinTree.Join(new JoinTree.Source("o", "orders"),
                new JoinTree.Source("c", "customers"), JoinKind.LEFT,
                List.of(new JoinTree.KeyPair(new JoinTree.ColumnRef("o", "customer_id"),
                        new JoinTree.ColumnRef("c", "id"))), false);
        return new JoinPlan(List.of(
                new OutputField("order_id", TapstateType.INT64, false,
                        new Expr.Column(new JoinTree.ColumnRef("o", "id"))),
                new OutputField("customer_name", TapstateType.STRING, true,
                        new Expr.Column(new JoinTree.ColumnRef("c", "name")))),
                from, Map.of("o", List.of("id", "customer_id"), "c", List.of("id", "name")));
    }

    @Test
    void losingAnInnerDimensionRemovesThePreviouslyPublishedRow() {
        JoinTree.Join left = (JoinTree.Join) plan.from();
        JoinPlan inner = new JoinPlan(plan.outputFields(),
                new JoinTree.Join(left.left(), left.right(), JoinKind.INNER, left.on(), false),
                plan.readColumns());
        stores.putFact(JoinKey.of(List.of(10L)).name(), Map.of("id", 10L, "customer_id", 1L));
        Envelope stale = Envelope.insert(1, "joined", Map.of("order_id", 10L, "customer_name", "Old"), null);
        assertThat(new JoinProjection(inner, List.of("id"), "joined", stores).refresh(List.of(stale)))
                .singleElement().satisfies(event -> {
                    assertThat(event.op()).isEqualTo(Op.DELETE);
                    assertThat(event.before()).containsEntry("order_id", 10L);
                });
    }

    @Test
    void aliasedCompositeKeysKeepFactsWithTheSameIdSeparate() {
        List<OutputField> fields = new ArrayList<>(plan.outputFields());
        fields.add(new OutputField("account", TapstateType.STRING, false,
                new Expr.Column(new JoinTree.ColumnRef("o", "tenant"))));
        JoinPlan composite = new JoinPlan(fields, plan.from(), plan.readColumns());
        for (String tenant : List.of("a", "b")) {
            stores.putFact(JoinKey.of(List.of(tenant, 10L)).name(),
                    Map.of("tenant", tenant, "id", 10L, "customer_id", tenant.equals("a") ? 1L : 2L));
        }
        stores.putDimensionRow("c", JoinKey.of(List.of(1L)).name(), Map.of("id", 1L, "name", "Ada"));
        stores.putDimensionRow("c", JoinKey.of(List.of(2L)).name(), Map.of("id", 2L, "name", "Bo"));
        List<Envelope> refreshed = new JoinProjection(composite, List.of("tenant", "id"), "joined", stores)
                .refresh(List.of(
                        Envelope.insert(1, "joined", Map.of("account", "a", "order_id", 10L), null),
                        Envelope.insert(1, "joined", Map.of("account", "b", "order_id", 10L), null)));
        assertThat(refreshed).extracting(event -> event.after().get("customer_name")).containsExactly("Ada", "Bo");
    }
}
