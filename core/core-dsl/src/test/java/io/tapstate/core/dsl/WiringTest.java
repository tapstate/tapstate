package io.tapstate.core.dsl;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Step;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class WiringTest {

    @Test
    void aViewKeepsSharedBranchesSeparateAndTheirStepsInRowOrder() {
        Wiring wiring = wiring("""
                transforms:
                  - { id: base, from: [src_orders.orders], type: filter, expr: "op == 'i'" }
                  - { id: numbers, from: [base], type: map, fields: { amount: 1 } }
                  - { id: unchanged, from: [base], type: filter, expr: "op == 'i'" }
                  - { id: merge, from: [numbers, unchanged], type: union }
                view: { id: orders_view, from: merge, primary_key: id }
                """);
        FromRef view = FromRef.literal("orders_view");

        List<Wiring.Route> routes = wiring.routesReaching(view);

        assertThat(routes).hasSize(2).extracting(Wiring.Route::upstream)
                .containsExactly(new Upstream("src_orders", "orders"), new Upstream("src_orders", "orders"));
        assertThat(routes.get(0).steps()).extracting(Step::id).containsExactly("base", "numbers", "merge");
        assertThat(routes.get(1).steps()).extracting(Step::id).containsExactly("base", "unchanged", "merge");
        assertThat(wiring.reaching(view)).containsExactly(new Upstream("src_orders", "orders"));
        assertThat(wiring.nodesReaching(FromClause.list(view)))
                .containsExactly("orders_view", "merge", "numbers", "base", "unchanged");
    }

    @Test
    void aCycleKeepsEverySourceInTheDiscoveryObligation() {
        Wiring wiring = wiring("""
                transforms:
                  - { id: first, from: [second], type: filter, expr: "op == 'i'" }
                  - { id: second, from: [first], type: union }
                """);
        FromClause cycle = FromClause.list(FromRef.literal("first"));

        assertThat(wiring.routesReaching(cycle)).containsExactly(
                new Wiring.Route(new Upstream("src_orders", null), List.of()),
                new Wiring.Route(new Upstream("src_archive", null), List.of()));
        assertThat(wiring.reaching(cycle)).containsExactly(
                new Upstream("src_orders", null), new Upstream("src_archive", null));
        assertThat(wiring.nodesReaching(cycle)).containsExactly("first", "second");
    }

    @Test
    void anUnattributableQualifiedReferenceKeepsEverySourceInPlay() {
        Wiring wiring = wiring("");
        FromRef unknown = FromRef.literal("other.orders");

        assertThat(wiring.routesReaching(unknown)).containsExactly(
                new Wiring.Route(new Upstream("src_orders", null), List.of()),
                new Wiring.Route(new Upstream("src_archive", null), List.of()));
        assertThat(wiring.reaching(unknown)).containsExactly(
                new Upstream("src_orders", null), new Upstream("src_archive", null));
    }

    private static Wiring wiring(String body) {
        PipelineResource pipeline = (PipelineResource) new DslParser().parse("""
                version: tapstate/v1
                kind: pipeline
                id: orders_out
                source: [src_orders, src_archive]
                """ + body);
        return new Wiring(pipeline, Map.of());
    }
}
