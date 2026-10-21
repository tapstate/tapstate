package io.tapstate.adapters.pdk;

import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.TransformBody;
import io.tapstate.runtime.engine.validation.ValidationNestRelations;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

/** Parsed declarations feed the independent relation plan, without invoking a production topology. */
class ValidationNestRelationsDslTest {
    @Test void parsedAliasesKeysAndFlatPathsReachTheIndependentPlan() {
        var pipeline = (PipelineResource) new DslParser().parse("""
                version: tapstate/v1
                kind: pipeline
                id: validation-relations
                source: source
                transforms:
                  - id: document
                    from: { orders: orders, items: items, customers: customers, profiles: profiles }
                    type: nest
                    root:
                      from: orders
                      key: [id]
                      embed:
                        - from: items
                          on: { order_id: id }
                          as: array
                          path: items
                          key: [line_id]
                          arrayKey: [sku]
                          embed:
                            - from: customers
                              on: { id: customer_id }
                              as: object
                              path: customer
                              key: [id]
                        - from: profiles
                          on: { order_id: id }
                          as: flat
                          key: [profile_id]
                serve:
                  from: document
                  sync: [ { id: target, source: target_source } ]
                """);
        var step = (Step.Inline) pipeline.transforms().getFirst();
        var body = (TransformBody.Nest) step.body();
        var aliases = ((FromClause.Aliases) step.from()).aliases().keySet();
        var plan = ValidationNestRelations.compile(step.id(), body.root(), aliases, Map.of(), new ValidationNestRelations.Limits(100, 16, 1024 * 1024));
        assertThat(plan.rootAlias()).isEqualTo("orders");
        assertThat(plan.rootKey()).contains(List.of("id"));
        var items = plan.children().getFirst();
        assertThat(items.direction()).isEqualTo(ValidationNestRelations.Direction.GATHER);
        assertThat(items.rowKey()).contains(List.of("line_id"));
        assertThat(items.arrayKey()).contains(List.of("sku"));
        var reference = items.children().getFirst();
        assertThat(reference.direction()).isEqualTo(ValidationNestRelations.Direction.REFERENCE);
        assertThat(reference.join()).containsExactly(new ValidationNestRelations.Pair("id", "customer_id"));
        assertThat(reference.treePath()).containsExactly("items", "customer");
        var flat = plan.children().get(1);
        assertThat(flat.outputPath()).isEmpty();
        assertThat(flat.treePath()).containsExactly("$flat[profiles]");
    }
}
