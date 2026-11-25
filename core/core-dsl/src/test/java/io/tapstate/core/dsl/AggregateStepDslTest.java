package io.tapstate.core.dsl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.RollupFunction;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.model.canonical.CanonicalWriter;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class AggregateStepDslTest {

    private static final String PREFIX = """
            version: tapstate/v1
            kind: pipeline
            id: sales
            source: src_orders
            transforms:
              - id: by_region
                type: aggregate
                from: [orders]
            """;
    private static final String SERVE = """
            serve:
              id: serve
              from: by_region
              sync:
                - id: dw
                  source: tgt
            """;

    private static TransformBody.Aggregate parsed(String payload) {
        PipelineResource pipeline = (PipelineResource) new DslParser().parse(PREFIX + payload + SERVE);
        return (TransformBody.Aggregate) ((Step.Inline) pipeline.transforms().getFirst()).body();
    }

    @Test
    @DisplayName("group names and fields keep the order written, which is the order of the output")
    void groupNamesAndFieldsKeepTheirOrder() {
        TransformBody.Aggregate step = parsed("""
                    key: [id, shard]
                    where: =after.status != 'cancelled'
                    groupBy:
                      region: $region
                      day: =after.created_day
                    fields:
                      revenue:
                        sum: $amount
                      orders:
                        count: true
                """);

        assertThat(step.key()).containsExactly("id", "shard");
        assertThat(step.where()).isEqualTo("=after.status != 'cancelled'");
        assertThat(step.groupBy()).containsExactly(
                java.util.Map.entry("region", "$region"), java.util.Map.entry("day", "=after.created_day"));
        assertThat(step.fields().keySet()).containsExactly("revenue", "orders");
        assertThat(step.fields().get("revenue").function()).isEqualTo(RollupFunction.SUM);
    }

    @Test
    @DisplayName("fields alone is one group over every row: no key, no predicate, no group names")
    void fieldsAloneIsOneGlobalGroup() {
        TransformBody.Aggregate step = parsed("""
                    fields:
                      orders:
                        count: true
                """);

        assertThat(step.key()).isNull();
        assertThat(step.where()).isNull();
        assertThat(step.groupBy()).isEmpty();
    }

    @Test
    @DisplayName("the canonical form is what was written, and is stable when read again")
    void theCanonicalFormRoundTrips() {
        String declared = PREFIX + """
                    key: [id]
                    where: =after.status != 'cancelled'
                    groupBy:
                      region: $region
                    fields:
                      revenue:
                        sum: $amount
                        default: 0
                """ + SERVE;
        CanonicalWriter writer = new CanonicalWriter();
        String canonical = writer.write(new DslParser().parse(declared));

        assertThat(canonical).isEqualTo(declared);
        assertThat(writer.write(new DslParser().parse(canonical))).isEqualTo(canonical);
    }

    @Test
    @DisplayName("a step with no output field is refused where fields is written")
    void aStepWithNoFieldIsRefused() {
        assertThatThrownBy(() -> parsed("    fields: {}\n"))
                .isInstanceOfSatisfying(DslException.class, error -> {
                    assertThat(error.code()).isEqualTo(DslError.ILLEGAL_VALUE);
                    assertThat(error.path()).isEqualTo("transforms[0].fields");
                });
    }

    @Test
    @DisplayName("a where that is not a predicate is refused as an expression")
    void aWhereThatIsNotAPredicateIsRefused() {
        assertThatThrownBy(() -> parsed("""
                    where: =after.amount + 1
                    fields:
                      orders:
                        count: true
                """))
                .isInstanceOfSatisfying(DslException.class, error -> {
                    assertThat(error.code()).isEqualTo(DslError.ILLEGAL_EXPRESSION);
                    assertThat(error.path()).isEqualTo("transforms[0].where");
                });
    }

    @Test
    @DisplayName("an empty key list is refused: it would identify no row")
    void anEmptyKeyIsRefused() {
        assertThatThrownBy(() -> parsed("""
                    key: []
                    fields:
                      orders:
                        count: true
                """))
                .isInstanceOfSatisfying(DslException.class, error ->
                        assertThat(error.path()).isEqualTo("transforms[0].key"));
    }

    @Test
    @DisplayName("an unknown function names the field and the functions that exist")
    void anUnknownFunctionIsRefused() {
        assertThatThrownBy(() -> parsed("""
                    fields:
                      middle:
                        median: $amount
                """))
                .isInstanceOfSatisfying(DslException.class, error ->
                        assertThat(error.code()).isEqualTo(DslError.ROLLUP_FUNCTION_UNKNOWN));
    }
}
