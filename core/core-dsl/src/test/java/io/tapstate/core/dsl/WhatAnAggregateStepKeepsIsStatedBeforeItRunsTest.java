package io.tapstate.core.dsl;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.model.Resource;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * What an aggregate step will keep in state is said when the batch is applied: the entries per input
 * row, which the definition decides, and the total on the rows its inputs were discovered to hold.
 */
class WhatAnAggregateStepKeepsIsStatedBeforeItRunsTest {

    private static final String SOURCE = """
            version: tapstate/v1
            kind: source
            id: src_orders
            connector: mysql
            mode: cdc
            tables: [ orders, refunds ]
            """;

    private static String pipeline(String from, String fields) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: sales
                source: src_orders
                transforms:
                  - id: by_region
                    from: [ %s ]
                    type: aggregate
                    groupBy: { region: $region }
                    fields:
                %s
                serve:
                  from: by_region
                  query: [ { type: rest } ]
                """.formatted(from, fields.stripTrailing());
    }

    private static List<Resource> batch(String pipeline) {
        DslParser parser = new DslParser();
        return List.of(parser.parse(SOURCE), parser.parse(pipeline));
    }

    private static DiscoveredTable table(String name, Long rows) {
        return new DiscoveredTable(name, Map.of(), rows);
    }

    @Test
    void aStepOfRunningTotalsKeepsTwoEntriesForEachInputRow() {
        List<Advisory> told = AggregateSizingRules.review(
                batch(pipeline("orders", """
                              orders: { count: true }
                              revenue: { sum: $amount }
                        """)),
                Map.of("src_orders", List.of(table("orders", 1_000_000L))));

        assertThat(told).singleElement().satisfies(advisory -> {
            assertThat(advisory.code()).isEqualTo(AggregateSizingError.STATE_ESTIMATE);
            assertThat(advisory.params()).containsOnlyKeys(AggregateSizingError.STATE_ESTIMATE.placeholders())
                    .containsEntry("pipeline", "sales").containsEntry("step", "by_region")
                    .containsEntry("entriesPerRow", 2).containsEntry("inputRows", 1_000_000L)
                    .containsEntry("estimatedEntries", 2_000_000L);
        });
    }

    @Test
    void eachFieldThatKeepsItsInputsAddsOneEntryForEachRow() {
        List<Advisory> told = AggregateSizingRules.review(
                batch(pipeline("orders", """
                              orders: { count: true }
                              most: { max: $amount }
                              latest: { last: { value: $amount, by: [ $updated_at ] } }
                        """)),
                Map.of("src_orders", List.of(table("orders", 10L))));

        assertThat(told).singleElement().satisfies(advisory -> assertThat(advisory.params())
                .containsEntry("entriesPerRow", 4).containsEntry("estimatedEntries", 40L));
    }

    @Test
    void anInputNobodyCountedLeavesTheTotalUnsaidRatherThanSmall() {
        // A table that reports no rows and a source nobody discovered are the same answer: a total
        // that left either out would read as the whole.
        String pipeline = pipeline("orders", """
                      orders: { count: true }
                """);

        assertThat(AggregateSizingRules.review(batch(pipeline),
                Map.of("src_orders", List.of(table("orders", null)))))
                .singleElement().satisfies(advisory -> {
                    assertThat(advisory.code()).isEqualTo(AggregateSizingError.STATE_ESTIMATE_UNSIZED);
                    assertThat(advisory.params())
                            .containsOnlyKeys(AggregateSizingError.STATE_ESTIMATE_UNSIZED.placeholders())
                            .containsEntry("entriesPerRow", 2);
                });
        assertThat(AggregateSizingRules.review(batch(pipeline), Map.of()))
                .singleElement().satisfies(advisory ->
                        assertThat(advisory.code()).isEqualTo(AggregateSizingError.STATE_ESTIMATE_UNSIZED));
    }

    @Test
    void aPipelineWithNoAggregateStepIsToldNothing() {
        Resource plain = new DslParser().parse("""
                version: tapstate/v1
                kind: pipeline
                id: copy
                source: src_orders
                serve:
                  from: orders
                  query: [ { type: rest } ]
                """);

        assertThat(AggregateSizingRules.review(List.of(new DslParser().parse(SOURCE), plain), Map.of())).isEmpty();
    }
}
