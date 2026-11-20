package io.tapstate.core.dsl;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.dsl.aggregate.RollupGuidance;
import io.tapstate.core.model.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * A rollup is refolded from every element of its level on each send, so a level much wider than rollups keep
 * up with makes its documents fall behind. That is said while the pipeline is applied, from the row counts
 * discovery took: a level's average width per document is its rows over its parent's.
 */
class ARollupLevelTooWideForItsRenderIsFlaggedTest {

    private static final long BUDGET = 1_000_000L;

    /** players with sessions rolled up at the root, and comps rolled up per visit. */
    private static final String PIPELINE = """
            version: tapstate/v1
            kind: pipeline
            id: p
            source: src
            transforms:
              - id: doc
                type: nest
                from: { pl: players, s: sessions, v: visits, c: comps }
                root:
                  from: pl
                  key: [player_id]
                  embed:
                    - from: s
                      on: { player_id: player_id }
                      as: rollup
                      arrayKey: [session_id]
                      rollup:
                        last_played: { max: $ended_at }
                    - from: v
                      on: { player_id: player_id }
                      as: array
                      path: visits
                      arrayKey: [visit_id]
                      embed:
                        - from: c
                          on: { visit_id: visit_id }
                          as: rollup
                          arrayKey: [comp_id]
                          rollup:
                            comps: { count: true }
            serve:
              from: doc
              query: [ { type: rest } ]
            """;

    @Test
    void aLevelAveragingMoreElementsPerDocumentThanRollupsKeepUpWithIsFlagged() {
        List<Advisory> findings = review(counted("players", 1_000L, "sessions", 50_000_000L,
                "visits", 20_000L, "comps", 100_000L));
        List<Advisory> wide = wide(findings);
        assertThat(wide).hasSize(1);
        assertThat(wide.getFirst().params()).containsEntry("embedPath", "$rollup[s]")
                .containsEntry("estimatedElements", 50_000L)
                .containsEntry("boundary", RollupGuidance.elementsPerDocument(0))
                .containsEntry("estimatedBytes", 50_000L * RollupGuidance.bytesPerElement(0))
                .containsEntry("pipeline", "p");
    }

    @Test
    void aNestedLevelIsMeasuredAgainstItsOwnParentLevelNotTheRoot() {
        // 5,000 comps per visit: the rollup lands on each visit, so its width is comps over visits
        List<Advisory> findings = review(counted("players", 1_000L, "sessions", 10_000L,
                "visits", 1_000L, "comps", 5_000_000L));
        assertThat(wide(findings)).singleElement()
                .satisfies(a -> assertThat(a.params()).containsEntry("embedPath", "visits.$rollup[c]")
                        .containsEntry("estimatedElements", 5_000L));
    }

    @Test
    void wideRowsMakeADocumentHeavierSoTheSameCountIsFlaggedOnlyAtTheirWidth() {
        // 2,000 sessions per player: within the boundary at an undiscovered (narrow) width, past it at 31 columns
        assertThat(wide(review(counted("players", 1_000L, "sessions", 2_000_000L,
                "visits", 1_000L, "comps", 1_000L)))).isEmpty();
        Map<String, List<DiscoveredTable>> wideSessions = new java.util.LinkedHashMap<>(counted(
                "players", 1_000L, "visits", 1_000L, "comps", 1_000L));
        Map<String, io.tapstate.core.common.TapstateType> columns = new java.util.LinkedHashMap<>();
        for (int c = 0; c < 31; c++) {
            columns.put("col" + c, io.tapstate.core.common.TapstateType.STRING);
        }
        List<DiscoveredTable> tables = new ArrayList<>(wideSessions.get("src"));
        tables.add(new DiscoveredTable("sessions", columns, 2_000_000L));
        assertThat(wide(review(Map.of("src", tables)))).singleElement()
                .satisfies(a -> assertThat(a.params()).containsEntry("boundary", RollupGuidance.elementsPerDocument(31))
                        .containsEntry("estimatedElements", 2_000L));
    }

    @Test
    void aLevelWithinTheWidthRollupsAreMeantForSaysNothing() {
        assertThat(wide(review(counted("players", 1_000L, "sessions", 900_000L,
                "visits", 1_000L, "comps", 100_000L)))).isEmpty();
    }

    @Test
    void aLevelWhoseRowsNobodyCountedIsNotGuessedAt() {
        assertThat(wide(review(counted("players", 1_000L, "visits", 1_000L, "comps", 100_000L)))).isEmpty();
    }

    private static List<Advisory> review(Map<String, List<DiscoveredTable>> tables) {
        List<Resource> batch = List.of(new DslParser().parse(PIPELINE));
        return NestSizingRules.review(batch, tables, BUDGET);
    }

    private static List<Advisory> wide(List<Advisory> findings) {
        return findings.stream().filter(a -> a.code() == NestSizingError.ROLLUP_LEVEL_WIDE).toList();
    }

    /** The tables of source {@code src}, each with the row count discovery reported for it. */
    private static Map<String, List<DiscoveredTable>> counted(Object... nameThenCount) {
        List<DiscoveredTable> tables = new ArrayList<>();
        for (int i = 0; i < nameThenCount.length; i += 2) {
            tables.add(new DiscoveredTable((String) nameThenCount[i], Map.of(), (Long) nameThenCount[i + 1]));
        }
        return Map.of("src", tables);
    }
}
