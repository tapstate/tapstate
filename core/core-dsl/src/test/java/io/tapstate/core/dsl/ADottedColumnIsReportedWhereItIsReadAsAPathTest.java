package io.tapstate.core.dsl;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.common.TapstateType;
import io.tapstate.core.model.Resource;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A source is free to name a column {@code price.usd}, and a store that addresses keys by path reads
 * those characters two ways: as the key it holds, and as a step into a {@code price} that is not there.
 * The column is written, correct, and unreachable by the spelling anyone would try first — so it is
 * reported when the batch that would write it is applied.
 *
 * <p>The other half is what makes the report worth reading: a target that holds the name as a name has
 * nothing wrong with it, and a rule that reported both would be reporting the dot rather than what the
 * dot costs. Both halves are measured here on one batch that differs only in the target it writes to.
 */
class ADottedColumnIsReportedWhereItIsReadAsAPathTest {

    /** Legal in the source, and a key that does not address itself once a document store holds it. */
    private static final String DOTTED_COLUMN = "price.usd";

    private final DslParser parser = new DslParser();

    @Test
    void aColumnWhoseNameHoldsADotIsReportedWhenADocumentStoreWillHoldIt() {
        List<Advisory> findings = DocumentKeyRules.review(batch("mongodb"), discovered(DOTTED_COLUMN));

        assertThat(findings).singleElement().satisfies(finding -> {
            assertThat(finding.code()).isEqualTo(DocumentKeyError.COLUMN_NAME_READS_AS_A_PATH);
            assertThat(finding.params())
                    .as("a finding that does not say which column it is about is one nobody can act on")
                    .containsEntry("column", DOTTED_COLUMN)
                    .containsEntry("table", "orders")
                    .containsEntry("source", "orders_src")
                    .containsEntry("target", "orders_dest")
                    .containsEntry("pipeline", "orders_sync");
        });
    }

    @Test
    void theSameColumnIsNothingToReportWhereTheTargetHoldsTheNameAsAName() {
        assertThat(DocumentKeyRules.review(batch("mysql"), discovered(DOTTED_COLUMN)))
                .as("the dot costs nothing in a store that addresses a column by name, so reporting it "
                        + "there would be reporting the dot rather than what it costs")
                .isEmpty();
    }

    @Test
    void anOrdinaryColumnNameIsNothingToReportAnywhere() {
        assertThat(DocumentKeyRules.review(batch("mongodb"), discovered("price_usd")))
                .isEmpty();
    }

    /** The same batch throughout: only the connector the sync writes to differs. */
    private List<Resource> batch(String targetConnector) {
        return List.of(
                parser.parse("""
                        version: tapstate/v1
                        kind: source
                        id: orders_src
                        connector: mysql
                        config: { host: 10.10.0.5, username: u, password: p }
                        mode: cdc
                        tables: [ orders ]
                        """),
                parser.parse("""
                        version: tapstate/v1
                        kind: source
                        id: orders_dest
                        connector: %s
                        config: { uri: "x" }
                        """.formatted(targetConnector)),
                parser.parse("""
                        version: tapstate/v1
                        kind: pipeline
                        id: orders_sync
                        source: orders_src
                        serve:
                          from: orders
                          sync: [ { id: sync_1, source: orders_dest } ]
                        """));
    }

    /** The source as discovery reports it: one table, carrying the column under test. */
    private static Map<String, List<DiscoveredTable>> discovered(String priceColumn) {
        Map<String, TapstateType> columns = new LinkedHashMap<>();
        columns.put("id", TapstateType.INT64);
        columns.put(priceColumn, TapstateType.DECIMAL);
        return Map.of("orders_src", List.of(new DiscoveredTable("orders", columns, List.of("id"), 1_000L)));
    }
}
