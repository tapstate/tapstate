package io.tapstate.adapters.pdk;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.common.TapstateType;
import io.tapstate.spi.store.ConnectionConfig;
import io.tapstate.spi.store.SchemaDiscoverer;
import io.tapdata.entity.schema.type.TapRaw;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceIndex;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SourceTable;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The schema-discovery PDK bridge: {@link PdkSchemaDiscoverer} driving a connector's frozen
 * {@code discoverSchema} and normalizing the reported PDK tables into a {@link SourceModel} — table
 * names, fields, primary key and indexes. A connector that throws out of discovery is a coded
 * connector-domain failure. Synthetic connectors compiled at test time prove the drive and the
 * coded-error path without a real connector jar or the PDK runtime.
 */
class PdkSchemaDiscovererTest {

    /** A discoverer over a provisioner that hands back one fixed connector ref, whatever id is asked for. */
    private static SchemaDiscoverer discoverer(Path jar, String className) {
        ConnectorRef ref = new ConnectorRef(List.of(jar), className, "2.0.8", null);
        return new PdkSchemaDiscoverer(connectorId -> ref);
    }

    /** As above, with the counting phase held to a budget of the test's choosing. */
    private static SchemaDiscoverer discoverer(Path jar, String className, Duration countBudget) {
        ConnectorRef ref = new ConnectorRef(List.of(jar), className, "2.0.8", null);
        return new PdkSchemaDiscoverer(connectorId -> ref, countBudget);
    }

    private static ConnectionConfig config() {
        return new ConnectionConfig("conn-1", "demo", Map.of());
    }

    @Test
    void discoversTablesWithTheirFieldsInOrder(@TempDir Path dir) {
        SchemaDiscoverer discoverer = discoverer(Synthetic.discoverableSource(dir), "synthetic.Discoverable");

        SourceModel model = discoverer.discover(config());

        assertThat(model.tables()).extracting(SourceTable::name).containsExactly("orders");
        SourceTable orders = model.tables().get(0);
        assertThat(orders.fields()).extracting(SourceField::name).containsExactly("id", "amount");
        assertThat(orders.fields()).extracting(SourceField::dataType).containsExactly("int", "decimal");
    }

    /** A connector declaring both of the discoverable source's column types, in its own spec's shape. */
    private static final String SPEC = """
            {"dataTypes": {"int": {"to": "TapNumber", "bit": 32},\
             "decimal": {"to": "TapNumber", "fixed": true}}}""";

    @Test
    void carriesTheTapstateTypeEachColumnResolvesTo(@TempDir Path dir) {
        // Discovery reports a column by the database's own spelling, which only the connector can read.
        // Resolving it is the connector's own job and so has to happen while the connector is still open -
        // once the model leaves here, nothing downstream can tell an exact column from an approximate one.
        ConnectorRef ref = new ConnectorRef(
                List.of(Synthetic.discoverableSource(dir)), "synthetic.Discoverable", "2.0.8", null, SPEC);
        SchemaDiscoverer discoverer = new PdkSchemaDiscoverer(connectorId -> ref);

        SourceModel model = discoverer.discover(config());

        assertThat(model.tables().get(0).fields())
                .extracting(SourceField::type)
                .containsExactly(TapstateType.INT64, TapstateType.DECIMAL);
    }

    /** A spec that declares one of the two columns, so the other reaches the mapping unresolved. */
    private static final String PARTIAL_SPEC = """
            {"dataTypes": {"int": {"to": "TapNumber", "bit": 32}}}""";

    @Test
    void carriesTheMappingsOwnAttributionForAColumnThatResolvedToNothing(@TempDir Path dir) {
        // The connector is open exactly here and nowhere after, so which unknown a column is has to
        // leave with it. A discoverer that answered with a remark of its own instead - "the type did not
        // resolve" - would be discarding the only fact that says whether the mapping is short a case,
        // and the connector it came from cannot be asked again.
        ConnectorRef ref = new ConnectorRef(
                List.of(Synthetic.discoverableSource(dir)), "synthetic.Discoverable", "2.0.8", null,
                PARTIAL_SPEC);
        SchemaDiscoverer discoverer = new PdkSchemaDiscoverer(connectorId -> ref);

        SourceModel model = discoverer.discover(config());

        SourceField unresolved = model.tables().get(0).fields().get(1);
        assertThat(unresolved.type()).isEqualTo(TapstateType.UNKNOWN);
        assertThat(unresolved.unknownBecause())
                .as("the attribution is the mapping's own, not a restatement made after the fact")
                .isEqualTo(PdkTypeMapping.resolve(new TapRaw()).unknownBecause())
                .contains("TapRaw");
        assertThat(model.tables().get(0).fields().get(0).unknownBecause())
                .as("a column that did resolve is attributed to nothing")
                .isNull();
    }

    @Test
    void carriesThePrimaryKeyColumns(@TempDir Path dir) {
        SchemaDiscoverer discoverer = discoverer(Synthetic.discoverableSource(dir), "synthetic.Discoverable");

        SourceModel model = discoverer.discover(config());

        assertThat(model.tables().get(0).primaryKey()).containsExactly("id");
    }

    @Test
    void carriesTheIndexesWithTheirFieldsAndUniqueness(@TempDir Path dir) {
        SchemaDiscoverer discoverer = discoverer(Synthetic.discoverableSource(dir), "synthetic.Discoverable");

        SourceModel model = discoverer.discover(config());

        List<SourceIndex> indexes = model.tables().get(0).indexes();
        assertThat(indexes).extracting(SourceIndex::name).containsExactly("idx_amount");
        assertThat(indexes.get(0).fields()).containsExactly("amount");
        assertThat(indexes.get(0).unique()).isFalse();
    }

    /**
     * An index part that names no column does not take discovery down with it.
     *
     * <p>A functional index has parts that are expressions over the row rather than columns, so the
     * database reports no column name for them - ordinary since MySQL 8.0.13, and present in databases
     * nobody built for us. Copying that absent name into the model threw, which failed the whole
     * discovery: one such index anywhere made every table in that database undiscoverable, and the
     * failure surfaced as an uncoded server error naming neither the table nor the index. The parts
     * that do name a column are kept, since those are what a key could ever be read from.
     */
    @Test
    void anIndexPartWithNoColumnNameDoesNotFailTheWholeDiscovery(@TempDir Path dir) {
        SchemaDiscoverer discoverer = discoverer(Synthetic.functionalIndexSource(dir), "synthetic.FunctionalIndex");

        SourceModel model = discoverer.discover(config());

        List<SourceIndex> indexes = model.tables().get(0).indexes();
        assertThat(indexes).extracting(SourceIndex::name).containsExactly("uq_mixed");
        assertThat(indexes.get(0).fields())
                .as("the named columns survive; the expression part contributes no column name")
                .containsExactly("amount");
        assertThat(indexes.get(0).unique())
                .as("what survives covers fewer columns than the constraint, so it is not unique: "
                        + "UNIQUE (amount, (expr)) permits duplicate amounts")
                .isFalse();
    }

    @Test
    void carriesTheRowCountFromAConnectorThatCanCount(@TempDir Path dir) {
        SchemaDiscoverer discoverer =
                discoverer(Synthetic.countingDiscoverableSource(dir), "synthetic.CountingDiscoverable");

        SourceModel model = discoverer.discover(config());

        assertThat(model.tables().get(0).approximateRowCount()).isEqualTo(4_200_000L);
    }

    @Test
    void aConnectorThatRegistersNoCountLeavesItsTablesUncounted(@TempDir Path dir) {
        SchemaDiscoverer discoverer = discoverer(Synthetic.discoverableSource(dir), "synthetic.Discoverable");

        SourceModel model = discoverer.discover(config());

        assertThat(model.tables().get(0).approximateRowCount())
                .as("counting is not a capability every connector has, and not having it is not a count of zero")
                .isNull();
    }

    @Test
    void aCountThatThrowsLeavesThatTableUncountedRatherThanFailingTheDiscovery(@TempDir Path dir) {
        SchemaDiscoverer discoverer = discoverer(Synthetic.throwingCountSource(dir), "synthetic.ThrowingCount");

        SourceModel model = discoverer.discover(config());

        // The count is a measurement taken alongside the schema, never the reason the schema is refused:
        // a source whose count fails is one an author can still wire, and turning that into a discover
        // failure would take away the schema over a number nobody asked for.
        assertThat(model.tables()).extracting(SourceTable::name).containsExactly("orders");
        assertThat(model.tables().get(0).fields()).extracting(SourceField::name).containsExactly("id", "amount");
        assertThat(model.tables().get(0).approximateRowCount()).isNull();
    }

    @Test
    void aCountFailureOnOneTableDoesNotCostTheOtherTablesTheirCounts(@TempDir Path dir) {
        SchemaDiscoverer discoverer =
                discoverer(Synthetic.partiallyCountableSource(dir), "synthetic.PartiallyCountable");

        SourceModel model = discoverer.discover(config());

        assertThat(model.tables()).extracting(SourceTable::name).containsExactly("orders", "items");
        assertThat(model.tables().get(0).approximateRowCount()).isNull();
        assertThat(model.tables().get(1).approximateRowCount())
                .as("one table refusing to be counted says nothing about the next one")
                .isEqualTo(7L);
    }

    @Test
    void aCountBudgetSpentPartWayThroughLeavesTheRemainingTablesUncounted(@TempDir Path dir) {
        // Counting is taken table by table, and discovery is a synchronous verb whose callers give the
        // whole round trip a fixed window. Counting a wide source without a bound can spend that window,
        // and what reaches the author is then a timeout rather than the schema - which, before a count
        // was taken alongside it, would have arrived. The budget bounds the counting instead, and a table
        // it does not reach is left uncounted: the same absence a connector that cannot count already
        // produces, which every reader of a count has to handle anyway.
        SchemaDiscoverer discoverer = discoverer(
                Synthetic.slowCountableTwoTableSource(dir), "synthetic.SlowCountableTwoTable",
                Duration.ofMillis(Synthetic.SLOW_COUNT_MILLIS / 3));

        SourceModel model = discoverer.discover(config());

        assertThat(model.tables()).extracting(SourceTable::name).containsExactly("orders", "items");
        assertThat(model.tables().get(0).approximateRowCount())
                .as("the budget bounds which counts are started, so the first table is always attempted")
                .isEqualTo(11L);
        assertThat(model.tables().get(1).approximateRowCount())
                .as("the first count spent the budget, so the second table is never asked")
                .isNull();
    }

    @Test
    void aCountBudgetWiderThanTheWholeSourceLeavesNoTableUncounted(@TempDir Path dir) {
        // The positive control for the test above: same two tables, same slow counts, only the budget
        // changed. Without it, a discoverer that had simply stopped counting after the first table would
        // satisfy that test just as well.
        SchemaDiscoverer discoverer = discoverer(
                Synthetic.slowCountableTwoTableSource(dir), "synthetic.SlowCountableTwoTable",
                Duration.ofHours(1));

        SourceModel model = discoverer.discover(config());

        assertThat(model.tables().get(0).approximateRowCount()).isEqualTo(11L);
        assertThat(model.tables().get(1).approximateRowCount())
                .as("a budget nothing can exhaust leaves counting exactly as it was before there was one")
                .isEqualTo(7L);
    }

    @Test
    void aNonPositiveCountBudgetIsRefusedRatherThanSilentlyCountingNothing() {
        // A budget of zero reaches no table at all while reading as configured, which surfaces as a source
        // whose every table is uncounted and nothing anywhere saying why.
        assertThatThrownBy(() -> new PdkSchemaDiscoverer(connectorId -> null, Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aConnectorWhoseDiscoverThrowsIsACodedDiscoverFailure(@TempDir Path dir) {
        SchemaDiscoverer discoverer = discoverer(Synthetic.throwingDiscoverSource(dir), "synthetic.ThrowingDiscover");

        assertThatThrownBy(() -> discoverer.discover(config()))
                .isInstanceOf(TapstateException.class)
                .satisfies(e -> {
                    TapstateException ce = (TapstateException) e;
                    assertThat(ce.code()).isEqualTo(ConnectorError.DISCOVER_FAILED);
                    assertThat(ce.args()).containsEntry("connector", "demo").containsKey("detail");
                });
    }

    @Test
    void discoverFailureDetailUnfoldsTheCauseChainSoANullMessageWrapperStillNamesTheFault() {
        // A connector's failure often surfaces as a wrapper that carries no message of its own - an
        // ExceptionInInitializerError, a reflective InvocationTargetException. Reporting only that
        // wrapper's class name buries the real fault under an opaque word; the detail unfolds the chain
        // and names the fault instead, so a coded discover failure is diagnosable.
        Throwable failure = new ExceptionInInitializerError(new IllegalStateException("the real fault"));

        assertThat(PdkSchemaDiscoverer.detail(failure))
                .contains("ExceptionInInitializerError")
                .contains("the real fault");
    }

    @Test
    void anIncompatibleConnectorIsRefusedBeforeDiscovery(@TempDir Path dir) {
        // Discovery provisions through the same open path as the read / write / test ports: an
        // incompatible declared level is refused up front, never downgraded into an empty model.
        ConnectorRef ref = new ConnectorRef(
                List.of(Synthetic.discoverableSource(dir)), "synthetic.Discoverable", "2.0.8", "99");
        SchemaDiscoverer discoverer = new PdkSchemaDiscoverer(connectorId -> ref);

        assertThatThrownBy(() -> discoverer.discover(config()))
                .isInstanceOf(TapstateException.class)
                .satisfies(e -> assertThat(((TapstateException) e).code()).isEqualTo(ConnectorError.API_LEVEL_INCOMPATIBLE));
    }
}
