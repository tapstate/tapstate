package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.common.TapstateType;
import io.tapstate.core.sql.JoinPlan;
import io.tapstate.core.sql.SourceColumn;
import io.tapstate.core.sql.SourceTable;
import io.tapstate.core.sql.SqlFrontEnd;
import io.tapstate.spi.store.DerivedSchema;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Holding a join to the columns it was recorded producing.
 *
 * <p>Both directions are asserted throughout, because each alone is passed by a check that does
 * nothing. Only asserting the refusal is passed by one that refuses everything; only asserting the
 * pass is passed by one that refuses nothing - and refusing nothing is exactly what a check quietly
 * turned off looks like.
 */
class JoinSchemaDriftTest {

    private static final String SQL =
            "SELECT o.o_id, o.o_total, c.c_name AS customer_name"
                    + " FROM orders o LEFT JOIN customers c ON o.o_cust_id = c.c_id";

    private static List<SourceTable> tables(TapstateType totalType) {
        return List.of(
                new SourceTable("orders", List.of(
                        new SourceColumn("o_id", TapstateType.INT64, false),
                        new SourceColumn("o_cust_id", TapstateType.INT64, true),
                        new SourceColumn("o_total", totalType, false))),
                new SourceTable("customers", List.of(
                        new SourceColumn("c_id", TapstateType.INT64, false),
                        new SourceColumn("c_name", TapstateType.STRING, false))));
    }

    private static JoinPlan plan(String sql, List<SourceTable> tables) {
        return SqlFrontEnd.derive(sql, tables);
    }

    private final InMemoryDerivedSchemaStore records = new InMemoryDerivedSchemaStore();
    private final JoinSchemaDrift drift = new JoinSchemaDrift(records);

    @Test
    @DisplayName("a first start records what the join produces and is allowed through")
    void aFirstStartRecordsAndPasses() {
        List<SourceTable> tables = tables(TapstateType.DECIMAL);

        assertThatCode(() -> drift.checkAndRecord("flow", "widen", SQL, plan(SQL, tables), tables))
                .doesNotThrowAnyException();

        DerivedSchema recorded = records.latest("flow", "widen").orElseThrow();
        assertThat(recorded.version()).isZero();
        assertThat(recorded.schema().keySet()).containsExactly("o_id", "o_total", "customer_name");
    }

    @Test
    @DisplayName("an unchanged join starts again and spends no new version")
    void anUnchangedJoinPasses() {
        List<SourceTable> tables = tables(TapstateType.DECIMAL);
        drift.checkAndRecord("flow", "widen", SQL, plan(SQL, tables), tables);

        assertThatCode(() -> drift.checkAndRecord("flow", "widen", SQL, plan(SQL, tables), tables))
                .doesNotThrowAnyException();

        assertThat(records.latest("flow", "widen").orElseThrow().version()).isZero();
    }

    @Test
    @DisplayName("a source column changing the output type is refused, attributed to the sources")
    void aChangedSourceColumnIsRefusedAndAttributedToTheSources() {
        List<SourceTable> before = tables(TapstateType.DECIMAL);
        drift.checkAndRecord("flow", "widen", SQL, plan(SQL, before), before);
        List<SourceTable> after = tables(TapstateType.DOUBLE);

        assertThatThrownBy(() -> drift.checkAndRecord("flow", "widen", SQL, plan(SQL, after), after))
                .isInstanceOfSatisfying(TapstateException.class, error -> {
                    assertThat(error.code().code())
                            .isEqualTo("actuation.join-output-schema-source-changed");
                    assertThat(error.args()).containsEntry("pipeline", "flow");
                    assertThat(error.args()).containsEntry("step", "widen");
                    // The difference names the column and both types: "one or more columns changed"
                    // sends the reader to compare two schemas by hand, which is the work this exists
                    // to have already done.
                    assertThat(String.valueOf(error.args().get("retyped")))
                            .isEqualTo("o_total: DECIMAL NOT NULL -> DOUBLE NOT NULL");
                    assertThat(error.args()).containsEntry("added", "none");
                    assertThat(error.args()).containsEntry("removed", "none");
                });
    }

    @Test
    @DisplayName("a refused start records nothing, so the next one still sees the difference")
    void aRefusedStartRecordsNothing() {
        List<SourceTable> before = tables(TapstateType.DECIMAL);
        drift.checkAndRecord("flow", "widen", SQL, plan(SQL, before), before);
        List<SourceTable> after = tables(TapstateType.DOUBLE);

        assertThatThrownBy(() -> drift.checkAndRecord("flow", "widen", SQL, plan(SQL, after), after))
                .isInstanceOf(TapstateException.class);

        // Absorbing the new shape here would make the difference undetectable by the time anyone
        // looked: the start would refuse once and then run on the new shape forever after.
        assertThat(records.latest("flow", "widen").orElseThrow().schema())
                .containsEntry("o_total", "DECIMAL NOT NULL");
        assertThatThrownBy(() -> drift.checkAndRecord("flow", "widen", SQL, plan(SQL, after), after))
                .isInstanceOf(TapstateException.class);
    }

    @Test
    @DisplayName("the same query and the same sources answering differently is attributed to us")
    void aChangedDerivationIsRefusedAndAttributedToUs() {
        // This one cannot be produced by deriving, because producing it means changing the derivation.
        // It is set up by recording a different answer under the provenance today's inputs really do
        // hash to - which is exactly the state a release that changed the derivation would leave.
        List<SourceTable> tables = tables(TapstateType.DECIMAL);
        drift.checkAndRecord("flow", "widen", SQL, plan(SQL, tables), tables);
        DerivedSchema asRecorded = records.latest("flow", "widen").orElseThrow();
        records.record("flow", "widen", Map.of("o_id", "INT64 NOT NULL"), asRecorded.statement(),
                asRecorded.derivedFrom(), "join-derivation/0+calcite/1.39.0");

        assertThatThrownBy(() -> drift.checkAndRecord("flow", "widen", SQL, plan(SQL, tables), tables))
                .isInstanceOfSatisfying(TapstateException.class, error -> {
                    assertThat(error.code().code())
                            .isEqualTo("actuation.join-output-schema-engine-changed");
                    // Nothing about the sources moved, so pointing at them would send an operator to
                    // look at a database that is exactly as they left it.
                    assertThat(error.args()).containsEntry("added", "o_total, customer_name");
                    assertThat(error.args())
                            .containsEntry("recordedBy", "join-derivation/0+calcite/1.39.0");
                    assertThat(error.args()).containsEntry("nowBy", SqlFrontEnd.DERIVATION_VERSION);
                });
    }

    @Test
    @DisplayName("an edited query producing different columns is not drift")
    void anEditedQueryPassesAndIsRecorded() {
        List<SourceTable> tables = tables(TapstateType.DECIMAL);
        drift.checkAndRecord("flow", "widen", SQL, plan(SQL, tables), tables);
        String edited = "SELECT o.o_id, c.c_name AS customer_name"
                + " FROM orders o LEFT JOIN customers c ON o.o_cust_id = c.c_id";

        // The author asked for the new shape. Refusing here would put a ceremony in front of every
        // ordinary edit, which is how a check gets turned off.
        assertThatCode(() -> drift.checkAndRecord("flow", "widen", edited, plan(edited, tables), tables))
                .doesNotThrowAnyException();

        DerivedSchema recorded = records.latest("flow", "widen").orElseThrow();
        assertThat(recorded.version()).isEqualTo(1L);
        assertThat(recorded.schema().keySet()).containsExactly("o_id", "customer_name");
    }

    @Test
    @DisplayName("a column disappearing and one appearing are reported apart")
    void addedAndRemovedColumnsAreReportedApart() {
        List<SourceTable> tables = tables(TapstateType.DECIMAL);
        drift.checkAndRecord("flow", "widen", SQL, plan(SQL, tables), tables);
        DerivedSchema asRecorded = records.latest("flow", "widen").orElseThrow();
        records.record("flow", "widen", Map.of("o_id", "INT64 NOT NULL", "gone", "STRING NULL"),
                asRecorded.statement(), "some-other-source-fingerprint", asRecorded.derivedBy());

        assertThatThrownBy(() -> drift.checkAndRecord("flow", "widen", SQL, plan(SQL, tables), tables))
                .isInstanceOfSatisfying(TapstateException.class, error -> {
                    assertThat(error.args()).containsEntry("added", "o_total, customer_name");
                    assertThat(error.args()).containsEntry("removed", "gone");
                    assertThat(error.args()).containsEntry("retyped", "none");
                });
    }

    @Test
    @DisplayName("two joins of one pipeline are held apart")
    void twoStepsAreHeldApart() {
        List<SourceTable> before = tables(TapstateType.DECIMAL);
        drift.checkAndRecord("flow", "widen", SQL, plan(SQL, before), before);
        drift.checkAndRecord("flow", "enrich", SQL, plan(SQL, before), before);
        List<SourceTable> after = tables(TapstateType.DOUBLE);

        assertThatThrownBy(() -> drift.checkAndRecord("flow", "widen", SQL, plan(SQL, after), after))
                .isInstanceOf(TapstateException.class);

        // The other step's record is its own and is untouched by the refusal next door.
        assertThat(records.latest("flow", "enrich").orElseThrow().schema())
                .containsEntry("o_total", "DECIMAL NOT NULL");
    }

    @Test
    @DisplayName("a source column the query never reads still moves the fingerprint")
    void anUnreadSourceColumnStillCountsAsTheWorldMoving() {
        // Deliberate: the fingerprint covers every column the derivation could see, not only the ones
        // this query happens to read. Narrowing it to the read set would make the fingerprint depend on
        // the query, and the two inputs then stop being separable - which is the whole mechanism.
        List<SourceTable> before = tables(TapstateType.DECIMAL);
        drift.checkAndRecord("flow", "widen", SQL, plan(SQL, before), before);
        String provenanceBefore = records.latest("flow", "widen").orElseThrow().derivedFrom();
        List<SourceTable> withAnExtraColumn = List.of(
                new SourceTable("orders", List.of(
                        new SourceColumn("o_id", TapstateType.INT64, false),
                        new SourceColumn("o_cust_id", TapstateType.INT64, true),
                        new SourceColumn("o_total", TapstateType.DECIMAL, false),
                        new SourceColumn("o_note", TapstateType.STRING, true))),
                before.get(1));

        drift.checkAndRecord("flow", "widen", SQL, plan(SQL, withAnExtraColumn), withAnExtraColumn);

        // The output did not move, so nothing is refused and no version is spent - but the provenance
        // is now today's. Left stale, the next genuine difference would be attributed to the sources
        // having moved when they had already moved here, without effect, and it was the derivation.
        DerivedSchema recorded = records.latest("flow", "widen").orElseThrow();
        assertThat(recorded.version()).isZero();
        assertThat(recorded.derivedFrom()).isNotEqualTo(provenanceBefore);
    }
}
