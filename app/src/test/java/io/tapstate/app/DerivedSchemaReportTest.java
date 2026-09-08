package io.tapstate.app;

import io.tapstate.control.core.AuditGate;
import io.tapstate.control.core.DerivedSchemas;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.core.common.TapstateType;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.dsl.Workspace;
import io.tapstate.core.model.Resource;
import io.tapstate.spi.store.AuditRecord;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SourceTable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What a person sees after a start is refused because a join's derived columns moved, and the one act
 * that lets them carry on.
 *
 * <p>The gate itself is covered next door. What is covered here is the half that makes the gate
 * shippable: without a way to look at the difference and a way to accept it, a refusal strands a
 * pipeline on a version - and in this release there is no second route, because pinning the old shape
 * by casting in the SELECT is not available at all (the SQL subset refuses CAST outright).
 */
class DerivedSchemaReportTest {

    private static final Clock FIXED = Clock.fixed(Instant.parse("2026-09-04T00:00:00Z"), ZoneOffset.UTC);

    private final List<AuditRecord> audited = new ArrayList<>();
    private final AuditGate auditGate = new AuditGate(audited::add, FIXED);

    @Test
    @DisplayName("the report puts what was recorded, what is derived now and what the target holds side by side")
    void theReportPutsAllThreeSidesSideBySide() {
        InMemoryStorePort store = seeded();
        discoverTarget(store);
        new StoreBackedDagSource(store).dagFor("wide");

        List<DerivedSchemas.StepReport> report = new StoreBackedDerivedSchemas(store, auditGate).compare("wide");

        assertThat(report).singleElement().satisfies(step -> {
            assertThat(step.step()).isEqualTo("widen");
            assertThat(step.targetTable()).isEqualTo("orders");
            assertThat(step.targetKnown()).isTrue();
            assertThat(step.columns()).extracting(DerivedSchemas.ColumnReport::column)
                    .containsExactly("order_id", "customer_name");
            DerivedSchemas.ColumnReport key = step.columns().get(0);
            // Nullable although the source table declares it a primary key: a discovered field carries
            // no nullability at all, so every column reaching the derivation is taken as nullable. That
            // widens the declared output and never narrows it, which is the safe direction - claiming
            // NOT NULL for a column that turns out to hold one is the promise that breaks a target.
            assertThat(key.recorded()).isEqualTo("INT64 NULL");
            assertThat(key.derived()).isEqualTo("INT64 NULL");
            // The target's own words, not the shared vocabulary's: whether the values fit is decided by
            // the width the target declares, and "INT64" has thrown that away.
            assertThat(key.target()).isEqualTo("bigint");
            assertThat(key.drifted()).isFalse();
        });
    }

    @Test
    @DisplayName("a target nobody has discovered is reported unknown, not filled in from a source of the same name")
    void anUndiscoveredTargetIsReportedUnknown() {
        // The discriminating case. The target table is called orders and so is a source table, and the
        // source has been discovered. A lookup by table name alone would find the source's columns and
        // report them as the target's - an answer that reads as agreement while nothing has looked at
        // the target at all, which is exactly the answer that gets a truncating pipeline started.
        InMemoryStorePort store = seeded();
        new StoreBackedDagSource(store).dagFor("wide");

        List<DerivedSchemas.StepReport> report = new StoreBackedDerivedSchemas(store, auditGate).compare("wide");

        assertThat(report).singleElement().satisfies(step -> {
            assertThat(step.targetKnown()).isFalse();
            assertThat(step.columns()).allSatisfy(column -> assertThat(column.target()).isNull());
        });
    }

    @Test
    @DisplayName("two targets holding a table of the same name are reported unknown, not one of the two")
    void twoTargetsWithTheSameTableNameAreReportedUnknown() {
        // A serve block may sync into several targets, and a table of one name in two of them is two
        // different tables. Answering with whichever was read last is worse than answering "unknown":
        // the point of this column is whether the values will fit, and a width read out of the wrong
        // database reads exactly like one read out of the right one.
        InMemoryStorePort store = seeded(TWO_TARGET_PIPELINE);
        discoverTargetAs(store, "orders_dest", "bigint");
        discoverTargetAs(store, "orders_dest_2", "varchar(8)");
        new StoreBackedDagSource(store).dagFor("wide");

        List<DerivedSchemas.StepReport> report = new StoreBackedDerivedSchemas(store, auditGate).compare("wide");

        assertThat(report).singleElement().satisfies(step -> {
            assertThat(step.targetKnown()).isFalse();
            assertThat(step.columns()).allSatisfy(column -> assertThat(column.target()).isNull());
        });
    }

    @Test
    @DisplayName("accepting records today's columns, so the start that was refused goes through")
    void acceptingClearsARefusal() {
        InMemoryStorePort store = seeded();
        new StoreBackedDagSource(store).dagFor("wide");
        widenTheFactKeyColumn(store);
        assertThatThrownBy(() -> new StoreBackedDagSource(store).dagFor("wide"))
                .isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.code().code())
                        .isEqualTo("actuation.join-output-schema-source-changed"));

        new StoreBackedDerivedSchemas(store, auditGate).accept("alice", "wide");

        assertThatCode(() -> new StoreBackedDagSource(store).dagFor("wide")).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("accepting leaves an audit record naming who moved what the check measures against")
    void acceptingIsAudited() {
        // Moving the baseline is the one act that can turn this check off for a pipeline, so it is worth
        // as much as the record itself to know who did it.
        InMemoryStorePort store = seeded();
        new StoreBackedDagSource(store).dagFor("wide");

        new StoreBackedDerivedSchemas(store, auditGate).accept("alice", "wide");

        assertThat(audited).singleElement().satisfies(record -> {
            assertThat(record.operationId()).isEqualTo("pipeline.accept-derived-schema");
            assertThat(record.principal()).isEqualTo("alice");
            assertThat(record.resourceId()).isEqualTo("wide");
        });
    }

    @Test
    @DisplayName("a pipeline with no join derives nothing and reports nothing")
    void aPipelineWithNoJoinReportsNothing() {
        InMemoryStorePort store = seeded();

        assertThat(new StoreBackedDerivedSchemas(store, auditGate).compare("plain")).isEmpty();
    }

    @Test
    @DisplayName("a running pipeline is reported on the version its run holds, not one recorded since")
    void aRunningPipelineIsReportedOnTheVersionItsRunHolds() {
        // The discriminating case for holding a version at all. Something writes a second version while
        // the job is going - the writer here stands in for whatever gets past the refusal, and the
        // refusal is covered below. Reporting the newest record would tell whoever is looking that the
        // running pipeline produces a decimal key, which no row it is emitting right now carries.
        InMemoryStorePort store = seeded();
        new StoreBackedDagSource(store).dagFor("wide");
        running(store);
        store.derivedSchemas().record("wide", "widen",
                Map.of("order_id", "DECIMAL NULL", "customer_name", "STRING NULL"),
                "another-statement", "another-source", "another-derivation");

        List<DerivedSchemas.StepReport> report = new StoreBackedDerivedSchemas(store, auditGate).compare("wide");

        assertThat(recordedKeyOf(report)).isEqualTo("INT64 NULL");
    }

    @Test
    @DisplayName("with no run going on, the report is on the newest record")
    void withNoRunGoingOnTheReportIsOnTheNewestRecord() {
        // The other half of the pair, and what keeps the one above from passing for the wrong reason: a
        // pin that was read whatever the pipeline was doing would freeze the report on the last run's
        // shape forever, so a sync taken while stopped would appear not to have happened.
        InMemoryStorePort store = seeded();
        new StoreBackedDagSource(store).dagFor("wide");
        store.derivedSchemas().record("wide", "widen",
                Map.of("order_id", "DECIMAL NULL", "customer_name", "STRING NULL"),
                "another-statement", "another-source", "another-derivation");

        List<DerivedSchemas.StepReport> report = new StoreBackedDerivedSchemas(store, auditGate).compare("wide");

        assertThat(recordedKeyOf(report)).isEqualTo("DECIMAL NULL");
    }

    @Test
    @DisplayName("syncing re-copies the physical model, so every step below it derives from today's columns")
    void syncingRecopiesThePhysicalModelBeforeDerivingBelowIt() {
        // Without the re-copy this passes on the join alone: the join compiles from the discovery
        // directly, so it would follow the widened column with the source node's copy left at yesterday's
        // answer - a record that agrees with itself and with nothing outside. The source node is the half
        // that discriminates.
        InMemoryStorePort store = seeded();
        new StoreBackedDagSource(store).dagFor("wide");
        widenTheFactKeyColumn(store);

        new StoreBackedDerivedSchemas(store, auditGate).accept("alice", "wide");

        assertThat(store.derivedSchemas().latest("wide", "orders_src.orders"))
                .get().extracting(recorded -> recorded.schema().get("id")).isEqualTo("DECIMAL NULL");
        assertThat(store.derivedSchemas().latest("wide", "widen"))
                .get().extracting(recorded -> recorded.schema().get("order_id")).isEqualTo("DECIMAL NULL");
    }

    @Test
    @DisplayName("syncing is refused while a job is carrying the pipeline, and says what it is doing")
    void syncingIsRefusedWhileAJobIsCarryingThePipeline() {
        InMemoryStorePort store = seeded();
        new StoreBackedDagSource(store).dagFor("wide");
        running(store);

        assertThatThrownBy(() -> new StoreBackedDerivedSchemas(store, auditGate).accept("alice", "wide"))
                .isInstanceOfSatisfying(TapstateException.class, error -> {
                    assertThat(error.code().code()).isEqualTo("actuation.schema-sync-while-running");
                    assertThat(error.args()).containsEntry("pipeline", "wide")
                            .containsEntry("state", "RUNNING");
                });
        // Refused before the write, not after it: a refusal that had already re-copied would leave the
        // record moved and the caller told it was not.
        assertThat(audited).isEmpty();
    }

    @Test
    @DisplayName("syncing a paused pipeline goes through - nothing is producing rows under it")
    void syncingAPausedPipelineGoesThrough() {
        InMemoryStorePort store = seeded();
        new StoreBackedDagSource(store).dagFor("wide");
        pausedAndStayingThere(store);
        widenTheFactKeyColumn(store);

        assertThatCode(() -> new StoreBackedDerivedSchemas(store, auditGate).accept("alice", "wide"))
                .doesNotThrowAnyException();

        assertThat(store.derivedSchemas().latest("wide", "orders_src.orders"))
                .get().extracting(recorded -> recorded.schema().get("id")).isEqualTo("DECIMAL NULL");
    }

    @Test
    @DisplayName("a paused pipeline already asked to resume is refused - it is a run about to carry on")
    void aPausedPipelineAlreadyAskedToResumeIsRefused() {
        // The case a checkpoint-only reading lets through. The job is not producing anything this instant,
        // and it is about to, under the assembly it was paused with.
        InMemoryStorePort store = seeded();
        new StoreBackedDagSource(store).dagFor("wide");
        pausedWithAResumeAskedFor(store);

        assertThatThrownBy(() -> new StoreBackedDerivedSchemas(store, auditGate).accept("alice", "wide"))
                .isInstanceOfSatisfying(TapstateException.class, error -> {
                    assertThat(error.code().code()).isEqualTo("actuation.schema-sync-while-running");
                    assertThat(error.args()).containsEntry("state", "PAUSED")
                            .containsEntry("desired", "RUNNING");
                });
    }

    @Test
    @DisplayName("a sync racing a start yields to the start, which is the arbitration between them")
    void aSyncRacingAStartYieldsToTheStart() {
        // Recording has no compare-and-swap, so two writers of one step need an order decided somewhere.
        // It is decided here: a start that has been asked for wins, and the sync is refused until the
        // pipeline is at rest again. The pipeline has not begun executing - nothing has written a
        // checkpoint yet - and that is exactly the window this covers.
        InMemoryStorePort store = seeded();
        new StoreBackedDagSource(store).dagFor("wide");
        store.desired().save(new DesiredState("wide", PipelineState.RUNNING, "revision"));

        assertThatThrownBy(() -> new StoreBackedDerivedSchemas(store, auditGate).accept("alice", "wide"))
                .isInstanceOfSatisfying(TapstateException.class, error -> {
                    assertThat(error.code().code()).isEqualTo("actuation.schema-sync-while-running");
                    assertThat(error.args()).containsEntry("state", "NEW")
                            .containsEntry("desired", "RUNNING");
                });
    }

    @Test
    @DisplayName("a re-copy is refused while a job is producing, now that a target is built from the copy")
    void aRecopyIsRefusedWhileAJobIsProducing() {
        // The copy has a reader below it: the table a sink creates is built from what the pipeline
        // publishes, worked forward from this copy. Moving it under a running job therefore moves what
        // that job was assembled from - the record would describe one shape while the job produces
        // another, and everything read off the record afterwards describes a pipeline that is not the
        // one running. Same refusal accepting has, and for the same reason.
        InMemoryStorePort store = seeded();
        new StoreBackedDagSource(store).dagFor("wide");
        store.desired().save(new DesiredState("wide", PipelineState.RUNNING, "revision"));

        assertThatThrownBy(() -> new StoreBackedDerivedSchemas(store, auditGate).derive("wide"))
                .isInstanceOfSatisfying(TapstateException.class, error -> {
                    assertThat(error.code().code()).isEqualTo("actuation.schema-sync-while-running");
                    assertThat(error.args()).containsEntry("state", "NEW")
                            .containsEntry("desired", "RUNNING");
                });
    }

    @Test
    @DisplayName("an apply re-copies the physical model without holding any step to what it recorded")
    void anApplyRecopiesThePhysicalModelWithoutHoldingAnyStepToIt() {
        // What apply triggers, and what it must not: the copy follows the source, and the difference the
        // join now shows is left standing rather than absorbed. Absorbing it here would mean an author
        // applying an unrelated edit turns off the check on a pipeline nobody looked at.
        InMemoryStorePort store = seeded();
        new StoreBackedDagSource(store).dagFor("wide");
        widenTheFactKeyColumn(store);

        new StoreBackedDerivedSchemas(store, auditGate).derive("wide");

        assertThat(store.derivedSchemas().latest("wide", "orders_src.orders"))
                .get().extracting(recorded -> recorded.schema().get("id")).isEqualTo("DECIMAL NULL");
        assertThat(store.derivedSchemas().latest("wide", "widen"))
                .get().extracting(recorded -> recorded.schema().get("order_id")).isEqualTo("INT64 NULL");
    }

    @Test
    @DisplayName("a half-finished re-derivation reads as a difference, and running the sync again finishes it")
    void aHalfFinishedRederivationReadsAsADifference() {
        // A sync re-records one step at a time, so a failure part way through leaves the steps below the
        // failure at their old shape. That state is not hidden: the report shows the un-redone step as
        // drifted, exactly as it shows a source that moved, and a second sync completes it. Standing in
        // for the failure is the copy-only path, which is the same half-done state reached deliberately.
        InMemoryStorePort store = seeded();
        new StoreBackedDagSource(store).dagFor("wide");
        widenTheFactKeyColumn(store);
        new StoreBackedDerivedSchemas(store, auditGate).derive("wide");

        List<DerivedSchemas.StepReport> partial =
                new StoreBackedDerivedSchemas(store, auditGate).compare("wide");
        assertThat(partial).singleElement().satisfies(step -> assertThat(step.columns())
                .filteredOn(column -> column.column().equals("order_id"))
                .singleElement()
                .satisfies(column -> assertThat(column.drifted()).isTrue()));

        new StoreBackedDerivedSchemas(store, auditGate).accept("alice", "wide");

        assertThat(new StoreBackedDerivedSchemas(store, auditGate).compare("wide"))
                .singleElement()
                .satisfies(step -> assertThat(step.columns()).noneMatch(DerivedSchemas.ColumnReport::drifted));
    }

    /** The recorded declared type of the join's key column, which is what every report above turns on. */
    private static String recordedKeyOf(List<DerivedSchemas.StepReport> report) {
        assertThat(report).hasSize(1);
        return report.get(0).columns().stream()
                .filter(column -> column.column().equals("order_id"))
                .findFirst()
                .orElseThrow()
                .recorded();
    }

    /** A job is carrying the pipeline, and is meant to be. */
    private static void running(InMemoryStorePort store) {
        store.state().create("wide", StateJson.of(PipelineState.RUNNING), Instant.EPOCH);
        store.desired().save(new DesiredState("wide", PipelineState.RUNNING, "revision"));
    }

    /** Suspended, with nothing asking for it back. */
    private static void pausedAndStayingThere(InMemoryStorePort store) {
        store.state().create("wide", StateJson.of(PipelineState.PAUSED), Instant.EPOCH);
        store.desired().save(new DesiredState("wide", PipelineState.PAUSED, "revision"));
    }

    /** Suspended, with a resume already asked for - the job is about to carry on where it left off. */
    private static void pausedWithAResumeAskedFor(InMemoryStorePort store) {
        store.state().create("wide", StateJson.of(PipelineState.PAUSED), Instant.EPOCH);
        store.desired().save(new DesiredState("wide", PipelineState.RUNNING, "revision"));
    }

    /**
     * Widens the fact key's declared type, which moves the derived output column with it. The tapstate
     * type is what the derivation reads, so a fixture that moved only the source's own spelling would
     * change nothing at all and this whole test would pass by accident.
     */
    private static void widenTheFactKeyColumn(InMemoryStorePort store) {
        store.schemas().save(new DiscoveredSourceModel("orders_src", "mysql", 0L,
                new SourceModel(List.of(new SourceTable("orders",
                        List.of(new SourceField("id", "decimal", TapstateType.DECIMAL),
                                new SourceField("customer_ref", "bigint", TapstateType.INT64)),
                        List.of("id"), null)))));
    }

    private static void discoverTarget(InMemoryStorePort store) {
        discoverTargetAs(store, "orders_dest", "bigint");
    }

    /** Records one target connection as holding an {@code orders} table with the given key type. */
    private static void discoverTargetAs(InMemoryStorePort store, String connectionId, String keyType) {
        store.schemas().save(new DiscoveredSourceModel(connectionId, "mongodb", 0L,
                new SourceModel(List.of(new SourceTable("orders",
                        List.of(new SourceField("order_id", keyType, TapstateType.INT64),
                                new SourceField("customer_name", "varchar(50)", TapstateType.STRING)),
                        List.of("order_id"), null)))));
    }

    private static InMemoryStorePort seeded() {
        return seeded(JOIN_PIPELINE);
    }

    private static InMemoryStorePort seeded(String joinPipeline) {
        DslParser parser = new DslParser();
        List<Resource> resources = new ArrayList<>();
        for (String document : List.of(ORDERS_SRC, CUSTOMERS_SRC, TARGET, TARGET_2, joinPipeline,
                PLAIN_PIPELINE)) {
            resources.add(parser.parse(document));
        }
        Workspace.of(resources);
        InMemoryStorePort store = new InMemoryStorePort();
        resources.forEach(store.artifacts()::save);
        OpenRingGenerations.forSources(store, "orders_src", "customers_src");
        store.schemas().save(new DiscoveredSourceModel("orders_src", "mysql", 0L,
                new SourceModel(List.of(new SourceTable("orders",
                        List.of(new SourceField("id", "bigint", TapstateType.INT64),
                                new SourceField("customer_ref", "bigint", TapstateType.INT64)),
                        List.of("id"), null)))));
        store.schemas().save(new DiscoveredSourceModel("customers_src", "mysql", 0L,
                new SourceModel(List.of(new SourceTable("customers",
                        List.of(new SourceField("cust_ref", "bigint", TapstateType.INT64),
                                new SourceField("name", "varchar", TapstateType.STRING)),
                        List.of("cust_ref"), null)))));
        return store;
    }

    private static final String ORDERS_SRC = """
            version: tapstate/v1
            kind: source
            id: orders_src
            connector: mysql
            config: { host: h }
            mode: cdc
            tables: [ orders ]
            """;

    private static final String CUSTOMERS_SRC = """
            version: tapstate/v1
            kind: source
            id: customers_src
            connector: mysql
            config: { host: h }
            mode: cdc
            tables: [ customers ]
            """;

    private static final String TARGET = """
            version: tapstate/v1
            kind: source
            id: orders_dest
            connector: mongodb
            config: { uri: u }
            """;

    private static final String TARGET_2 = """
            version: tapstate/v1
            kind: source
            id: orders_dest_2
            connector: mongodb
            config: { uri: u2 }
            """;

    private static final String TWO_TARGET_PIPELINE = """
            version: tapstate/v1
            kind: pipeline
            id: wide
            source: [ orders_src, customers_src ]
            transforms:
              - id: widen
                type: join
                from: { o: orders, c: customers }
                engine: builtin
                sql: |
                  SELECT o.id AS order_id, c.name AS customer_name
                  FROM o LEFT JOIN c ON o.customer_ref = c.cust_ref
            serve:
              from: widen
              sync:
                - { id: sync_1, source: orders_dest }
                - { id: sync_2, source: orders_dest_2 }
            """;

    private static final String JOIN_PIPELINE = """
            version: tapstate/v1
            kind: pipeline
            id: wide
            source: [ orders_src, customers_src ]
            transforms:
              - id: widen
                type: join
                from: { o: orders, c: customers }
                engine: builtin
                sql: |
                  SELECT o.id AS order_id, c.name AS customer_name
                  FROM o LEFT JOIN c ON o.customer_ref = c.cust_ref
            serve:
              from: widen
              sync: [ { id: sync_1, source: orders_dest } ]
            """;

    private static final String PLAIN_PIPELINE = """
            version: tapstate/v1
            kind: pipeline
            id: plain
            source: [ orders_src ]
            serve:
              from: orders
              sync: [ { id: sync_p, source: orders_dest } ]
            """;
}
