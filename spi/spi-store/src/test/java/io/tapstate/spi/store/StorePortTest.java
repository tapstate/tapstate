package io.tapstate.spi.store;

import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.event.ChainPosition;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.catalog.ConnectorCatalogEntry;
import io.tapstate.core.lifecycle.CasOutcome;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.EpochCas;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.ViewResource;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * The store port seen through an in-memory implementation: proves the persistence contract is
 * implementable and usable, and pins the shape it documents — an artifact truth layer
 * (save / get / list of canonical resources), a state store whose only write path is the
 * epoch-fencing compare-and-swap, a plain-upsert desired-intent store, a connection catalog store, a
 * discovered source-schema store, a connector distribution registry, the derived connector catalog
 * store, the content-addressed connector spec source store, the latest connection-test result store, a
 * plain-upsert per-pipeline observation store, and the SRS meta store.
 */
class StorePortTest {

    private static final Instant T0 = Instant.parse("2026-07-03T00:00:00Z");
    private static final Instant T1 = Instant.parse("2026-07-03T00:00:01Z");

    private static SourceResource source(String id, String connector) {
        return new SourceResource(id, null, connector, Map.of(), null, null, null, null, null);
    }

    // --- facade ---

    @Test
    void facadeExposesTheElevenStores() {
        StorePort store = new InMemoryStore();

        assertThat(store.artifacts()).isNotNull();
        assertThat(store.state()).isNotNull();
        assertThat(store.desired()).isNotNull();
        assertThat(store.catalog()).isNotNull();
        assertThat(store.schemas()).isNotNull();
        assertThat(store.connectors()).isNotNull();
        assertThat(store.connectorCatalog()).isNotNull();
        assertThat(store.connectorSpecs()).isNotNull();
        assertThat(store.connectionTestResults()).isNotNull();
        assertThat(store.observations()).isNotNull();
        assertThat(store.meta()).isNotNull();
    }

    // --- artifacts (the canonical truth layer) ---

    @Test
    void artifactSaveThenGetRoundTrips() {
        ArtifactStore artifacts = new InMemoryStore().artifacts();
        Resource orders = source("orders", "mysql");

        artifacts.save(orders);

        assertThat(artifacts.get("orders")).contains(orders);
    }

    @Test
    void artifactGetAbsentIsEmpty() {
        assertThat(new InMemoryStore().artifacts().get("missing")).isEmpty();
    }

    @Test
    void artifactSaveUpsertsById() {
        ArtifactStore artifacts = new InMemoryStore().artifacts();
        artifacts.save(source("orders", "mysql"));
        artifacts.save(source("orders", "postgres"));

        assertThat(((SourceResource) artifacts.get("orders").orElseThrow()).connector()).isEqualTo("postgres");
        assertThat(artifacts.list()).hasSize(1);
    }

    @Test
    void artifactListReturnsEverySaved() {
        ArtifactStore artifacts = new InMemoryStore().artifacts();
        artifacts.save(source("orders", "mysql"));
        artifacts.save(new ViewResource("mdm", null, null, null, null, null));

        assertThat(artifacts.list()).extracting(Resource::id).containsExactlyInAnyOrder("orders", "mdm");
    }

    @Test
    void artifactSaveAllUpsertsEveryResourceById() {
        ArtifactStore artifacts = new InMemoryStore().artifacts();

        artifacts.saveAll(List.of(source("orders", "mysql"), new ViewResource("mdm", null, null, null, null, null)));

        assertThat(artifacts.list()).extracting(Resource::id).containsExactlyInAnyOrder("orders", "mdm");
        // A second batch upserts by id in place rather than accumulating documents.
        artifacts.saveAll(List.of(source("orders", "postgres")));
        assertThat(((SourceResource) artifacts.get("orders").orElseThrow()).connector()).isEqualTo("postgres");
        assertThat(artifacts.list()).hasSize(2);
    }

    @Test
    void artifactSaveAllOfAnEmptyBatchWritesNothing() {
        ArtifactStore artifacts = new InMemoryStore().artifacts();

        artifacts.saveAll(List.of());

        assertThat(artifacts.list()).isEmpty();
    }

    // --- state (the epoch-fencing compare-and-swap) ---

    @Test
    void stateCreateThenReadRoundTrips() {
        StateStore state = new InMemoryStore().state();

        state.create("p1", "NEW", T0);

        assertThat(state.read("p1")).contains(CheckpointDoc.initial("p1", "NEW", T0));
    }

    @Test
    void stateReadAbsentIsEmpty() {
        assertThat(new InMemoryStore().state().read("p1")).isEmpty();
    }

    @Test
    void stateCompareAndSwapAppliesAndBumpsEpochOnMatchingEpoch() {
        StateStore state = new InMemoryStore().state();
        state.create("p1", "NEW", T0);

        CasOutcome outcome = state.compareAndSwap("p1", 0, "RUNNING", T1);

        assertThat(outcome).isInstanceOf(CasOutcome.Applied.class);
        CheckpointDoc next = ((CasOutcome.Applied) outcome).next();
        assertThat(next.epoch()).isEqualTo(1);
        assertThat(next.stateJson()).isEqualTo("RUNNING");
        assertThat(state.read("p1")).contains(next);
    }

    @Test
    void stateCompareAndSwapIsFencedByAStaleEpoch() {
        StateStore state = new InMemoryStore().state();
        state.create("p1", "NEW", T0);
        state.compareAndSwap("p1", 0, "RUNNING", T1); // the epoch is now 1

        CasOutcome outcome = state.compareAndSwap("p1", 0, "PAUSED", T1); // a stale writer still at epoch 0

        assertThat(outcome).isInstanceOf(CasOutcome.Fenced.class);
        assertThat(((CasOutcome.Fenced) outcome).currentEpoch()).isEqualTo(1);
        assertThat(state.read("p1").orElseThrow().stateJson()).isEqualTo("RUNNING"); // the loser never overwrote
    }

    @Test
    void stateCreateIsInsertOnlyAndDoesNotResetTheEpoch() {
        StateStore state = new InMemoryStore().state();
        state.create("p1", "NEW", T0);
        state.compareAndSwap("p1", 0, "RUNNING", T1); // the epoch is now 1

        state.create("p1", "NEW", T0); // a second seed must not overwrite the advanced checkpoint

        CheckpointDoc stored = state.read("p1").orElseThrow();
        assertThat(stored.epoch()).isEqualTo(1); // the fence was not reset
        assertThat(stored.stateJson()).isEqualTo("RUNNING");
    }

    // --- catalog (connection / connector-instance config) ---

    @Test
    void catalogSaveThenGetRoundTrips() {
        CatalogStore catalog = new InMemoryStore().catalog();
        ConnectionConfig conn = new ConnectionConfig("orders-db", "mysql", Map.of("host", "db"));

        catalog.save(conn);

        assertThat(catalog.get("orders-db")).contains(conn);
    }

    @Test
    void catalogGetAbsentIsEmpty() {
        assertThat(new InMemoryStore().catalog().get("missing")).isEmpty();
    }

    @Test
    void catalogListReturnsEverySaved() {
        CatalogStore catalog = new InMemoryStore().catalog();
        catalog.save(new ConnectionConfig("orders-db", "mysql", Map.of()));
        catalog.save(new ConnectionConfig("events", "kafka", Map.of()));

        assertThat(catalog.list()).extracting(ConnectionConfig::id).containsExactlyInAnyOrder("orders-db", "events");
    }

    // --- schemas (discovered source models) ---

    @Test
    void schemaSaveThenGetRoundTrips() {
        SchemaStore schemas = new InMemoryStore().schemas();
        DiscoveredSourceModel discovered = new DiscoveredSourceModel(
                "orders-db",
                "mysql",
                1L,
                new SourceModel(List.of(
                        new SourceTable("orders", List.of(new SourceField("id", "bigint")), List.of("id"), List.of()))));

        schemas.save(discovered);

        assertThat(schemas.get("orders-db")).contains(discovered);
    }

    @Test
    void schemaGetAbsentConnectionIsEmpty() {
        assertThat(new InMemoryStore().schemas().get("never-discovered")).isEmpty();
    }

    @Test
    void reDiscoveryReplacesTheStoredModelInPlace() {
        SchemaStore schemas = new InMemoryStore().schemas();
        schemas.save(new DiscoveredSourceModel("orders-db", "mysql", 1L, new SourceModel(List.of(
                new SourceTable("orders", List.of(), List.of(), List.of())))));
        DiscoveredSourceModel rediscovered = new DiscoveredSourceModel(
                "orders-db",
                "mysql",
                2L,
                new SourceModel(List.of(
                        new SourceTable("orders", List.of(new SourceField("id", "bigint")), List.of("id"), List.of()),
                        new SourceTable("customers", List.of(), List.of(), List.of()))));

        schemas.save(rediscovered);

        assertThat(schemas.get("orders-db")).contains(rediscovered);
    }

    // --- connectors (the distribution registry) ---

    @Test
    void connectorRegisterIsContentHashIdempotent() {
        ConnectorRegistry connectors = new InMemoryStore().connectors();
        byte[] jar = "jar-bytes".getBytes(StandardCharsets.UTF_8);

        RegistrationOutcome first = connectors.register("mysql", "1.3.5", RegistrationSource.REGISTER, jar);
        RegistrationOutcome again = connectors.register("mysql", "1.3.5", RegistrationSource.REGISTER, jar);

        assertThat(first.newlyRegistered()).isTrue();
        assertThat(again.newlyRegistered()).isFalse();
        assertThat(again.registration()).isEqualTo(first.registration());
    }

    @Test
    void connectorRegisterStoresRetrievableArtifactBytes() {
        ConnectorRegistry connectors = new InMemoryStore().connectors();
        byte[] jar = "jar-bytes".getBytes(StandardCharsets.UTF_8);

        RegistrationOutcome outcome = connectors.register("mysql", "1.3.5", RegistrationSource.SEED, jar);

        assertThat(connectors.artifact(outcome.registration().contentHash()).orElseThrow()).isEqualTo(jar);
    }

    @Test
    void connectorListReturnsEveryRegistered() {
        ConnectorRegistry connectors = new InMemoryStore().connectors();
        connectors.register("mysql", "1.3.5", RegistrationSource.SEED, "a".getBytes(StandardCharsets.UTF_8));
        connectors.register("postgres", "1.3.5", RegistrationSource.REGISTER, "b".getBytes(StandardCharsets.UTF_8));

        assertThat(connectors.list())
                .extracting(ConnectorRegistration::connectorId)
                .containsExactlyInAnyOrder("mysql", "postgres");
    }

    // --- connector catalog (derived rows, latest-only per connector id) ---

    private static ConnectorCatalogEntry row(String id, String displayName) {
        // Only the identity and a distinguishing field matter to the store contract; the row's
        // capability fields are the catalog assembler's concern, exercised where a real row is built.
        return new ConnectorCatalogEntry(id, id, displayName, null, null, List.of(), null, null, false, List.of(), null);
    }

    @Test
    void connectorCatalogUpsertThenGetRoundTrips() {
        ConnectorCatalogStore rows = new InMemoryStore().connectorCatalog();
        ConnectorCatalogEntry mysql = row("mysql", "MySQL");

        rows.upsert(mysql);

        assertThat(rows.get("mysql")).contains(mysql);
    }

    @Test
    void connectorCatalogGetAbsentIsEmpty() {
        assertThat(new InMemoryStore().connectorCatalog().get("never-registered")).isEmpty();
    }

    @Test
    void connectorCatalogListReturnsEveryUpserted() {
        ConnectorCatalogStore rows = new InMemoryStore().connectorCatalog();
        rows.upsert(row("mysql", "MySQL"));
        rows.upsert(row("postgres", "PostgreSQL"));

        assertThat(rows.list()).extracting(ConnectorCatalogEntry::id).containsExactlyInAnyOrder("mysql", "postgres");
    }

    @Test
    void reRegisterReplacesTheDerivedRowInPlace() {
        ConnectorCatalogStore rows = new InMemoryStore().connectorCatalog();
        rows.upsert(row("mysql", "MySQL"));
        ConnectorCatalogEntry reDerived = row("mysql", "MySQL (v2)");

        rows.upsert(reDerived);

        assertThat(rows.get("mysql")).contains(reDerived);
        assertThat(rows.list()).hasSize(1);
    }

    // --- connector spec sources (content-addressed, byte-exact) ---

    @Test
    void connectorSpecPutThenGetRoundTripsTheBytesExactly() {
        // The source is kept because the derived row is a lossy projection of it. A store that returned
        // an equivalent re-encoding would defeat the point, so the contract is the bytes themselves.
        ConnectorSpecStore specs = new InMemoryStore().connectorSpecs();
        byte[] source = "{\"properties\":{\"id\":\"mysql\"},\"zz\":1,\"a\":2}".getBytes(StandardCharsets.UTF_8);

        specs.put("sha256:2f1a", source);

        assertThat(specs.get("sha256:2f1a")).contains(source);
    }

    @Test
    void connectorSpecGetAbsentIsEmpty() {
        // Distinct from a stored-but-unreadable source: nothing filed under the hash is a plain absence,
        // which the read face states as such rather than treating as a failure.
        assertThat(new InMemoryStore().connectorSpecs().get("sha256:never-stored")).isEmpty();
    }

    @Test
    void connectorSpecPresenceAgreesWithWhatIsStored() {
        // Presence is a question a caller asks instead of reading a whole connector form back. It has to
        // answer what get() would: a presence test that disagreed would send a re-register into either
        // rewriting what is filed or skipping a source that is not there.
        ConnectorSpecStore specs = new InMemoryStore().connectorSpecs();
        specs.put("sha256:2f1a", "{\"properties\":{\"id\":\"mysql\"}}".getBytes(StandardCharsets.UTF_8));

        assertThat(specs.has("sha256:2f1a")).isTrue();
        assertThat(specs.has("sha256:never-stored")).isFalse();
    }

    @Test
    void connectorSpecPutIsIdempotentUnderItsContentHash() {
        // Content-addressed: a second write under a hash carries the same bytes by construction, so
        // re-registering the same connector replaces in place instead of accumulating copies.
        ConnectorSpecStore specs = new InMemoryStore().connectorSpecs();
        byte[] source = "{\"properties\":{\"id\":\"mysql\"}}".getBytes(StandardCharsets.UTF_8);

        specs.put("sha256:2f1a", source);
        specs.put("sha256:2f1a", source);

        assertThat(specs.get("sha256:2f1a")).contains(source);
    }

    // --- connection test results (latest-only per connection) ---

    private static ConnectionTestResult passed(String connectionId, String connectorId) {
        return new ConnectionTestResult(
                connectionId,
                connectorId,
                ConnectionTestResult.Outcome.PASSED,
                List.of(new ConnectionTestItem("Connection", ConnectionTestItem.Status.PASSED, null, null, null, null)),
                1783939200000L);
    }

    @Test
    void connectionTestResultSaveThenFindRoundTrips() {
        ConnectionTestResultStore results = new InMemoryStore().connectionTestResults();
        ConnectionTestResult result = passed("orders-db", "mysql");

        results.save(result);

        assertThat(results.find("orders-db")).contains(result);
    }

    @Test
    void connectionTestResultFindAbsentConnectionIsEmpty() {
        assertThat(new InMemoryStore().connectionTestResults().find("never-tested")).isEmpty();
    }

    @Test
    void reTestReplacesTheStoredResultInPlace() {
        ConnectionTestResultStore results = new InMemoryStore().connectionTestResults();
        results.save(passed("orders-db", "mysql"));
        ConnectionTestResult reTested = new ConnectionTestResult(
                "orders-db",
                "mysql",
                ConnectionTestResult.Outcome.FAILED,
                List.of(new ConnectionTestItem(
                        "Login", ConnectionTestItem.Status.FAILED, "auth failed", null, null, "11000")),
                1783939300000L);

        results.save(reTested);

        assertThat(results.find("orders-db")).contains(reTested);
    }

    // --- desired (the plain-upsert desired-intent store) ---

    @Test
    void desiredSaveThenReadRoundTrips() {
        DesiredStore desired = new InMemoryStore().desired();
        DesiredState want = new DesiredState("p1", PipelineState.RUNNING, "rev-abc");

        desired.save(want);

        assertThat(desired.read("p1")).contains(want);
    }

    @Test
    void desiredReadAbsentIsEmpty() {
        assertThat(new InMemoryStore().desired().read("p1")).isEmpty();
    }

    @Test
    void desiredSaveUpsertsByPipelineId() {
        DesiredStore desired = new InMemoryStore().desired();
        desired.save(new DesiredState("p1", PipelineState.RUNNING, "rev-1"));
        desired.save(new DesiredState("p1", PipelineState.STOPPED, "rev-2"));

        DesiredState stored = desired.read("p1").orElseThrow();
        assertThat(stored.targetState()).isEqualTo(PipelineState.STOPPED);
        assertThat(stored.revision()).isEqualTo("rev-2");
    }

    // --- observations (the plain-upsert per-pipeline observation store) ---

    @Test
    void observationSaveThenReadRoundTrips() {
        ObservationStore observations = new InMemoryStore().observations();
        Observation obs = new Observation("p1", PipelineState.RUNNING, Map.of("recordCount", 7L), Map.of());

        observations.save(obs);

        assertThat(observations.read("p1")).contains(obs);
    }

    @Test
    void observationReadAbsentIsEmpty() {
        assertThat(new InMemoryStore().observations().read("p1")).isEmpty();
    }

    @Test
    void observationSaveUpsertsByPipelineId() {
        ObservationStore observations = new InMemoryStore().observations();
        observations.save(new Observation("p1", PipelineState.RUNNING, Map.of("recordCount", 1L), Map.of()));
        observations.save(new Observation("p1", PipelineState.PAUSED, Map.of("recordCount", 2L), Map.of()));

        Observation stored = observations.read("p1").orElseThrow();
        assertThat(stored.state()).isEqualTo(PipelineState.PAUSED);
        assertThat(stored.metrics()).containsEntry("recordCount", 2L);
    }

    // --- meta (the SRS mining-chain coordination store) ---

    @Test
    void metaCreateThenReadRoundTrips() {
        SrsMetaStore meta = new InMemoryStore().meta();

        meta.create("orders@mysql-1", "7d");

        SrsMeta seeded = meta.read("orders@mysql-1").orElseThrow();
        assertThat(seeded.miningChainId()).isEqualTo("orders@mysql-1");
        assertThat(seeded.retention()).isEqualTo("7d");
        assertThat(seeded.sourceReadOffset()).isNull();
        assertThat(seeded.cdcStartPosition()).isNull();
        assertThat(seeded.consumerOffsets()).isEmpty();
        assertThat(seeded.schemaHistory()).isEmpty();
    }

    @Test
    void metaReadAbsentIsEmpty() {
        assertThat(new InMemoryStore().meta().read("never-mined")).isEmpty();
    }

    @Test
    void metaCreateIsInsertOnlyAndDoesNotDiscardAccumulatedTruth() {
        SrsMetaStore meta = new InMemoryStore().meta();
        meta.create("chain", "7d");
        meta.advanceSourceReadOffset("chain", "gtid:aaa-1:500");

        meta.create("chain", "30d"); // a second seed must not wipe the advanced offset

        assertThat(meta.read("chain").orElseThrow().sourceReadOffset()).isEqualTo("gtid:aaa-1:500");
    }

    @Test
    void metaAdvanceSourceReadOffsetPersistsTheOpaqueToken() {
        SrsMetaStore meta = new InMemoryStore().meta();
        meta.create("chain", null);

        meta.advanceSourceReadOffset("chain", "gtid:aaa-1:900");

        assertThat(meta.read("chain").orElseThrow().sourceReadOffset()).isEqualTo("gtid:aaa-1:900");
    }

    @Test
    void metaUpsertConsumerOffsetInsertsThenReplacesByPipelineId() {
        SrsMetaStore meta = new InMemoryStore().meta();
        meta.create("chain", null);

        meta.upsertConsumerOffset("chain", new ConsumerOffset("p1", Map.of("orders", 10L), null));
        meta.upsertConsumerOffset("chain", new ConsumerOffset("p2", Map.of("orders", 20L), null));
        meta.upsertConsumerOffset("chain", new ConsumerOffset("p1", Map.of("orders", 99L), new ChainPosition(new SourceOrder(1, 99), "gtid:aaa-1:99")));

        List<ConsumerOffset> cursors = meta.read("chain").orElseThrow().consumerOffsets();
        assertThat(cursors).extracting(ConsumerOffset::pipelineId).containsExactly("p1", "p2");
        ConsumerOffset p1 = cursors.stream().filter(c -> c.pipelineId().equals("p1")).findFirst().orElseThrow();
        assertThat(p1.perTableSeq()).containsEntry("orders", 99L);
        assertThat(p1.sinkAckedSrcpos()).isEqualTo("gtid:aaa-1:99");
    }

    @Test
    void metaSetCdcStartPersistsTheSeamPositionAndTheGenerationItsSnapshotIsPinnedTo() {
        SrsMetaStore meta = new InMemoryStore().meta();
        meta.create("chain", null);

        meta.setCdcStart("chain", "binlog.000042:1024", 3L);

        // One call, both fields. A restart mid-snapshot has to answer "which generation did this snapshot
        // begin in", and the seam position is the only record that it began at all -- so a store that could
        // write the position without its generation would leave a snapshot that resumes with no way to know
        // what to pin its rows to. The pair is written together because it is only ever read together.
        SrsMeta record = meta.read("chain").orElseThrow();
        assertThat(record.cdcStartPosition()).isEqualTo("binlog.000042:1024");
        assertThat(record.snapshotEpoch()).isEqualTo(3L);
    }

    @Test
    void metaOpenEpochAllocatesTheNextGenerationAndRemembersIt() {
        SrsMetaStore meta = new InMemoryStore().meta();
        meta.create("chain", null);
        assertThat(meta.read("chain").orElseThrow().epoch()).isZero();

        assertThat(meta.openEpoch("chain")).isEqualTo(1L);
        assertThat(meta.openEpoch("chain")).isEqualTo(2L);
        assertThat(meta.read("chain").orElseThrow().epoch()).isEqualTo(2L);
    }

    @Test
    void metaOpenEpochNeverRepeatsAGenerationAcrossChains() {
        SrsMetaStore meta = new InMemoryStore().meta();
        meta.create("a", null);
        meta.create("b", null);

        // Generations are per chain: an order is only ever compared against another order of the same
        // chain, and a shared counter would make one chain's restart look like progress on another's.
        assertThat(meta.openEpoch("a")).isEqualTo(1L);
        assertThat(meta.openEpoch("a")).isEqualTo(2L);
        assertThat(meta.openEpoch("b")).isEqualTo(1L);
        assertThat(meta.read("a").orElseThrow().epoch()).isEqualTo(2L);
    }

    @Test
    void metaGenerationsSurviveALaterUnrelatedMutation() {
        SrsMetaStore meta = new InMemoryStore().meta();
        meta.create("chain", null);
        long running = meta.openEpoch("chain");
        meta.setCdcStart("chain", "binlog.000042:1024", running);

        meta.markSnapshotComplete("chain", "orders");
        meta.advanceSourceReadOffset("chain", "gtid:aaa-1:900");
        meta.appendSchemaVersion("chain", new SchemaVersion(0, Map.of("id", "int"), 0));

        // Each facet is an independent writer of one record. A mutator that rebuilt the record without
        // carrying the generations through would reset them to "none opened" without failing anything of
        // its own, and every change after it would compare against a generation the chain has left behind.
        SrsMeta record = meta.read("chain").orElseThrow();
        assertThat(record.epoch()).isEqualTo(running);
        assertThat(record.snapshotEpoch()).isEqualTo(running);
    }

    @Test
    void metaOpenEpochLeavesTheSnapshotsPinnedGenerationAlone() {
        SrsMetaStore meta = new InMemoryStore().meta();
        meta.create("chain", null);
        long running = meta.openEpoch("chain");
        meta.setCdcStart("chain", "binlog.000042:1024", running);

        meta.openEpoch("chain");

        // The restart that opens generation 2 is exactly when a snapshot that had not drained must keep
        // generation 1. A store that advanced both would hand the rerun's rows the newer generation and
        // let them overwrite changes the older one had already applied.
        SrsMeta record = meta.read("chain").orElseThrow();
        assertThat(record.epoch()).isEqualTo(2L);
        assertThat(record.snapshotEpoch()).isEqualTo(1L);
    }

    @Test
    void metaAppendSchemaVersionIsAppendOnly() {
        SrsMetaStore meta = new InMemoryStore().meta();
        meta.create("chain", null);

        meta.appendSchemaVersion("chain", new SchemaVersion(0, Map.of("id", "int"), 0));
        meta.appendSchemaVersion("chain", new SchemaVersion(1, Map.of("id", "int", "name", "string"), 12));

        List<SchemaVersion> history = meta.read("chain").orElseThrow().schemaHistory();
        assertThat(history).extracting(SchemaVersion::version).containsExactly(0L, 1L);
        assertThat(history.get(1).ddlSeq()).isEqualTo(12L);
    }

    @Test
    void metaMarkSnapshotCompleteIsPerTableAndIdempotent() {
        SrsMetaStore meta = new InMemoryStore().meta();
        meta.create("chain", null);

        meta.markSnapshotComplete("chain", "orders");
        meta.markSnapshotComplete("chain", "order_items");
        meta.markSnapshotComplete("chain", "orders");

        // One chain carries many tables, each snapshotted by its own capture run, so the mark is per table
        // rather than a chain-level flag. Re-marking is set membership: a replayed or re-run snapshot marks
        // the same table again and must not accumulate entries.
        assertThat(meta.read("chain").orElseThrow().snapshotCompletedTables())
                .containsExactly("orders", "order_items");
    }

    @Test
    void metaSnapshotMarksSurviveALaterUnrelatedMutation() {
        SrsMetaStore meta = new InMemoryStore().meta();
        meta.create("chain", null);
        meta.markSnapshotComplete("chain", "orders");

        meta.advanceSourceReadOffset("chain", "gtid:aaa-1:900");
        meta.setCdcStart("chain", "binlog.000042:1024", 1L);
        meta.openEpoch("chain");
        meta.appendSchemaVersion("chain", new SchemaVersion(0, Map.of("id", "int"), 0));

        // Each facet is an independent writer of one record. A mutator that rebuilt the record without
        // carrying the marks through would erase the completion signal without failing anything of its
        // own -- and the reader that depends on it would then see a table that had drained as un-drained.
        assertThat(meta.read("chain").orElseThrow().snapshotCompletedTables()).containsExactly("orders");
    }

    @Test
    void metaMutateOnAnUnseededChainIsAnOrderingError() {
        SrsMetaStore meta = new InMemoryStore().meta();
        // every mutator requires the chain to have been seeded by create first; a mutate on an unseeded
        // chain is a caller ordering error, surfaced bare.
        assertThatThrownBy(() -> meta.advanceSourceReadOffset("nope", "x")).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> meta.upsertConsumerOffset("nope", new ConsumerOffset("p", Map.of(), null)))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> meta.setCdcStart("nope", "x", 1L)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> meta.appendSchemaVersion("nope", new SchemaVersion(0, Map.of(), 0)))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> meta.markSnapshotComplete("nope", "orders"))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> meta.openEpoch("nope")).isInstanceOf(IllegalStateException.class);
    }

    /**
     * An in-memory store: one map behind each sub-port. The state store applies the real fencing CAS,
     * so the port composes with the core checkpoint contract, not a re-implementation. The registry
     * keys artifacts by a deterministic content key (raw-byte hex here, SHA-256 in the real store), so
     * it witnesses the register-if-absent contract without depending on the hash algorithm.
     */
    private static final class InMemoryStore implements StorePort {

        private final Map<String, Resource> artifacts = new HashMap<>();
        private final Map<String, CheckpointDoc> checkpoints = new HashMap<>();
        private final Map<String, ConnectionConfig> connections = new HashMap<>();
        private final Map<String, DiscoveredSourceModel> schemas = new HashMap<>();
        private final Map<String, ConnectorRegistration> registrations = new HashMap<>();
        private final Map<String, byte[]> connectorArtifacts = new HashMap<>();
        private final Map<String, ConnectorCatalogEntry> connectorCatalogRows = new HashMap<>();
        private final Map<String, byte[]> connectorSpecs = new HashMap<>();
        private final Map<String, ConnectionTestResult> testResults = new HashMap<>();
        private final Map<String, DesiredState> desired = new HashMap<>();
        private final Map<String, Observation> observations = new HashMap<>();
        private final Map<String, SrsMeta> srsMeta = new HashMap<>();
        private final Map<String, byte[]> keyedState = new HashMap<>();

        @Override
        public ArtifactStore artifacts() {
            return new ArtifactStore() {
                @Override
                public void saveAll(List<Resource> batch) {
                    // atomic in spirit: the map puts cannot fail partway, so either the batch is applied
                    // in full or (never, here) not at all.
                    for (Resource artifact : batch) {
                        artifacts.put(artifact.id(), artifact);
                    }
                }

                @Override
                public Optional<Resource> get(String id) {
                    return Optional.ofNullable(artifacts.get(id));
                }

                @Override
                public List<Resource> list() {
                    return new ArrayList<>(artifacts.values());
                }
            };
        }

        @Override
        public StateStore state() {
            return new StateStore() {
                @Override
                public Optional<CheckpointDoc> read(String pipelineId) {
                    return Optional.ofNullable(checkpoints.get(pipelineId));
                }

                @Override
                public void create(String pipelineId, String stateJson, Instant touchTime) {
                    // insert-only: a second seed must never overwrite and reset the fencing epoch
                    checkpoints.putIfAbsent(pipelineId, CheckpointDoc.initial(pipelineId, stateJson, touchTime));
                }

                @Override
                public CasOutcome compareAndSwap(
                        String pipelineId, long expectedEpoch, String nextStateJson, Instant touchTime) {
                    CasOutcome outcome =
                            EpochCas.swap(checkpoints.get(pipelineId), expectedEpoch, nextStateJson, touchTime);
                    if (outcome instanceof CasOutcome.Applied applied) {
                        checkpoints.put(pipelineId, applied.next());
                    }
                    return outcome;
                }
            };
        }

        @Override
        public CatalogStore catalog() {
            return new CatalogStore() {
                @Override
                public void save(ConnectionConfig connection) {
                    connections.put(connection.id(), connection);
                }

                @Override
                public Optional<ConnectionConfig> get(String id) {
                    return Optional.ofNullable(connections.get(id));
                }

                @Override
                public List<ConnectionConfig> list() {
                    return new ArrayList<>(connections.values());
                }
            };
        }

        @Override
        public SchemaStore schemas() {
            return new SchemaStore() {
                @Override
                public void save(DiscoveredSourceModel discovered) {
                    schemas.put(discovered.connectionId(), discovered);
                }

                @Override
                public Optional<DiscoveredSourceModel> get(String connectionId) {
                    return Optional.ofNullable(schemas.get(connectionId));
                }
            };
        }

        @Override
        public ConnectorRegistry connectors() {
            return new ConnectorRegistry() {
                @Override
                public RegistrationOutcome register(
                        String connectorId, String pdkApiVersion, RegistrationSource source, byte[] artifact) {
                    String contentHash = HexFormat.of().formatHex(artifact);
                    ConnectorRegistration existing = registrations.get(contentHash);
                    if (existing != null) {
                        return new RegistrationOutcome(existing, false);
                    }
                    ConnectorRegistration registration =
                            new ConnectorRegistration(connectorId, contentHash, pdkApiVersion, source);
                    registrations.put(contentHash, registration);
                    connectorArtifacts.put(contentHash, artifact.clone());
                    return new RegistrationOutcome(registration, true);
                }

                @Override
                public List<ConnectorRegistration> list() {
                    return new ArrayList<>(registrations.values());
                }

                @Override
                public Optional<byte[]> artifact(String contentHash) {
                    byte[] bytes = connectorArtifacts.get(contentHash);
                    return bytes == null ? Optional.empty() : Optional.of(bytes.clone());
                }

                @Override
                public boolean hasArtifact(String contentHash) {
                    return artifact(contentHash).isPresent();
                }
            };
        }

        @Override
        public ConnectorCatalogStore connectorCatalog() {
            return new ConnectorCatalogStore() {
                @Override
                public void upsert(ConnectorCatalogEntry entry) {
                    connectorCatalogRows.put(entry.id(), entry);
                }

                @Override
                public Optional<ConnectorCatalogEntry> get(String connectorId) {
                    return Optional.ofNullable(connectorCatalogRows.get(connectorId));
                }

                @Override
                public List<ConnectorCatalogEntry> list() {
                    return new ArrayList<>(connectorCatalogRows.values());
                }
            };
        }

        @Override
        public ConnectorSpecStore connectorSpecs() {
            return new ConnectorSpecStore() {
                @Override
                public void put(String contentHash, byte[] spec) {
                    connectorSpecs.put(contentHash, spec.clone());
                }

                @Override
                public Optional<byte[]> get(String contentHash) {
                    return Optional.ofNullable(connectorSpecs.get(contentHash)).map(byte[]::clone);
                }
            };
        }

        @Override
        public ConnectionTestResultStore connectionTestResults() {
            return new ConnectionTestResultStore() {
                @Override
                public void save(ConnectionTestResult result) {
                    testResults.put(result.connectionId(), result);
                }

                @Override
                public Optional<ConnectionTestResult> find(String connectionId) {
                    return Optional.ofNullable(testResults.get(connectionId));
                }
            };
        }

        @Override
        public DesiredStore desired() {
            return new DesiredStore() {
                @Override
                public void save(DesiredState desiredState) {
                    desired.put(desiredState.pipelineId(), desiredState);
                }

                @Override
                public Optional<DesiredState> read(String pipelineId) {
                    return Optional.ofNullable(desired.get(pipelineId));
                }

                @Override
                public List<String> pipelineIds() {
                    return List.copyOf(desired.keySet());
                }
            };
        }

        @Override
        public ObservationStore observations() {
            return new ObservationStore() {
                @Override
                public void save(Observation observation) {
                    observations.put(observation.pipelineId(), observation);
                }

                @Override
                public Optional<Observation> read(String pipelineId) {
                    return Optional.ofNullable(observations.get(pipelineId));
                }
            };
        }

        @Override
        public KeyedStateStore keyedState() {
            return new KeyedStateStore() {
                @Override
                public Optional<byte[]> load(String namespace, String key) {
                    return Optional.ofNullable(keyedState.get(namespace + "/" + key));
                }

                @Override
                public void save(String namespace, String key, byte[] state) {
                    keyedState.put(namespace + "/" + key, state);
                }

                @Override
                public void delete(String namespace, String key) {
                    keyedState.remove(namespace + "/" + key);
                }

                @Override
                public void dropNamespace(String namespace) {
                    keyedState.keySet().removeIf(id -> id.startsWith(namespace + "/"));
                }

                @Override
                public long count(String namespace) {
                    return keyedState.keySet().stream().filter(id -> id.startsWith(namespace + "/")).count();
                }
            };
        }

        @Override
        public SrsMetaStore meta() {
            return new SrsMetaStore() {
                @Override
                public Optional<SrsMeta> read(String miningChainId) {
                    return Optional.ofNullable(srsMeta.get(miningChainId));
                }

                @Override
                public void create(String miningChainId, String retention) {
                    // insert-only: a second seed must never discard the accumulated offset / cursor /
                    // schema truth the chain has built up.
                    srsMeta.putIfAbsent(miningChainId,
                            new SrsMeta(miningChainId, null, List.of(), null, List.of(), retention));
                }

                @Override
                public void advanceSourceReadOffset(String miningChainId, String sourceReadOffset) {
                    SrsMeta current = require(miningChainId);
                    srsMeta.put(miningChainId, new SrsMeta(current.miningChainId(), sourceReadOffset,
                            current.consumerOffsets(), current.cdcStartPosition(), current.schemaHistory(),
                            current.retention(), current.snapshotCompletedTables(), current.epoch(), current.snapshotEpoch()));
                }

                @Override
                public void upsertConsumerOffset(String miningChainId, ConsumerOffset offset) {
                    SrsMeta current = require(miningChainId);
                    List<ConsumerOffset> merged = new ArrayList<>();
                    boolean replaced = false;
                    for (ConsumerOffset existing : current.consumerOffsets()) {
                        if (existing.pipelineId().equals(offset.pipelineId())) {
                            merged.add(offset);
                            replaced = true;
                        } else {
                            merged.add(existing);
                        }
                    }
                    if (!replaced) {
                        merged.add(offset);
                    }
                    srsMeta.put(miningChainId, new SrsMeta(current.miningChainId(), current.sourceReadOffset(),
                            merged, current.cdcStartPosition(), current.schemaHistory(), current.retention(),
                            current.snapshotCompletedTables(), current.epoch(), current.snapshotEpoch()));
                }

                @Override
                public void advanceConsumerReadSeq(String miningChainId, String pipelineId, String table, long lastReadSeq) {
                    SrsMeta current = require(miningChainId);
                    List<ConsumerOffset> merged = new ArrayList<>();
                    boolean advanced = false;
                    for (ConsumerOffset existing : current.consumerOffsets()) {
                        if (existing.pipelineId().equals(pipelineId)) {
                            Map<String, Long> perTable = new HashMap<>(existing.perTableSeq());
                            perTable.put(table, lastReadSeq);
                            // Advance the read cursor only; the consumer's sink-acked position is untouched.
                            merged.add(new ConsumerOffset(pipelineId, perTable, existing.sinkAcked()));
                            advanced = true;
                        } else {
                            merged.add(existing);
                        }
                    }
                    if (!advanced) {
                        // A reader may advance before the sink first acks: create the entry, acked absent.
                        merged.add(new ConsumerOffset(pipelineId, Map.of(table, lastReadSeq), null));
                    }
                    srsMeta.put(miningChainId, new SrsMeta(current.miningChainId(), current.sourceReadOffset(),
                            merged, current.cdcStartPosition(), current.schemaHistory(), current.retention(),
                            current.snapshotCompletedTables(), current.epoch(), current.snapshotEpoch()));
                }

                @Override
                public void advanceSinkAcked(String miningChainId, String pipelineId, ChainPosition position) {
                    SrsMeta current = require(miningChainId);
                    List<ConsumerOffset> merged = new ArrayList<>();
                    boolean advanced = false;
                    for (ConsumerOffset existing : current.consumerOffsets()) {
                        if (existing.pipelineId().equals(pipelineId)) {
                            // Advance the sink-acked position only; the consumer's read cursor is untouched.
                            merged.add(new ConsumerOffset(pipelineId, existing.perTableSeq(), position));
                            advanced = true;
                        } else {
                            merged.add(existing);
                        }
                    }
                    if (!advanced) {
                        // A sink may ack before the reader first publishes a cursor: create the entry, cursor empty.
                        merged.add(new ConsumerOffset(pipelineId, Map.of(), position));
                    }
                    srsMeta.put(miningChainId, new SrsMeta(current.miningChainId(), current.sourceReadOffset(),
                            merged, current.cdcStartPosition(), current.schemaHistory(), current.retention(),
                            current.snapshotCompletedTables(), current.epoch(), current.snapshotEpoch()));
                }

                @Override
                public void setCdcStart(String miningChainId, String cdcStartPosition, long snapshotEpoch) {
                    SrsMeta current = require(miningChainId);
                    srsMeta.put(miningChainId, new SrsMeta(current.miningChainId(), current.sourceReadOffset(),
                            current.consumerOffsets(), cdcStartPosition, current.schemaHistory(),
                            current.retention(), current.snapshotCompletedTables(), current.epoch(), snapshotEpoch));
                }

                @Override
                public long openEpoch(String miningChainId) {
                    SrsMeta current = require(miningChainId);
                    long opened = current.epoch() + 1;
                    srsMeta.put(miningChainId, new SrsMeta(current.miningChainId(), current.sourceReadOffset(),
                            current.consumerOffsets(), current.cdcStartPosition(), current.schemaHistory(),
                            current.retention(), current.snapshotCompletedTables(), opened, current.snapshotEpoch()));
                    return opened;
                }

                @Override
                public void appendSchemaVersion(String miningChainId, SchemaVersion version) {
                    SrsMeta current = require(miningChainId);
                    List<SchemaVersion> history = new ArrayList<>(current.schemaHistory());
                    history.add(version);
                    srsMeta.put(miningChainId, new SrsMeta(current.miningChainId(), current.sourceReadOffset(),
                            current.consumerOffsets(), current.cdcStartPosition(), history, current.retention(),
                            current.snapshotCompletedTables(), current.epoch(), current.snapshotEpoch()));
                }

                @Override
                public void markSnapshotComplete(String miningChainId, String table) {
                    SrsMeta current = require(miningChainId);
                    if (current.snapshotCompletedTables().contains(table)) {
                        return;
                    }
                    List<String> completed = new ArrayList<>(current.snapshotCompletedTables());
                    completed.add(table);
                    srsMeta.put(miningChainId, new SrsMeta(current.miningChainId(), current.sourceReadOffset(),
                            current.consumerOffsets(), current.cdcStartPosition(), current.schemaHistory(),
                            current.retention(), completed, current.epoch(), current.snapshotEpoch()));
                }

                private SrsMeta require(String miningChainId) {
                    SrsMeta current = srsMeta.get(miningChainId);
                    if (current == null) {
                        throw new IllegalStateException("srs meta mutate on an unseeded mining chain: "
                                + miningChainId + " (create must seed it first)");
                    }
                    return current;
                }
            };
        }
    }
}
