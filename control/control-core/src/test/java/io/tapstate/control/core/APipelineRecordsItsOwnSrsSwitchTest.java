package io.tapstate.control.core;

import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.canonical.CanonicalWriter;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.AuditRecord;
import io.tapstate.spi.store.AuditStore;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A pipeline records its own srs switch for every source it reads, and the runtime reads only that.
 *
 * <p>Changing a source's srs configuration used to move every pipeline reading it at once -- a
 * consent nobody could give per pipeline. The switch now lives on each pipeline's own reference to
 * that source, materialized from the source the first time that reference is applied and following
 * only the file after. These cases pin the four-row rule and both of its corollaries.
 *
 * <p>The row that is easiest to get wrong is the last one: re-applying an unchanged file has to be a
 * no-op. It only is if materialization happens before the content hash is taken -- otherwise the
 * incoming draft (no switch) and the stored artifact (with one) hash differently, every apply looks
 * like an edit, and each rewrites the artifact without the switch and materializes it again.
 */
class APipelineRecordsItsOwnSrsSwitchTest {

    private static final Clock FIXED_CLOCK = Clock.fixed(Instant.parse("2026-09-03T10:15:30Z"), ZoneOffset.UTC);

    private final InMemoryArtifactStore store = new InMemoryArtifactStore();
    private final ApplyService service = new ApplyService(
            TapstateCatalog::load, store, new AuditGate(new DiscardingAuditStore(), FIXED_CLOCK),
            new EmptySchemaStore(), PlanAdvisories.none());

    /** A cdc source that buffers through the shared replay store. */
    private static final String BUFFERED = """
            version: tapstate/v1
            kind: source
            id: orders_src
            connector: mysql
            mode: cdc
            config: { host: 10.30.0.5, username: writer, password: My_2026 }
            tables: [orders]
            srs: { enabled: true }
            """;

    /** The same source with buffering off. */
    private static final String UNBUFFERED = BUFFERED.replace("enabled: true", "enabled: false");

    /** A second cdc source, buffered, for the multi-source rows. */
    private static final String SECOND = """
            version: tapstate/v1
            kind: source
            id: cust_src
            connector: postgres
            mode: cdc
            config: { host: 10.30.0.9, database: crm, username: writer, password: Pg_2026 }
            tables: [customers]
            srs: { enabled: false }
            """;

    private static String pipelineReading(String sourceClause) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: p1
                source: %s
                view:
                  id: p1
                  from: orders
                  primary_key: id
                  storage: { warm: { collection: p1 } }
                """.formatted(sourceClause);
    }

    // ---- row 1: first apply, the author wrote no switch ---------------------------------------

    @Test
    void firstApplyTakesTheSwitchFromTheSourceWhenTheAuthorWroteNone() {
        service.apply("author", List.of(draft(UNBUFFERED), draft(pipelineReading("orders_src"))));

        assertThat(switchOn("p1", "orders_src")).isFalse();
    }

    // ---- row 2: first apply, the author wrote one -------------------------------------------

    @Test
    void firstApplyKeepsTheSwitchTheAuthorWroteEvenWhereItDisagreesWithTheSource() {
        service.apply("author", List.of(
                draft(BUFFERED), draft(pipelineReading("[ { id: orders_src, srs: false } ]"))));

        assertThat(switchOn("p1", "orders_src")).isFalse();
    }

    // ---- row 3: later apply, the file changed it ---------------------------------------------

    @Test
    void aLaterApplyTakesAChangedSwitchFromTheFile() {
        service.apply("author", List.of(draft(BUFFERED), draft(pipelineReading("orders_src"))));
        assertThat(switchOn("p1", "orders_src")).isTrue();

        service.apply("author", List.of(
                draft(BUFFERED), draft(pipelineReading("[ { id: orders_src, srs: false } ]"))));

        assertThat(switchOn("p1", "orders_src")).isFalse();
    }

    // ---- row 4: later apply, unchanged or absent --------------------------------------------

    @Test
    void reApplyingTheSameFilesWritesNothingRatherThanReMaterializing() {
        List<ArtifactDraft> batch = List.of(draft(BUFFERED), draft(pipelineReading("orders_src")));
        service.apply("author", batch);
        int afterFirst = store.saveCount;

        service.apply("author", batch);

        assertThat(store.saveCount)
                .as("re-applying unedited files must be a no-op, not a rewrite plus a fresh materialization")
                .isEqualTo(afterFirst);
        assertThat(switchOn("p1", "orders_src")).isTrue();
    }

    /**
     * The case the whole field exists for: the source moves, and a pipeline that already recorded its
     * own switch does not move with it. This is also the one case that cannot pass unless the already
     * recorded value is read back from the store -- materializing from the source every time would
     * silently re-route this pipeline.
     */
    @Test
    void changingTheSourceLeavesAPipelineThatAlreadyRecordedItsOwnSwitchAlone() {
        service.apply("author", List.of(draft(BUFFERED), draft(pipelineReading("orders_src"))));

        service.apply("author", List.of(draft(UNBUFFERED), draft(pipelineReading("orders_src"))));

        assertThat(switchOn("p1", "orders_src"))
                .as("the source moved to false, but this pipeline pinned true on its first apply")
                .isTrue();
    }

    @Test
    void removingTheKeyFromTheFileKeepsTheStoredValueRatherThanUnpinningIt() {
        service.apply("author", List.of(
                draft(BUFFERED), draft(pipelineReading("[ { id: orders_src, srs: false } ]"))));

        service.apply("author", List.of(draft(BUFFERED), draft(pipelineReading("orders_src"))));

        assertThat(switchOn("p1", "orders_src"))
                .as("deleting the key is 'not written', which keeps the stored value -- unpinning would be invisible")
                .isFalse();
    }

    /**
     * The three segments walked consecutively in one case, which is the only shape that discriminates.
     *
     * <p>Split across three cases, an implementation that re-takes the source's value on every apply
     * passes the first and the third -- it lands the right answer for the wrong reason -- and only the
     * middle segment, reached from a state the first segment produced, tells them apart. The middle
     * segment re-applies a file whose bytes did not change while the source underneath it did.
     */
    @Test
    void theSwitchIsTakenOnceThenFollowsTheFileAndNotTheSource() {
        // 1. first creation, nothing written on the reference -> the source's value at that moment.
        service.apply("author", List.of(draft(BUFFERED), draft(pipelineReading("orders_src"))));
        assertThat(switchOn("p1", "orders_src"))
                .as("segment 1: materialized from the source, which says buffered")
                .isTrue();

        // 2. the source moves, the pipeline file does not -> the recorded value stands.
        service.apply("author", List.of(draft(UNBUFFERED), draft(pipelineReading("orders_src"))));
        assertThat(switchOn("p1", "orders_src"))
                .as("segment 2: the same unedited file re-applied over a moved source does not re-take it")
                .isTrue();

        // 3. the file writes it -> the file wins.
        service.apply("author", List.of(
                draft(UNBUFFERED), draft(pipelineReading("[ { id: orders_src, srs: false } ]"))));
        assertThat(switchOn("p1", "orders_src"))
                .as("segment 3: written on the reference, so the file decides")
                .isFalse();
    }

    // ---- corollary: 'first' is judged per source, not per pipeline ---------------------------

    @Test
    void aSourceAddedLaterMaterializesOnItsOwnWhileTheOthersKeepWhatTheyHave() {
        service.apply("author", List.of(draft(BUFFERED), draft(pipelineReading("orders_src"))));
        assertThat(switchOn("p1", "orders_src")).isTrue();

        // orders_src moves to false after p1 pinned it; cust_src has never been referenced by p1.
        service.apply("author", List.of(draft(UNBUFFERED), draft(SECOND), draft(twoSourcePipeline())));

        assertThat(switchOn("p1", "orders_src"))
                .as("already recorded, so the source's move does not reach it")
                .isTrue();
        assertThat(switchOn("p1", "cust_src"))
                .as("a new reference is a first creation for that source, so it takes the source's value")
                .isFalse();
    }

    private static String twoSourcePipeline() {
        return """
                version: tapstate/v1
                kind: pipeline
                id: p1
                source: [ orders_src, cust_src ]
                transforms:
                  - id: joined
                    type: join
                    from: { o: orders, c: customers }
                    engine: duckdb
                    sql: SELECT o.id AS id FROM o JOIN c ON c.id = o.customer_id
                view:
                  id: p1
                  from: joined
                  primary_key: id
                  storage: { warm: { collection: p1 } }
                """;
    }

    // ---- helpers -----------------------------------------------------------------------------

    /** The switch this pipeline recorded for that source, read back off what was actually stored. */
    private Boolean switchOn(String pipelineId, String sourceId) {
        Resource stored = store.get(pipelineId).orElseThrow();
        io.tapstate.core.model.PipelineResource pipeline = (io.tapstate.core.model.PipelineResource) stored;
        for (io.tapstate.core.model.SourceRef ref : pipeline.sources()) {
            if (ref.id().equals(sourceId)) {
                assertThat(ref)
                        .as("no switch was recorded for %s on %s", sourceId, pipelineId)
                        .isInstanceOf(io.tapstate.core.model.SourceRef.Spec.class);
                return ((io.tapstate.core.model.SourceRef.Spec) ref).srs();
            }
        }
        throw new AssertionError(pipelineId + " does not read " + sourceId);
    }

    private static ArtifactDraft draft(String yaml) {
        return new ArtifactDraft(null, yaml);
    }

    private static final class DiscardingAuditStore implements AuditStore {
        @Override
        public void record(AuditRecord record) {
        }
    }

    /** Keeps canonical text and re-parses on read, the way the real store round-trips. */
    private static final class InMemoryArtifactStore implements ArtifactStore {
        private final CanonicalWriter writer = new CanonicalWriter();
        private final DslParser parser = new DslParser();
        private final Map<String, String> byId = new LinkedHashMap<>();
        int saveCount = 0;

        @Override
        public void saveAll(List<Resource> artifacts) {
            for (Resource artifact : artifacts) {
                byId.put(artifact.id(), writer.write(artifact));
            }
            saveCount += artifacts.size();
        }

        @Override
        public Optional<Resource> get(String id) {
            String canonical = byId.get(id);
            return canonical == null ? Optional.empty() : Optional.of(parser.parse(canonical));
        }

        @Override
        public List<Resource> list() {
            List<Resource> resources = new ArrayList<>();
            for (String canonical : byId.values()) {
                resources.add(parser.parse(canonical));
            }
            return resources;
        }
    }
}
