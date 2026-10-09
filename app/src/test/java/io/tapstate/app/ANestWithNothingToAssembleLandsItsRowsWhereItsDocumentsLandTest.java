package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.function.SupplierEx;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ReadMode;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.Settings;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TableRef;
import io.tapstate.core.model.TransformBody;
import io.tapstate.runtime.engine.SinkTarget;
import io.tapstate.spi.sink.DdlPolicy;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.TargetField;
import io.tapstate.spi.sink.TargetTable;
import io.tapstate.spi.sink.WriteMode;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SourceTable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * A nest with nothing to assemble passes its root's rows on as they are, under their own stream, and they land
 * where the nest's documents land: in the root's table, matched on the root's key, and routed among a sink's
 * writers by that table and key.
 *
 * <p>The sink is told about the nest's own stream, as for any nest, and no row arriving from this one is on it.
 * Told nothing more, its writers fall back to a bare table name carrying no key, and a sink running several
 * writers - the default - has no target to route the rows by and fails the run on the first of them.
 *
 * <p>The root's key here is not the table's primary key, so a sink that took the table's own model for these rows
 * - matching re-sent rows on a key the nest did not declare - is told apart from one that lands them as the nest's
 * documents.
 */
class ANestWithNothingToAssembleLandsItsRowsWhereItsDocumentsLandTest {

    private static final String SOURCE = "src_customers";
    private static final String TABLE = "customers";
    private static final String DEST_ID = "tgt";
    private static final String STEP = "pass";
    private static final String PIPELINE = "pass_customers";
    private static final String SINK = "serve.sync_1";

    @Test
    void theRootsRowsAreWrittenAndRoutedAsTheNestsDocuments() {
        AtomicReference<Map<String, TargetTable>> bound = new AtomicReference<>();

        DagSource.PlannedDag planned = new StoreBackedDagSource(seedStore(), capturing(bound))
                .plannedDagFor(PIPELINE, null);

        assertThat(bound.get()).as("the streams the sink's writers have a target for").containsKey(TABLE);
        TargetTable landing = bound.get().get(TABLE);
        assertThat(landing.name()).as("the table the rows land in, the root's").isEqualTo(TABLE);
        assertThat(landing.fields().stream().filter(TargetField::primaryKey).map(TargetField::name))
                .as("the key a re-sent row is matched on: the root's, as for the nest's documents")
                .containsExactly("cust_no");
        assertThat(planned.shape().isNative(SINK)).as("the sink runs its default several writers").isTrue();
        assertThat(planned.shape().sinkTargetsOf(SINK))
                .as("what the edge into the sink's writers routes each row by")
                .containsEntry(TABLE, new SinkTarget(TABLE, List.of("cust_no")));
    }

    /** Records the map handed to the sink binder without building a writer. */
    private static StoreBackedDagSource.SinkWriterBinder capturing(
            AtomicReference<Map<String, TargetTable>> bound) {
        return new StoreBackedDagSource.SinkWriterBinder() {

            @Override
            public SupplierEx<? extends SinkWriter> bind(String connectorId, Map<String, Object> settings,
                    WriteMode writeMode, DdlPolicy ddl, TargetTable target, PipelineNode node) {
                return (SupplierEx<SinkWriter>) () -> null;
            }

            @Override
            public SupplierEx<? extends SinkWriter> bind(String connectorId, Map<String, Object> settings,
                    WriteMode writeMode, DdlPolicy ddl, Map<String, TargetTable> targets, PipelineNode node) {
                bound.set(targets);
                return (SupplierEx<SinkWriter>) () -> null;
            }
        };
    }

    /** One source table whose primary key is not the nest's root key, and a root-only nest served to one sink. */
    private static InMemoryStorePort seedStore() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(new SourceResource(SOURCE, null, "fake", Map.of("host", "h"), SourceMode.CDC,
                List.of(TableRef.literal(TABLE)), null, null));
        artifacts.save(new SourceResource(DEST_ID, null, "fake", Map.of("host", "d"), null, null, null, null));
        TransformBody.Nest body = new TransformBody.Nest(null, null,
                new NestRoot("c", List.of("cust_no"), null, null, List.of()));
        Step step = Step.inline(STEP, FromClause.aliases(Map.of("c", FromRef.literal(TABLE))), body, null);
        artifacts.save(new PipelineResource(PIPELINE, null, List.of(SourceRef.spec(SOURCE, true)), List.of(step),
                null,
                new ServeBlock.Inline(null, FromRef.literal(STEP),
                        List.of(new SyncElement("sync_1", DEST_ID, null, null, null)), null, null),
                new Settings(null, null, null, null, ReadMode.SNAPSHOT_AND_CDC, "earliest"), null));

        InMemoryStorePort store = new InMemoryStorePort(artifacts);
        store.schemas().save(new DiscoveredSourceModel(SOURCE, "fake", 0L, new SourceModel(List.of(
                new SourceTable(TABLE, List.of(new SourceField("id", "int"), new SourceField("cust_no", "string"),
                        new SourceField("name", "string")), List.of("id"), List.of())))));
        return store;
    }
}
