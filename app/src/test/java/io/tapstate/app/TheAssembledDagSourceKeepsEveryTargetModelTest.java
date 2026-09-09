package io.tapstate.app;

import io.tapstate.core.model.PipelineNode;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.runtime.engine.nest.NestSettings;
import io.tapstate.spi.sink.DdlPolicy;
import io.tapstate.spi.sink.TargetField;
import io.tapstate.spi.sink.TargetTable;
import io.tapstate.spi.sink.WriteMode;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The sink binder the product assembles keeps every resolved model, however many streams reach the sink.
 *
 * <p>The binder has two shapes: one that is handed a single model and one that is handed a model per
 * stream. The single-model shape carries a default for the other, and that default has to answer the
 * question "which one of these is the model" - which for anything but exactly one stream has no answer, so
 * it answers none, and every model resolved for that pipeline is dropped on the floor.
 *
 * <p>That is a reasonable default for a binder that genuinely holds one model. It is not reasonable as the
 * binder a multi-stream product is assembled with, and it was: the constructor the runtime is wired through
 * passed a method reference, which binds to the single-model shape, so a pipeline reading two tables reached
 * its sink with nothing - no key to upsert on and no table name but the stream's own. A pipeline reading one
 * table was unaffected, which is why every witness that had ever run stayed green.
 *
 * <p>What this pins is the property rather than the wiring: given more streams than one, the models survive.
 * An implementation that fixed the constructor and left the collapsing default in the path of some other
 * caller would still fail here as soon as that caller was the one assembled.
 */
class TheAssembledDagSourceKeepsEveryTargetModelTest {

    private static final Map<String, TargetTable> TWO = Map.of(
            "orders", new TargetTable("orders", List.of(new TargetField("id", "int", true))),
            "order_items", new TargetTable("order_items", List.of(new TargetField("id", "int", true))));

    @Test
    void theBinderTheProductIsAssembledWithKeepsBothModels() {
        StoreBackedDagSource.SinkWriterBinder binder = StoreBackedDagSource.assembledSinkWriterBinder();

        Object bound = binder.bind("mongodb", Map.of(), WriteMode.UPSERT, DdlPolicy.FAIL, TWO,
                new PipelineNode("p1", "to_mongo"));

        assertThat(bound).isInstanceOf(PdkSinkWriterFactory.class);
        assertThat(((PdkSinkWriterFactory) bound).targets())
                .as("the models the sink will be handed for a pipeline whose streams are %s", TWO.keySet())
                .containsOnlyKeys("orders", "order_items");
    }

    @Test
    void aSingleModelStillReachesTheSink() {
        StoreBackedDagSource.SinkWriterBinder binder = StoreBackedDagSource.assembledSinkWriterBinder();
        TargetTable only = new TargetTable("orders", List.of(new TargetField("id", "int", true)));

        Object bound = binder.bind("mongodb", Map.of(), WriteMode.UPSERT, DdlPolicy.FAIL,
                Map.of("orders", only), new PipelineNode("p1", "to_mongo"));

        assertThat(((PdkSinkWriterFactory) bound).targets()).containsOnlyKeys("orders");
    }

    /**
     * The product's own binder passes the node on rather than dropping it. The binder is the only seam
     * between a topology that knows which node it is building and a factory that opens the connector on
     * another member, so a binder that ignored the argument would leave every sink writing correctly with
     * nothing scoping what its connector records for itself - visible nowhere in the rows.
     */
    @Test
    void theNodeTheSinkWritesForReachesTheFactory() {
        StoreBackedDagSource.SinkWriterBinder binder = StoreBackedDagSource.assembledSinkWriterBinder();

        Object bound = binder.bind("mongodb", Map.of(), WriteMode.UPSERT, DdlPolicy.FAIL, TWO,
                new PipelineNode("p1", "to_mongo"));

        assertThat(((PdkSinkWriterFactory) bound).node()).isEqualTo(new PipelineNode("p1", "to_mongo"));
    }

    /** The nest-capable constructor is the one the runtime is wired through; it must use that binder. */
    @Test
    void theNestCapableSourceIsBuiltOnThatBinder() {
        StoreBackedDagSource source =
                new StoreBackedDagSource(new InMemoryStorePort(), NestSettings.defaults());

        assertThat(source.sinkWriterBinder())
                .as("the binder the nest-capable constructor wires")
                .isInstanceOf(StoreBackedDagSource.PdkSinkWriterBinder.class);
    }
}
