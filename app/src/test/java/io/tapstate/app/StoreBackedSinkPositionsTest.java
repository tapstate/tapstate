package io.tapstate.app;

import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.TableRef;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The read-side counterpart of the sink-ack writer: given a pipeline id, it resolves each table the
 * pipeline's sources read to the durable source position that pipeline's sink has acked, keyed by table. It
 * keys off the same source -> table -> mining-chain resolution the writer advances under, so the position a
 * sink advanced is the position this reads back. A table whose sink has not acked yet, a chain with no
 * consumer record for the pipeline, and a pipeline no longer stored all resolve to no entry -- an absent
 * position reads as "not acked yet", never as a sentinel.
 */
class StoreBackedSinkPositionsTest {

    private static final String PIPELINE = "orders-pipe";

    @Test
    void keysTheSinkAckedPositionByTheTableItsSourceReads() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        SourceResource source = source("orders_src", "orders", "h-orders");
        artifacts.save(source);
        artifacts.save(pipeline(PIPELINE, "orders_src"));
        InMemoryStorePort store = new InMemoryStorePort(artifacts);
        String chain = chainOf(source);
        store.meta().create(chain, null);
        store.meta().advanceSinkAcked(chain, PIPELINE, new ChainPosition(new SourceOrder(1, 6), "w6"));

        assertThat(new StoreBackedSinkPositions(store).apply(PIPELINE))
                .containsExactly(entry("orders", "w6"));
    }

    @Test
    void resolvesADistinctPositionPerTableAcrossThePipelinesSources() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        SourceResource ordersSrc = source("orders_src", "orders", "h-orders");
        SourceResource itemsSrc = source("items_src", "items", "h-items");
        artifacts.save(ordersSrc);
        artifacts.save(itemsSrc);
        artifacts.save(pipeline(PIPELINE, "orders_src", "items_src"));
        InMemoryStorePort store = new InMemoryStorePort(artifacts);
        seedAck(store, ordersSrc, "w3");
        seedAck(store, itemsSrc, "w5");

        assertThat(new StoreBackedSinkPositions(store).apply(PIPELINE))
                .containsOnly(entry("orders", "w3"), entry("items", "w5"));
    }

    @Test
    void omitsATableWhoseSinkHasNotAckedYet() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        SourceResource source = source("orders_src", "orders", "h-orders");
        artifacts.save(source);
        artifacts.save(pipeline(PIPELINE, "orders_src"));
        InMemoryStorePort store = new InMemoryStorePort(artifacts);
        String chain = chainOf(source);
        store.meta().create(chain, null);
        // A read cursor exists, but the sink has not acked a source position yet: sinkAckedSrcpos stays null.
        store.meta().advanceConsumerReadSeq(chain, PIPELINE, "orders", 7L);

        assertThat(new StoreBackedSinkPositions(store).apply(PIPELINE)).isEmpty();
    }

    @Test
    void resolvesOnlyThisPipelinesAckWhenAnotherConsumerSharesTheChain() {
        // Two pipelines reading the same source share one mining chain, so its record carries both consumers'
        // offsets; the position projected must be this pipeline's, not whichever consumer sorts first.
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        SourceResource source = source("orders_src", "orders", "h-orders");
        artifacts.save(source);
        artifacts.save(pipeline(PIPELINE, "orders_src"));
        InMemoryStorePort store = new InMemoryStorePort(artifacts);
        String chain = chainOf(source);
        store.meta().create(chain, null);
        store.meta().advanceSinkAcked(chain, "other-pipe", new ChainPosition(new SourceOrder(1, 9), "w9"));
        store.meta().advanceSinkAcked(chain, PIPELINE, new ChainPosition(new SourceOrder(1, 6), "w6"));

        assertThat(new StoreBackedSinkPositions(store).apply(PIPELINE))
                .containsExactly(entry("orders", "w6"));
    }

    @Test
    void omitsAnotherConsumersAckWhenThisPipelineHasNotAckedOnTheSharedChain() {
        // Only a foreign consumer on the shared chain has acked; this pipeline has no position of its own yet.
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        SourceResource source = source("orders_src", "orders", "h-orders");
        artifacts.save(source);
        artifacts.save(pipeline(PIPELINE, "orders_src"));
        InMemoryStorePort store = new InMemoryStorePort(artifacts);
        String chain = chainOf(source);
        store.meta().create(chain, null);
        store.meta().advanceSinkAcked(chain, "other-pipe", new ChainPosition(new SourceOrder(1, 9), "w9"));

        assertThat(new StoreBackedSinkPositions(store).apply(PIPELINE)).isEmpty();
    }

    @Test
    void omitsAChainWithNoConsumerRecordForThePipeline() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        SourceResource source = source("orders_src", "orders", "h-orders");
        artifacts.save(source);
        artifacts.save(pipeline(PIPELINE, "orders_src"));
        InMemoryStorePort store = new InMemoryStorePort(artifacts);
        store.meta().create(chainOf(source), null); // seeded, but no consumer offset for this pipeline

        assertThat(new StoreBackedSinkPositions(store).apply(PIPELINE)).isEmpty();
    }

    @Test
    void isEmptyForAPipelineNoLongerStored() {
        assertThat(new StoreBackedSinkPositions(new InMemoryStorePort()).apply("gone")).isEmpty();
    }

    private static void seedAck(InMemoryStorePort store, SourceResource source, String srcpos) {
        String chain = chainOf(source);
        store.meta().create(chain, null);
        store.meta().advanceSinkAcked(chain, PIPELINE, new ChainPosition(new SourceOrder(1, 1), srcpos));
    }

    private static String chainOf(SourceResource source) {
        return SourceCaptureResolution.of(source).chainId().value();
    }

    private static SourceResource source(String id, String table, String host) {
        return new SourceResource(id, null, "fake", Map.of("host", host), SourceMode.CDC,
                List.of(TableRef.literal(table)), null, null, null);
    }

    private static PipelineResource pipeline(String id, String... sourceIds) {
        return new PipelineResource(id, null, List.of(sourceIds), null, null, null, null, null);
    }
}
