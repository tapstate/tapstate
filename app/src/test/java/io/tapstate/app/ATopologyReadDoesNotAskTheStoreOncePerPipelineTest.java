package io.tapstate.app;

import io.tapstate.control.core.ClusterClaimView;
import io.tapstate.control.core.ClusterPipelineTopologyService;
import io.tapstate.control.core.ClusterPipelineView;
import io.tapstate.control.core.LivePipelineRuns;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ReadMode;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.Settings;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TableRef;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SourceTable;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimStore;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A look at the topology answers for every pipeline in the cluster at once, on a member whose store is
 * also carrying every claim renewal in the cluster. Asked of the store a pipeline at a time -- its claim,
 * each of its sources, each source's discovery, the claim over each of its captures -- that is a round
 * trip for every pipeline and every source reference, one after the other: a few hundred pipelines turn
 * one look into a thousand sequential reads, and nothing reports it but the clock.
 *
 * <p>What the answer needs does not grow that way. Pipelines share their sources, so the artifacts and
 * discoveries to read are the distinct ones, and the claims are one question however many there are.
 * Counted through the stores themselves, over the derivation the runtime uses, because an answer that is
 * right says nothing about how many trips it took.
 */
class ATopologyReadDoesNotAskTheStoreOncePerPipelineTest {

    private static final String CLUSTER = "cluster-a";
    private static final Duration TTL = Duration.ofSeconds(30);
    private static final WorkloadOwner OWNER = new WorkloadOwner("node-a", "boot-a");

    /**
     * Four pipelines reading one source, and two reading that one and a second. The first source has been
     * discovered and the second not yet, because no discovery is an answer too: one that is not kept is
     * asked for again by every pipeline naming the source.
     */
    private static final List<String> PIPELINES = List.of("p1", "p2", "p3", "p4", "p5", "p6");

    @Test
    void sixPipelinesOverTwoSharedSourcesAskForTheirClaimsOnceAndReadEachSourceOnce() {
        InMemoryArtifactStore artifacts = artifacts();
        InMemoryStorePort store = seeded(artifacts);
        // Filed from a store holding the same artifacts, so that every read counted below is the read
        // face's own.
        InMemoryWorkloadClaimStore claims = claimsFiledFor(new StoreBackedPipelineCaptures(seeded(artifacts())));
        AtomicInteger claimQuestions = new AtomicInteger();
        ClusterPipelineTopologyService topology = new ClusterPipelineTopologyService(
                LivePipelineRuns.none(), new StoreBackedPipelineCaptures(store),
                counting(WorkloadClaimStore.class, claims, claimQuestions), store.desired(), CLUSTER);

        List<ClusterPipelineView> pipelines = topology.pipelines(List.of());

        // It really did answer for every pipeline with every claim filed for it: a read that found nothing
        // would have asked for less, and the counts below could be the counts this is looking for.
        StoreBackedPipelineCaptures alone = new StoreBackedPipelineCaptures(seeded(artifacts()));
        assertThat(pipelines).extracting(ClusterPipelineView::pipelineId).containsExactlyElementsOf(PIPELINES);
        assertThat(pipelines).allSatisfy(pipeline -> {
            assertThat(pipeline.controllerClaim()).isNotNull();
            assertThat(pipeline.captureClaims())
                    .extracting(ClusterClaimView::resourceId)
                    .as("the captures %s names when it is asked about on its own", pipeline.pipelineId())
                    .containsExactlyElementsOf(new TreeSet<>(alone.captureIds(pipeline.pipelineId())));
        });
        assertThat(pipelines)
                .extracting(pipeline -> pipeline.captureClaims().size())
                .containsExactly(1, 1, 1, 1, 2, 2);

        assertThat(new Asked(claimQuestions.get(), new TreeMap<>(artifacts.reads()), store.schemaStore().reads()))
                .as("one question for every claim the answer reports, and each artifact and each discovery "
                        + "read once however many pipelines name it")
                .isEqualTo(new Asked(1, onceEach("billing-source", "orders-source", "p1", "p2", "p3", "p4",
                        "p5", "p6"), 2));
    }

    /**
     * Reading what the pipelines share once changes how often the store is read and nothing about the
     * answer: every pipeline in a batch is answered as it is when asked about on its own, including every
     * shape that names no capture -- and one batch reads a pipeline's own artifact once even where another
     * pipeline names it as a source.
     */
    @Test
    void everyPipelineInABatchIsAnsweredAsItIsAnsweredAlone() {
        InMemoryArtifactStore artifacts = artifacts();
        artifacts.save(pipeline("bare", FromClause.list(FromRef.literal("orders-source.orders")),
                List.of(SourceRef.bare("orders-source"))));
        artifacts.save(pipeline("missing-source", FromClause.list(FromRef.literal("nowhere")), "nowhere"));
        artifacts.save(pipeline("reads-a-pipeline", FromClause.list(FromRef.literal("p1")), "p1"));
        artifacts.save(pipeline("undiscovered-table",
                FromClause.list(FromRef.literal("orders-source.invoices")), "orders-source"));
        artifacts.save(pipeline("reads-none-of-billing",
                FromClause.list(FromRef.literal("orders-source.orders")), "orders-source", "billing-source"));
        StoreBackedPipelineCaptures captures = new StoreBackedPipelineCaptures(seeded(artifacts));
        List<String> asked = List.of("p1", "p5", "bare", "missing-source", "reads-a-pipeline",
                "undiscovered-table", "reads-none-of-billing", "orders-source", "gone");

        Map<String, List<String>> batched = captures.captureIdsByPipeline(asked);

        assertThat(artifacts.reads().values()).containsOnly(1);
        Map<String, List<String>> alone = new LinkedHashMap<>();
        asked.forEach(pipelineId -> alone.put(pipelineId, captures.captureIds(pipelineId)));
        assertThat(batched).isEqualTo(alone);
        // Not two empty answers agreeing: the pipelines that read the sources name their captures, the one
        // reading none of its second source names only the first's, and every other shape names none.
        assertThat(batched.get("p1")).hasSize(1);
        assertThat(batched.get("p5")).hasSize(2).contains(batched.get("p1").getFirst());
        assertThat(batched.get("reads-none-of-billing")).isEqualTo(batched.get("p1"));
        assertThat(asked.subList(2, 6)).allSatisfy(pipelineId ->
                assertThat(batched.get(pipelineId)).as(pipelineId).isEmpty());
        assertThat(batched.get("orders-source")).as("a source asked about as a pipeline").isEmpty();
        assertThat(batched.get("gone")).as("a pipeline with no artifact").isEmpty();
    }

    /** What one read asked the stores for. */
    private record Asked(int claimQuestions, Map<String, Integer> artifactReads, int discoveryReads) {
    }

    private static Map<String, Integer> onceEach(String... ids) {
        Map<String, Integer> once = new TreeMap<>();
        Arrays.stream(ids).forEach(id -> once.put(id, 1));
        return once;
    }

    /** The two sources, the one discovery, and the six pipelines, every one of them wanted running. */
    private static InMemoryStorePort seeded(InMemoryArtifactStore artifacts) {
        InMemoryStorePort store = new InMemoryStorePort(artifacts);
        store.schemaStore().save(discovery("orders-source", "orders", "customers"));
        for (String pipelineId : PIPELINES) {
            store.desired().save(new DesiredState(pipelineId, PipelineState.RUNNING, "r1", false));
        }
        return store;
    }

    private static InMemoryArtifactStore artifacts() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(source("orders-source", "orders.internal", null));
        artifacts.save(source("billing-source", "billing.internal", List.of(TableRef.literal("invoices"))));
        for (String pipelineId : List.of("p1", "p2", "p3", "p4")) {
            artifacts.save(pipeline(pipelineId, FromClause.list(FromRef.literal("orders-source.orders")),
                    "orders-source"));
        }
        for (String pipelineId : List.of("p5", "p6")) {
            artifacts.save(pipeline(pipelineId,
                    FromClause.list(FromRef.literal("orders-source.orders"), FromRef.literal("billing-source")),
                    "orders-source", "billing-source"));
        }
        return artifacts;
    }

    /**
     * Every claim a running cluster would hold over the pipelines: each one's controller, and each capture
     * its start files, under the ids the captures are derived to.
     */
    private static InMemoryWorkloadClaimStore claimsFiledFor(StoreBackedPipelineCaptures captures) {
        InMemoryWorkloadClaimStore claims = new InMemoryWorkloadClaimStore();
        for (String pipelineId : PIPELINES) {
            claims.acquire(new WorkloadClaimKey(CLUSTER, WorkloadClaimType.PIPELINE_ACTUATION, pipelineId),
                    OWNER, 7, TTL);
            for (String captureId : captures.captureIds(pipelineId)) {
                claims.acquire(new WorkloadClaimKey(CLUSTER, WorkloadClaimType.CAPTURE, captureId), OWNER, 7, TTL);
            }
        }
        return claims;
    }

    /**
     * A cdc source. One declaring no tables leaves which it has to its discovery; one naming its tables
     * literally may be read before anything has been discovered off it.
     */
    private static SourceResource source(String id, String host, List<TableRef> tables) {
        return new SourceResource(id, null, "mysql", Map.of("host", host), SourceMode.CDC, tables, null, null);
    }

    private static DiscoveredSourceModel discovery(String sourceId, String... tables) {
        return new DiscoveredSourceModel(sourceId, "mysql", 1L, new SourceModel(Arrays.stream(tables)
                .map(table -> new SourceTable(table, List.of(), List.of(), List.of()))
                .toList()));
    }

    /** A cdc-only pipeline over {@code sourceIds}, reading through the shared ring, serving {@code from}. */
    private static PipelineResource pipeline(String id, FromClause from, String... sourceIds) {
        return pipeline(id, from, Arrays.stream(sourceIds)
                .map(sourceId -> (SourceRef) SourceRef.spec(sourceId, true))
                .toList());
    }

    private static PipelineResource pipeline(String id, FromClause from, List<SourceRef> sources) {
        return new PipelineResource(
                id, null, sources, null, null,
                new ServeBlock.Inline(null, from, List.of(new SyncElement("sync", "target", null, null, null)),
                        null, null),
                new Settings(null, null, null, null, ReadMode.CDC_ONLY, "earliest"), null);
    }

    /**
     * {@code port}, answering as {@code answering} does and counting every question put to it, whichever of
     * the port's methods asked it: what is measured is how often the caller goes to the store, not which
     * door it goes through.
     */
    private static <T> T counting(Class<T> port, T answering, AtomicInteger questions) {
        return port.cast(Proxy.newProxyInstance(
                port.getClassLoader(), new Class<?>[] {port}, (proxy, method, arguments) -> {
                    if (method.getDeclaringClass() != Object.class) {
                        questions.incrementAndGet();
                    }
                    try {
                        return method.invoke(answering, arguments);
                    } catch (InvocationTargetException thrown) {
                        throw thrown.getCause();
                    }
                }));
    }
}
