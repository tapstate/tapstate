package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.lifecycle.ExecutionPlan;
import io.tapstate.core.lifecycle.InitialLoad;
import io.tapstate.core.lifecycle.MetricAttributes;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.MetricType;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceRef;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.ObservationStore;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * The nest progress read: which tables a nest step depends on, read off its definition, joined with the
 * per-table load the pipeline published and the documents its root level sent - and the step's waiting line
 * names the tables that hold it, not just how many rows went by.
 */
class NestProgressServiceTest {

    private static final Instant NOW = Instant.parse("2026-11-08T12:00:00Z");
    private static final ExecutionPlan PLAN = new ExecutionPlan("order_doc", 1L, 2L, 3L, List.of(), List.of(), NOW);

    private static final String PIPELINE = """
            version: tapstate/v1
            kind: pipeline
            id: order_doc
            source: [ shop_core, shop_lines ]
            transforms:
              - id: tidy
                type: filter
                from: [ customers ]
                expr: "true"
              - id: doc
                type: nest
                from: { o: orders, i: order_items, c: tidy }
                root:
                  from: o
                  key: [ id ]
                  embed:
                    - from: i
                      on: { order_id: id }
                      as: array
                      path: items
                      arrayKey: [ id ]
                    - from: c
                      on: { id: customer_id }
                      as: object
                      path: customer
            serve:
              from: doc
              sync:
                - source: shop_target
            """;

    @Test
    void aStepHeldByAnUnreadTableSaysSoAndCountsWhatWasRead() {
        NestProgress progress = service(load(Map.of(
                "orders", new InitialLoad.Table(InitialLoad.Stage.PENDING, null, 300L),
                "order_items", new InitialLoad.Table(InitialLoad.Stage.LANDING, 600L, 600L),
                "customers", new InitialLoad.Table(InitialLoad.Stage.LANDED, 12L, 12L))), List.of())
                .progress("order_doc", null);

        assertThat(progress.steps()).singleElement().satisfies(step -> {
            assertThat(step.step()).isEqualTo("doc");
            assertThat(step.activity()).isEqualTo(NestProgress.Activity.LOADING);
            assertThat(step.waitingFor()).containsExactly("orders");
            assertThat(step.rootTable()).isEqualTo("orders");
            assertThat(step.rootsEstimate()).isEqualTo(300L);
            assertThat(step.tables()).extracting(NestProgress.Table::table)
                    .containsExactly("orders", "order_items", "customers");
            assertThat(step.tables()).extracting(NestProgress.Table::path)
                    .containsExactly("", "items", "customer");
            assertThat(step.tables().get(1).rowsRead()).isEqualTo(600L);
            assertThat(step.tables()).extracting(NestProgress.Table::rowsStaged).containsOnlyNulls();
        });
    }

    @Test
    void everyEmbedStillReadingIsNamedBeforeTheRoot() {
        NestProgress progress = service(load(Map.of(
                "orders", new InitialLoad.Table(InitialLoad.Stage.READING, 10L, 300L),
                "order_items", new InitialLoad.Table(InitialLoad.Stage.READING, 40L, 600L),
                "customers", new InitialLoad.Table(InitialLoad.Stage.LANDED, 12L, 12L))), List.of())
                .progress("order_doc", "doc");

        assertThat(progress.steps().getFirst().waitingFor()).containsExactly("order_items", "orders");
    }

    @Test
    void documentsSentAreReadOffTheRootLevelOfTheStep() {
        List<MetricFact> facts = List.of(
                new MetricFact("tapstate.pipeline.nest.namespace.info", MetricType.GAUGE, "1", List.of(
                        MetricPoint.reading(origin("ns-root", "doc", "[]", "state"), NOW, 1L),
                        MetricPoint.reading(origin("ns-items", "doc", "[\"items\"]", "state"), NOW, 1L))),
                new MetricFact("tapstate.pipeline.nest.roots.rendered", MetricType.COUNTER, "{document}", List.of(
                        MetricPoint.accumulated(Map.of(MetricAttributes.PIPELINE_ID, "order_doc",
                                MetricAttributes.NEST_NAMESPACE, "ns-root"), NOW, NOW, 120L))));
        NestProgress progress = service(load(Map.of(
                "orders", new InitialLoad.Table(InitialLoad.Stage.LANDED, 300L, 300L),
                "order_items", new InitialLoad.Table(InitialLoad.Stage.LANDED, 600L, 600L),
                "customers", new InitialLoad.Table(InitialLoad.Stage.LANDED, 12L, 12L))), facts)
                .progress("order_doc", null);

        NestProgress.Step step = progress.steps().getFirst();
        assertThat(step.rootsRendered()).isEqualTo(120L);
        assertThat(step.activity()).isEqualTo(NestProgress.Activity.MAINTAINING);
        assertThat(step.waitingFor()).isEmpty();
    }

    @Test
    void aStepThatIsNotANestIsRefusedNamingTheOnesThereAre() {
        TapstateException refused = catchThrowableOfType(TapstateException.class,
                () -> service(load(Map.of()), List.of()).progress("order_doc", "tidy"));

        assertThat(refused.code()).isEqualTo(MonitorError.UNKNOWN_NEST_STEP);
        assertThat(refused.args()).containsEntry("steps", "doc").containsEntry("step", "tidy");
    }

    @Test
    void aPipelineThatDeclaresNoTransformsHasNoNestStep() {
        PipelineResource bare = new PipelineResource("bare", null, List.of(SourceRef.bare("shop_core")),
                null, null, null, null, null);

        assertThat(NestProgressService.steps(bare)).isEmpty();
        assertThat(NestProgressService.nests(bare)).isEmpty();
    }

    private static Map<String, String> origin(String namespace, String step, String path, String map) {
        return Map.of(MetricAttributes.PIPELINE_ID, "order_doc", MetricAttributes.NEST_NAMESPACE, namespace,
                MetricAttributes.NEST_STEP, step, MetricAttributes.NEST_PATH, path, MetricAttributes.NEST_MAP, map);
    }

    private static InitialLoad load(Map<String, InitialLoad.Table> tables) {
        return new InitialLoad(InitialLoad.executionKey(PLAN), NOW, "snapshot_and_cdc", tables);
    }

    private static NestProgressService service(InitialLoad load, List<MetricFact> facts) {
        Observation observation = new Observation("order_doc", PipelineState.RUNNING, Map.of(), Map.of(), Map.of(),
                null, NOW, facts, load);
        ObservationStore observations = new ObservationStore() {
            @Override
            public void save(Observation saved) {
            }

            @Override
            public Optional<Observation> read(String pipelineId) {
                return Optional.of(observation).filter(o -> o.pipelineId().equals(pipelineId));
            }

            @Override
            public void delete(String pipelineId) {
            }
        };
        Resource pipeline = new DslParser().parse(PIPELINE);
        ArtifactQueryService artifacts = new ArtifactQueryService(new ArtifactStore() {
            @Override
            public void saveAll(List<Resource> artifacts) {
            }

            @Override
            public Optional<Resource> get(String id) {
                return Optional.of(pipeline).filter(r -> r.id().equals(id));
            }

            @Override
            public List<Resource> list() {
                return List.of(pipeline);
            }
        });
        return new NestProgressService(artifacts, new PipelineObservationQueryService(artifacts, observations,
                ids -> Map.of("order_doc", PLAN), List::of, Clock.fixed(NOW, ZoneOffset.UTC)));
    }
}
