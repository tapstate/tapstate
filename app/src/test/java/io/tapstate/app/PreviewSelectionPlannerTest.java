package io.tapstate.app;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Embed;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.model.ViewBlock;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.capture.BoundedQueryCancellation;
import io.tapstate.spi.capture.BoundedSnapshotQueryPort;
import io.tapstate.spi.capture.BoundedSnapshotQueryResult;
import io.tapstate.spi.capture.FieldSchema;
import io.tapstate.spi.capture.TableSchema;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceTable;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PreviewSelectionPlannerTest {

    private static final Instant NOW = Instant.parse("2026-10-03T00:00:00Z");
    private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);
    private static final PipelineResource PIPELINE = new PipelineResource("pipeline", null,
            List.of(SourceRef.spec("orders_src", true)), List.of(),
            new ViewBlock.Inline("orders_view", io.tapstate.core.model.FromRef.literal("orders_src.orders"),
                    "id", null), null, null, null);

    @Test
    void readsBoundedRootsSequencesRowsAndServesTheNextReadFromCache() throws Exception {
        withPlanner(oneSource(), (request, calls) -> result(request.limit(), false), (planner, member, calls) -> {
            PreviewSelectionPlanner.Sample first = planner.load("author", "pipeline", "sample-a", PIPELINE,
                    null, 2, NOW.plusSeconds(10), new BoundedQueryCancellation());
            assertThat(first.rootRows()).isEqualTo(2);
            assertThat(first.inputRows()).isEqualTo(2);
            assertThat(first.rootSourceKeys()).containsExactly("orders_src.orders");
            assertThat(first.rowsBySourceKey().get("orders_src.orders"))
                    .extracting(Envelope::position)
                    .extracting(io.tapstate.core.event.ChainPosition::order)
                    .containsExactly(new SourceOrder(0, 0), new SourceOrder(0, 1));
            assertThat(first.cacheHit()).isFalse();
            assertThat(calls).hasValue(1);

            PreviewSelectionPlanner.Sample cached = planner.load("author", "pipeline", "sample-a", PIPELINE,
                    null, 2, NOW.plusSeconds(10), new BoundedQueryCancellation());
            assertThat(cached.cacheHit()).isTrue();
            assertThat(cached.cachedReads()).isEqualTo(1);
            assertThat(calls).hasValue(1);
        });
    }

    @Test
    void rejectsExpiredCancelledAndUnanchoredSamplesBeforeReading() throws Exception {
        withPlanner(oneSource(), (request, calls) -> result(request.limit(), false), (planner, member, calls) -> {
            assertThatThrownBy(() -> planner.load("author", "pipeline", "expired", PIPELINE,
                    null, 1, NOW, new BoundedQueryCancellation()))
                    .isInstanceOf(TapstateException.class);
            BoundedQueryCancellation cancellation = new BoundedQueryCancellation();
            cancellation.cancel();
            assertThatThrownBy(() -> planner.load("author", "pipeline", "cancelled", PIPELINE,
                    null, 1, NOW.plusSeconds(10), cancellation))
                    .isInstanceOf(IllegalStateException.class);
            PreviewSelectionPlanner.SourceView noInputs = new PreviewSelectionPlanner.SourceView() {
                @Override
                public List<PreviewSourceTable> sourceTables(PipelineResource pipeline) {
                    return List.of(table());
                }

                @Override
                public Set<String> inputSourceKeys(PipelineResource pipeline) {
                    return Set.of();
                }
            };
            PreviewSelectionPlanner unanchored = new PreviewSelectionPlanner(
                    request -> result(request.limit(), false), new PreviewSampleCache(member), CLOCK, noInputs);
            assertThatThrownBy(() -> unanchored.load("author", "pipeline", "unanchored", PIPELINE,
                    null, 1, NOW.plusSeconds(10), new BoundedQueryCancellation()))
                    .isInstanceOf(TapstateException.class);
            assertThat(calls).hasValue(0);
        });
    }

    @Test
    void refusesTruncatedRootSelectionAndTooManyRootSources() throws Exception {
        withPlanner(oneSource(), (request, calls) -> result(request.limit(), true), (planner, member, calls) -> {
            PreviewSelectionPlanner.Sample truncated = planner.load("author", "pipeline", "truncated", PIPELINE,
                    null, 1, NOW.plusSeconds(10), new BoundedQueryCancellation());
            assertThat(truncated.rootTruncated()).isTrue();

            PreviewSelectionPlanner.SourceView twoRoots = new PreviewSelectionPlanner.SourceView() {
                @Override
                public List<PreviewSourceTable> sourceTables(PipelineResource pipeline) {
                    return List.of(table(), table("archive_src.orders", "archive_src"));
                }

                @Override
                public Set<String> inputSourceKeys(PipelineResource pipeline) {
                    return Set.of("orders_src.orders", "archive_src.orders");
                }
            };
            PreviewSelectionPlanner plannerWithTwoRoots = new PreviewSelectionPlanner(
                    request -> result(request.limit(), false), new PreviewSampleCache(member), CLOCK, twoRoots);
            assertThatThrownBy(() -> plannerWithTwoRoots.load("author", "pipeline", "roots", PIPELINE,
                    null, 1, NOW.plusSeconds(10), new BoundedQueryCancellation()))
                    .isInstanceOf(TapstateException.class);
        });
    }

    @Test
    void loadsOnlyExactNestedRelationTuplesAndRefusesRelationsWithoutJoinKeys() throws Exception {
        PipelineResource nested = nestedPipeline(Map.of("parent_id", "id"));
        PreviewSelectionPlanner.SourceView sourceView = sourceView(
                List.of(table(), childTable()), Set.of("orders_src.orders", "orders_src.items"));
        withPlanner(sourceView, (request, calls) -> {
            if (request.selection() instanceof io.tapstate.spi.capture.BoundedSnapshotQueryRequest.AllRows) {
                return rows(List.of(Envelope.read(NOW.toEpochMilli(), "orders_src.orders", Map.of("id", 7), Map.of())));
            }
            var exact = (io.tapstate.spi.capture.BoundedSnapshotQueryRequest.ExactTuples) request.selection();
            assertThat(exact.tuples()).containsExactly(Map.of("parent_id", 7));
            return rows(List.of(Envelope.read(NOW.toEpochMilli(), "orders_src.items",
                    Map.of("parent_id", 7, "label", "priority"), Map.of())));
        }, (planner, member, calls) -> {
            PreviewSelectionPlanner.Sample sample = planner.load("author", "pipeline", "nested", nested,
                    null, 2, NOW.plusSeconds(10), new BoundedQueryCancellation());
            assertThat(sample.rootSourceKeys()).containsExactly("orders_src.orders");
            assertThat(sample.queryCount()).isEqualTo(2);
            assertThat(sample.rowsBySourceKey().get("orders_src.items")).hasSize(1);
            assertThat(calls).hasValue(2);
        });

        PipelineResource invalid = nestedPipeline(Map.of());
        withPlanner(sourceView, (request, calls) -> rows(List.of()), (planner, member, calls) ->
                assertThatThrownBy(() -> planner.load("author", "pipeline", "invalid-nest", invalid,
                        null, 2, NOW.plusSeconds(10), new BoundedQueryCancellation()))
                        .isInstanceOf(TapstateException.class));
    }

    private static void withPlanner(PreviewSelectionPlanner.SourceView sourceView, Query query, PlannerCase test)
            throws Exception {
        HazelcastProperties properties = new HazelcastProperties();
        properties.setClusterName("preview-planner-test-" + UUID.randomUUID());
        properties.setMemberPort(0);
        Config config = HazelcastConfiguration.memberConfig(properties);
        config.getNetworkConfig().setPort(0).setPortAutoIncrement(false);
        HazelcastInstance member = Hazelcast.newHazelcastInstance(config);
        try {
            AtomicInteger calls = new AtomicInteger();
            BoundedSnapshotQueryPort port = new BoundedSnapshotQueryPort() {
                @Override
                public String cacheIdentity(io.tapstate.spi.capture.BoundedSnapshotQueryRequest request) {
                    return "test-connector-v1";
                }

                @Override
                public BoundedSnapshotQueryResult query(
                        io.tapstate.spi.capture.BoundedSnapshotQueryRequest request) {
                    calls.incrementAndGet();
                    return query.run(request, calls);
                }
            };
            PreviewSelectionPlanner planner = new PreviewSelectionPlanner(
                    port, new PreviewSampleCache(member), CLOCK, sourceView);
            test.run(planner, member, calls);
        } finally {
            member.shutdown();
        }
    }

    private static PreviewSourceTable table() {
        return table("orders_src.orders", "orders_src");
    }

    private static PreviewSourceTable table(String key, String sourceId) {
        SourceTable model = new SourceTable("orders", List.of(new SourceField("id", "integer")), List.of("id"),
                List.of());
        return new PreviewSourceTable(key, sourceId, "test", Map.of(),
                new TableSchema("orders", List.of(new FieldSchema("id", "integer"))), model);
    }

    private static PreviewSourceTable childTable() {
        SourceTable model = new SourceTable("items", List.of(
                new SourceField("parent_id", "integer"), new SourceField("label", "varchar")),
                List.of("parent_id", "label"), List.of());
        return new PreviewSourceTable("orders_src.items", "orders_src", "test", Map.of(),
                new TableSchema("items", List.of(new FieldSchema("parent_id", "integer"),
                        new FieldSchema("label", "varchar"))), model);
    }

    private static PreviewSelectionPlanner.SourceView oneSource() {
        return sourceView(List.of(table()), Set.of("orders_src.orders"));
    }

    private static PreviewSelectionPlanner.SourceView sourceView(List<PreviewSourceTable> tables, Set<String> keys) {
        return new PreviewSelectionPlanner.SourceView() {
            @Override
            public List<PreviewSourceTable> sourceTables(PipelineResource pipeline) {
                return tables;
            }

            @Override
            public Set<String> inputSourceKeys(PipelineResource pipeline) {
                return keys;
            }
        };
    }

    private static PipelineResource nestedPipeline(Map<String, String> joinKeys) {
        Embed items = new Embed("items", joinKeys, EmbedAs.ARRAY, "items", null, null, null, null, null);
        Map<String, FromRef> aliases = Map.of(
                "root", FromRef.literal("orders_src.orders"), "items", FromRef.literal("orders_src.items"));
        Step nest = Step.inline("nest", FromClause.aliases(aliases),
                new TransformBody.Nest(null, null, new NestRoot("root", List.of("id"), null, null, List.of(items))),
                null);
        return new PipelineResource("pipeline", null, List.of(SourceRef.spec("orders_src", true)), List.of(nest),
                new ViewBlock.Inline("orders_view", FromRef.literal("nest"), "id", null), null, null, null);
    }

    private static BoundedSnapshotQueryResult rows(List<Envelope> rows) {
        return new BoundedSnapshotQueryResult(rows, true, false, true, 1, NOW);
    }

    private static BoundedSnapshotQueryResult result(int limit, boolean hasMore) {
        List<Envelope> rows = java.util.stream.IntStream.range(0, limit)
                .mapToObj(id -> Envelope.read(NOW.toEpochMilli(), "orders_src.orders", Map.of("id", id), Map.of()))
                .toList();
        return new BoundedSnapshotQueryResult(rows, !hasMore, hasMore, true, 1, NOW);
    }

    @FunctionalInterface
    private interface Query {
        BoundedSnapshotQueryResult run(
                io.tapstate.spi.capture.BoundedSnapshotQueryRequest request, AtomicInteger calls);
    }

    @FunctionalInterface
    private interface PlannerCase {
        void run(PreviewSelectionPlanner planner, HazelcastInstance member, AtomicInteger calls) throws Exception;
    }
}
