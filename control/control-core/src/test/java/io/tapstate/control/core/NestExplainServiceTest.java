package io.tapstate.control.core;

import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.lifecycle.NestTree;
import io.tapstate.core.model.Resource;
import io.tapstate.spi.store.ArtifactStore;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Explaining a nest before it runs hands back the compiled tree and warns, from a sample, about the three
 * shapes that fail a run later: a one-per-parent embed that is not, documents near their element limit, and
 * a shared row pointed at by very many documents. Each warning names the embed and the count it came from.
 */
class NestExplainServiceTest {

    private static final String PIPELINE = """
            version: tapstate/v1
            kind: pipeline
            id: order_doc
            source: [ shop_core ]
            transforms:
              - id: doc
                type: nest
                from: { o: orders, i: order_items, c: customers, s: shipments }
                root:
                  from: o
                  key: [ id ]
                  embed:
                    - { from: i, on: { order_id: id }, as: array, path: items, arrayKey: [ id ] }
                    - { from: s, on: { order_id: id }, as: object, path: shipment }
                    - { from: c, on: { id: customer_id }, as: object, path: customer }
            serve:
              from: doc
              sync:
                - source: shop_target
            """;

    private static NestTree tree(long elementLimit, long referrerLimit) {
        return new NestTree("order_doc", "doc", "o", List.of("id"), elementLimit, List.of(
                new NestTree.Level("items", "i", "array", true, Map.of("order_id", "id"), List.of("id"), 0,
                        List.of()),
                new NestTree.Level("shipment", "s", "object", true, Map.of("order_id", "id"), List.of(), 0,
                        List.of()),
                new NestTree.Level("customer", "c", "object", false, Map.of("id", "customer_id"), List.of("id"),
                        referrerLimit, List.of())));
    }

    /** 10 orders, all of customer 7; order 1 has 3 shipments and 9 items, 100 items in all. */
    private static NestSampler sampler(List<String> read) {
        return (source, table, rows) -> {
            read.add(source + "." + table);
            return switch (table) {
                case "orders" -> new NestSampler.Sample(10L, rowsOf(10, i -> Map.of("id", i, "customer_id", 7)),
                        null);
                case "order_items" -> new NestSampler.Sample(100L, rowsOf(9, i -> Map.of("id", i, "order_id", 1)),
                        null);
                case "shipments" -> new NestSampler.Sample(3L, rowsOf(3, i -> Map.of("id", i, "order_id", 1)),
                        null);
                case "customers" -> new NestSampler.Sample(1L, List.of(Map.of("id", 7)), null);
                default -> throw new AssertionError(table);
            };
        };
    }

    @Test
    void theTreeComesBackAsCompiledWithTheBuildLineAndNoWarningsUnderTheLimits() {
        List<String> read = new ArrayList<>();
        NestExplanation explained = service(tree(20_000, 100_000), sampler(read)).explain("order_doc", "doc");

        assertThat(explained.tree().embeds()).extracting(NestTree.Level::owned).containsExactly(true, true, false);
        assertThat(explained.build()).startsWith("not available");
        assertThat(read).containsExactly("shop_core.orders", "shop_core.order_items", "shop_core.shipments",
                "shop_core.customers");
        assertThat(explained.samples()).extracting(NestExplanation.Sampled::path)
                .containsExactly("items", "shipment", "customer");
        NestExplanation.Sampled items = explained.samples().getFirst();
        assertThat(items.largestInSample()).isEqualTo(9L);
        assertThat(items.averagePerParent()).isEqualTo(10.0);
        // The object embed matched three rows under one order: that one is a warning at any limit.
        assertThat(explained.warnings()).extracting(NestExplanation.Warning::code)
                .containsExactly(MonitorError.NEST_OBJECT_KEY_NOT_UNIQUE);
        assertThat(explained.warnings().getFirst().args()).containsEntry("embed", "shipment")
                .containsEntry("count", 3L).containsEntry("sampled", 3);
    }

    @Test
    void embedsEachUnderTheLimitAreWarnedAboutWhenTogetherTheyNearIt() {
        // 10 items and 1 shipment per order: neither is near 13 alone (80% is 10.4), but 11 together are.
        NestExplanation explained = service(tree(13, 100_000), sampler(new ArrayList<>())).explain("order_doc", "doc");

        assertThat(explained.warnings()).extracting(NestExplanation.Warning::code).containsExactly(
                MonitorError.NEST_OBJECT_KEY_NOT_UNIQUE, MonitorError.NEST_DOCUMENT_NEAR_ELEMENT_LIMIT);
        assertThat(explained.warnings().getLast().args()).containsEntry("embeds", "items, shipment")
                .containsEntry("count", 11L).containsEntry("limit", 13L);
    }

    @Test
    void documentsNearTheElementLimitAndARowSharedByNearlyAsManyAsAllowedAreWarnedAbout() {
        NestExplanation explained = service(tree(12, 11), sampler(new ArrayList<>())).explain("order_doc", "doc");

        assertThat(explained.warnings()).extracting(NestExplanation.Warning::code).containsExactly(
                MonitorError.NEST_NEAR_ELEMENT_LIMIT, MonitorError.NEST_OBJECT_KEY_NOT_UNIQUE,
                MonitorError.NEST_REFERENCE_WIDELY_SHARED);
        assertThat(explained.warnings().getFirst().args()).containsEntry("embed", "items")
                .containsEntry("count", 10L).containsEntry("limit", 12L);
        assertThat(explained.warnings().getLast().args()).containsEntry("embed", "customer")
                .containsEntry("table", "customers").containsEntry("count", 10L).containsEntry("limit", 11L);
    }

    @Test
    void aTableTheBrowserCannotReadIsReportedAsNotSampledAndWarnsAboutNothing() {
        NestExplanation explained = service(tree(1, 1),
                (source, table, rows) -> NestSampler.Sample.unavailable("source.connector-not-browsable"))
                .explain("order_doc", "doc");

        assertThat(explained.warnings()).isEmpty();
        assertThat(explained.samples()).extracting(NestExplanation.Sampled::notSampled)
                .containsOnly("source.connector-not-browsable");
    }

    @Test
    void theLargestGroupIgnoresRowsMissingAJoinColumn() {
        assertThat(NestExplainService.largestGroup(List.of(Map.of("a", 1), Map.of("a", 1), Map.of("b", 1)),
                List.of("a"))).isEqualTo(2);
    }

    private static List<Map<String, Object>> rowsOf(int n, java.util.function.IntFunction<Map<String, Object>> row) {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (int i = 1; i <= n; i++) {
            rows.add(row.apply(i));
        }
        return rows;
    }

    private static NestExplainService service(NestTree tree, NestSampler sampler) {
        Resource pipeline = new DslParser().parse(PIPELINE);
        ArtifactQueryService artifacts = new ArtifactQueryService(new ArtifactStore() {
            @Override
            public void saveAll(List<Resource> saved) {
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
        return new NestExplainService(artifacts, (p, s) -> tree, sampler);
    }
}
