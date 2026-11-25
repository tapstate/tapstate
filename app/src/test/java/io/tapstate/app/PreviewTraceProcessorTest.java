package io.tapstate.app;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.test.TestInbox;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.event.Envelope;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class PreviewTraceProcessorTest {

    @Test
    void recordsBoundedSamplesAndEscapedNestedPointerPatterns() throws Exception {
        String mapName = "preview-trace-" + UUID.randomUUID();
        HazelcastInstance member = member();
        try {
            PreviewTraceProcessor processor = processor("transform-1", mapName);
            TestInbox inbox = new TestInbox();
            inbox.add("ignored non-envelope");
            inbox.add(Envelope.insert(17L, "orders", Map.of(
                    "a/b~c", Map.of("tags", List.of(Map.of("name", "first"), Map.of("name", "second")))), null));

            processor.process(0, inbox);
            processor.close();

            Map<String, String> rows = member.getMap(mapName);
            assertThat(rows).containsKeys("sample:transform-1:00", "summary:transform-1");
            Map<String, Object> sample = object(rows.get("sample:transform-1:00"));
            assertThat(sample).containsEntry("op", "i").containsEntry("source", "orders");
            Map<String, Object> summary = object(rows.get("summary:transform-1"));
            assertThat(summary).containsEntry("rowsSeen", 1L).containsEntry("samplesWritten", 1L)
                    .containsEntry("truncated", false).containsEntry("pointerPatternsComplete", true);
            assertThat(((List<?>) summary.get("pointerPatterns")).stream().map(String.class::cast).toList())
                    .contains("/a~1b~0c", "/a~1b~0c/tags", "/a~1b~0c/tags/*", "/a~1b~0c/tags/*/name");
        } finally {
            member.shutdown();
        }
    }

    @Test
    void boundsSampleCountBytesAndPointerCollection() throws Exception {
        String mapName = "preview-trace-bounds-" + UUID.randomUUID();
        HazelcastInstance member = member();
        try {
            PreviewTraceProcessor countLimited = processor("count", mapName);
            TestInbox countInbox = new TestInbox();
            for (int index = 0; index < 11; index++) {
                countInbox.add(Envelope.insert(index, "orders", Map.of("id", index), null));
            }
            countLimited.process(0, countInbox);
            countLimited.close();
            Map<String, String> rows = member.getMap(mapName);
            Map<String, Object> countSummary = object(rows.get("summary:count"));
            assertThat(countSummary).containsEntry("rowsSeen", 11L).containsEntry("samplesWritten", 10L)
                    .containsEntry("truncated", true);
            assertThat(rows).doesNotContainKey("sample:count:10");

            PreviewTraceProcessor byteLimited = processor("bytes", mapName);
            TestInbox byteInbox = new TestInbox();
            byteInbox.add(Envelope.insert(12L, "orders", Map.of("payload", "x".repeat(300_000)), null));
            byteLimited.process(0, byteInbox);
            byteLimited.close();
            Map<String, Object> byteSummary = object(rows.get("summary:bytes"));
            assertThat(byteSummary).containsEntry("samplesWritten", 0L).containsEntry("truncated", true);

            Map<String, Object> wide = new LinkedHashMap<>();
            for (int index = 0; index < 600; index++) {
                wide.put("field-" + index, index);
            }
            PreviewTraceProcessor pointerLimited = processor("pointers", mapName);
            TestInbox pointerInbox = new TestInbox();
            pointerInbox.add(Envelope.insert(13L, "orders", wide, null));
            pointerLimited.process(0, pointerInbox);
            pointerLimited.close();
            Map<String, Object> pointerSummary = object(rows.get("summary:pointers"));
            assertThat((List<?>) pointerSummary.get("pointerPatterns")).hasSize(512);
            assertThat(pointerSummary).containsEntry("pointerPatternsComplete", false);
        } finally {
            member.shutdown();
        }
    }

    @Test
    void inputTapWritesOnlyItsAliasSummaryAndSupplierBuilds() throws Exception {
        String mapName = "preview-trace-input-" + UUID.randomUUID();
        HazelcastInstance member = member();
        try {
            assertThat(PreviewTraceProcessor.metaSupplier("trace", "node", mapName))
                    .isInstanceOf(ProcessorMetaSupplier.class);
            assertThat(PreviewTraceProcessor.inputMetaSupplier("trace-input", "node", "source-a", mapName))
                    .isInstanceOf(ProcessorMetaSupplier.class);

            PreviewTraceProcessor processor = inputProcessor("node", mapName, "source-a");
            TestInbox inbox = new TestInbox();
            inbox.add(Envelope.insert(14L, "orders", Map.of("id", 1), null));
            processor.process(0, inbox);
            processor.close();

            Map<String, String> rows = member.getMap(mapName);
            assertThat(rows).containsOnlyKeys("summary:node" + (char) 0 + "input" + (char) 0 + "source-a");
            Map<String, Object> summary = object(rows.values().iterator().next());
            assertThat(summary).containsEntry("inputAlias", "source-a")
                    .containsEntry("rowsSeen", 1L).containsEntry("samplesWritten", 0L);
        } finally {
            member.shutdown();
        }
    }

    @Test
    void tracesDeleteRowsArraysAndBoundedIterableAndDepthPointers() throws Exception {
        String mapName = "preview-trace-shapes-" + UUID.randomUUID();
        HazelcastInstance member = member();
        try {
            Map<String, Object> deep = new LinkedHashMap<>();
            Map<String, Object> cursor = deep;
            for (int index = 0; index < 36; index++) {
                Map<String, Object> child = new LinkedHashMap<>();
                cursor.put("level", child);
                cursor = child;
            }
            Map<String, Object> before = new LinkedHashMap<>();
            before.put("array", new int[] {1, 2});
            before.put("items", java.util.stream.IntStream.range(0, 12).boxed().toList());
            before.put("deep", deep);
            PreviewTraceProcessor processor = processor("delete", mapName);
            var collectPointers = PreviewTraceProcessor.class.getDeclaredMethod(
                    "collectPointers", Object.class, String.class, int.class);
            collectPointers.setAccessible(true);
            collectPointers.invoke(processor, Map.of(7, "ignored non-string pointer"), "/invalid", 0);
            TestInbox inbox = new TestInbox();
            inbox.add(Envelope.delete(18L, "orders", before, null).withRemoved(java.util.Set.of("stale")));
            processor.process(0, inbox);
            processor.close();

            Map<String, String> rows = member.getMap(mapName);
            Map<String, Object> sample = object(rows.get("sample:delete:00"));
            assertThat(sample).containsKey("before").containsEntry("removed", List.of("stale"));
            Map<String, Object> summary = object(rows.get("summary:delete"));
            assertThat(summary).containsEntry("pointerPatternsComplete", false);
            List<String> pointers = ((List<?>) summary.get("pointerPatterns"))
                    .stream().map(String.class::cast).toList();
            assertThat(pointers).contains("/array/*", "/items/*", "/deep/level");

            PreviewTraceProcessor input = inputProcessor("input-delete", mapName, "source-a");
            TestInbox inputInbox = new TestInbox();
            inputInbox.add(Envelope.delete(19L, "orders", before, null));
            input.process(0, inputInbox);
            input.close();
            assertThat(member.<String, String>getMap(mapName).keySet())
                    .doesNotContain("sample:input-delete:00")
                    .contains("summary:input-delete" + (char) 0 + "input" + (char) 0 + "source-a");
        } finally {
            member.shutdown();
        }
    }

    private static HazelcastInstance member() {
        Config config = new Config();
        config.setClusterName("preview-trace-test-" + UUID.randomUUID());
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
        config.getNetworkConfig().setPort(0).setPortAutoIncrement(false);
        return Hazelcast.newHazelcastInstance(config);
    }

    private static PreviewTraceProcessor processor(String nodeId, String mapName) throws Exception {
        Constructor<PreviewTraceProcessor> constructor = PreviewTraceProcessor.class
                .getDeclaredConstructor(String.class, String.class);
        constructor.setAccessible(true);
        return constructor.newInstance(nodeId, mapName);
    }

    private static PreviewTraceProcessor inputProcessor(String nodeId, String mapName, String alias) throws Exception {
        Constructor<PreviewTraceProcessor> constructor = PreviewTraceProcessor.class
                .getDeclaredConstructor(String.class, String.class, String.class);
        constructor.setAccessible(true);
        return constructor.newInstance(nodeId, mapName, alias);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> object(String json) {
        return (Map<String, Object>) JsonReader.parse(json);
    }
}
