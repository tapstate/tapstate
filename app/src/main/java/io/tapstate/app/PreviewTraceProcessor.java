package io.tapstate.app;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.EventJsonValues;
import java.lang.reflect.Array;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Bounded per-node sample tap used only by Pipeline preview DAGs. */
final class PreviewTraceProcessor extends AbstractProcessor {

    private static final int MAX_ROWS = 10;
    private static final int MAX_BYTES = 256 * 1024;
    private static final int MAX_POINTERS = 512;

    private final String nodeId;
    private final String mapName;
    private final String inputAlias;
    private long rowsSeen;
    private int samplesWritten;
    private int sampleBytes;
    private boolean truncated;
    private final Set<String> pointerPatterns = new LinkedHashSet<>();
    private boolean pointerPatternsTruncated;

    private PreviewTraceProcessor(String nodeId, String mapName) {
        this(nodeId, mapName, null);
    }

    private PreviewTraceProcessor(String nodeId, String mapName, String inputAlias) {
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId");
        this.mapName = Objects.requireNonNull(mapName, "mapName");
        this.inputAlias = inputAlias;
    }

    static ProcessorMetaSupplier metaSupplier(String vertexName, String nodeId, String mapName) {
        SupplierEx<Processor> supplier = () -> new PreviewTraceProcessor(nodeId, mapName);
        return ProcessorMetaSupplier.forceTotalParallelismOne(ProcessorSupplier.of(supplier), vertexName);
    }

    static ProcessorMetaSupplier inputMetaSupplier(
            String vertexName, String nodeId, String inputAlias, String mapName) {
        SupplierEx<Processor> supplier = () -> new PreviewTraceProcessor(nodeId, mapName, inputAlias);
        return ProcessorMetaSupplier.forceTotalParallelismOne(ProcessorSupplier.of(supplier), vertexName);
    }

    @Override
    public void process(int ordinal, Inbox inbox) {
        while (!inbox.isEmpty()) {
            Object item = inbox.peek();
            if (!(item instanceof Envelope event)) {
                inbox.remove();
                continue;
            }
            rowsSeen++;
            Object row = event.after() == null ? event.before() : event.after();
            collectPointers(row, "", 0);
            if (inputAlias == null && samplesWritten < MAX_ROWS) {
                Map<String, Object> sample = sample(event, nodeId);
                long remainingBytes = MAX_BYTES - sampleBytes;
                long size = EventJsonValues.encodedSize(PreviewJsonValues.normalize(sample), remainingBytes);
                if (size <= remainingBytes) {
                    String key = "sample:" + nodeId + ":" + String.format("%02d", samplesWritten);
                    memberMap().put(key, JsonWriter.write(PreviewJsonValues.normalize(sample)));
                    samplesWritten++;
                    sampleBytes += (int) size;
                } else {
                    truncated = true;
                }
            } else if (inputAlias == null) {
                truncated = true;
            }
            inbox.remove();
        }
    }

    @Override
    public void close() throws Exception {
        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("nodeId", nodeId);
        summary.put("rowsSeen", rowsSeen);
        summary.put("samplesWritten", samplesWritten);
        summary.put("sampleBytes", sampleBytes);
        summary.put("truncated", truncated);
        summary.put("pointerPatterns", List.copyOf(pointerPatterns));
        summary.put("pointerPatternsComplete", !pointerPatternsTruncated);
        if (inputAlias != null) {
            summary.put("inputAlias", inputAlias);
        }
        memberMap().put("summary:" + (inputAlias == null ? nodeId : inputTraceKey(nodeId, inputAlias)),
                JsonWriter.write(summary));
        super.close();
    }

    private void collectPointers(Object value, String parent, int depth) {
        if (depth > 32 || pointerPatterns.size() >= MAX_POINTERS) {
            pointerPatternsTruncated = true;
            return;
        }
        if (value instanceof Map<?, ?> map) {
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                if (!(entry.getKey() instanceof String field)) {
                    continue;
                }
                String path = parent + "/" + field.replace("~", "~0").replace("/", "~1");
                if (pointerPatterns.size() < MAX_POINTERS) {
                    pointerPatterns.add(path);
                } else {
                    pointerPatternsTruncated = true;
                    return;
                }
                collectPointers(entry.getValue(), path, depth + 1);
                if (pointerPatterns.size() >= MAX_POINTERS) {
                    pointerPatternsTruncated = true;
                    return;
                }
            }
        } else if (value instanceof Iterable<?> values) {
            String itemPath = parent + "/*";
            pointerPatterns.add(itemPath);
            int visited = 0;
            for (Object item : values) {
                if (visited >= 10) {
                    pointerPatternsTruncated = true;
                    break;
                }
                collectPointers(item, itemPath, depth + 1);
                visited++;
                if (pointerPatterns.size() >= MAX_POINTERS) {
                    pointerPatternsTruncated = true;
                    break;
                }
            }
            if (value instanceof Collection<?> collection && collection.size() > visited) {
                pointerPatternsTruncated = true;
            }
        } else if (value != null && value.getClass().isArray() && !(value instanceof byte[])) {
            String itemPath = parent + "/*";
            pointerPatterns.add(itemPath);
            int length = Array.getLength(value);
            int limit = Math.min(length, 10);
            for (int index = 0; index < limit; index++) {
                collectPointers(Array.get(value, index), itemPath, depth + 1);
                if (pointerPatterns.size() >= MAX_POINTERS) {
                    pointerPatternsTruncated = true;
                    break;
                }
            }
            if (length > limit) {
                pointerPatternsTruncated = true;
            }
        }
    }

    private static String inputTraceKey(String nodeId, String inputAlias) {
        return nodeId + "\u0000input\u0000" + inputAlias;
    }

    private static Map<String, Object> sample(Envelope event, String nodeId) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("nodeId", nodeId);
        row.put("op", event.op().symbol());
        row.put("source", event.src());
        row.put("ts", event.ts());
        row.put("before", event.before());
        row.put("after", event.after());
        if (!event.removed().isEmpty()) {
            row.put("removed", event.removed());
        }
        return row;
    }

    private com.hazelcast.map.IMap<String, String> memberMap() {
        HazelcastInstance member = Hazelcast.getAllHazelcastInstances().stream()
                .filter(instance -> instance.getLifecycleService().isRunning())
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("preview member is not running"));
        return member.getMap(mapName);
    }
}
