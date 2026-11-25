package io.tapstate.app;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.map.IMap;
import com.hazelcast.function.SupplierEx;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.WriteMode;
import io.tapstate.spi.sink.WriteResult;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Serializable preview-only materializer coordinates; it never opens a connector or store. */
final class PreviewMaterializerSinkWriterFactory implements SupplierEx<SinkWriter> {

    private static final long serialVersionUID = 1L;

    private final String mapName;
    private final Map<String, List<String>> keysByStream;
    private final WriteMode mode;

    PreviewMaterializerSinkWriterFactory(
            String mapName, Map<String, List<String>> keysByStream, WriteMode mode) {
        this.mapName = Objects.requireNonNull(mapName, "mapName");
        Map<String, List<String>> copied = new LinkedHashMap<>();
        keysByStream.forEach((stream, keys) -> copied.put(stream, List.copyOf(keys)));
        this.keysByStream = Map.copyOf(copied);
        this.mode = Objects.requireNonNull(mode, "mode");
    }

    @Override
    public SinkWriter getEx() {
        return new PreviewMaterializerSinkWriter(localMember().getMap(mapName), keysByStream, mode);
    }

    private static HazelcastInstance localMember() {
        Set<HazelcastInstance> instances = Hazelcast.getAllHazelcastInstances();
        if (instances.size() != 1) {
            throw new IllegalStateException("expected exactly one local Hazelcast member, found "
                    + instances.size());
        }
        HazelcastInstance member = instances.iterator().next();
        if (!member.getLifecycleService().isRunning()) {
            throw new HazelcastInstanceNotActiveException("preview member is not running");
        }
        return member;
    }

    private static final class PreviewMaterializerSinkWriter implements SinkWriter {

        private static final long MAX_BYTES = 8L * 1024L * 1024L;
        private static final int MAX_DOCUMENTS = 20_000;

        private final IMap<String, String> rows;
        private final Map<String, List<String>> keysByStream;
        private final WriteMode mode;
        private long appendSequence;
        private long encodedBytes;
        private boolean closed;

        private PreviewMaterializerSinkWriter(
                IMap<String, String> rows, Map<String, List<String>> keysByStream, WriteMode mode) {
            this.rows = rows;
            this.keysByStream = keysByStream;
            this.mode = mode;
        }

        @Override
        public synchronized CompletionStage<WriteResult> write(List<Envelope> records) {
            if (closed) {
                return CompletableFuture.failedFuture(new IllegalStateException("preview materializer is closed"));
            }
            try {
                long written = 0;
                for (Envelope event : records) {
                    if (event.op() == Op.DDL) {
                        continue;
                    }
                    if (mode == WriteMode.APPEND) {
                        Map<String, Object> payload = event.after() == null ? event.before() : event.after();
                        if (payload == null) {
                            continue;
                        }
                        String key = String.format("append:%020d", appendSequence++);
                        String document = PreviewDocumentStorage.encode(payload);
                        long newBytes = PreviewDocumentStorage.encodedSize(document);
                        long remaining = MAX_BYTES - encodedBytes;
                        if (newBytes > remaining) {
                            throw refused("the final preview result exceeds the 8 MiB response limit");
                        }
                        putBounded(key, document, 0L, newBytes);
                        written++;
                        continue;
                    }
                    List<String> keyFields = keysByStream.get(event.src());
                    if (keyFields == null || keyFields.isEmpty()) {
                        throw refused("the output identity for stream '" + event.src() + "' is unresolved");
                    }
                    Map<String, Object> before = event.before();
                    Map<String, Object> after = event.after();
                    String previousKey = before == null ? null : rowKey(event.src(), before, keyFields);
                    String nextKey = after == null ? null : rowKey(event.src(), after, keyFields);
                    if (event.op() == Op.DELETE) {
                        if (previousKey != null) {
                            String old = rows.remove(previousKey);
                            encodedBytes -= jsonBytes(old);
                        }
                        written++;
                        continue;
                    }
                    if (after == null) {
                        continue;
                    }
                    if (nextKey == null) {
                        throw refused("the output identity for stream '" + event.src() + "' is missing from a row");
                    }
                    if (previousKey != null && !previousKey.equals(nextKey)) {
                        String old = rows.remove(previousKey);
                        encodedBytes -= jsonBytes(old);
                    }
                    String prior = rows.get(nextKey);
                    Map<String, Object> merged = new LinkedHashMap<>();
                    if (prior != null) {
                        Map<String, Object> existing = PreviewDocumentStorage.decode(prior);
                        existing.forEach((key, value) -> merged.put((String) key, value));
                    }
                    merged.putAll(after);
                    event.removed().forEach(merged::remove);
                    long oldBytes = jsonBytes(prior);
                    String document = PreviewDocumentStorage.encode(merged);
                    long newBytes = PreviewDocumentStorage.encodedSize(document);
                    long remaining = MAX_BYTES - (encodedBytes - oldBytes);
                    if (newBytes > remaining) {
                        throw refused("the final preview result exceeds the 8 MiB response limit");
                    }
                    putBounded(nextKey, document, oldBytes, newBytes);
                    written++;
                }
                return CompletableFuture.completedFuture(new WriteResult(written));
            } catch (RuntimeException failure) {
                return CompletableFuture.failedFuture(failure);
            }
        }

        private void putBounded(String key, String document, long oldBytes, long newBytes) {
            long nextSize = mode == WriteMode.APPEND ? encodedBytes + newBytes : encodedBytes + newBytes - oldBytes;
            boolean newKey = !rows.containsKey(key);
            if (nextSize > MAX_BYTES) {
                throw refused("the final preview result exceeds the 8 MiB response limit");
            }
            if (newKey && rows.size() >= MAX_DOCUMENTS) {
                throw refused("the final preview result exceeds 20,000 documents");
            }
            rows.put(key, document);
            encodedBytes = nextSize;
        }

        private static String rowKey(String stream, Map<String, Object> row, List<String> keyFields) {
            List<Object> values = new ArrayList<>(keyFields.size());
            for (String field : keyFields) {
                if (!row.containsKey(field) || row.get(field) == null) {
                    return null;
                }
                values.add(PreviewJsonValues.normalize(row.get(field)));
            }
            return stream + ":" + JsonWriter.write(values);
        }

        private static long jsonBytes(String json) {
            return json == null ? 0 : json.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        }

        @Override
        public synchronized void close() {
            closed = true;
        }
    }

    private static TapstateException refused(String reason) {
        return new TapstateException(ActuationError.PREVIEW_REFUSED, Map.of("reason", reason), null);
    }
}
