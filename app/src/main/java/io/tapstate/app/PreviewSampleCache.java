package io.tapstate.app;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.EventJsonValues;
import io.tapstate.spi.capture.BoundedSnapshotQueryRequest;
import io.tapstate.spi.capture.BoundedSnapshotQueryResult;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/** Short-lived, identity-scoped cache of repeatable raw bounded source reads. */
final class PreviewSampleCache {

    static final String MAP_NAME = "__preview.samples";
    static final Duration TTL = Duration.ofMinutes(5);
    private static final long MAX_ENTRY_BYTES = 32L * 1024L * 1024L;

    private final IMap<String, Entry> entries;

    PreviewSampleCache(HazelcastInstance member) {
        this.entries = Objects.requireNonNull(member, "member").getMap(MAP_NAME);
    }

    BoundedSnapshotQueryResult get(String principal, String pipelineId, String sampleId,
            String connectorIdentity, BoundedSnapshotQueryRequest request) {
        String key = key(principal, pipelineId, sampleId, connectorIdentity, request);
        Entry entry;
        try {
            entry = entries.get(key);
        } catch (HazelcastException | ClassCastException unavailable) {
            return null;
        }
        if (entry == null) {
            return null;
        }
        return new BoundedSnapshotQueryResult(
                entry.rows(), !entry.hasMore(), entry.hasMore(), true, 0, entry.sampledAt());
    }

    void put(String principal, String pipelineId, String sampleId,
            String connectorIdentity, BoundedSnapshotQueryRequest request, BoundedSnapshotQueryResult result) {
        boolean truncatedRootSample = request.selection() instanceof BoundedSnapshotQueryRequest.AllRows
                && result.hasMore();
        if ((!result.complete() && !truncatedRootSample)
                || !result.repeatable()) {
            return;
        }
        long encodedBytes = 0;
        for (Envelope row : result.rows()) {
            if (row.after() == null) {
                return;
            }
            long remaining = MAX_ENTRY_BYTES - encodedBytes;
            long rowBytes = EventJsonValues.encodedSize(PreviewJsonValues.normalize(row.after()), remaining);
            if (rowBytes > remaining) {
                return;
            }
            encodedBytes += rowBytes;
        }
        String key = key(principal, pipelineId, sampleId, connectorIdentity, request);
        try {
            entries.put(key, new Entry(result.rows(), result.hasMore(), result.sampledAt()),
                    TTL.toMillis(), TimeUnit.MILLISECONDS);
        } catch (HazelcastException unavailable) {
            // Caching is opportunistic; a running preview must not depend on cache availability.
        }
    }

    private static String key(String principal, String pipelineId, String sampleId, String connectorIdentity,
            BoundedSnapshotQueryRequest request) {
        Map<String, Object> identity = new java.util.LinkedHashMap<>();
        identity.put("version", 1);
        identity.put("principal", principal);
        identity.put("pipelineId", pipelineId);
        identity.put("sampleId", sampleId);
        identity.put("sourceId", request.sourceId());
        identity.put("connectorId", request.connectorId());
        identity.put("connectorIdentity", connectorIdentity);
        identity.put("settings", request.settings());
        identity.put("table", Map.of(
                "name", request.table().name(),
                "fields", request.table().fields().stream()
                        .map(field -> field.type() == null
                                ? Map.of("name", field.name())
                                : Map.of("name", field.name(), "type", field.type()))
                        .toList()));
        identity.put("stableOrder", request.stableOrder());
        identity.put("projection", request.projection());
        identity.put("selection", selection(request.selection()));
        identity.put("limit", request.limit());
        identity.put("maxBytes", request.maxBytes());
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(JsonWriter.write(PreviewJsonValues.normalize(identity))
                            .getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }

    private static Map<String, Object> selection(BoundedSnapshotQueryRequest.Selection selection) {
        if (selection instanceof BoundedSnapshotQueryRequest.AllRows) {
            return Map.of("kind", "all");
        }
        BoundedSnapshotQueryRequest.ExactTuples exact = (BoundedSnapshotQueryRequest.ExactTuples) selection;
        List<String> tuples = exact.tuples().stream()
                .map(tuple -> JsonWriter.write(PreviewJsonValues.normalize(tuple)))
                .sorted()
                .toList();
        return Map.of("kind", "exact-tuples", "tuples", tuples);
    }

    record Entry(List<Envelope> rows, boolean hasMore, Instant sampledAt) {
        Entry {
            rows = List.copyOf(rows);
            Objects.requireNonNull(sampledAt, "sampledAt");
        }
    }

    /** Reuses the platform's typed Envelope serializer so cache hits preserve connector value types. */
    public static final class EntrySerializer implements StreamSerializer<Entry> {

        public static final int TYPE_ID = 10003;

        @Override
        public int getTypeId() {
            return TYPE_ID;
        }

        @Override
        public void write(ObjectDataOutput out, Entry entry) throws IOException {
            out.writeLong(entry.sampledAt().getEpochSecond());
            out.writeInt(entry.sampledAt().getNano());
            out.writeBoolean(entry.hasMore());
            out.writeInt(entry.rows().size());
            for (Envelope row : entry.rows()) {
                out.writeObject(row);
            }
        }

        @Override
        public Entry read(ObjectDataInput in) throws IOException {
            Instant sampledAt = Instant.ofEpochSecond(in.readLong(), in.readInt());
            boolean hasMore = in.readBoolean();
            int size = in.readInt();
            if (size < 0 || size > PreviewSelectionPlanner.MAX_INPUT_ROWS) {
                throw new IOException("preview sample cache row count is out of range");
            }
            java.util.ArrayList<Envelope> rows = new java.util.ArrayList<>(size);
            for (int index = 0; index < size; index++) {
                Object row = in.readObject();
                if (!(row instanceof Envelope envelope)) {
                    throw new IOException("preview sample cache contains a non-envelope row");
                }
                rows.add(envelope);
            }
            return new Entry(rows, hasMore, sampledAt);
        }
    }
}
