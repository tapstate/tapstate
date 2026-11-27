package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.ResumeMetrics;
import io.tapstate.spi.store.ResumeMetrics.Producer;
import java.util.Date;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.codecs.DocumentCodec;

/** Bounded private evidence stored beside the logical observation under its existing run key. */
final class MongoResumeWindow {
    static final String FIELD = "resumeWindow";
    static final String PRIOR = "resumeObservation";
    private static final int MAX_BYTES = 256;
    private MongoResumeWindow() {}

    static Document encode(ResumeMetrics.Window window) {
        var doc = new Document("formatVersion", 1).append("intentRevision", window.intentRevision())
                .append("jobId", window.jobId());
        for (var producer : Producer.values()) {
            var slot = window.slots().get(producer);
            if (slot != null) doc.append(name(producer),
                    new Document("nativeStart", slot.startMillis()).append("carry", slot.carry()));
        }
        return doc;
    }

    static Document bounded(ResumeMetrics.Window window, String pipeline) {
        var doc = encode(window);
        if (new RawBsonDocument(new Document(FIELD, doc), new DocumentCodec()).getByteBuffer().remaining() > MAX_BYTES)
            throw new TapstateException(IoError.DOCUMENT_TOO_LARGE, Map.of("id", pipeline), null);
        return doc;
    }

    static ResumeMetrics.Window decode(Document row, String pipeline) {
        if (!row.containsKey(FIELD)) return null;
        try {
            var doc = row.get(FIELD, Document.class);
            var slots = new EnumMap<Producer, ResumeMetrics.Slot>(Producer.class);
            for (var producer : Producer.values()) {
                if (!doc.containsKey(name(producer))) continue;
                var slot = doc.get(name(producer), Document.class);
                slots.put(producer, new ResumeMetrics.Slot(slot.getLong("nativeStart"), slot.getBoolean("carry")));
            }
            var window = new ResumeMetrics.Window(doc.getLong("intentRevision"), doc.getLong("jobId"), slots);
            if (encode(window).equals(doc)) return window;
        } catch (ClassCastException | NullPointerException | IllegalArgumentException invalid) {
            throw unreadable(pipeline, invalid);
        }
        throw unreadable(pipeline, null);
    }

    private static TapstateException unreadable(String pipeline, Throwable cause) {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", pipeline, "field", FIELD), cause);
    }

    private static String name(Producer producer) { return producer.name().toLowerCase(Locale.ROOT); }

    /** Join the original observation inside the checkpoint read, keeping only bounded publication evidence. */
    static Document lookup(Object checkpoint) {
        String fence = "$$checkpoint.resumeMetrics.applied.fence.";
        var scope = new Document("clusterId", "$$checkpoint.scope.clusterId").append("bindingVersion", "$$checkpoint.scope.bindingVersion")
                .append("cacheIncarnation", "$$checkpoint.scope.cacheIncarnation").append("pipelineId", "$$checkpoint.scope.pipelineId")
                .append("stateGeneration", "$$checkpoint.scope.stateGeneration");
        var key = new Document("scope", scope).append("epoch", "$$checkpoint.epoch")
                .append("claimGeneration", fence + "claimGeneration").append("executionGeneration", fence + "executionGeneration")
                .append("topologyRevision", fence + "topologyRevision").append("node", fence + "ownerNodeId")
                .append("boot", fence + "ownerBootId");
        var producers = new Document("$switch", new Document("branches", List.of(
                branch(new Document("$in", List.of("$$this.name", List.of("tapstate.pipeline.records", "tapstate.pipeline.bytes"))),
                        "$$this.points.attributes.direction"),
                branch(new Document("$eq", List.of("$$this.name", "tapstate.pipeline.record.delivery.duration")), List.of("out")),
                branch(new Document("$eq", List.of("$$this.name", "tapstate.pipeline.process.duration")), List.of("stage"))))
                .append("default", List.of()));
        var cumulative = new Document("$reduce", new Document("input", "$facts").append("initialValue", List.of())
                .append("in", new Document("$setUnion", List.of("$$value", producers))));
        var flatKeys = new Document("$map", new Document("input", new Document("$objectToArray", "$metrics"))
                .append("in", "$$this.k"));
        // Return an invalid marker for oversized stored evidence without materializing it beside the checkpoint.
        var size = new Document("$bsonSize", new Document(FIELD, "$" + FIELD));
        var proof = new Document("$cond", List.of(new Document("$lte", List.of(size, MAX_BYTES)), "$" + FIELD, false));
        var projection = new Document("pipelineId", 1).append("observedAt", 1).append(FIELD, proof)
                .append("flatKeys", new Document("$setIntersection", List.of(flatKeys,
                        List.of("records.in", "records.out", "bytes.in", "bytes.out"))))
                .append("cumulative", cumulative);
        return new Document("$lookup", new Document("from", SystemCollections.CACHE_PIPELINE_OBSERVATION.collectionName())
                .append("let", new Document("key", new Document("$let",
                        new Document("vars", new Document("checkpoint", checkpoint)).append("in", key))))
                .append("pipeline", List.of(
                        new Document("$match", new Document("$expr", new Document("$eq", List.of("$_id", "$$key")))),
                        new Document("$project", projection))).append("as", PRIOR));
    }

    private static Document branch(Document condition, Object value) {
        return new Document("case", condition).append("then", value);
    }

    static ResumeMetrics.Checkpoint checkpoint(MongoCachePipelineAccess access, Document state) {
        var binding = MongoResumeMetrics.read(state).applied();
        ResumeMetrics.Prior prior = null;
        if (binding != null) {
            var rows = state.getList(PRIOR, Document.class);
            prior = ResumeMetrics.Prior.ABSENT;
            if (!rows.isEmpty()) {
                var row = rows.getFirst();
                String pipeline = access.scope.pipelineId();
                var key = MongoCachePipelineObservationStore.keyOfFence(access, state.getLong("epoch"), binding.fence());
                if (!key.equals(row.get("_id")) || !pipeline.equals(row.get("pipelineId"))) throw unreadable(pipeline, null);
                var cumulative = EnumSet.noneOf(Producer.class);
                var flat = row.getList("flatKeys", String.class, List.of());
                if (flat.contains("records.in") || flat.contains("bytes.in")) cumulative.add(Producer.IN);
                if (flat.contains("records.out") || flat.contains("bytes.out")) cumulative.add(Producer.OUT);
                var names = row.getList("cumulative", String.class);
                if (names != null) for (var producer : Producer.values())
                    if (names.contains(name(producer))) cumulative.add(producer);
                Date at = row.getDate("observedAt");
                prior = new ResumeMetrics.Prior(true, at == null ? null : at.toInstant(), decode(row, pipeline), cumulative);
            }
        }
        return new ResumeMetrics.Checkpoint(MongoStateStore.toCheckpoint(state), binding,
                MongoResumeMetrics.unavailableRevision(state), prior);
    }
}
