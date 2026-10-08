package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.UpdateOptions;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.PreExecutionFailure;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Binary;

/** Low-frequency refusal publication shares the current manifest and its existing chunk lease protocol. */
final class MongoPreExecutionFailureStorage {
    private final MongoCollection<Document> manifests;
    private final MongoLatestObservationStorage latest;
    private final MongoStopReservationWrites guards;
    private final MongoObservationContinuation continuations;

    MongoPreExecutionFailureStorage(MongoCollection<Document> manifests, MongoLatestObservationStorage latest,
            MongoStopReservationWrites guards, MongoObservationContinuation continuations) {
        this.manifests = manifests; this.latest = latest; this.guards = guards; this.continuations = continuations;
    }

    boolean save(Observation observation, PreExecutionFailure.Receipt receipt) {
        requireFrame(observation, receipt.owner());
        ObservationBsonBounds.requireHeader(observation.pipelineId(), base(receipt.owner(), observation.observedAt()));
        if (guards == null) { throw new UnsupportedOperationException("refusal publication requires lifecycle guards"); }
        // A live CONTINUE handoff must already have a complete, qualified carrier before public replacement.
        var carried = continuations.read(observation.pipelineId());
        SourceProof source = sourceProof(receipt.owner(), carried.orElse(null));
        Publication writer = new Publication(observation, receipt, carried.map(ObservationStore.StoredContinuation::receipt).orElse(null), source);
        try {
            var encoded = LatestObservationPayloadCodec.encode(observation, writer);
            Document descriptor = descriptor(receipt.owner(), observation.observedAt(), encoded, writer.token);
            ObservationBsonBounds.requireDescriptor(observation.pipelineId(), descriptor);
            return guards.withPreExecutionFailure(receipt, session -> {
                Document header = header(session, receipt.owner());
                if (header != null && !allows(header.get("current", Document.class), receipt.owner(),
                        observation.observedAt(), encoded.payloadDigest())) { throw fenced(); }
                requireContinuation(session, receipt.owner(), writer.carried, writer.source, header);
                if (writer.begun && (header == null || !pendingOwned(header, receipt.owner(), writer.token))) { throw fenced(); }
                if (!writer.begun && header != null && header.get("pending") instanceof Document pending
                        && !allows(pending, receipt.owner(), observation.observedAt(), null)) { throw fenced(); }
                Document filter = revisionFilter(receipt.owner(), header);
                if (writer.begun) {
                    filter.append("pending.publicationToken", writer.token)
                            .append("$expr", new Document("$gt", List.of("$pending.publishUntil", "$$NOW")));
                }
                Document set = new Document("formatVersion", MongoLatestObservationStorage.FORMAT_VERSION)
                        .append("ownerDigest", MongoLatestObservationStorage.ownerDigest(observation.pipelineId()))
                        .append("revision", UUID.randomUUID().toString()).append("legacyFallback", false)
                        .append("legacyResidue", header == null ? legacyExists(session, observation.pipelineId())
                                : header.getBoolean("legacyResidue"))
                        .append("current", new Document("$literal", descriptor));
                // A diagnostic has no generation. Preserve the existing verified private carrier explicitly.
                List<Bson> update = List.of(new Document("$set", set), new Document("$unset", "pending"));
                var written = manifests.updateOne(session, filter, update, new UpdateOptions().upsert(header == null));
                if (written.getMatchedCount() == 0 && written.getUpsertedId() == null) { throw fenced(); }
                return true;
            }).orElse(false);
        } catch (Refused stale) { return false; }
    }

    boolean current(PreExecutionFailure.Owner owner) {
        if (guards == null) { return false; }
        return guards.currentPreExecutionFailure(owner, session -> {
            Document header = header(session, owner);
            if (header == null) { return false; }
            for (String field : List.of("current", "pending")) {
                Document descriptor = header.get(field, Document.class);
                if (descriptor != null && PreExecutionFailureOwnerCodec.REFUSAL.equals(descriptor.get(PreExecutionFailureOwnerCodec.KIND))
                        && owner.equals(PreExecutionFailureOwnerCodec.read(owner.pipelineId(), descriptor))) { return true; }
            }
            return false;
        });
    }

    boolean refresh(String pipelineId, Instant observedAt) {
        var stored = latest.read(pipelineId).filter(saved -> saved.refusal().isPresent()).orElse(null);
        if (stored == null || guards == null) { return false; }
        var owner = stored.refusal().orElseThrow();
        var receipt = guards.preExecutionFailureReceipt(owner, session -> {
            Document header = header(session, owner);
            Document descriptor = header == null ? null : header.get("current", Document.class);
            return descriptor != null && PreExecutionFailureOwnerCodec.REFUSAL.equals(descriptor.get(PreExecutionFailureOwnerCodec.KIND))
                    && owner.equals(PreExecutionFailureOwnerCodec.read(pipelineId, descriptor));
        });
        return receipt.isPresent() && save(new Observation(pipelineId, PipelineState.FAILED, java.util.Map.of(),
                java.util.Map.of(), java.util.Map.of(), stored.observation().failure(), observedAt, List.of()), receipt.orElseThrow());
    }

    private void requireContinuation(ClientSession session, PreExecutionFailure.Owner owner,
            ObservationStore.ContinuationReceipt carried, SourceProof source, Document header) {
        guards.requirePreExecutionContinuation(session, owner, carried, header, source.current(), source.covered());
        if (source.current() == null && (header == null || header.getBoolean("legacyFallback"))) {
            Document actual = manifests.find(session, new Document("_id", owner.pipelineId())).first();
            if (!Objects.equals(source.legacy(), actual)) { throw fenced(); }
            if (source.legacy() == null) { return; }
            String state = source.legacy().getString("state");
            if (manifests.updateOne(session, MongoLatestObservationStorage.legacyFence(source.legacy()).append("state", state),
                    new Document("$set", new Document("state", ""))).getMatchedCount() != 1) { throw fenced(); }
            if (manifests.updateOne(session, MongoLatestObservationStorage.legacyFence(source.legacy()).append("state", ""),
                    new Document("$set", new Document("state", state))).getMatchedCount() != 1) { throw fenced(); }
        }
    }

    private record SourceProof(Document current, Document legacy, boolean covered) { }

    private SourceProof sourceProof(PreExecutionFailure.Owner owner, ObservationStore.StoredContinuation carried) {
        Document header = StoreIo.call(() -> manifests.withTimeout(MongoLatestObservationStorage.IO_DEADLINE_SECONDS, TimeUnit.SECONDS)
                .find(new Document("_id", MongoLatestObservationStorage.manifestKey(owner.pipelineId()))).first());
        if (header != null) { MongoLatestObservationStorage.validateHeader(header, owner.pipelineId(), MongoLatestObservationStorage.ownerDigest(owner.pipelineId())); }
        Document descriptor = header == null ? null : header.get("current", Document.class);
        Document legacy = descriptor == null && (header == null || header.getBoolean("legacyFallback"))
                ? StoreIo.call(() -> manifests.withTimeout(MongoLatestObservationStorage.IO_DEADLINE_SECONDS, TimeUnit.SECONDS)
                        .find(new Document("_id", owner.pipelineId())).first()) : null;
        if (descriptor != null && descriptor.containsKey(PreExecutionFailureOwnerCodec.KIND)
                || descriptor == null && legacy == null) { return new SourceProof(descriptor, legacy, true); }
        var stored = descriptor == null ? legacyStored(owner.pipelineId(), legacy) : latest.readPublicDescriptor(owner.pipelineId(), descriptor);
        return new SourceProof(descriptor, legacy, coversPublicSource(carried, stored));
    }

    static boolean coversPublicSource(ObservationStore.StoredContinuation carried, ObservationStore.Stored stored) {
        if (carried == null) { return stored.observation().facts().stream().noneMatch(
                fact -> fact.type() != io.tapstate.core.lifecycle.MetricType.GAUGE && fact.points().stream().anyMatch(point -> point.startTime() != null)); }
        var floor = carried.continuation();
        var scope = stored.scope().orElse(null);
        if (scope == null) { return true; }
        boolean qualifiedSource = Objects.equals(scope, floor.sourceScope())
                || floor.baselineOrigin().filter(origin -> origin.scope().equals(scope)).isPresent()
                || floor.target().filter(target -> target.scope().equals(scope)).isPresent();
        if (!qualifiedSource) { return true; }
        var retained = new java.util.ArrayList<io.tapstate.core.lifecycle.MetricFact>(floor.baselineFacts());
        floor.producerStates().forEach(state -> retained.add(new io.tapstate.core.lifecycle.MetricFact(
                state.name(), state.type(), state.unit(), state.published())));
        for (var fact : stored.observation().facts()) {
            if (fact.type() == io.tapstate.core.lifecycle.MetricType.GAUGE) { continue; }
            for (var point : fact.points()) {
                if (point.startTime() == null) { continue; }
                boolean covered = retained.stream().filter(saved -> saved.name().equals(fact.name())
                        && saved.type() == fact.type() && saved.unit().equals(fact.unit()))
                        .flatMap(saved -> saved.points().stream()).anyMatch(saved -> covers(saved, point));
                if (!covered) { return false; }
            }
        }
        return true;
    }

    private static ObservationStore.Stored legacyStored(String id, Document legacy) {
        java.util.Optional<ObservationStore.Scope> scope = java.util.Optional.empty();
        if (legacy.containsKey("pipelineIncarnationId") || legacy.containsKey("executionGeneration")) {
            Object incarnation = legacy.get("pipelineIncarnationId"), generation = legacy.get("executionGeneration");
            if (!(incarnation instanceof String value) || value.isBlank()
                    || !(generation instanceof Long || generation instanceof Integer) || ((Number) generation).longValue() <= 0) {
                throw unreadable(id);
            }
            scope = java.util.Optional.of(new ObservationStore.Scope((String) incarnation, ((Number) generation).longValue()));
        }
        return new ObservationStore.Stored(MongoObservationStore.toObservation(legacy), scope);
    }

    private static boolean covers(io.tapstate.core.lifecycle.MetricPoint saved, io.tapstate.core.lifecycle.MetricPoint current) {
        if (!saved.attributes().equals(current.attributes()) || !Objects.equals(saved.startTime(), current.startTime())) { return false; }
        if (current.value() != null) { return saved.value() != null && saved.value() >= current.value(); }
        var one = saved.histogram(); var two = current.histogram();
        if (one == null || !one.bounds().equals(two.bounds()) || one.count() < two.count() || one.sum() < two.sum()) { return false; }
        for (int at = 0; at < two.bucketCounts().size(); at++) {
            if (one.bucketCounts().get(at) < two.bucketCounts().get(at)) { return false; }
        }
        return true;
    }

    private final class Publication implements LatestObservationPayloadCodec.ChunkWriter {
        private final Observation observation;
        private final PreExecutionFailure.Receipt receipt;
        private final ObservationStore.ContinuationReceipt carried;
        private final SourceProof source;
        private final String token = UUID.randomUUID().toString();
        private boolean begun;
        private long heartbeatAt;
        Publication(Observation observation, PreExecutionFailure.Receipt receipt, ObservationStore.ContinuationReceipt carried, SourceProof source) {
            this.observation = observation; this.receipt = receipt; this.carried = carried; this.source = source;
        }
        @Override public void begin() {
            if (begun) { throw new IllegalStateException("a refusal publication acquires one pending lease"); }
            boolean acquired = guards.withPreExecutionFailure(receipt, session -> {
                Document header = header(session, receipt.owner());
                for (String field : List.of("current", "pending")) {
                    if (header != null && !allows(header.get(field, Document.class), receipt.owner(), observation.observedAt(), null)) {
                        throw fenced();
                    }
                }
                requireContinuation(session, receipt.owner(), carried, source, header);
                Document pending = base(receipt.owner(), observation.observedAt())
                        .append("encodingVersion", LatestObservationPayloadCodec.ENCODING_VERSION)
                        .append("publicationToken", token);
                Document set = new Document("formatVersion", MongoLatestObservationStorage.FORMAT_VERSION)
                        .append("ownerDigest", MongoLatestObservationStorage.ownerDigest(observation.pipelineId()))
                        .append("revision", UUID.randomUUID().toString())
                        .append("legacyFallback", header == null || header.getBoolean("legacyFallback"))
                        .append("legacyResidue", header == null ? legacyExists(session, observation.pipelineId()) : header.getBoolean("legacyResidue"))
                        .append("pending", new Document("$literal", pending));
                var applied = manifests.updateOne(session, revisionFilter(receipt.owner(), header),
                        List.of(new Document("$set", set), new Document("$set",
                                new Document("pending.publishUntil", MongoLatestObservationStorage.leaseUntilExpression()))),
                        new UpdateOptions().upsert(header == null));
                if (applied.getMatchedCount() == 0 && applied.getUpsertedId() == null) { throw fenced(); }
                return true;
            }).orElse(false);
            if (!acquired) { throw new Refused(); }
            begun = true; heartbeatAt = System.nanoTime() + MongoLatestObservationStorage.PUBLISH_HEARTBEAT_NANOS;
        }
        @Override public void write(LatestObservationPayloadCodec.Chunk chunk) {
            if (!begun) { throw new IllegalStateException("refusal chunks need their owned lease"); }
            if (Thread.currentThread().isInterrupted()) { throw new Refused(); }
            if (System.nanoTime() - heartbeatAt >= 0) {
                Document filter = MongoLatestObservationStorage.headerFilter(MongoLatestObservationStorage.manifestKey(observation.pipelineId()),
                        MongoLatestObservationStorage.ownerDigest(observation.pipelineId()))
                        .append("pending.publicationToken", token)
                        .append("pending." + PreExecutionFailureOwnerCodec.OWNER, PreExecutionFailureOwnerCodec.encode(receipt.owner()))
                        .append("$expr", new Document("$gt", List.of("$pending.publishUntil", "$$NOW")));
                long matched = StoreIo.call(() -> manifests.withTimeout(MongoLatestObservationStorage.IO_DEADLINE_SECONDS, TimeUnit.SECONDS)
                        .updateOne(filter, List.of(new Document("$set", new Document("pending.publishUntil",
                                MongoLatestObservationStorage.leaseUntilExpression()).append("revision", UUID.randomUUID().toString()))))
                        .getMatchedCount());
                if (matched == 0) { throw new Refused(); }
                heartbeatAt = System.nanoTime() + MongoLatestObservationStorage.PUBLISH_HEARTBEAT_NANOS;
            }
            latest.writeChunk(observation.pipelineId(), MongoLatestObservationStorage.manifestKey(observation.pipelineId()),
                    MongoLatestObservationStorage.ownerDigest(observation.pipelineId()), token, chunk);
        }
    }

    private Document header(ClientSession session, PreExecutionFailure.Owner owner) {
        Document header = manifests.find(session, new Document("_id", MongoLatestObservationStorage.manifestKey(owner.pipelineId()))).first();
        if (header != null) { MongoLatestObservationStorage.validateHeader(header, owner.pipelineId(),
                MongoLatestObservationStorage.ownerDigest(owner.pipelineId())); }
        return header;
    }
    private boolean legacyExists(ClientSession session, String id) {
        return manifests.find(session, new Document("_id", id)).projection(new Document("_id", 1)).first() != null;
    }
    private static Document revisionFilter(PreExecutionFailure.Owner owner, Document header) {
        Document filter = new Document("_id", MongoLatestObservationStorage.manifestKey(owner.pipelineId()));
        return header == null ? filter.append("formatVersion", new Document("$exists", false))
                : filter.append("revision", header.getString("revision"));
    }
    private static boolean pendingOwned(Document header, PreExecutionFailure.Owner owner, String token) {
        Document pending = header.get("pending", Document.class);
        return pending != null && token.equals(pending.get("publicationToken"))
                && owner.equals(PreExecutionFailureOwnerCodec.read(owner.pipelineId(), pending));
    }
    static boolean allows(Document previous, PreExecutionFailure.Owner incoming, Instant observedAt, byte[] digest) {
        if (previous == null) { return true; }
        if (!previous.containsKey(PreExecutionFailureOwnerCodec.KIND)) {
            Object value = previous.get("executionGeneration");
            if (!(value instanceof Long || value instanceof Integer)) { throw unreadable(incoming.pipelineId()); }
            long generation = ((Number) value).longValue();
            return generation > 0 && incoming.generationFrontier().isPresent()
                    && generation <= incoming.generationFrontier().getAsLong();
        }
        var owner = PreExecutionFailureOwnerCodec.read(incoming.pipelineId(), previous);
        if (!owner.pipelineIncarnationId().equals(incoming.pipelineIncarnationId())) {
            return owner.generationFrontier().orElse(0L) <= incoming.generationFrontier().orElse(0L);
        }
        if (!owner.equals(incoming)) { return incoming.checkpointEpoch() > owner.checkpointEpoch(); }
        if (!(previous.get("observedAt") instanceof Date before)) { throw unreadable(incoming.pipelineId()); }
        int order = observedAt.compareTo(before.toInstant());
        return order > 0 || order == 0 && (digest == null || Objects.equals(previous.get("payloadDigest"), new Binary(digest)));
    }
    private static Document base(PreExecutionFailure.Owner owner, Instant observedAt) {
        return new Document("pipelineIncarnationId", owner.pipelineIncarnationId())
                .append(PreExecutionFailureOwnerCodec.KIND, PreExecutionFailureOwnerCodec.REFUSAL)
                .append(PreExecutionFailureOwnerCodec.OWNER, PreExecutionFailureOwnerCodec.encode(owner))
                .append("observedAt", Date.from(observedAt));
    }
    private static Document descriptor(PreExecutionFailure.Owner owner, Instant at,
            LatestObservationPayloadCodec.Encoded encoded, String token) {
        Document descriptor = base(owner, at).append("encodingVersion", LatestObservationPayloadCodec.ENCODING_VERSION)
                .append("payloadDigest", new Binary(encoded.payloadDigest())).append("encodedBytes", encoded.encodedBytes());
        return encoded.inline() ? descriptor.append("mode", "inline").append("inlinePayload", new Binary(encoded.inlinePayload()))
                : descriptor.append("mode", "chunked").append("publicationToken", token).append("chunkCount", encoded.chunkCount());
    }
    private static void requireFrame(Observation frame, PreExecutionFailure.Owner owner) {
        if (!owner.pipelineId().equals(frame.pipelineId()) || frame.state() != PipelineState.FAILED || frame.failure() == null
                || frame.observedAt() == null || frame.observedAt().getNano() % 1_000_000 != 0
                || !frame.metrics().isEmpty() || !frame.snapshot().isEmpty() || !frame.positions().isEmpty() || !frame.facts().isEmpty()) {
            throw new IllegalArgumentException("a refusal publishes only its real coded state and observation time");
        }
    }
    private static RuntimeException fenced() { return MongoStopReservationWrites.fencedHandoff(); }
    private static io.tapstate.core.common.TapstateException unreadable(String id) {
        return new io.tapstate.core.common.TapstateException(io.tapstate.spi.store.IoError.DOCUMENT_UNREADABLE,
                java.util.Map.of("id", id, "field", "latest current owner"), null);
    }
    private static final class Refused extends RuntimeException { private static final long serialVersionUID = 1L; }
}
