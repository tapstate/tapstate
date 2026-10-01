package io.tapstate.adapters.mongostore;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.UpdateOptions;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.CardinalityBudget;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricType;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.spi.store.HandoffIdentity;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.ObservationContinuation;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.StopReservation;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Binary;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/** One private carrier beside the public current descriptor, sharing its owner, chunks and bounded IO lanes. */
final class MongoObservationContinuation {
    static final String CONTINUATION = "continuation";
    static final String CONTINUATION_PENDING = "continuationPending";
    static final int ENCODING_VERSION = LatestObservationPayloadCodec.CONTINUATION_ENCODING_VERSION;
    private static final long IO_SECONDS = MongoLatestObservationStorage.IO_DEADLINE_SECONDS;
    private static final long READ_SECONDS = MongoLatestObservationStorage.READ_DEADLINE_SECONDS;
    private static final FindOneAndUpdateOptions AFTER = new FindOneAndUpdateOptions()
            .returnDocument(ReturnDocument.AFTER);

    private final MongoClient client;
    private final MongoCollection<Document> manifests;
    private final MongoCollection<Document> chunks;
    private final MongoLatestObservationStorage latest;
    private final MongoStopReservationWrites guard;

    MongoObservationContinuation(MongoClient client, MongoCollection<Document> manifests,
            MongoCollection<Document> chunks, MongoLatestObservationStorage latest, MongoStopReservationWrites guard) {
        this.client = Objects.requireNonNull(client, "client");
        this.manifests = manifests.withReadConcern(ReadConcern.MAJORITY).withReadPreference(ReadPreference.primary())
                .withWriteConcern(MongoLatestObservationStorage.CHUNK_WRITE_CONCERN);
        this.chunks = chunks.withReadConcern(ReadConcern.MAJORITY).withReadPreference(ReadPreference.primary())
                .withWriteConcern(MongoLatestObservationStorage.CHUNK_WRITE_CONCERN);
        this.latest = Objects.requireNonNull(latest, "latest");
        this.guard = guard;
    }

    Optional<ObservationStore.ContinuationReceipt> save(String pipelineId, StopReservation expected,
            Optional<ObservationStore.ContinuationReceipt> previous, ObservationContinuation requested) {
        requireGuard();
        requireMarker(pipelineId, expected, requested);
        ObservationContinuation next = requested;
        Optional<ObservationStore.StoredContinuation> existing = read(pipelineId);
        if (!qualifiedOrigin(expected, previous, requested, existing)) { return Optional.empty(); }
        if (!qualifiedRetarget(previous, requested, existing)) { return Optional.empty(); }
        Optional<ObservationStore.ContinuationReceipt> expectedReceipt = previous;
        if (!requested.knownBaseline() && existing.filter(saved -> sameLineage(saved.continuation(), requested)
                && saved.continuation().knownBaseline()).isPresent()) {
            ObservationStore.StoredContinuation saved = existing.orElseThrow();
            ObservationContinuation known = saved.continuation();
            if (!known.baselineOrigin().equals(requested.baselineOrigin())) {
                return Optional.empty();
            }
            if (previous.filter(saved.receipt()::equals).isEmpty()) { return Optional.empty(); }
            if (sameBinding(known, requested)) { return retainKnown(pipelineId, expected, saved.receipt()); }
            if (known.target().flatMap(ObservationContinuation.Target::realJob).isPresent()) { return Optional.empty(); }
            expectedReceipt = Optional.of(saved.receipt());
            next = new ObservationContinuation(known.token(), known.sourceScope(), requested.target(),
                    known.baselineOrigin(), known.baselineFacts(), known.target().equals(requested.target())
                            ? known.producerStates() : List.of());
        }
        SourceProof proof = next.knownBaseline() ? null : proveUnknownSource(pipelineId, next);
        if (proof != null && proof.hasKnownFacts()) { return Optional.empty(); }
        final Optional<ObservationStore.ContinuationReceipt> compared = expectedReceipt;
        FloorWriter writer = new FloorWriter(pipelineId, next, compared, expected);
        Document descriptor;
        try {
            var encoded = LatestObservationPayloadCodec.encodeContinuation(pipelineId, next, writer);
            descriptor = descriptor(pipelineId, next, writer.token, encoded);
        } catch (StaleFloor refused) {
            return Optional.empty();
        }
        final Document committed = descriptor;
        return guard.withExpectedHandoff(expected, session -> {
            Binary key = MongoLatestObservationStorage.manifestKey(pipelineId);
            Binary owner = MongoLatestObservationStorage.ownerDigest(pipelineId);
            ensureHeader(session, pipelineId, key, owner);
            Document header = manifests.find(session, new Document("_id", key)).first();
            MongoLatestObservationStorage.validateHeader(header, pipelineId, owner);
            if (!expectedMatches(header, compared) || proof != null && (!proof.unchanged(header)
                    || !Objects.equals(proof.legacy(), manifests.find(session, new Document("_id", pipelineId)).first()))) {
                throw MongoStopReservationWrites.fencedHandoff();
            }
            Document filter = MongoLatestObservationStorage.headerFilter(key, owner);
            appendExpected(filter, compared);
            if (writer.begun) {
                filter.append(CONTINUATION_PENDING + ".publicationToken", writer.token)
                        .append("$expr", new Document("$gt", List.of("$" + CONTINUATION_PENDING + ".publishUntil", "$$NOW")));
            }
            if (proof != null) { proof.appendGuard(filter); }
            Document changed = manifests.findOneAndUpdate(session, filter,
                    new Document("$set", new Document(CONTINUATION, committed)
                            .append("revision", UUID.randomUUID().toString()))
                            .append("$unset", new Document(CONTINUATION_PENDING, true)), AFTER);
            if (changed == null) { throw MongoStopReservationWrites.fencedHandoff(); }
            return receipt(pipelineId, document(changed.get(CONTINUATION), pipelineId, CONTINUATION));
        });
    }

    private Optional<ObservationStore.ContinuationReceipt> retainKnown(String pipelineId, StopReservation expected,
            ObservationStore.ContinuationReceipt receipt) {
        return guard.withExpectedHandoff(expected, session -> {
            Document filter = MongoLatestObservationStorage.headerFilter(MongoLatestObservationStorage.manifestKey(pipelineId),
                    MongoLatestObservationStorage.ownerDigest(pipelineId));
            appendExpected(filter, Optional.of(receipt));
            Document retained = manifests.findOneAndUpdate(session, filter,
                    new Document("$set", new Document("revision", UUID.randomUUID().toString())), AFTER);
            if (retained == null) { throw MongoStopReservationWrites.fencedHandoff(); }
            return receipt(pipelineId, document(retained.get(CONTINUATION), pipelineId, CONTINUATION));
        });
    }

    boolean clear(String pipelineId, StopReservation expected, ObservationStore.ContinuationReceipt previous) {
        requireGuard();
        if (!pipelineId.equals(expected.pipelineId()) || expected.counterPolicy() != StopReservation.CounterPolicy.RESET
                || !pipelineId.equals(previous.pipelineId())) {
            throw new IllegalArgumentException("private reset requires the exact reset pipeline and carrier receipt");
        }
        return guard.withExpectedHandoff(expected, session -> {
            Binary key = MongoLatestObservationStorage.manifestKey(pipelineId);
            Binary owner = MongoLatestObservationStorage.ownerDigest(pipelineId);
            Document filter = MongoLatestObservationStorage.headerFilter(key, owner)
                    .append(CONTINUATION + ".revision", previous.revision())
                    .append(CONTINUATION + ".integrityDigest", previous.digest());
            return manifests.updateOne(session, filter,
                    new Document("$unset", new Document(CONTINUATION, true))
                            .append("$set", new Document("revision", UUID.randomUUID().toString())))
                    .getMatchedCount() != 0;
        }).orElse(false);
    }

    ObservationStore.PublicationResult publish(Observation observation, ObservationStore.Scope scope,
            ObservationStore.ContinuationWrite write) {
        if (write instanceof ObservationStore.ContinuationWrite.Keep) {
            return new ObservationStore.PublicationResult(latest.save(observation, scope), Optional.empty());
        }
        if (write instanceof ObservationStore.ContinuationWrite.Reset reset) {
            HandoffIdentity identity = reset.currentHandoff();
            if (!identity.pipelineId().equals(observation.pipelineId()) || !identity.targetScope().equals(scope)) {
                throw new IllegalArgumentException("reset publication requires its current admitted execution");
            }
            return publishPacket(observation, scope, null, reset.expectedReceipt(), null, true);
        }
        var store = (ObservationStore.ContinuationWrite.Store) write;
        ObservationContinuation next = store.next();
        if (next.target().flatMap(ObservationContinuation.Target::realJob).isEmpty()
                || next.target().filter(target -> scope.equals(target.scope())).isEmpty()) {
            throw new IllegalArgumentException("a private publication needs the same real target as its public frame");
        }
        if (store.expectedReceipt().isPresent()) {
            ObservationStore.ContinuationReceipt prior = store.expectedReceipt().orElseThrow();
            if (!prior.token().equals(next.token()) || !Objects.equals(prior.sourceScope(), next.sourceScope())
                    || !prior.target().equals(next.target()) || !prior.baselineOrigin().equals(next.baselineOrigin())) {
                return new ObservationStore.PublicationResult(false, Optional.empty());
            }
        }
        // Only the cold UNKNOWN branch reads private state; a known same-target packet carries its cached receipt.
        if (!next.knownBaseline()) {
            Optional<ObservationStore.StoredContinuation> existing = read(observation.pipelineId());
            if (existing.isPresent() && !sameBinding(existing.orElseThrow().continuation(), next)) {
                return new ObservationStore.PublicationResult(false, Optional.empty());
            }
            if (existing.filter(saved -> saved.continuation().knownBaseline()).isPresent()) {
                return new ObservationStore.PublicationResult(false, Optional.empty());
            }
        }
        SourceProof proof = next.knownBaseline() ? null : proveUnknownSource(observation.pipelineId(), next);
        if (proof != null && proof.hasKnownFacts()) {
            return new ObservationStore.PublicationResult(false, Optional.empty());
        }
        FloorWriter writer = new FloorWriter(observation.pipelineId(), next, store.expectedReceipt(), null);
        try {
            var encoded = LatestObservationPayloadCodec.encodeContinuation(observation.pipelineId(), next, writer);
            Document floor = descriptor(observation.pipelineId(), next, writer.token, encoded);
            return publishPacket(observation, scope, floor, store.expectedReceipt(), proof, false);
        } catch (StaleFloor refused) {
            return new ObservationStore.PublicationResult(false, Optional.empty());
        }
    }

    private ObservationStore.PublicationResult publishPacket(Observation observation, ObservationStore.Scope scope,
            Document floor, Optional<ObservationStore.ContinuationReceipt> expected, SourceProof proof, boolean reset) {
        MongoLatestObservationStorage.PreparedCurrent publicFrame = latest.prepareCurrent(observation, scope);
        Binary key = MongoLatestObservationStorage.manifestKey(observation.pipelineId());
        Binary owner = MongoLatestObservationStorage.ownerDigest(observation.pipelineId());
        Document filter = new Document(publicFrame.filter());
        appendExpected(filter, expected);
        if (proof != null) { proof.appendGuard(filter); }
        if (floor != null && "chunked".equals(floor.get("mode"))) {
            filter.append(CONTINUATION_PENDING + ".publicationToken", floor.get("publicationToken"));
            List<Object> clauses = new ArrayList<>();
            if (filter.containsKey("$and")) { clauses.addAll(filter.getList("$and", Object.class)); }
            clauses.add(new Document("$expr", new Document("$gt", List.of(
                    "$" + CONTINUATION_PENDING + ".publishUntil", "$$NOW"))));
            filter.put("$and", clauses);
        }
        List<Bson> update = new ArrayList<>(MongoLatestObservationStorage.currentUpdate(owner, publicFrame.descriptor()));
        Document first = (Document) ((Document) update.getFirst()).get("$set");
        if (floor != null) { first.append(CONTINUATION, new Document("$literal", floor)); }
        if (reset) { update.add(new Document("$unset", CONTINUATION)); }
        if (floor != null) { update.add(new Document("$unset", CONTINUATION_PENDING)); }
        try {
            Document applied = StoreIo.call(observation.pipelineId(), () -> manifests.withTimeout(IO_SECONDS, TimeUnit.SECONDS)
                    .findOneAndUpdate(new Document("$and", List.of(filter,
                            new Document("current", new Document("$exists", true)))), update, AFTER));
            Document before = applied != null ? null : StoreIo.call(observation.pipelineId(), () -> manifests
                    .withTimeout(IO_SECONDS, TimeUnit.SECONDS).find(new Document("_id", key))
                    .projection(new Document("current", 1).append("legacyFallback", 1).append("revision", 1)).first());
            if (applied == null && before != null && !before.containsKey("current")) {
                applied = StoreIo.call(observation.pipelineId(), () -> {
                    try (ClientSession session = client.startSession()) {
                        return session.withTransaction(() -> {
                            Document header = manifests.find(session, new Document("_id", key)).first();
                            if (header == null || !Objects.equals(header.get("revision"), before.get("revision"))) {
                                return null;
                            }
                            Document legacy = manifests.find(session, new Document("_id", observation.pipelineId())).first();
                            if (proof != null && !Objects.equals(legacy, proof.legacy())) { return null; }
                            if (!MongoLatestObservationStorage.legacyAllows(legacy, observation, scope)) { return null; }
                            if (legacy != null) {
                                String state = text(legacy.get("state"));
                                Document fence = MongoLatestObservationStorage.legacyFence(legacy).append("state", state);
                                if (manifests.updateOne(session, fence, new Document("$set", new Document("state", "")))
                                        .getMatchedCount() != 1) { throw MongoStopReservationWrites.fencedHandoff(); }
                                if (manifests.updateOne(session, MongoLatestObservationStorage.legacyFence(legacy)
                                        .append("state", ""), new Document("$set", new Document("state", state)))
                                        .getMatchedCount() != 1) { throw MongoStopReservationWrites.fencedHandoff(); }
                            }
                            first.append("legacyResidue", legacy != null);
                            return manifests.findOneAndUpdate(session, filter, update, AFTER);
                        });
                    }
                });
            }
            if (applied == null) { return new ObservationStore.PublicationResult(false, Optional.empty()); }
            Optional<ObservationStore.ContinuationReceipt> receipt = applied.get(CONTINUATION) instanceof Document retained
                    ? Optional.of(receipt(observation.pipelineId(), retained)) : Optional.empty();
            return new ObservationStore.PublicationResult(true, receipt);
        } finally {
            latest.abandon(publicFrame);
        }
    }

    private record SourceProof(Document current, Document legacy, boolean hasKnownFacts) {
        private boolean unchanged(Document header) { return Objects.equals(current, header.get("current")); }
        private void appendGuard(Document filter) {
            filter.append("current", current == null ? new Document("$exists", false) : current);
        }
    }

    private SourceProof proveUnknownSource(String pipelineId, ObservationContinuation next) {
        Binary key = MongoLatestObservationStorage.manifestKey(pipelineId);
        Document header = StoreIo.call(pipelineId, () -> manifests.withTimeout(IO_SECONDS, TimeUnit.SECONDS)
                .find(new Document("_id", key)).first());
        Document current = header == null || !header.containsKey("current") ? null
                : document(header.get("current"), pipelineId, "current");
        ObservationStore.Scope source = next.baselineOrigin().map(ObservationContinuation.Target::scope)
                .orElse(next.sourceScope());
        Document legacy = StoreIo.call(pipelineId, () -> manifests.withTimeout(IO_SECONDS, TimeUnit.SECONDS)
                .find(new Document("_id", pipelineId)).first());
        if (source == null) { return new SourceProof(current, legacy, false); }
        Observation publicSource;
        if (current == null) {
            if (legacy == null || !(legacy.get("pipelineIncarnationId") instanceof String incarnation)
                    || !(legacy.get("executionGeneration") instanceof Number generation)
                    || !source.pipelineIncarnationId().equals(incarnation)
                    || source.executionGeneration() != generation.longValue()) {
                return new SourceProof(null, legacy, false);
            }
            publicSource = MongoObservationStore.toObservation(legacy);
        } else {
            ObservationStore.Stored saved = latest.readPublicDescriptor(pipelineId, current);
            if (saved.scope().filter(source::equals).isEmpty()) { return new SourceProof(current, legacy, false); }
            publicSource = saved.observation();
        }
        boolean known = publicSource.facts().stream()
                .anyMatch(fact -> fact.type() != MetricType.GAUGE
                        && fact.points().stream().anyMatch(point -> point.startTime() != null)
                        && CardinalityBudget.forInstrument(fact.name()).isPresent());
        return new SourceProof(current, legacy, known);
    }

    private void ensureHeader(ClientSession session, String pipelineId, Binary key, Binary owner) {
        boolean legacyExists = manifests.find(session, new Document("_id", pipelineId))
                .projection(new Document("_id", 1)).first() != null;
        manifests.updateOne(session, new Document("_id", key),
                new Document("$setOnInsert", new Document("formatVersion", MongoLatestObservationStorage.FORMAT_VERSION)
                        .append("ownerDigest", owner).append("revision", UUID.randomUUID().toString())
                        .append("legacyFallback", true).append("legacyResidue", legacyExists)), new UpdateOptions().upsert(true));
    }

    private static boolean expectedMatches(Document header, Optional<ObservationStore.ContinuationReceipt> previous) {
        if (previous.isEmpty()) { return !header.containsKey(CONTINUATION); }
        return header.get(CONTINUATION) instanceof Document present
                && previous.orElseThrow().revision().equals(present.get("revision"))
                && previous.orElseThrow().digest().equals(present.get("integrityDigest"));
    }

    private static void appendExpected(Document filter, Optional<ObservationStore.ContinuationReceipt> previous) {
        if (previous.isEmpty()) { filter.append(CONTINUATION, new Document("$exists", false)); }
        else { filter.append(CONTINUATION + ".revision", previous.orElseThrow().revision())
                .append(CONTINUATION + ".integrityDigest", previous.orElseThrow().digest()); }
    }

    private static boolean sameLineage(ObservationContinuation one, ObservationContinuation two) {
        return one.token().equals(two.token()) && Objects.equals(one.sourceScope(), two.sourceScope());
    }
    private static boolean sameBinding(ObservationContinuation one, ObservationContinuation two) {
        return sameLineage(one, two) && one.target().equals(two.target())
                && one.baselineOrigin().equals(two.baselineOrigin());
    }

    private static boolean qualifiedOrigin(StopReservation expected,
            Optional<ObservationStore.ContinuationReceipt> previous, ObservationContinuation requested,
            Optional<ObservationStore.StoredContinuation> existing) {
        Optional<ObservationContinuation> same = existing.map(ObservationStore.StoredContinuation::continuation)
                .filter(saved -> sameLineage(saved, requested));
        if (requested.baselineOrigin().isEmpty()) {
            return same.flatMap(ObservationContinuation::baselineOrigin).isEmpty();
        }
        ObservationContinuation.Target origin = requested.baselineOrigin().orElseThrow();
        if (expected.phase() == StopReservation.Phase.SUCCESSOR_BOUND && expected.successor() != null
                && origin.scope().equals(expected.successor().scope())
                && origin.realJob().filter(expected.successor().job()::equals).isPresent()) { return true; }
        if (existing.isEmpty() || previous.filter(existing.orElseThrow().receipt()::equals).isEmpty() || same.isEmpty()) {
            return false;
        }
        ObservationContinuation saved = same.orElseThrow();
        if (saved.baselineOrigin().filter(origin::equals).isPresent()) { return true; }
        return saved.target().filter(origin::equals).isPresent() && expected.writerAuthority() != null
                && origin.scope().executionGeneration() <= expected.writerAuthority().executionGeneration()
                && (expected.successor() == null
                        || origin.scope().executionGeneration() < expected.successor().scope().executionGeneration())
                && origin.realJob().filter(job -> job.clusterId().equals(expected.writerAuthority().clusterId())).isPresent();
    }

    private static boolean qualifiedRetarget(Optional<ObservationStore.ContinuationReceipt> previous,
            ObservationContinuation requested, Optional<ObservationStore.StoredContinuation> existing) {
        if (existing.isEmpty() || !sameLineage(existing.orElseThrow().continuation(), requested)) { return true; }
        ObservationStore.StoredContinuation saved = existing.orElseThrow();
        Optional<ObservationContinuation.Target> oldTarget = saved.continuation().target()
                .filter(target -> target.realJob().isPresent());
        if (oldTarget.isEmpty() || oldTarget.equals(requested.target())) { return true; }
        if (previous.filter(saved.receipt()::equals).isEmpty()
                || !oldTarget.equals(requested.baselineOrigin()) || !requested.producerStates().isEmpty()) { return false; }
        Map<String, MetricFact> next = new java.util.HashMap<>();
        requested.baselineFacts().forEach(fact -> next.put(fact.name(), fact));
        for (MetricFact floor : saved.continuation().baselineFacts()) {
            if (!dominates(next.get(floor.name()), floor)) { return false; }
        }
        for (ObservationContinuation.ProducerState state : saved.continuation().producerStates()) {
            if (!dominates(next.get(state.name()), new MetricFact(state.name(), state.type(), state.unit(), state.published()))) {
                return false;
            }
        }
        return true;
    }

    private static boolean dominates(MetricFact next, MetricFact previous) {
        if (previous.points().isEmpty()) { return true; }
        if (next == null || next.type() != previous.type() || !next.unit().equals(previous.unit())) { return false; }
        Map<Map<String, String>, io.tapstate.core.lifecycle.MetricPoint> points = new java.util.HashMap<>();
        next.points().forEach(point -> points.put(point.attributes(), point));
        for (var old : previous.points()) {
            var point = points.get(old.attributes());
            if (point == null) { return false; }
            if (old.value() != null) {
                if (point.value() == null || point.value() < old.value()) { return false; }
            } else {
                if (point.histogram() == null || point.histogram().count() < old.histogram().count()
                        || point.histogram().sum() < old.histogram().sum()) { return false; }
                for (int index = 0; index < old.histogram().bucketCounts().size(); index++) {
                    if (point.histogram().bucketCounts().get(index) < old.histogram().bucketCounts().get(index)) { return false; }
                }
            }
        }
        return true;
    }

    private void requireGuard() {
        if (guard == null) { throw new UnsupportedOperationException("the store has no shared handoff write guard"); }
    }
    private static void requireMarker(String pipelineId, StopReservation expected, ObservationContinuation next) {
        if (!pipelineId.equals(expected.pipelineId()) || !expected.token().equals(next.token())
                || expected.counterPolicy() != StopReservation.CounterPolicy.CONTINUE
                || !Objects.equals(expected.source().scope(), next.sourceScope())) {
            throw new IllegalArgumentException("the private carrier belongs to the exact continuing handoff");
        }
        if (next.target().isPresent() && (expected.successor() == null
                || !expected.successor().scope().equals(next.target().orElseThrow().scope())
                || !next.target().orElseThrow().realJob().equals(Optional.ofNullable(expected.successor().job())))) {
            throw new IllegalArgumentException("the private carrier target is the recorded successor");
        }
    }

    private final class FloorWriter implements LatestObservationPayloadCodec.ChunkWriter {
        private final String pipelineId;
        private final ObservationContinuation value;
        private final Optional<ObservationStore.ContinuationReceipt> previous;
        private final StopReservation expected;
        private final Binary key, owner;
        private final String token = UUID.randomUUID().toString();
        private boolean begun;
        private long nextHeartbeat;

        private FloorWriter(String pipelineId, ObservationContinuation value,
                Optional<ObservationStore.ContinuationReceipt> previous, StopReservation expected) {
            this.pipelineId = pipelineId; this.value = value; this.previous = previous; this.expected = expected;
            ObservationBsonBounds.requireHeader(pipelineId, metadata(value));
            key = MongoLatestObservationStorage.manifestKey(pipelineId);
            owner = MongoLatestObservationStorage.ownerDigest(pipelineId);
        }

        @Override public void begin() {
            if (begun) { throw new IllegalStateException("one private encoding acquires one pending lease"); }
            Document pending = metadata(value).append("publicationToken", token).append("encodingVersion", ENCODING_VERSION);
            ObservationBsonBounds.requireDescriptor(pipelineId, pending);
            Document pendingExpression = new Document("$mergeObjects", List.of(new Document("$literal", pending),
                    new Document("publishUntil", MongoLatestObservationStorage.leaseUntilExpression())));
            java.util.function.Function<ClientSession, Boolean> reserve = session -> {
                if (session != null) { ensureHeader(session, pipelineId, key, owner); }
                Document filter = MongoLatestObservationStorage.headerFilter(key, owner);
                appendExpected(filter, previous);
                filter.append("$or", List.of(new Document(CONTINUATION_PENDING, new Document("$exists", false)),
                        new Document("$expr", new Document("$lte", List.of(
                                "$" + CONTINUATION_PENDING + ".publishUntil", "$$NOW")))));
                List<Bson> update = List.of(new Document("$set", new Document(CONTINUATION_PENDING, pendingExpression)
                        .append("revision", UUID.randomUUID().toString())));
                return (session == null ? manifests.withTimeout(IO_SECONDS, TimeUnit.SECONDS).updateOne(filter, update)
                        : manifests.updateOne(session, filter, update)).getMatchedCount() != 0;
            };
            boolean reserved = expected != null ? guard.withExpectedHandoff(expected, reserve).orElse(false)
                    : StoreIo.call(pipelineId, () -> reserve.apply(null));
            if (!reserved) { throw new StaleFloor(); }
            begun = true;
            nextHeartbeat = System.nanoTime() + MongoLatestObservationStorage.PUBLISH_HEARTBEAT_NANOS;
        }

        @Override public void write(LatestObservationPayloadCodec.Chunk chunk) {
            if (!begun || Thread.currentThread().isInterrupted()) { throw new StaleFloor(); }
            if (System.nanoTime() - nextHeartbeat >= 0) {
                Document filter = MongoLatestObservationStorage.headerFilter(key, owner)
                        .append(CONTINUATION_PENDING + ".publicationToken", token)
                        .append("$expr", new Document("$gt", List.of(
                                "$" + CONTINUATION_PENDING + ".publishUntil", "$$NOW")));
                long matched = StoreIo.call(pipelineId, () -> manifests.withTimeout(IO_SECONDS, TimeUnit.SECONDS)
                        .updateOne(filter, List.of(new Document("$set", new Document(
                                CONTINUATION_PENDING + ".publishUntil", MongoLatestObservationStorage.leaseUntilExpression()))))
                        .getMatchedCount());
                if (matched == 0) { throw new StaleFloor(); }
                nextHeartbeat = System.nanoTime() + MongoLatestObservationStorage.PUBLISH_HEARTBEAT_NANOS;
            }
            Document row = new Document("_id", MongoLatestObservationStorage.chunkId(key, owner, token, chunk.ordinal(), ENCODING_VERSION))
                    .append("manifestKey", key).append("ownerDigest", owner).append("encodingVersion", ENCODING_VERSION)
                    .append("publicationToken", token).append("ordinal", chunk.ordinal())
                    .append("chunkDigest", new Binary(chunk.digest())).append("state", "ACTIVE")
                    .append("payload", new Binary(chunk.payload()));
            StoreIo.run(() -> chunks.withTimeout(IO_SECONDS, TimeUnit.SECONDS).insertOne(row));
        }
    }

    private static final class StaleFloor extends RuntimeException { }

    Optional<ObservationStore.StoredContinuation> read(String pipelineId) {
        Binary key = MongoLatestObservationStorage.manifestKey(pipelineId);
        Binary owner = MongoLatestObservationStorage.ownerDigest(pipelineId);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(READ_SECONDS);
        for (int attempt = 0; attempt < 2; attempt++) {
            Document header = StoreIo.call(pipelineId, () -> manifests.withTimeout(remaining(deadline), TimeUnit.MILLISECONDS)
                    .find(new Document("_id", key)).first());
            if (header == null) { return Optional.empty(); }
            MongoLatestObservationStorage.validateHeader(header, pipelineId, owner);
            Object raw = header.get(CONTINUATION);
            if (raw == null) {
                if (header.containsKey(CONTINUATION)) { throw corrupt(pipelineId, CONTINUATION); }
                return Optional.empty();
            }
            Document descriptor = document(raw, pipelineId, CONTINUATION);
            try {
                return Optional.of(decode(pipelineId, key, owner, descriptor, deadline));
            } catch (TapstateException failure) {
                if (attempt != 0) { throw failure; }
                Document current = StoreIo.call(pipelineId, () -> manifests
                        .withTimeout(remaining(deadline), TimeUnit.MILLISECONDS).find(new Document("_id", key))
                        .projection(new Document(CONTINUATION + ".revision", 1)).first());
                if (current == null || !(current.get(CONTINUATION) instanceof Document found)
                        || Objects.equals(found.get("revision"), descriptor.get("revision"))) { throw failure; }
            }
        }
        throw new IllegalStateException("a bounded private read exhausted its fixed attempts");
    }

    private ObservationStore.StoredContinuation decode(String pipelineId, Binary key, Binary owner,
            Document descriptor, long deadline) {
        try {
            int encoding = integer(descriptor.get("encodingVersion"));
            if (encoding != ENCODING_VERSION) { throw new IllegalArgumentException("private payload version"); }
            long bytes = positiveLong(descriptor.get("encodedBytes"));
            byte[] digest = binary(descriptor.get("payloadDigest"));
            LatestObservationPayloadCodec.ContinuationState state;
            if ("inline".equals(descriptor.get("mode"))) {
                state = LatestObservationPayloadCodec.decodeContinuationInline(binary(descriptor.get("inlinePayload")),
                        digest, bytes);
            } else if ("chunked".equals(descriptor.get("mode"))) {
                String token = text(descriptor.get("publicationToken"));
                long count = positiveLong(descriptor.get("chunkCount"));
                try (MongoCursor<Document> cursor = StoreIo.call(pipelineId, () -> chunks
                        .withTimeout(remaining(deadline), TimeUnit.MILLISECONDS)
                        .find(new Document("manifestKey", key).append("ownerDigest", owner)
                                .append("publicationToken", token).append("encodingVersion", ENCODING_VERSION))
                        .sort(new Document("ordinal", 1)).batchSize(MongoLatestObservationStorage.READ_CHUNK_BATCH_SIZE)
                        .iterator())) {
                    Iterable<LatestObservationPayloadCodec.Chunk> stream = () -> new Iterator<>() {
                        @Override public boolean hasNext() {
                            remaining(deadline);
                            return StoreIo.call(cursor::hasNext);
                        }
                        @Override public LatestObservationPayloadCodec.Chunk next() {
                            remaining(deadline);
                            Document chunk = StoreIo.call(cursor::next);
                            if (!key.equals(chunk.get("manifestKey")) || !owner.equals(chunk.get("ownerDigest"))
                                    || !token.equals(chunk.get("publicationToken"))
                                    || integer(chunk.get("encodingVersion")) != ENCODING_VERSION) {
                                throw corrupt(pipelineId, "private chunk identity");
                            }
                            return new LatestObservationPayloadCodec.Chunk(nonnegativeLong(chunk.get("ordinal")),
                                    binary(chunk.get("payload")), binary(chunk.get("chunkDigest")));
                        }
                    };
                    state = LatestObservationPayloadCodec.decodeContinuationChunks(stream, count, bytes, digest);
                }
            } else {
                throw new IllegalArgumentException("private payload mode");
            }
            if (!pipelineId.equals(state.pipelineId())) { throw new IllegalArgumentException("private pipeline owner"); }
            ObservationContinuation continuation = new ObservationContinuation(text(descriptor.get("handoffToken")),
                    scopeOrNull(descriptor.get("sourceScope")), target(descriptor.get("target")),
                    target(descriptor.get("baselineOrigin")), state.baselineFacts(), state.producerStates());
            boolean known = bool(descriptor.get("knownBaseline"));
            if (known != continuation.knownBaseline()
                    || !receiptDigest(descriptor).equals(text(descriptor.get("integrityDigest")))) {
                throw new IllegalArgumentException("private carrier integrity");
            }
            return new ObservationStore.StoredContinuation(continuation, receipt(pipelineId, descriptor));
        } catch (TapstateException coded) {
            throw coded;
        } catch (RuntimeException malformed) {
            throw corrupt(pipelineId, "private continuation payload");
        }
    }

    private static ObservationStore.ContinuationReceipt receipt(String pipelineId, Document descriptor) {
        return new ObservationStore.ContinuationReceipt(pipelineId, text(descriptor.get("revision")),
                text(descriptor.get("integrityDigest")), integer(descriptor.get("encodingVersion")),
                text(descriptor.get("handoffToken")), scopeOrNull(descriptor.get("sourceScope")),
                target(descriptor.get("target")), target(descriptor.get("baselineOrigin")),
                bool(descriptor.get("knownBaseline")));
    }

    static Document metadata(ObservationContinuation value) {
        Document header = new Document("handoffToken", value.token()).append("knownBaseline", value.knownBaseline());
        if (value.sourceScope() != null) { header.append("sourceScope", scopeDocument(value.sourceScope())); }
        value.target().ifPresent(target -> header.append("target", targetDocument(target)));
        value.baselineOrigin().ifPresent(origin -> header.append("baselineOrigin", targetDocument(origin)));
        return header;
    }

    private static Document descriptor(String pipelineId, ObservationContinuation value, String publicationToken,
            LatestObservationPayloadCodec.Encoded encoded) {
        Document descriptor = metadata(value).append("revision", UUID.randomUUID().toString())
                .append("encodingVersion", ENCODING_VERSION).append("publicationToken", publicationToken)
                .append("mode", encoded.inline() ? "inline" : "chunked")
                .append("payloadDigest", new Binary(encoded.payloadDigest())).append("encodedBytes", encoded.encodedBytes());
        if (encoded.inline()) { descriptor.append("inlinePayload", new Binary(encoded.inlinePayload())); }
        else { descriptor.append("chunkCount", encoded.chunkCount()); }
        descriptor.append("integrityDigest", receiptDigest(descriptor));
        ObservationBsonBounds.requireDescriptor(pipelineId, descriptor);
        return descriptor;
    }

    private static String receiptDigest(Document descriptor) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update("tapstate/private-continuation/v1\0".getBytes(StandardCharsets.UTF_8));
            Document identity = new Document("handoffToken", descriptor.get("handoffToken"))
                    .append("sourceScope", descriptor.get("sourceScope")).append("target", descriptor.get("target"))
                    .append("baselineOrigin", descriptor.get("baselineOrigin"))
                    .append("knownBaseline", descriptor.get("knownBaseline"))
                    .append("encodingVersion", descriptor.get("encodingVersion"))
                    .append("revision", descriptor.get("revision"))
                    .append("publicationToken", descriptor.get("publicationToken"))
                    .append("mode", descriptor.get("mode"))
                    .append("encodedBytes", descriptor.get("encodedBytes"))
                    .append("chunkCount", descriptor.get("chunkCount"));
            digest.update(identity.toJson().getBytes(StandardCharsets.UTF_8));
            digest.update(binary(descriptor.get("payloadDigest")));
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("the JDK has no SHA-256 implementation", impossible);
        }
    }

    static Document scopeDocument(ObservationStore.Scope scope) {
        return new Document("pipelineIncarnationId", scope.pipelineIncarnationId())
                .append("executionGeneration", scope.executionGeneration());
    }

    private static Document targetDocument(ObservationContinuation.Target target) {
        Document document = new Document("scope", scopeDocument(target.scope()));
        target.realJob().ifPresent(job -> document.append("job", new Document("clusterId", job.clusterId())
                .append("jobId", job.jobId()).append("bootId", job.bootId())));
        return document;
    }

    static ObservationStore.Scope scopeOrNull(Object raw) {
        if (raw == null) { return null; }
        Document scope = document(raw, "continuation", "scope");
        return new ObservationStore.Scope(text(scope.get("pipelineIncarnationId")),
                positiveLong(scope.get("executionGeneration")));
    }

    static Optional<ObservationContinuation.Target> target(Object raw) {
        if (raw == null) { return Optional.empty(); }
        Document target = document(raw, "continuation", "target");
        ObservationStore.Scope scope = Objects.requireNonNull(scopeOrNull(target.get("scope")), "target scope");
        Optional<StopReservation.JobIdentity> job = Optional.empty();
        if (target.containsKey("job")) {
            Document real = document(target.get("job"), "continuation", "job");
            if (!(real.get("jobId") instanceof Long id)) { throw new IllegalArgumentException("a real job id is int64"); }
            job = Optional.of(new StopReservation.JobIdentity(text(real.get("clusterId")), id, text(real.get("bootId"))));
        }
        return Optional.of(new ObservationContinuation.Target(scope, job));
    }

    private static long remaining(long deadline) {
        long millis = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
        if (millis <= 0) { throw new TapstateException(IoError.STORE_UNAVAILABLE,
                Map.of("detail", "the bounded private continuation read deadline expired"), null); }
        return millis;
    }

    private static Document document(Object value, String pipelineId, String field) {
        if (!(value instanceof Document document)) { throw corrupt(pipelineId, field); }
        return document;
    }
    private static String text(Object value) {
        if (!(value instanceof String text) || text.isBlank()) { throw new IllegalArgumentException("private string"); }
        return text;
    }
    private static int integer(Object value) {
        if (!(value instanceof Integer number)) { throw new IllegalArgumentException("private integer"); }
        return number;
    }
    private static long nonnegativeLong(Object value) {
        if (!(value instanceof Long number) || number < 0) { throw new IllegalArgumentException("private count"); }
        return number;
    }
    private static long positiveLong(Object value) {
        long number = nonnegativeLong(value);
        if (number == 0) { throw new IllegalArgumentException("private count is positive"); }
        return number;
    }
    private static byte[] binary(Object value) {
        if (!(value instanceof Binary binary)) { throw new IllegalArgumentException("private binary"); }
        return binary.getData();
    }
    private static boolean bool(Object value) {
        if (!(value instanceof Boolean flag)) { throw new IllegalArgumentException("private boolean"); }
        return flag;
    }
    private static TapstateException corrupt(String pipelineId, String field) {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", pipelineId, "field", field), null);
    }
}
