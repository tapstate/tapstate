package io.tapstate.adapters.mongostore;

import com.mongodb.MongoException;
import com.mongodb.MongoWriteException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.ClientSession;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimAttempt;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimReading;
import io.tapstate.spi.store.WorkloadClaimStore;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.bson.Document;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** Mongo server-time implementation of the cluster-scoped workload-claim port. */
public final class MongoWorkloadClaimStore implements WorkloadClaimStore {

    private static final int DUPLICATE_KEY = 11000;
    private static final String LEASE_REMAINING = "leaseRemainingMillis";
    private static final List<String> CONTEXT_FIELDS = List.of("contextExecutionGeneration", "executionClaimGeneration",
            "executionNodeIds", "failureClaimGeneration", "failureAfterMemberLoss");
    private final MongoCollection<Document> collection;
    private final ClientSession session;
    private final boolean strict;

    public MongoWorkloadClaimStore(MongoCollection<Document> collection) {
        this(collection, null, false);
    }

    private MongoWorkloadClaimStore(MongoCollection<Document> collection, ClientSession session, boolean strict) {
        this.session = session; this.strict = strict;
        this.collection = Objects.requireNonNull(collection, "collection")
                .withReadPreference(ReadPreference.primary())
                .withReadConcern(ReadConcern.MAJORITY)
                .withWriteConcern(WriteConcern.MAJORITY.withJournal(true));
    }

    /** A callback-local view; the owning cache domain controls transaction scope and authorization. */
    MongoWorkloadClaimStore inSession(ClientSession original) {
        return new MongoWorkloadClaimStore(collection, Objects.requireNonNull(original, "session"), strict);
    }

    /** New-layout domains reject damaged facts before any update can default or reconstruct them. */
    MongoWorkloadClaimStore inStrictSession(ClientSession original) {
        return new MongoWorkloadClaimStore(collection, Objects.requireNonNull(original, "session"), true);
    }

    private void validateExisting(WorkloadClaimKey key) { if (strict) read(key); }

    @Override
    public WorkloadClaimAttempt acquire(
            WorkloadClaimKey key, WorkloadOwner owner, long topologyRevision, Duration ttl) {
        validate(key, owner, topologyRevision, ttl);
        validateExisting(key);
        Document id = id(key);
        Document eligible = new Document("$and", List.of(
                new Document("_id", id),
                new Document("$or", List.of(
                        ownerFilter(owner),
                        new Document("$expr", new Document("$lte", List.of("$leaseUntil", "$$NOW")))))));
        Document updated = findOneAndUpdate(eligible, acquirePipeline(key, owner, topologyRevision, ttl), false);
        if (updated != null) {
            return WorkloadClaimAttempt.acquired(read(updated));
        }

        // A duplicate-key error aborts a caller-owned transaction. An existing other tenure is a
        // normal refusal, so report it before attempting the insert-only upsert below.
        Optional<WorkloadClaimReading> held = read(key);
        if (held.isPresent()) return WorkloadClaimAttempt.refused(held.get().claim());

        Document absent = new Document("$and", List.of(
                new Document("_id", id),
                new Document("ownerNodeId", new Document("$exists", false))));
        try {
            updated = rawFindOneAndUpdate(absent, acquirePipeline(key, owner, topologyRevision, ttl), true);
        } catch (MongoException raced) {
            if (session != null) throw raced;
            if (!duplicateKey(raced)) {
                throw StoreIo.coded(raced);
            }
            updated = null;
        }
        if (updated != null) {
            return WorkloadClaimAttempt.acquired(read(updated));
        }
        WorkloadClaim current = read(key).map(WorkloadClaimReading::claim).orElseThrow(
                () -> new IllegalStateException("contended workload claim vanished: " + key));
        return WorkloadClaimAttempt.refused(current);
    }

    /** Existing-only continuation for a retained preparation; this method has no absent-key branch. */
    WorkloadClaimAttempt acquireExisting(WorkloadClaimKey key, WorkloadOwner owner, long topologyRevision, Duration ttl) {
        validate(key, owner, topologyRevision, ttl);
        var existing = read(key).orElseThrow(() -> new io.tapstate.core.common.TapstateException(
                io.tapstate.spi.store.IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null));
        Document eligible = new Document("_id", id(key)).append("$or", List.of(ownerFilter(owner),
                new Document("$expr", new Document("$lte", List.of("$leaseUntil", "$$NOW")))));
        Document updated = findOneAndUpdate(eligible, acquirePipeline(key, owner, topologyRevision, ttl), false);
        return updated == null ? WorkloadClaimAttempt.refused(existing.claim()) : WorkloadClaimAttempt.acquired(read(updated));
    }

    @Override
    public Optional<WorkloadClaim> renew(WorkloadClaim expected, Duration ttl) {
        Objects.requireNonNull(expected, "expected");
        positive(ttl);
        validateExisting(expected.key());
        Document filter = liveExpected(expected, expected.topologyRevision());
        Document lease = new Document("$set", new Document("leaseUntil", leaseUntil(ttl)));
        Document renewed = findOneAndUpdate(filter, List.of(lease), false);
        return Optional.ofNullable(renewed).map(this::read);
    }

    @Override
    public boolean release(WorkloadClaim expected) {
        Objects.requireNonNull(expected, "expected");
        validateExisting(expected.key());
        Document filter = expected(expected);
        Document expired = new Document("$set", new Document("leaseUntil", "$$NOW"));
        return findOneAndUpdate(filter, List.of(expired), false) != null;
    }

    @Override
    public Optional<WorkloadClaim> advanceExecution(
            WorkloadClaim expected, long topologyRevision, Set<String> executionNodeIds) {
        Objects.requireNonNull(expected, "expected");
        Objects.requireNonNull(executionNodeIds, "executionNodeIds");
        if (topologyRevision < 0) {
            throw new IllegalArgumentException("topologyRevision must not be negative");
        }
        validateExisting(expected.key());
        Document nextGeneration = new Document("$add", List.of(
                new Document("$ifNull", List.of("$executionGeneration", 0L)), 1L));
        Document next = new Document("$set", new Document("executionGeneration", nextGeneration)
                .append("contextExecutionGeneration", nextGeneration)
                .append("executionClaimGeneration", "$claimGeneration")
                .append("executionNodeIds", executionNodeIds.stream().sorted().toList())
                .append("failureClaimGeneration", 0L)
                .append("failureAfterMemberLoss", false));
        Document advanced = findOneAndUpdate(liveExpected(expected, topologyRevision), List.of(next), false);
        return Optional.ofNullable(advanced).map(this::read);
    }

    @Override
    public Optional<WorkloadClaim> recordExecutionFailure(WorkloadClaim expected, boolean afterMemberLoss) {
        Objects.requireNonNull(expected, "expected");
        validateExisting(expected.key());
        Document unrecorded = new Document("$eq", List.of(
                new Document("$ifNull", List.of("$failureClaimGeneration", 0L)), 0L));
        Document next = new Document("$set", new Document("failureClaimGeneration",
                new Document("$cond", List.of(unrecorded, "$claimGeneration", "$failureClaimGeneration")))
                .append("failureAfterMemberLoss", new Document("$cond", List.of(
                        unrecorded, afterMemberLoss, "$failureAfterMemberLoss"))));
        Document sameExecution = new Document("$expr", new Document("$eq", List.of(
                "$contextExecutionGeneration", "$executionGeneration")));
        Document filter = new Document("$and", List.of(
                liveExpected(expected, expected.topologyRevision()), sameExecution));
        Document recorded = findOneAndUpdate(filter, List.of(next), false);
        return Optional.ofNullable(recorded).map(this::read);
    }

    @Override
    public Optional<WorkloadClaimReading> read(WorkloadClaimKey key) {
        Objects.requireNonNull(key, "key");
        // An aggregation rather than a find, for one field: how long this lease still has to run has to be
        // worked out where $$NOW is -- on the server that holds the lease -- so that no clock of ours is
        // ever subtracted from a deadline of theirs.
        List<Document> pipeline = List.of(
                new Document("$match", new Document("_id", id(key))),
                leaseRemaining());
        Document found = io(() -> (session == null ? collection.aggregate(pipeline)
                : collection.aggregate(session, pipeline)).first());
        return Optional.ofNullable(found).map(this::reading);
    }

    /**
     * One aggregation for every key: the same match on the whole id and the same lease arithmetic as
     * {@link #read}, so each claim comes back as its own read would answer it, and every lease in the
     * answer is measured against the one {@code $$NOW} the server evaluated the aggregation at.
     *
     * <p>The ids are built by {@link #id}, which is not a tidiness: BSON compares sub-documents field by
     * field in order, so an id assembled in any other order matches nothing -- and matching nothing is also
     * what a key nobody has claimed answers, so every claim would read as unowned. Each document is filed
     * back under the key whose id it matched, which is the key it was asked for.
     */
    @Override
    public Map<WorkloadClaimKey, WorkloadClaimReading> readAll(Collection<WorkloadClaimKey> keys) {
        Objects.requireNonNull(keys, "keys");
        Map<Document, WorkloadClaimKey> asked = new LinkedHashMap<>();
        for (WorkloadClaimKey key : keys) {
            asked.put(id(Objects.requireNonNull(key, "key")), key);
        }
        Map<WorkloadClaimKey, WorkloadClaimReading> readings = new LinkedHashMap<>();
        if (asked.isEmpty()) {
            return readings;
        }
        List<Document> pipeline = List.of(
                new Document("$match", new Document("_id",
                        new Document("$in", new ArrayList<>(asked.keySet())))),
                leaseRemaining());
        List<Document> found = io(() -> (session == null ? collection.aggregate(pipeline)
                : collection.aggregate(session, pipeline)).into(new ArrayList<>()));
        for (Document document : found) {
            readings.put(asked.get(document.get("_id", Document.class)), reading(document));
        }
        return readings;
    }

    /** How long a lease still has to run, worked out on the server, where {@code $$NOW} is. */
    private Document leaseRemaining() {
        Document remaining = new Document("$subtract", List.of("$leaseUntil", "$$NOW"));
        if (strict) remaining = new Document("$cond", List.of(
                new Document("$eq", List.of(new Document("$type", "$leaseUntil"), "date")), remaining, 0L));
        return new Document("$set", new Document(LEASE_REMAINING, remaining));
    }

    private Document findOneAndUpdate(Document filter, List<Document> update, boolean upsert) {
        return io(() -> rawFindOneAndUpdate(filter, update, upsert));
    }

    /** Let a caller-owned transaction observe Mongo retry labels before IO errors are translated. */
    private <T> T io(java.util.function.Supplier<T> operation) {
        return session == null ? StoreIo.call(operation) : operation.get();
    }

    private Document rawFindOneAndUpdate(Document filter, List<Document> update, boolean upsert) {
        var options = new FindOneAndUpdateOptions().upsert(upsert).returnDocument(ReturnDocument.AFTER);
        return session == null ? collection.findOneAndUpdate(filter, update, options)
                : collection.findOneAndUpdate(session, filter, update, options);
    }

    private static List<Document> acquirePipeline(
            WorkloadClaimKey key, WorkloadOwner owner, long topologyRevision, Duration ttl) {
        Document sameOwner = new Document("$and", List.of(
                new Document("$eq", List.of("$ownerNodeId", owner.nodeId())),
                new Document("$eq", List.of("$ownerBootId", owner.bootId())),
                new Document("$gt", List.of("$leaseUntil", "$$NOW"))));
        Document generation = new Document("$cond", List.of(
                sameOwner,
                new Document("$ifNull", List.of("$claimGeneration", 1L)),
                new Document("$add", List.of(
                        new Document("$ifNull", List.of("$claimGeneration", 0L)), 1L))));
        Document fields = new Document("clusterId", key.clusterId())
                .append("resourceType", key.type().name())
                .append("resourceId", key.resourceId())
                .append("ownerNodeId", owner.nodeId())
                .append("ownerBootId", owner.bootId())
                .append("claimGeneration", generation)
                .append("executionGeneration",
                        new Document("$ifNull", List.of("$executionGeneration", 0L)))
                .append("topologyRevision", topologyRevision)
                .append("leaseUntil", leaseUntil(ttl));
        return List.of(new Document("$set", fields));
    }

    private static Document liveExpected(WorkloadClaim expected, long topologyRevision) {
        return new Document("$and", List.of(
                expected(expected),
                new Document("topologyRevision", topologyRevision),
                new Document("$expr", new Document("$gt", List.of("$leaseUntil", "$$NOW")))));
    }

    private static Document expected(WorkloadClaim expected) {
        return new Document("_id", id(expected.key()))
                .append("ownerNodeId", expected.owner().nodeId())
                .append("ownerBootId", expected.owner().bootId())
                .append("claimGeneration", expected.claimGeneration())
                .append("executionGeneration", expected.executionGeneration());
    }

    private static Document ownerFilter(WorkloadOwner owner) {
        return new Document("ownerNodeId", owner.nodeId()).append("ownerBootId", owner.bootId());
    }

    static Document id(WorkloadClaimKey key) {
        return new Document("clusterId", key.clusterId())
                .append("resourceType", key.type().name())
                .append("resourceId", key.resourceId());
    }

    private static Document leaseUntil(Duration ttl) {
        return new Document("$dateAdd", new Document("startDate", "$$NOW")
                .append("unit", "millisecond")
                .append("amount", ttl.toMillis()));
    }

    /** Decode a document already read by the caller's transaction, preserving the strict view's validation. */
    WorkloadClaim decodeStored(Document document) { return read(document); }

    private WorkloadClaim read(Document document) {
        if (!strict) return decode(document);
        try {
            for (String field : List.of("claimGeneration", "executionGeneration", "topologyRevision")) {
                if (!(document.get(field) instanceof Long value) || value < 0 || (field.equals("claimGeneration") && value == 0)) throw unreadableClaim();
            }
            boolean publishedContext = document.getLong("executionGeneration") > 0 || CONTEXT_FIELDS.stream().anyMatch(document::containsKey);
            if (publishedContext && CONTEXT_FIELDS.stream().anyMatch(field -> !document.containsKey(field))) throw unreadableClaim();
            for (String field : List.of("contextExecutionGeneration", "executionClaimGeneration", "failureClaimGeneration")) {
                if (document.containsKey(field) && (!(document.get(field) instanceof Long value) || value < 0)) throw unreadableClaim();
            }
            if (!(document.get("leaseUntil") instanceof Date)) throw unreadableClaim();
            if (document.containsKey("failureAfterMemberLoss") && !(document.get("failureAfterMemberLoss") instanceof Boolean)) throw unreadableClaim();
            if (document.containsKey("executionNodeIds")) {
                if (!(document.get("executionNodeIds") instanceof List<?> nodes) || nodes.stream().anyMatch(node -> !(node instanceof String text) || text.isBlank())
                        || new java.util.HashSet<>(nodes).size() != nodes.size()) throw unreadableClaim();
            }
            WorkloadClaim result = decode(document);
            if (!id(result.key()).equals(document.get("_id"))) throw unreadableClaim();
            return result;
        } catch (NullPointerException | IllegalArgumentException | ClassCastException | IllegalStateException invalid) {
            throw unreadableClaim();
        }
    }

    private static io.tapstate.core.common.TapstateException unreadableClaim() {
        return new io.tapstate.core.common.TapstateException(io.tapstate.spi.store.IoError.DOCUMENT_UNREADABLE,
                Map.of("id", "workload", "field", "claim"), null);
    }

    private static WorkloadClaim decode(Document document) {
        return new WorkloadClaim(
                new WorkloadClaimKey(
                        document.getString("clusterId"),
                        WorkloadClaimType.valueOf(document.getString("resourceType")),
                        document.getString("resourceId")),
                new WorkloadOwner(document.getString("ownerNodeId"), document.getString("ownerBootId")),
                number(document, "claimGeneration"),
                number(document, "executionGeneration"),
                number(document, "topologyRevision"),
                date(document, "leaseUntil").toInstant(),
                numberOrZero(document, "contextExecutionGeneration"),
                numberOrZero(document, "executionClaimGeneration"),
                Set.copyOf(document.getList("executionNodeIds", String.class, List.of())),
                numberOrZero(document, "failureClaimGeneration"),
                Boolean.TRUE.equals(document.getBoolean("failureAfterMemberLoss")));
    }

    private WorkloadClaimReading reading(Document document) {
        if (strict && !(document.get(LEASE_REMAINING) instanceof Long)) throw unreadableClaim();
        return new WorkloadClaimReading(
                read(document), Duration.ofMillis(number(document, LEASE_REMAINING)));
    }

    private static long number(Document document, String field) {
        Number value = document.get(field, Number.class);
        if (value == null) {
            throw new IllegalStateException("workload claim has no " + field);
        }
        return value.longValue();
    }

    private static long numberOrZero(Document document, String field) {
        Number value = document.get(field, Number.class);
        return value == null ? 0 : value.longValue();
    }

    private static Date date(Document document, String field) {
        Date value = document.getDate(field);
        if (value == null) {
            throw new IllegalStateException("workload claim has no " + field);
        }
        return value;
    }

    private static void validate(
            WorkloadClaimKey key, WorkloadOwner owner, long topologyRevision, Duration ttl) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(owner, "owner");
        if (topologyRevision < 0) {
            throw new IllegalArgumentException("topologyRevision must not be negative");
        }
        positive(ttl);
    }

    private static void positive(Duration ttl) {
        Objects.requireNonNull(ttl, "ttl");
        if (ttl.isZero() || ttl.isNegative()) {
            throw new IllegalArgumentException("ttl must be positive");
        }
    }

    private static boolean duplicateKey(MongoException failure) {
        return failure.getCode() == DUPLICATE_KEY
                || failure instanceof MongoWriteException write && write.getError().getCode() == DUPLICATE_KEY;
    }
}
