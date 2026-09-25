package io.tapstate.adapters.mongostore;

import com.mongodb.MongoException;
import com.mongodb.MongoWriteException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
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
import java.util.OptionalLong;

/** Mongo server-time implementation of the cluster-scoped workload-claim port. */
public final class MongoWorkloadClaimStore implements WorkloadClaimStore {

    private static final int DUPLICATE_KEY = 11000;
    private static final String LEASE_REMAINING = "leaseRemainingMillis";
    private final MongoCollection<Document> collection;

    public MongoWorkloadClaimStore(MongoCollection<Document> collection) {
        this.collection = Objects.requireNonNull(collection, "collection")
                .withReadPreference(ReadPreference.primary())
                .withReadConcern(ReadConcern.MAJORITY)
                .withWriteConcern(WriteConcern.MAJORITY.withJournal(true));
    }

    @Override
    public WorkloadClaimAttempt acquire(
            WorkloadClaimKey key, WorkloadOwner owner, long topologyRevision, Duration ttl) {
        validate(key, owner, topologyRevision, ttl);
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

        Document absent = new Document("$and", List.of(
                new Document("_id", id),
                new Document("ownerNodeId", new Document("$exists", false))));
        try {
            updated = collection.findOneAndUpdate(absent, acquirePipeline(key, owner, topologyRevision, ttl),
                    new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER));
        } catch (MongoException raced) {
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

    @Override
    public Optional<WorkloadClaim> renew(WorkloadClaim expected, Duration ttl) {
        Objects.requireNonNull(expected, "expected");
        positive(ttl);
        Document filter = liveExpected(expected, expected.topologyRevision());
        Document lease = new Document("$set", new Document("leaseUntil", leaseUntil(ttl)));
        Document renewed = findOneAndUpdate(filter, List.of(lease), false);
        return Optional.ofNullable(renewed).map(MongoWorkloadClaimStore::read);
    }

    @Override
    public boolean release(WorkloadClaim expected) {
        Objects.requireNonNull(expected, "expected");
        Document filter = expected(expected);
        Document expired = new Document("$set", new Document("leaseUntil", "$$NOW"));
        return findOneAndUpdate(filter, List.of(expired), false) != null;
    }

    @Override
    public Optional<WorkloadClaim> advanceUnderClaim(WorkloadClaim expected, long topologyRevision) {
        Objects.requireNonNull(expected, "expected");
        if (topologyRevision < 0) {
            throw new IllegalArgumentException("topologyRevision must not be negative");
        }
        Document next = new Document("$set", new Document("executionGeneration", nextExecutionGeneration()));
        Document advanced = findOneAndUpdate(liveExpected(expected, topologyRevision), List.of(next), false);
        return Optional.ofNullable(advanced).map(MongoWorkloadClaimStore::read);
    }

    @Override
    public OptionalLong advanceStandalone(String clusterId, String pipelineId) {
        WorkloadClaimKey key = new WorkloadClaimKey(
                clusterId, WorkloadClaimType.PIPELINE_ACTUATION, pipelineId);
        Document fields = new Document("clusterId", key.clusterId())
                .append("resourceType", key.type().name())
                .append("resourceId", key.resourceId())
                .append("executionGeneration", nextExecutionGeneration());
        Document eligible = new Document("$and", List.of(
                new Document("_id", id(key)),
                new Document("$or", List.of(
                        new Document("ownerNodeId", new Document("$exists", false)),
                        new Document("$and", List.of(
                                new Document("leaseUntil", new Document("$type", "date")),
                                new Document("$expr", new Document("$lte", List.of("$leaseUntil", "$$NOW")))))))));
        List<Document> update = List.of(new Document("$set", fields));
        Document advanced = findOneAndUpdate(eligible, update, false);
        if (advanced != null) {
            return OptionalLong.of(number(advanced, "executionGeneration"));
        }
        Document absentClaim = new Document("_id", id(key))
                .append("ownerNodeId", new Document("$exists", false));
        try {
            advanced = collection.findOneAndUpdate(absentClaim, update,
                    new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER));
        } catch (MongoException raced) {
            if (!duplicateKey(raced)) {
                throw StoreIo.coded(raced);
            }
            // A simultaneous insert or an ineligible live claim can occupy this id. Retry only the
            // conditional advance: the former gets the next number, the latter stays refused.
            advanced = findOneAndUpdate(eligible, update, false);
        }
        return advanced == null ? OptionalLong.empty()
                : OptionalLong.of(number(advanced, "executionGeneration"));
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
        Document found = StoreIo.call(() -> collection.aggregate(pipeline).first());
        return Optional.ofNullable(found)
                .filter(document -> document.containsKey("ownerNodeId"))
                .map(MongoWorkloadClaimStore::reading);
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
        List<Document> found = StoreIo.call(() -> collection.aggregate(pipeline).into(new ArrayList<>()));
        for (Document document : found) {
            if (document.containsKey("ownerNodeId")) {
                readings.put(asked.get(document.get("_id", Document.class)), reading(document));
            }
        }
        return readings;
    }

    /** How long a lease still has to run, worked out on the server, where {@code $$NOW} is. */
    private static Document leaseRemaining() {
        return new Document("$set", new Document(LEASE_REMAINING,
                new Document("$subtract", List.of("$leaseUntil", "$$NOW"))));
    }

    private Document findOneAndUpdate(Document filter, List<Document> update, boolean upsert) {
        return StoreIo.call(() -> collection.findOneAndUpdate(filter, update,
                new FindOneAndUpdateOptions().upsert(upsert).returnDocument(ReturnDocument.AFTER)));
    }

    private static List<Document> acquirePipeline(
            WorkloadClaimKey key, WorkloadOwner owner, long topologyRevision, Duration ttl) {
        Document sameOwner = new Document("$and", List.of(
                new Document("$eq", List.of("$ownerNodeId", owner.nodeId())),
                new Document("$eq", List.of("$ownerBootId", owner.bootId()))));
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

    private static Document nextExecutionGeneration() {
        return new Document("$add", List.of(
                new Document("$ifNull", List.of("$executionGeneration", 0L)), 1L));
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

    private static Document id(WorkloadClaimKey key) {
        return new Document("clusterId", key.clusterId())
                .append("resourceType", key.type().name())
                .append("resourceId", key.resourceId());
    }

    private static Document leaseUntil(Duration ttl) {
        return new Document("$dateAdd", new Document("startDate", "$$NOW")
                .append("unit", "millisecond")
                .append("amount", ttl.toMillis()));
    }

    private static WorkloadClaim read(Document document) {
        return new WorkloadClaim(
                new WorkloadClaimKey(
                        document.getString("clusterId"),
                        WorkloadClaimType.valueOf(document.getString("resourceType")),
                        document.getString("resourceId")),
                new WorkloadOwner(document.getString("ownerNodeId"), document.getString("ownerBootId")),
                number(document, "claimGeneration"),
                number(document, "executionGeneration"),
                number(document, "topologyRevision"),
                date(document, "leaseUntil").toInstant());
    }

    private static WorkloadClaimReading reading(Document document) {
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
