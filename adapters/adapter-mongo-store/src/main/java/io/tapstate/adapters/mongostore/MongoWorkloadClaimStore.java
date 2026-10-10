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
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimAttempt;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimReading;
import io.tapstate.spi.store.WorkloadClaimStore;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import io.tapstate.spi.store.ClusterExecutionMember;
import io.tapstate.spi.store.IoError;
import io.tapstate.core.common.TapstateException;
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
    private final MongoCollection<Document> collection;
    private final MongoClusterProfileStore profiles;

    public MongoWorkloadClaimStore(MongoCollection<Document> collection) {
        this(collection, null);
    }

    public MongoWorkloadClaimStore(MongoCollection<Document> collection, MongoClusterProfileStore profiles) {
        this.collection = Objects.requireNonNull(collection, "collection")
                .withReadPreference(ReadPreference.primary())
                .withReadConcern(ReadConcern.MAJORITY)
                .withWriteConcern(WriteConcern.MAJORITY.withJournal(true));
        this.profiles = profiles;
    }

    @Override
    public WorkloadClaimAttempt acquire(
            WorkloadClaimKey key, WorkloadOwner owner, long topologyRevision, Duration ttl) {
        validate(key, owner, topologyRevision, ttl);
        if (profiles != null) {
            return acquireProfiled(key, owner, topologyRevision, ttl);
        }
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
        if (profiles != null) {
            return profiledWrite(expected, session -> {
                Document node = expected.key().type() == WorkloadClaimType.NODE_SESSION ? null
                        : profiles.guard(session, expected.key().clusterId(), expected.owner(), expected.profileGeneration());
                if (expected.key().type() != WorkloadClaimType.NODE_SESSION && node == null) {
                    return null;
                }
                Document lease = new Document("$set", new Document("leaseUntil", node == null
                        ? leaseUntil(ttl) : clampedLease(ttl, node.getDate("leaseUntil"))));
                Document renewed = collection.findOneAndUpdate(session, liveExpected(expected, expected.topologyRevision()),
                        List.of(lease), new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));
                if (renewed != null && expected.key().type() == WorkloadClaimType.NODE_SESSION) {
                    profiles.extendHorizon(session, expected.key().clusterId(), expected.profileGeneration(),
                            renewed.getDate("leaseUntil"));
                }
                return renewed;
            });
        }
        Document filter = liveExpected(expected, expected.topologyRevision());
        Document lease = new Document("$set", new Document("leaseUntil", leaseUntil(ttl)));
        Document renewed = findOneAndUpdate(filter, List.of(lease), false);
        return Optional.ofNullable(renewed).map(MongoWorkloadClaimStore::read);
    }

    @Override
    public boolean release(WorkloadClaim expected) {
        Objects.requireNonNull(expected, "expected");
        Document filter = expected(expected);
        Document expired = new Document("$set", new Document("leaseUntil", "$$NOW")
                .append("retiredAuthorizationUntil", retiredDeadline()));
        if (profiles != null) {
            return profiledWrite(expected, session -> {
                Document released = collection.findOneAndUpdate(session, filter,
                        List.of(expired), new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));
                if (released != null && expected.key().type() == WorkloadClaimType.NODE_SESSION) {
                    // Early session release must revoke every shorter business lease in the same commit.
                    Document owned = new Document("clusterId", expected.key().clusterId())
                            .append("ownerNodeId", expected.owner().nodeId())
                            .append("ownerBootId", expected.owner().bootId())
                            .append("profileGeneration", expected.profileGeneration())
                            .append("resourceType", new Document("$ne", WorkloadClaimType.NODE_SESSION.name()));
                    collection.updateMany(session, owned, List.of(expired));
                }
                return released;
            }).isPresent();
        }
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
        if (profiles != null) {
            return profiles.transaction(session -> advanceExecution(session, expected, topologyRevision, executionNodeIds));
        }
        Document advanced = findOneAndUpdate(liveExpected(expected, topologyRevision),
                executionAdvance(executionNodeIds), false);
        return Optional.ofNullable(advanced).map(MongoWorkloadClaimStore::read);
    }

    /** Shares the real issuer with a transaction that persists the corresponding successor receipt. */
    Optional<WorkloadClaim> advanceExecution(ClientSession session, WorkloadClaim expected,
            long topologyRevision, Set<String> executionNodeIds) {
        Objects.requireNonNull(expected, "expected");
        Objects.requireNonNull(executionNodeIds, "executionNodeIds");
        if (profiles == null || expected.profileGeneration() < 1
                || topologyRevision < 1
                || profiles.guard(session, expected.key().clusterId(), expected.owner(), expected.profileGeneration()) == null) {
            return Optional.empty();
        }
        Document snapshot = profiles.executionProfileSnapshot(session, expected.key().clusterId(), expected.profileGeneration());
        if (snapshot == null) {
            throw new IllegalStateException("the guarded execution profile disappeared inside its transaction");
        }
        List<Document> update = executionAdvance(executionNodeIds, snapshot);
        update.getFirst().get("$set", Document.class).append("executionTopologyRevision", topologyRevision);
        Document advanced = collection.findOneAndUpdate(session, liveExpected(expected, topologyRevision), update,
                new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));
        return Optional.ofNullable(advanced).map(MongoWorkloadClaimStore::read);
    }

    /** A real conditional claim write makes a fenced queue transition conflict with an owner takeover. */
    Document conditionTouchClaim(ClientSession session, WorkloadClaimFence expected) {
        if (profiles == null || expected.profileGeneration() < 1
                || profiles.guard(session, expected.key().clusterId(), expected.owner(), expected.profileGeneration()) == null) {
            return null;
        }
        return collection.findOneAndUpdate(session, WorkloadClaimDocuments.live(expected),
                new Document("$inc", new Document("queueFenceSerial", 1L)),
                new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));
    }

    private static List<Document> executionAdvance(Set<String> executionNodeIds) {
        return executionAdvance(executionNodeIds, null);
    }

    private static List<Document> executionAdvance(Set<String> executionNodeIds, Document executionProfile) {
        Document nextGeneration = new Document("$add", List.of(
                new Document("$ifNull", List.of("$executionGeneration", 0L)), 1L));
        Document next = new Document("$set", new Document("executionGeneration", nextGeneration)
                .append("contextExecutionGeneration", nextGeneration)
                .append("executionClaimGeneration", "$claimGeneration")
                .append("executionNodeIds", new Document("$literal", executionNodeIds.stream().sorted().toList()))
                .append("executionMembers", new Document("$literal", List.of()))
                .append("executionIncarnation", null).append("executionRevision", null)
                .append("failureClaimGeneration", 0L)
                .append("failureAfterMemberLoss", false)
                .append("retiredAuthorizationUntil", retiredDeadline()));
        if (executionProfile != null) {
            next.get("$set", Document.class).append("executionProfile", new Document("$literal", executionProfile))
                    .append("executionProfileVersion", 1);
        }
        return List.of(next);
    }

    @Override
    public Optional<WorkloadClaim> recordExecutionFailure(WorkloadClaim expected, boolean afterMemberLoss) {
        Objects.requireNonNull(expected, "expected");
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
        if (profiles != null) {
            return profiledWrite(expected, session -> collection.findOneAndUpdate(session, filter,
                    List.of(next), new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER)));
        }
        Document recorded = findOneAndUpdate(filter, List.of(next), false);
        return Optional.ofNullable(recorded).map(MongoWorkloadClaimStore::read);
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
        return Optional.ofNullable(found).map(MongoWorkloadClaimStore::reading);
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
            readings.put(asked.get(document.get("_id", Document.class)), reading(document));
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
                new Document("$eq", List.of("$ownerNodeId", new Document("$literal", owner.nodeId()))),
                new Document("$eq", List.of("$ownerBootId", new Document("$literal", owner.bootId())))));
        Document generation = new Document("$cond", List.of(
                sameOwner,
                new Document("$ifNull", List.of("$claimGeneration", 1L)),
                new Document("$add", List.of(
                        new Document("$ifNull", List.of("$claimGeneration", 0L)), 1L))));
        Document fields = new Document("clusterId", new Document("$literal", key.clusterId()))
                .append("resourceType", key.type().name())
                .append("resourceId", new Document("$literal", key.resourceId()))
                .append("ownerNodeId", new Document("$literal", owner.nodeId()))
                .append("ownerBootId", new Document("$literal", owner.bootId()))
                .append("claimGeneration", generation)
                .append("executionGeneration",
                        new Document("$ifNull", List.of("$executionGeneration", 0L)))
                .append("topologyRevision", topologyRevision)
                .append("retiredAuthorizationUntil", retiredDeadline())
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
        Document filter = new Document("_id", id(expected.key()))
                .append("ownerNodeId", expected.owner().nodeId())
                .append("ownerBootId", expected.owner().bootId())
                .append("claimGeneration", expected.claimGeneration())
                .append("executionGeneration", expected.executionGeneration());
        if (expected.profileGeneration() > 0) {
            filter.append("profileGeneration", expected.profileGeneration());
        } else {
            filter.append("$or", List.of(new Document("profileGeneration", 0L),
                    new Document("profileGeneration", new Document("$exists", false))));
        }
        return filter;
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

    static WorkloadClaim readDocument(Document document) {
        if (document.containsKey("executionProfileVersion")
                && (!(document.get("executionProfileVersion") instanceof Number version) || version.longValue() != 1)) {
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                    Map.of("id", String.valueOf(document.get("_id")), "field", "executionProfileVersion"), null);
        }
        if (!document.containsKey("executionProfileVersion") && document.get("executionProfile") != null) {
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                    Map.of("id", String.valueOf(document.get("_id")), "field", "executionProfileVersion"), null);
        }
        if (numberOrZero(document, "executionProfileVersion") > 0
                && (document.get("executionProfile", Document.class) == null
                        || !(document.get("executionTopologyRevision") instanceof Number))) {
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                    Map.of("id", String.valueOf(document.get("_id")), "field",
                            document.get("executionProfile", Document.class) == null ? "executionProfile"
                                    : "executionTopologyRevision"), null);
        }
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
                Boolean.TRUE.equals(document.getBoolean("failureAfterMemberLoss")),
                numberOrZero(document, "profileGeneration"),
                MongoClusterProfileStore.executionProfile(document.get("executionProfile", Document.class)),
                document.get("executionTopologyRevision") instanceof Number revision ? revision.longValue() : null,
                document.getString("executionIncarnation"), document.getString("executionRevision"), executionMembers(document));
    }

    private static Map<String, ClusterExecutionMember> executionMembers(Document document) {
        try {
            Map<String, ClusterExecutionMember> members = new LinkedHashMap<>();
            for (Document member : document.getList("executionMembers", Document.class, List.of())) {
                ClusterExecutionMember identity = new ClusterExecutionMember(member.getString("nodeId"), member.getString("bootId"), member.getString("memberUuid"));
                if (members.put(identity.nodeId(), identity) != null) {
                    throw new IllegalArgumentException("duplicate original execution member");
                }
            }
            if (!members.isEmpty() && !members.keySet().equals(Set.copyOf(document.getList("executionNodeIds", String.class, List.of())))) {
                throw new IllegalArgumentException("original execution member cohort is incomplete");
            }
            return Map.copyOf(members);
        } catch (RuntimeException invalid) {
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                    Map.of("id", String.valueOf(document.get("_id")), "field", "executionMembers"), invalid);
        }
    }

    private static WorkloadClaim read(Document document) {
        return readDocument(document);
    }

    private WorkloadClaimAttempt acquireProfiled(WorkloadClaimKey key, WorkloadOwner owner,
            long topologyRevision, Duration ttl) {
        if (key.type() == WorkloadClaimType.NODE_SESSION) {
            throw new TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null);
        }
        return profiles.transaction(session -> {
            Document node = profiles.owningSession(session, key.clusterId(), owner);
            if (node == null) {
                throw new TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null);
            }
            Document current = collection.aggregate(session, List.of(
                    new Document("$match", new Document("_id", id(key))), leaseRemaining())).first();
            if (current != null && number(current, LEASE_REMAINING) > 0
                    && (!owner.nodeId().equals(current.getString("ownerNodeId"))
                            || !owner.bootId().equals(current.getString("ownerBootId")))) {
                return WorkloadClaimAttempt.refused(read(current));
            }
            List<Document> updates = acquirePipeline(key, owner, topologyRevision, ttl);
            Document fields = updates.getFirst().get("$set", Document.class);
            fields.append("profileGeneration", number(node, "profileGeneration"))
                    .append("leaseUntil", clampedLease(ttl, node.getDate("leaseUntil")));
            if (current != null && number(current, LEASE_REMAINING) <= 0) {
                fields.append("claimGeneration", Math.addExact(number(current, "claimGeneration"), 1L));
            }
            Document acquired = collection.findOneAndUpdate(session, new Document("_id", id(key)), updates,
                    new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER));
            return WorkloadClaimAttempt.acquired(read(acquired));
        });
    }

    private Optional<WorkloadClaim> profiledWrite(WorkloadClaim expected,
            java.util.function.Function<ClientSession, Document> operation) {
        if (expected.profileGeneration() < 1) {
            return Optional.empty();
        }
        return profiles.transaction(session -> {
            boolean permitted = expected.key().type() == WorkloadClaimType.NODE_SESSION
                    ? profiles.profileGuard(session, expected.key().clusterId(), expected.profileGeneration())
                    : profiles.guard(session, expected.key().clusterId(), expected.owner(), expected.profileGeneration()) != null;
            return permitted ? Optional.ofNullable(operation.apply(session)).map(MongoWorkloadClaimStore::read)
                    : Optional.empty();
        });
    }

    static Document clampedLease(Duration ttl, Date nodeDeadline) {
        Objects.requireNonNull(nodeDeadline, "nodeDeadline");
        return new Document("$min", List.of(leaseUntil(ttl), nodeDeadline));
    }

    private static Document retiredDeadline() {
        return new Document("$max", List.of(
                new Document("$ifNull", List.of("$retiredAuthorizationUntil", new Date(0))),
                new Document("$ifNull", List.of("$leaseUntil", "$$NOW"))));
    }

    /** Generation mismatch retires store writes immediately; cached calls retire only after their promise. */
    boolean provesRetired(ClientSession session, WorkloadClaimFence expected) {
        Document filter = new Document("_id", id(expected.key()))
                .append("$nor", List.of(WorkloadClaimDocuments.liveAuthority(expected)))
                .append("$expr", new Document("$lte", List.of(
                        new Document("$ifNull", List.of("$retiredAuthorizationUntil", new Date(Long.MAX_VALUE))), "$$NOW")));
        return collection.find(session, filter).first() != null;
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
