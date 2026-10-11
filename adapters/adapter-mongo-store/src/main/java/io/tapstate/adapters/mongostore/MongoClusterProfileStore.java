package io.tapstate.adapters.mongostore;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.UpdateOptions;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.ClusterExecutionProfile;
import io.tapstate.spi.store.ClusterNativeMember;
import io.tapstate.spi.store.ClusterNativeObservation;
import io.tapstate.spi.store.ClusterNativeView;
import io.tapstate.spi.store.ClusterNodeReading;
import io.tapstate.spi.store.ClusterNodeRegistration;
import io.tapstate.spi.store.ClusterNodeReservation;
import io.tapstate.spi.store.ClusterProfileStore;
import io.tapstate.spi.store.ExecutionProfile;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.bson.Document;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/** Mongo-time profile admission serialized with session and business-claim writes. */
public final class MongoClusterProfileStore implements ClusterProfileStore {
    private static final TransactionOptions DURABLE = TransactionOptions.builder()
            .readConcern(ReadConcern.SNAPSHOT)
            .writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();
    private static final FindOneAndUpdateOptions AFTER =
            new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER);
    private final MongoClient client;
    private final MongoCollection<Document> profiles;
    private final MongoCollection<Document> claims;
    private final MongoCollection<Document> registry;
    private final MongoCollection<Document> membership;

    public MongoClusterProfileStore(MongoClient client, MongoCollection<Document> profiles,
            MongoCollection<Document> claims, MongoCollection<Document> registry) {
        this(client, profiles, claims, registry, null);
    }

    public MongoClusterProfileStore(MongoClient client, MongoCollection<Document> profiles,
            MongoCollection<Document> claims, MongoCollection<Document> registry, MongoCollection<Document> membership) {
        this.client = Objects.requireNonNull(client, "client");
        this.profiles = durable(profiles);
        this.claims = durable(claims);
        this.registry = durable(registry);
        this.membership = membership == null ? null : durable(membership);
    }

    private static MongoCollection<Document> durable(MongoCollection<Document> collection) {
        return Objects.requireNonNull(collection, "collection").withReadPreference(ReadPreference.primary())
                .withReadConcern(ReadConcern.MAJORITY)
                .withWriteConcern(WriteConcern.MAJORITY.withJournal(true));
    }

    @Override
    public ClusterNodeReservation reserve(String clusterId, WorkloadOwner owner, URI controlUrl,
            ExecutionProfile proposed, Duration ttl) {
        Objects.requireNonNull(owner, "owner");
        Objects.requireNonNull(controlUrl, "controlUrl");
        Objects.requireNonNull(proposed, "proposed");
        positive(ttl);
        if (clusterId == null || clusterId.isBlank() || !controlUrl.isAbsolute()) {
            throw new IllegalArgumentException("reservation identity and control URL are invalid");
        }
        // This empty guard carries no generation or authorization. All first reservations contend on it.
        StoreIo.run(() -> profiles.updateOne(new Document("_id", clusterId),
                new Document("$setOnInsert", new Document("generation", 0L).append("writeSerial", 0L)),
                new UpdateOptions().upsert(true)));
        return transaction(session -> reserve(session, clusterId, owner, controlUrl, proposed, ttl));
    }

    private ClusterNodeReservation reserve(ClientSession session, String clusterId, WorkloadOwner owner,
            URI controlUrl, ExecutionProfile proposed, Duration ttl) {
        Document current = profiles.findOneAndUpdate(session, new Document("_id", clusterId),
                new Document("$inc", new Document("writeSerial", 1L)), AFTER);
        if (current == null) {
            throw new IllegalStateException("profile guard disappeared during admission");
        }
        long generation = number(current, "generation");
        ClusterExecutionProfile profile = generation == 0 ? null : profile(current);
        Document legacyAuthority = new Document("clusterId", clusterId).append("$expr",
                new Document("$or", List.of(new Document("$gt", List.of("$leaseUntil", "$$NOW")),
                        new Document("$gt", List.of("$retiredAuthorizationUntil", "$$NOW")))));
        if (generation == 0 && claims.find(session, legacyAuthority).first() != null) {
            return refused(ClusterNodeReservation.Outcome.LEGACY_LEASES_ACTIVE, profile);
        }
        boolean anyLive = claims.find(session, liveClaims(clusterId, WorkloadClaimType.NODE_SESSION)).first() != null;
        if (anyLive && (profile == null || !profile.profile().hash().equals(proposed.hash()))) {
            return refused(ClusterNodeReservation.Outcome.INCOMPATIBLE, profile);
        }
        if (!anyLive && generation > 0 && profiles.find(session,
                new Document("_id", clusterId).append("$expr",
                        new Document("$gt", List.of("$authorizationUntil", "$$NOW")))).first() != null) {
            return refused(ClusterNodeReservation.Outcome.AUTHORIZATION_HORIZON_ACTIVE, profile);
        }
        Document key = claimId(clusterId, owner.nodeId());
        Document previous = claims.aggregate(session, List.of(
                new Document("$match", new Document("_id", key)), remaining())).first();
        if (previous != null && number(previous, "leaseRemainingMillis") > 0
                && !ownerMatches(previous, owner)) {
            return refused(ClusterNodeReservation.Outcome.NODE_IN_USE, profile);
        }
        if (!anyLive) {
            generation = Math.addExact(generation, 1L);
            profile = new ClusterExecutionProfile(clusterId, generation, proposed);
            profiles.updateOne(session, new Document("_id", clusterId),
                    new Document("$set", profileFields(profile)));
            if (generation == 1) {
                // This bounded marker preserves the actual initial retirement proof across later cold boots.
                profiles.updateOne(session, new Document("_id", clusterId),
                        List.of(new Document("$set", new Document("legacyAuthorityRetiredAt", "$$NOW"))));
            }
        }
        boolean sameSession = previous != null && ownerMatches(previous, owner)
                && numberOrZero(previous, "profileGeneration") == generation
                && number(previous, "leaseRemainingMillis") > 0;
        long claimGeneration = sameSession ? number(previous, "claimGeneration")
                : Math.addExact(previous == null ? 0L : number(previous, "claimGeneration"), 1L);
        Document fields = new Document("clusterId", literal(clusterId))
                .append("resourceType", WorkloadClaimType.NODE_SESSION.name()).append("resourceId", literal(owner.nodeId()))
                .append("ownerNodeId", literal(owner.nodeId())).append("ownerBootId", literal(owner.bootId()))
                .append("claimGeneration", claimGeneration).append("executionGeneration", 0L)
                .append("topologyRevision", 0L).append("profileGeneration", generation)
                .append("leaseUntil", leaseUntil(ttl));
        Document acquired = claims.findOneAndUpdate(session, new Document("_id", key),
                List.of(new Document("$set", fields)),
                new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER));
        extendHorizon(session, clusterId, generation, acquired.getDate("leaseUntil"));
        Document nodeFields = new Document("clusterId", clusterId).append("nodeId", owner.nodeId())
                .append("bootId", owner.bootId()).append("profileGeneration", generation)
                .append("controlUrl", controlUrl.toString())
                .append("formatVersion", profile.profile().formatVersion())
                .append("attributes", new Document(profile.profile().attributes()))
                .append("hash", profile.profile().hash());
        if (!sameSession) {
            nodeFields.append("joined", false).append("memberUuid", null)
                    .append("memberAddress", null).append("joinedAt", null);
        }
        Document registryUpdate = new Document("$set", nodeFields);
        if (!sameSession) { registryUpdate.append("$unset", new Document("nativeView", "")); }
        registry.updateOne(session, new Document("_id", registryId(clusterId, owner.nodeId())),
                registryUpdate, new UpdateOptions().upsert(true));
        Document reading = claims.aggregate(session, List.of(new Document("$match", new Document("_id", key)),
                remaining())).first();
        WorkloadClaim claim = MongoWorkloadClaimStore.readDocument(acquired);
        Document node = registry.find(session,
                new Document("_id", registryId(clusterId, owner.nodeId()))).first();
        return new ClusterNodeReservation(ClusterNodeReservation.Outcome.ACQUIRED,
                new ClusterNodeReading(registration(node, claim, profile),
                        Duration.ofMillis(number(reading, "leaseRemainingMillis"))), profile);
    }

    private static ClusterNodeReservation refused(ClusterNodeReservation.Outcome reason,
            ClusterExecutionProfile profile) {
        return new ClusterNodeReservation(reason, null, profile);
    }

    @Override
    public Optional<ClusterExecutionProfile> profile(String clusterId) {
        return StoreIo.call(() -> Optional.ofNullable(profiles.find(new Document("_id", clusterId)).first())
                .filter(document -> number(document, "generation") > 0).map(MongoClusterProfileStore::profile));
    }

    @Override
    public List<ClusterNodeReading> nodes(String clusterId) {
        return transaction(session -> {
            Document current = profiles.find(session, new Document("_id", clusterId)).first();
            if (current == null || number(current, "generation") == 0) {
                return List.of();
            }
            List<Document> nodes = registry.find(session, new Document("clusterId", clusterId))
                    .into(new ArrayList<>());
            List<ClusterNodeReading> result = new ArrayList<>();
            for (Document node : nodes) {
                Document claim = claims.aggregate(session, List.of(new Document("$match",
                        new Document("_id", claimId(clusterId, node.getString("nodeId")))), remaining())).first();
                if (claim == null) {
                    throw unreadable(node, "nodeSession", null);
                }
                // A registry retains the immutable profile it joined under even after a later cold generation.
                ClusterExecutionProfile nodeProfile = new ClusterExecutionProfile(clusterId,
                        number(node, "profileGeneration"), profileFromFields(node));
                result.add(new ClusterNodeReading(registration(node,
                        MongoWorkloadClaimStore.readDocument(claim), nodeProfile),
                        Duration.ofMillis(number(claim, "leaseRemainingMillis"))));
            }
            return List.copyOf(result);
        });
    }

    @Override
    public boolean markJoined(WorkloadClaim expected, String memberUuid, String memberAddress) {
        Objects.requireNonNull(expected, "expected");
        if (expected.key().type() != WorkloadClaimType.NODE_SESSION || memberUuid == null || memberUuid.isBlank()
                || memberAddress == null || memberAddress.isBlank()) {
            throw new IllegalArgumentException("join evidence requires an exact node session and runtime identity");
        }
        return transaction(session -> {
            Document node = guard(session, expected.key().clusterId(), expected.owner(), expected.profileGeneration());
            if (node == null || number(node, "claimGeneration") != expected.claimGeneration()) {
                return false;
            }
            Document filter = new Document("_id", registryId(expected.key().clusterId(), expected.owner().nodeId()))
                    .append("bootId", expected.owner().bootId()).append("profileGeneration", expected.profileGeneration());
            Document fields = new Document("joined", true).append("memberUuid", literal(memberUuid))
                    .append("memberAddress", literal(memberAddress)).append("joinedAt", "$$NOW")
                    .append("nativeView", new Document("$cond", List.of(new Document("$and", List.of(
                            new Document("$eq", List.of("$memberUuid", literal(memberUuid))),
                            new Document("$eq", List.of("$memberAddress", literal(memberAddress))))),
                            "$nativeView", "$$REMOVE")));
            return registry.updateOne(session, filter, List.of(new Document("$set", fields))).getMatchedCount() == 1;
        });
    }

    @Override
    public boolean recordNativeView(WorkloadClaim expected, long expectedTopologyRevision,
            ClusterNativeObservation observation, Duration observationTtl) {
        Objects.requireNonNull(expected, "expected");
        Objects.requireNonNull(observation, "observation");
        positive(observationTtl);
        if (expected.key().type() != WorkloadClaimType.NODE_SESSION || expectedTopologyRevision < 1
                || !expected.key().resourceId().equals(expected.owner().nodeId())) {
            throw new IllegalArgumentException("native publication requires its exact node and profile scope");
        }
        if (membership == null || !nativeProtocol(observation.profile())
                || !expected.key().clusterId().equals(observation.profile().clusterId())
                || expected.profileGeneration() != observation.profile().generation()) { return false; }
        return transaction(session -> {
            ClusterExecutionProfile profile = observation.profile();
            Document stamped = nativeProfileGuard(session, profile);
            if (stamped == null || committedGuard(session, profile, expectedTopologyRevision) == null) { return false; }
            Document publisher = claims.findOneAndUpdate(session, WorkloadClaimDocuments.live(WorkloadClaimFence.from(expected)),
                    new Document("$inc", new Document("nativeViewFenceSerial", 1L)), AFTER);
            ClusterNativeMember local = observation.membersByNode().get(expected.owner().nodeId());
            if (publisher == null || local == null || !local.identity().bootId().equals(expected.owner().bootId())) { return false; }
            for (ClusterNativeMember peer : new java.util.TreeMap<>(observation.membersByNode()).values()) {
                if (guardNativeMember(session, profile, peer) == null) { return false; }
            }
            Document registered = registry.find(session, new Document("_id", registryId(profile.clusterId(), expected.owner().nodeId()))).first();
            if (registered == null) { return false; }
            ClusterNativeView previous = registered.containsKey("nativeView") ? ClusterNativeViewDocuments.read(registered) : null;
            Instant now = stamped.getDate("nativeViewStoreTime").toInstant();
            Instant deadline = now.plusMillis(observationTtl.toMillis());
            Instant leaseUntil = nativeDate(publisher, "leaseUntil").toInstant();
            if (deadline.isAfter(leaseUntil)) { deadline = leaseUntil; }
            if (!deadline.isAfter(now)) { return false; }
            ClusterNativeView next = new ClusterNativeView(WorkloadClaimFence.from(expected), profile,
                    expectedTopologyRevision, previous == null ? 1 : Math.addExact(previous.viewRevision(), 1L),
                    ClusterNativeViewDocuments.hash(profile, expectedTopologyRevision, observation.membersByNode()),
                    now, deadline, observation.membersByNode());
            Document filter = joinedNativeFilter(profile, local);
            filter.append("nativeView.viewRevision", previous == null ? new Document("$exists", false) : previous.viewRevision());
            return registry.updateOne(session, filter,
                    new Document("$set", new Document("nativeView", ClusterNativeViewDocuments.document(next)))).getMatchedCount() == 1;
        });
    }

    @Override
    public Optional<ClusterNativeView> nativeView(String clusterId, String publisherNodeId) {
        return StoreIo.call(() -> {
            Document registered = registry.find(new Document("_id", registryId(clusterId, publisherNodeId))).first();
            return registered == null || !registered.containsKey("nativeView") ? Optional.empty()
                    : Optional.of(ClusterNativeViewDocuments.read(registered));
        });
    }

    /** Common member facts after each publisher and its latest whole-pool view were conditionally touched. */
    record QualifiedNativeCohort(ClusterExecutionProfile profile, long topologyRevision, String cohortHash,
            Instant validUntil, Map<String, ClusterNativeMember> membersByNode) {
        QualifiedNativeCohort { membersByNode = Map.copyOf(membersByNode); }
        Set<String> nodeIds() { return membersByNode.keySet(); }
    }

    Optional<QualifiedNativeCohort> guardNativeCohort(ClientSession session, WorkloadClaimFence requester,
            ClusterExecutionProfile profile, Document exactCommittedMembership) {
        if (membership == null || !nativeProtocol(profile) || exactCommittedMembership == null
                || !requester.key().clusterId().equals(profile.clusterId()) || requester.profileGeneration() != profile.generation()
                || (requester.key().type() != WorkloadClaimType.CLUSTER_RECOVERY
                        && requester.key().type() != WorkloadClaimType.PIPELINE_ACTUATION)
                || nativeProfileGuard(session, profile) == null) { return Optional.empty(); }
        Document current = committedGuard(session, profile, number(exactCommittedMembership, "revision"));
        if (current == null) { return Optional.empty(); }
        Document currentRequester = claims.findOneAndUpdate(session, WorkloadClaimDocuments.live(requester),
                new Document("$inc", new Document("nativeViewFenceSerial", 1L)), AFTER);
        if (currentRequester == null) { return Optional.empty(); }
        Set<String> committed = nativeCommittedNodes(current);
        Document owner = registry.find(session, new Document("_id", registryId(profile.clusterId(), requester.owner().nodeId()))).first();
        if (owner == null || !owner.containsKey("nativeView")) { return Optional.empty(); }
        ClusterNativeView candidate = ClusterNativeViewDocuments.read(owner);
        if (!candidate.profile().equals(profile) || candidate.topologyRevision() != number(current, "revision")
                || !candidate.publisherClaim().owner().equals(requester.owner())
                || !committed.containsAll(candidate.membersByNode().keySet())) { return Optional.empty(); }
        int required = "PROCESS_FAILURE_ONLY".equals(profile.profile().attributes().get("haProfile"))
                && "2".equals(profile.profile().attributes().get("bootstrapMinMembers")) ? 1 : committed.size() / 2 + 1;
        if (candidate.membersByNode().size() < required) { return Optional.empty(); }
        Instant deadline = candidate.validUntil();
        Instant requesterDeadline = nativeDate(currentRequester, "leaseUntil").toInstant();
        if (requesterDeadline.isBefore(deadline)) { deadline = requesterDeadline; }
        for (var entry : new java.util.TreeMap<>(candidate.membersByNode()).entrySet()) {
            Document registered = registry.find(session, new Document("_id", registryId(profile.clusterId(), entry.getKey()))).first();
            if (registered == null || !registered.containsKey("nativeView")) { return Optional.empty(); }
            ClusterNativeView peer = ClusterNativeViewDocuments.read(registered);
            if (!ClusterNativeViewDocuments.sameCohort(candidate, peer)
                    || !peer.publisherClaim().owner().nodeId().equals(entry.getKey())
                    || !peer.publisherClaim().owner().bootId().equals(entry.getValue().identity().bootId())) { return Optional.empty(); }
            Document livePublisher = claims.findOneAndUpdate(session, WorkloadClaimDocuments.live(peer.publisherClaim()),
                    new Document("$inc", new Document("nativeViewFenceSerial", 1L)), AFTER);
            if (livePublisher == null) { return Optional.empty(); }
            Document rawView = registered.get("nativeView", Document.class);
            Document exactView = joinedNativeFilter(profile, entry.getValue())
                    .append("nativeView.publisherClaim", rawView.get("publisherClaim"))
                    .append("nativeView.profile", rawView.get("profile")).append("nativeView.members", rawView.get("members"))
                    .append("nativeView.schemaVersion", 1).append("nativeView.viewRevision", peer.viewRevision())
                    .append("nativeView.cohortHash", peer.cohortHash())
                    .append("nativeView.observedAt", Date.from(peer.observedAt())).append("nativeView.validUntil", Date.from(peer.validUntil()))
                    .append("$expr", new Document("$gt", List.of("$nativeView.validUntil", "$$NOW")));
            if (!registrationProfile(registered).equals(profile) || registry.updateOne(session, exactView,
                    new Document("$inc", new Document("nativeViewFenceSerial", 1L))).getMatchedCount() != 1) { return Optional.empty(); }
            if (peer.validUntil().isBefore(deadline)) { deadline = peer.validUntil(); }
            Instant publisherDeadline = nativeDate(livePublisher, "leaseUntil").toInstant();
            if (publisherDeadline.isBefore(deadline)) { deadline = publisherDeadline; }
        }
        QualifiedNativeCohort qualified = new QualifiedNativeCohort(profile, candidate.topologyRevision(), candidate.cohortHash(),
                deadline, candidate.membersByNode());
        return conditionTouchNativeDeadline(session, qualified) ? Optional.of(qualified) : Optional.empty();
    }

    /** Complements the full cohort guard at an admission boundary; it grants no workload authority. */
    boolean conditionTouchNativeDeadline(ClientSession session, QualifiedNativeCohort qualified) {
        ClusterExecutionProfile profile = qualified.profile();
        Document current = new Document("_id", profile.clusterId()).append("generation", profile.generation())
                .append("hash", profile.profile().hash()).append("$expr",
                        new Document("$gt", List.of(Date.from(qualified.validUntil()), "$$NOW")));
        return profiles.updateOne(session, current,
                new Document("$inc", new Document("nativeViewFenceSerial", 1L))).getMatchedCount() == 1;
    }

    private Document nativeProfileGuard(ClientSession session, ClusterExecutionProfile expected) {
        Document stored = profiles.findOneAndUpdate(session,
                new Document("_id", expected.clusterId()).append("generation", expected.generation()).append("hash", expected.profile().hash()),
                List.of(new Document("$set", new Document("nativeViewStoreTime", "$$NOW")
                        .append("nativeViewFenceSerial", new Document("$add", List.of(
                                new Document("$ifNull", List.of("$nativeViewFenceSerial", 0L)), 1L))))), AFTER);
        return stored != null && profile(stored).equals(expected) ? stored : null;
    }

    private Document committedGuard(ClientSession session, ClusterExecutionProfile profile, long revision) {
        return membership.findOneAndUpdate(session, new Document("_id", profile.clusterId())
                .append("profileGeneration", profile.generation()).append("revision", revision),
                new Document("$inc", new Document("nativeViewFenceSerial", 1L)), AFTER);
    }

    private Document guardNativeMember(ClientSession session, ClusterExecutionProfile profile, ClusterNativeMember member) {
        var identity = member.identity();
        Document live = new Document("_id", claimId(profile.clusterId(), identity.nodeId()))
                .append("clusterId", profile.clusterId()).append("resourceType", WorkloadClaimType.NODE_SESSION.name())
                .append("resourceId", identity.nodeId()).append("ownerNodeId", identity.nodeId()).append("ownerBootId", identity.bootId())
                .append("profileGeneration", profile.generation()).append("$expr", new Document("$gt", List.of("$leaseUntil", "$$NOW")));
        if (claims.findOneAndUpdate(session, live, new Document("$inc", new Document("nativeViewFenceSerial", 1L)), AFTER) == null) { return null; }
        Document registered = registry.findOneAndUpdate(session, joinedNativeFilter(profile, member),
                new Document("$inc", new Document("nativeViewFenceSerial", 1L)), AFTER);
        return registered != null && registrationProfile(registered).equals(profile) ? registered : null;
    }

    private static Document joinedNativeFilter(ClusterExecutionProfile profile, ClusterNativeMember member) {
        var identity = member.identity();
        return new Document("_id", registryId(profile.clusterId(), identity.nodeId())).append("clusterId", profile.clusterId())
                .append("nodeId", identity.nodeId()).append("bootId", identity.bootId()).append("profileGeneration", profile.generation())
                .append("hash", profile.profile().hash()).append("joined", true)
                .append("memberUuid", identity.memberUuid()).append("memberAddress", member.memberAddress());
    }

    private static ClusterExecutionProfile registrationProfile(Document registered) {
        return profile(new Document(registered).append("_id", registered.getString("clusterId"))
                .append("generation", registered.get("profileGeneration")));
    }

    private static Set<String> nativeCommittedNodes(Document current) {
        if (!(current.get("activeNodeIds") instanceof List<?> rows) || rows.isEmpty()) {
            throw unreadable(current, "activeNodeIds", null);
        }
        Set<String> nodes = new java.util.HashSet<>();
        for (Object raw : rows) {
            if (!(raw instanceof String node) || node.isBlank() || !nodes.add(node)) {
                throw unreadable(current, "activeNodeIds", null);
            }
        }
        return Set.copyOf(nodes);
    }

    private static Date nativeDate(Document current, String field) {
        if (!(current.get(field) instanceof Date date)) { throw unreadable(current, field, null); }
        return date;
    }

    private static boolean nativeProtocol(ClusterExecutionProfile profile) {
        return "2".equals(profile.profile().attributes().get("profile-protocol"));
    }

    /** Real shared writes serialize a caller's transaction with session expiry, renewal and profile changes. */
    Document guard(ClientSession session, String clusterId, WorkloadOwner owner, long generation) {
        if (generation < 1) {
            return null;
        }
        Document profile = profiles.findOneAndUpdate(session,
                new Document("_id", clusterId).append("generation", generation),
                new Document("$inc", new Document("writeSerial", 1L)), AFTER);
        if (profile == null) {
            return null;
        }
        return claims.findOneAndUpdate(session, liveSession(clusterId, owner, generation),
                new Document("$inc", new Document("guardSerial", 1L)), AFTER);
    }

    <T> T transaction(Function<ClientSession, T> operation) {
        return StoreIo.call(() -> {
            try (ClientSession session = client.startSession()) {
                return session.withTransaction(() -> operation.apply(session), DURABLE);
            }
        });
    }

    Document owningSession(ClientSession session, String clusterId, WorkloadOwner owner) {
        Document current = profiles.find(session, new Document("_id", clusterId)).first();
        return current == null ? null : guard(session, clusterId, owner, number(current, "generation"));
    }

    boolean profileGuard(ClientSession session, String clusterId, long generation) {
        return profiles.findOneAndUpdate(session, new Document("_id", clusterId).append("generation", generation),
                new Document("$inc", new Document("writeSerial", 1L)), AFTER) != null;
    }

    /** Every committed candidate is an exact live session with real join evidence from that boot. */
    boolean guardJoinedNodes(ClientSession session, String clusterId, long generation, Set<String> nodeIds) {
        if (nodeIds.isEmpty() || !profileGuard(session, clusterId, generation)) {
            return false;
        }
        for (String nodeId : nodeIds.stream().sorted().toList()) {
            Document filter = new Document("_id", claimId(clusterId, nodeId))
                    .append("profileGeneration", generation)
                    .append("$expr", new Document("$gt", List.of("$leaseUntil", "$$NOW")));
            Document node = claims.findOneAndUpdate(session, filter,
                    new Document("$inc", new Document("membershipFenceSerial", 1L)), AFTER);
            if (node == null) {
                return false;
            }
            Document registered = new Document("_id", registryId(clusterId, nodeId))
                    .append("bootId", node.getString("ownerBootId"))
                    .append("profileGeneration", generation).append("joined", true);
            if (registry.updateOne(session, registered,
                    new Document("$inc", new Document("membershipFenceSerial", 1L))).getMatchedCount() != 1) {
                return false;
            }
        }
        return true;
    }

    Document executionProfileSnapshot(ClientSession session, String clusterId, long generation) {
        Document stored = profiles.find(session,
                new Document("_id", clusterId).append("generation", generation)).first();
        if (stored == null) {
            return null;
        }
        ClusterExecutionProfile current = profile(stored);
        return profileFields(current).append("schemaVersion", 1).append("clusterId", clusterId);
    }

    static ClusterExecutionProfile executionProfile(Document snapshot) {
        if (snapshot == null) {
            return null;
        }
        if (number(snapshot, "schemaVersion") != 1) {
            throw unreadable(snapshot, "schemaVersion", null);
        }
        Document restored = new Document(snapshot).append("_id", snapshot.getString("clusterId"));
        return profile(restored);
    }

    /** A release cannot withdraw time already promised to a member-local authorization cache. */
    void extendHorizon(ClientSession session, String clusterId, long generation, Date deadline) {
        Document horizon = new Document("$max", List.of(
                new Document("$ifNull", List.of("$authorizationUntil", new Date(0))), deadline));
        if (profiles.updateOne(session, new Document("_id", clusterId).append("generation", generation),
                List.of(new Document("$set", new Document("authorizationUntil", horizon)))).getMatchedCount() != 1) {
            throw new IllegalStateException("the guarded profile changed inside its transaction");
        }
    }

    static Document liveSession(String clusterId, WorkloadOwner owner, long generation) {
        return new Document("_id", claimId(clusterId, owner.nodeId()))
                .append("ownerNodeId", owner.nodeId()).append("ownerBootId", owner.bootId())
                .append("profileGeneration", generation)
                .append("$expr", new Document("$gt", List.of("$leaseUntil", "$$NOW")));
    }

    static Document liveClaims(String clusterId, WorkloadClaimType type) {
        Document filter = new Document("clusterId", clusterId)
                .append("$expr", new Document("$gt", List.of("$leaseUntil", "$$NOW")));
        if (type != null) {
            filter.append("resourceType", type.name());
        }
        return filter;
    }

    static Document claimId(String clusterId, String nodeId) {
        return new Document("clusterId", clusterId).append("resourceType", WorkloadClaimType.NODE_SESSION.name())
                .append("resourceId", nodeId);
    }

    private static Document registryId(String clusterId, String nodeId) {
        return new Document("clusterId", clusterId).append("nodeId", nodeId);
    }

    private static Document profileFields(ClusterExecutionProfile profile) {
        return new Document("generation", profile.generation())
                .append("formatVersion", profile.profile().formatVersion())
                .append("attributes", new Document(profile.profile().attributes()))
                .append("hash", profile.profile().hash());
    }

    static ClusterExecutionProfile profile(Document document) {
        try {
            return new ClusterExecutionProfile(document.getString("_id"), number(document, "generation"),
                    profileFromFields(document));
        } catch (RuntimeException invalid) {
            if (invalid instanceof TapstateException) {
                throw invalid;
            }
            throw unreadable(document, "profile", invalid);
        }
    }

    private static ExecutionProfile profileFromFields(Document document) {
        try {
            Document attributes = document.get("attributes", Document.class);
            if (attributes == null) {
                throw unreadable(document, "attributes", null);
            }
            Map<String, String> fields = new java.util.TreeMap<>();
            attributes.forEach((key, value) -> fields.put(key, (String) value));
            ExecutionProfile profile = new ExecutionProfile(Math.toIntExact(number(document, "formatVersion")), fields);
            if (!profile.hash().equals(document.getString("hash"))) {
                throw unreadable(document, "hash", null);
            }
            return profile;
        } catch (RuntimeException invalid) {
            if (invalid instanceof TapstateException) {
                throw invalid;
            }
            throw unreadable(document, "profile", invalid);
        }
    }

    private static ClusterNodeRegistration registration(Document document, WorkloadClaim claim,
            ClusterExecutionProfile profile) {
        try {
            Date joined = document.getDate("joinedAt");
            return new ClusterNodeRegistration(claim, profile, URI.create(document.getString("controlUrl")),
                    Boolean.TRUE.equals(document.getBoolean("joined")), document.getString("memberUuid"),
                    document.getString("memberAddress"), joined == null ? null : joined.toInstant());
        } catch (RuntimeException invalid) {
            throw unreadable(document, "registration", invalid);
        }
    }

    private static boolean ownerMatches(Document document, WorkloadOwner owner) {
        return owner.nodeId().equals(document.getString("ownerNodeId"))
                && owner.bootId().equals(document.getString("ownerBootId"));
    }

    private static Document remaining() {
        return new Document("$set", new Document("leaseRemainingMillis",
                new Document("$subtract", List.of("$leaseUntil", "$$NOW"))));
    }

    static Document leaseUntil(Duration ttl) {
        return new Document("$dateAdd", new Document("startDate", "$$NOW")
                .append("unit", "millisecond").append("amount", ttl.toMillis()));
    }

    private static Document literal(Object value) {
        return new Document("$literal", value);
    }

    private static void positive(Duration ttl) {
        Objects.requireNonNull(ttl, "ttl");
        if (ttl.isNegative() || ttl.isZero() || ttl.toMillis() == 0) {
            throw new IllegalArgumentException("session lease must be at least one millisecond");
        }
    }

    static long number(Document document, String field) {
        Number number = document == null ? null : document.get(field, Number.class);
        if (number == null) {
            throw unreadable(document, field, null);
        }
        return number.longValue();
    }

    private static long numberOrZero(Document document, String field) {
        Number number = document.get(field, Number.class);
        return number == null ? 0 : number.longValue();
    }

    private static TapstateException unreadable(Document document, String field, Throwable cause) {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE,
                Map.of("id", String.valueOf(document == null ? "unknown" : document.get("_id")), "field", field), cause);
    }
}
