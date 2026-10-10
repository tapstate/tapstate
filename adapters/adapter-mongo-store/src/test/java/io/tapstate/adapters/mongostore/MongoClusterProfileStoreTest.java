package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.TransactionBody;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.ClusterNodeReservation;
import io.tapstate.spi.store.ExecutionProfile;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Protocol routing tests; server-time arbitration and write conflicts require the replica-set witnesses. */
class MongoClusterProfileStoreTest {
    private static final ExecutionProfile PROFILE = new ExecutionProfile(1, Map.of("buildVersion", "0.6.0"));

    @Test
    void incompatibleCandidateTouchesTheSharedGuardButWritesNoNodeOrLease() {
        List<String> writes = new ArrayList<>();
        MongoClusterProfileStore store = store(profile(4), new Document("live", true), writes);

        ClusterNodeReservation outcome = store.reserve("east", new WorkloadOwner("b", "boot-b"),
                URI.create("http://b:8080"), new ExecutionProfile(1, Map.of("buildVersion", "0.7.0")),
                Duration.ofSeconds(30));

        assertThat(outcome.outcome()).isEqualTo(ClusterNodeReservation.Outcome.INCOMPATIBLE);
        assertThat(outcome.profile().generation()).isEqualTo(4);
        assertThat(writes).containsExactly("seed-guard", "transaction", "profile-guard");
    }

    @Test
    void initialProtocolInstallationRefusesEveryLiveLegacyWorkloadIncludingBusinessClaims() {
        List<String> writes = new ArrayList<>();
        MongoClusterProfileStore store = store(new Document("_id", "east").append("generation", 0L),
                new Document("resourceType", "PIPELINE_ACTUATION"), writes);

        ClusterNodeReservation outcome = store.reserve("east", new WorkloadOwner("a", "boot-a"),
                URI.create("http://a:8080"), PROFILE, Duration.ofSeconds(30));

        assertThat(outcome.outcome()).isEqualTo(ClusterNodeReservation.Outcome.LEGACY_LEASES_ACTIVE);
        assertThat(writes).containsExactly("seed-guard", "transaction", "profile-guard");
    }

    @Test
    void profileHashCorruptionIsCodedRatherThanAcceptedAsCompatibility() {
        Document corrupt = profile(1).append("hash", "forged");
        assertThatThrownBy(() -> MongoClusterProfileStore.profile(corrupt))
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
    }

    @Test
    void allReleasedSessionsCannotAdvancePastAnOutstandingAuthorizationHorizon() {
        List<String> writes = new ArrayList<>();
        MongoClusterProfileStore store = store(profile(4), null, writes, true);

        ClusterNodeReservation outcome = store.reserve("east", new WorkloadOwner("b", "boot-b"),
                URI.create("http://b:8080"), PROFILE, Duration.ofSeconds(30));

        assertThat(outcome.outcome()).isEqualTo(ClusterNodeReservation.Outcome.AUTHORIZATION_HORIZON_ACTIVE);
        assertThat(outcome.profile().generation()).isEqualTo(4);
        assertThat(writes).containsExactly("seed-guard", "transaction", "profile-guard");
    }

    @Test
    void liveDurableFenceIncludesProfileAndLegacyFenceCannotMatchAProfiledClaim() {
        WorkloadClaimFence current = new WorkloadClaimFence(
                new WorkloadClaimKey("east", WorkloadClaimType.CAPTURE, "chain"),
                new WorkloadOwner("a", "boot-a"), 1, 0, 3, 7);
        assertThat(WorkloadClaimDocuments.live(current).getLong("profileGeneration")).isEqualTo(7L);
        assertThat(WorkloadClaimDocuments.stored(current).getLong("profileGeneration")).isEqualTo(7L);
        WorkloadClaimFence legacy = new WorkloadClaimFence(current.key(), current.owner(), 1, 0, 3);
        assertThat(WorkloadClaimDocuments.live(legacy).getList("$or", Document.class))
                .containsExactly(new Document("profileGeneration", 0L),
                        new Document("profileGeneration", new Document("$exists", false)));
    }

    @Test
    void executionProfileAndTopologyStayDistinctFromTheNewOwnersCurrentAuthority() {
        Document source = profile(2).append("schemaVersion", 1).append("clusterId", "east");
        source.remove("_id");
        Document claim = new Document("_id", "pipeline").append("clusterId", "east")
                .append("resourceType", "PIPELINE_ACTUATION").append("resourceId", "orders")
                .append("ownerNodeId", "new-owner").append("ownerBootId", "new-boot")
                .append("claimGeneration", 5L).append("executionGeneration", 7L).append("topologyRevision", 9L)
                .append("leaseUntil", new Date(0)).append("profileGeneration", 4L)
                .append("executionProfileVersion", 1).append("executionProfile", source)
                .append("executionTopologyRevision", 3L);
        var decoded = MongoWorkloadClaimStore.readDocument(claim);
        assertThat(decoded.profileGeneration()).isEqualTo(4);
        assertThat(decoded.executionProfile().generation()).isEqualTo(2);
        assertThat(decoded.topologyRevision()).isEqualTo(9);
        assertThat(decoded.executionTopologyRevision()).isEqualTo(3);
        claim.remove("executionProfile");
        assertThatThrownBy(() -> MongoWorkloadClaimStore.readDocument(claim))
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
    }

    @Test
    void nodeDeadlineClampsABusinessLeaseWithoutUsingTheNodeWallClock() {
        Date deadline = new Date(120_000L);
        Document lease = MongoWorkloadClaimStore.clampedLease(Duration.ofSeconds(30), deadline);
        assertThat(lease.getList("$min", Object.class)).containsExactly(
                new Document("$dateAdd", new Document("startDate", "$$NOW")
                        .append("unit", "millisecond").append("amount", 30_000L)), deadline);
        Document live = MongoClusterProfileStore.liveSession("east", new WorkloadOwner("a", "boot-a"), 7);
        assertThat(live.getString("ownerBootId")).isEqualTo("boot-a");
        assertThat(live.getLong("profileGeneration")).isEqualTo(7L);
        assertThat(live.get("$expr")).isEqualTo(new Document("$gt", List.of("$leaseUntil", "$$NOW")));
    }

    private static Document profile(long generation) {
        return new Document("_id", "east").append("generation", generation)
                .append("formatVersion", PROFILE.formatVersion()).append("attributes", new Document(PROFILE.attributes()))
                .append("hash", PROFILE.hash());
    }

    @SuppressWarnings("unchecked")
    private static MongoClusterProfileStore store(Document profile, Document liveClaim, List<String> writes) {
        return store(profile, liveClaim, writes, false);
    }

    @SuppressWarnings("unchecked")
    private static MongoClusterProfileStore store(Document profile, Document liveClaim, List<String> writes,
            boolean horizonActive) {
        ClientSession session = proxy(ClientSession.class, (name, args) -> {
            if (name.equals("withTransaction")) {
                writes.add("transaction");
                return ((TransactionBody<?>) args[0]).execute();
            }
            return null;
        });
        MongoClient client = proxy(MongoClient.class, (name, args) -> name.equals("startSession") ? session : null);
        MongoCollection<Document>[] ref = new MongoCollection[3];
        for (int i = 0; i < ref.length; i++) {
            final int index = i;
            ref[i] = proxy(MongoCollection.class, (name, args) -> {
                if (name.startsWith("with")) {
                    return ref[index];
                }
                if (name.equals("updateOne")) {
                    writes.add(index == 0 ? "seed-guard" : "node-write");
                    return null;
                }
                if (name.equals("findOneAndUpdate")) {
                    writes.add(index == 0 ? "profile-guard" : "claim-write");
                    return index == 0 ? profile : null;
                }
                if (name.equals("find")) {
                    return proxy(FindIterable.class, (operation, parameters) -> {
                        if (operation.equals("first")) {
                            return index == 0 && horizonActive ? profile : liveClaim;
                        }
                        throw new AssertionError(operation);
                    });
                }
                throw new AssertionError(name);
            });
        }
        return new MongoClusterProfileStore(client, ref[0], ref[1], ref[2]);
    }

    private interface Call { Object invoke(String name, Object[] args); }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> type, Call call) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type},
                (instance, method, args) -> call.invoke(method.getName(), args));
    }
}
