package io.tapstate.adapters.mongostore;

import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.CachePreparation;
import io.tapstate.spi.store.CacheBusinessScope;
import org.bson.Document;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Initial business DDL and readiness publish together; existing readiness permits verification only. */
final class MongoCacheBusinessPreparation {
    private static final String BUSINESS_PHASE = "businessPhase";
    private static final String BUSINESS_RETIREMENT_ID = "businessRetirementId";
    private static final String EXISTS = "$exists";
    private static final String BUSINESS_PREPARATION_ID = "businessPreparationId";
    private static final String CLAIM_ADMISSION_FENCE = "claimAdmissionFence";
    // This value exists only inside an uncommitted DDL transaction; only READY is published.
    static final String CREATING = "CREATING";
    private static final TransactionOptions TRANSACTION = TransactionOptions.builder().readConcern(ReadConcern.LOCAL)
            .writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();
    private final MongoClient client;
    private final MongoDatabase database;
    private final CachePreparation original;
    private final Runnable active;
    private final MongoCacheMetadataStructure structure;

    MongoCacheBusinessPreparation(MongoClient client, MongoDatabase database, CachePreparation original, Duration retention, Runnable active) {
        this.client = Objects.requireNonNull(client, "client"); this.database = Objects.requireNonNull(database, "database");
        this.original = Objects.requireNonNull(original, "original"); this.active = Objects.requireNonNull(active, "active");
        this.structure = new MongoCacheMetadataStructure(database, retention);
    }

    CacheBusinessScope prepare() {
        return StoreIo.call(() -> {
            try {
                active.run();
                try (var session = client.startSession()) {
                    session.withTransaction(() -> {
                        active.run();
                        Document row = SystemCollections.CACHE_IDENTITY.on(database).find(session, new Document("_id", "cluster")).first();
                        if (row == null || !original.authScope().clusterId().equals(row.get("clusterId"))
                                || !Long.valueOf(original.authScope().bindingVersion()).equals(row.get("bindingVersion"))
                                || !original.authScope().cacheIncarnation().equals(row.get("cacheIncarnation"))
                                || !MongoConfigurationDomain.LAYOUT.equals(row.get("layoutId"))) throw refused();
                        if ("READY".equals(row.get(BUSINESS_PHASE))) {
                            if (!original.operationId().equals(row.get(BUSINESS_PREPARATION_ID))) throw refused();
                            new MongoCacheBusinessIdentity(database, original.businessScope()).requireAdmissionState(session);
                            active.run();
                            return null;
                        }
                        if (!"PREPARING".equals(row.get(BUSINESS_PHASE)) || !original.operationId().equals(row.get(BUSINESS_PREPARATION_ID))
                                || row.containsKey(CLAIM_ADMISSION_FENCE) || row.containsKey(BUSINESS_RETIREMENT_ID)
                                || SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(database).find(session, new Document("_id", "business")).first() != null) throw refused();
                        Document pending = new Document("_id", "cluster").append("clusterId", original.authScope().clusterId())
                                .append("bindingVersion", original.authScope().bindingVersion()).append("cacheIncarnation", original.authScope().cacheIncarnation())
                                .append(BUSINESS_PHASE, "PREPARING").append(BUSINESS_PREPARATION_ID, original.operationId())
                                .append(CLAIM_ADMISSION_FENCE, new Document(EXISTS, false)).append(BUSINESS_RETIREMENT_ID, new Document(EXISTS, false));
                        // Serialize with another preparation before observing structures outside this snapshot.
                        if (SystemCollections.CACHE_IDENTITY.on(database).updateOne(session, pending,
                                new Document("$set", new Document(BUSINESS_PHASE, CREATING))).getMatchedCount() != 1) throw refused();
                        requireOnlyAuthCollections();
                        structure.prepareBusinessInTransaction(session, active);
                        MongoCacheClusterMembershipStore.prepareInitial(session, database, original.businessScope(), original.operationId());
                        SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).insertOne(session,
                                MongoCacheBusinessIdentity.claimNamespaceRecord(original.businessScope(), original.operationId()));
                        Document filter = new Document("_id", "cluster").append("clusterId", original.authScope().clusterId())
                                .append("bindingVersion", original.authScope().bindingVersion()).append("cacheIncarnation", original.authScope().cacheIncarnation())
                                .append(BUSINESS_PHASE, CREATING).append(BUSINESS_PREPARATION_ID, original.operationId())
                                .append(CLAIM_ADMISSION_FENCE, new Document(EXISTS, false))
                                .append(BUSINESS_RETIREMENT_ID, new Document(EXISTS, false));
                        if (SystemCollections.CACHE_IDENTITY.on(database).updateOne(session, filter, new Document("$set", new Document(BUSINESS_PHASE, "READY")
                                .append(BUSINESS_PREPARATION_ID, original.operationId()).append(CLAIM_ADMISSION_FENCE, 0L)
                                .append(MongoCacheBusinessIdentity.CLAIM_NAMESPACE_PROOF_VERSION,
                                        MongoCacheBusinessIdentity.CLAIM_NAMESPACE_PROOF_V1))).getMatchedCount() != 1) throw refused();
                        SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(database).insertOne(session, new Document("_id", "business").append("installedVersion", 1)
                                .append("epoch", 1L).append("operationId", original.operationId()).append("phase", "READY"));
                        active.run();
                        return null;
                    }, TRANSACTION);
                }
                verify(); active.run();
                return original.businessScope();
            } catch (RuntimeException failure) { active.run(); throw failure; }
        });
    }

    void verify() {
        active.run(); structure.verifyBusiness();
        new MongoCacheClusterMembershipStore(client, database, original.businessScope(), null, active).read(original.authScope().clusterId());
        active.run();
    }

    private void requireOnlyAuthCollections() {
        var allowed = new HashSet<>(Set.of("cache_identity", "schema_migrations"));
        if (original.authScope().mode() == io.tapstate.spi.store.StorageBinding.Mode.OP) allowed.add("op_sessions");
        else { allowed.add("cloud_sessions"); allowed.add("cloud_revocations"); }
        var existing = new HashSet<>(database.listCollectionNames().into(new ArrayList<>()));
        if (!allowed.containsAll(existing) || !existing.containsAll(Set.of("cache_identity", "schema_migrations"))) throw refused();
    }
    private static TapstateException refused() { return new TapstateException(StoreError.INITIALIZATION_REFUSED, Map.of(), null); }
}
