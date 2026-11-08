package io.tapstate.adapters.mongostore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.KeyedStateStore;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * What a store that validates once per lease still refuses. Each case opens the production cache store in
 * lease mode, proves a lease with a first write, changes the cache underneath it the way a takeover, an
 * expiry, a generation switch or an operator would, and checks the next write: refused as a unit or not,
 * and whether the store then fails closed. The per-transaction mode runs the same structure case as the
 * contrast, because it is the one change a lease is allowed to miss until it is proved again.
 */
class LeaseValidationFencingCheck {

    private static MongoDBContainer mongo;
    private static String url;
    private BenchCacheStore cache;

    @BeforeAll
    static void start() {
        mongo = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
        mongo.start();
        url = "mongodb://" + mongo.getHost() + ":" + mongo.getMappedPort(27017) + "/?directConnection=true";
    }

    @AfterAll
    static void stop() { mongo.stop(); }

    @AfterEach
    void close() {
        System.clearProperty(MongoCacheOperatorStateStore.VALIDATION_PROPERTY);
        System.clearProperty(MongoCacheOperatorStateStore.LEASE_PROPERTY);
        if (cache != null) cache.close();
    }

    private KeyedStateStore open(String validation, long leaseMillis) {
        System.setProperty(MongoCacheOperatorStateStore.VALIDATION_PROPERTY, validation);
        System.setProperty(MongoCacheOperatorStateStore.LEASE_PROPERTY, Long.toString(leaseMillis));
        cache = BenchCacheStore.open(url, url);
        return cache.stores().inDatabase(null).state();
    }

    private static Map<String, byte[]> keys(String prefix, int count) {
        Map<String, byte[]> out = new LinkedHashMap<>();
        for (int i = 0; i < count; i++) out.put(prefix + i, new byte[] {(byte) i, 1, 2, 3});
        return out;
    }

    private long stored() { return SystemCollections.CACHE_NEST_STATE.on(cache.database()).countDocuments(); }

    private void onClaim(Document update) {
        assertThat(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(cache.database())
                .updateOne(new Document("_id.resourceType", "PIPELINE_ACTUATION"), update).getMatchedCount()).isEqualTo(1);
    }

    private static void refusedAsClaimFenced(Runnable action) {
        assertThatThrownBy(action::run).isInstanceOfSatisfying(TapstateException.class,
                refused -> assertThat(refused.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED));
    }

    @Test
    void aClaimTakenOverWithinTheLeaseRefusesTheNextCommitAndEveryOperationAfterIt() {
        KeyedStateStore store = open("lease", 60_000);
        store.saveAll("ns", keys("a", 10));
        long before = stored();
        onClaim(new Document("$inc", new Document("claimGeneration", 1L)));

        refusedAsClaimFenced(() -> store.saveAll("ns", keys("b", 200)));
        assertThat(stored()).describedAs("nothing of the refused group lands").isEqualTo(before);
        // The lease is dropped: the next operation proves again and fails closed, reads included.
        refusedAsClaimFenced(() -> store.loadAll("ns", List.of("a1")));
        refusedAsClaimFenced(() -> store.save("ns", "c", new byte[] {1}));
        assertThat(stored()).isEqualTo(before);
    }

    @Test
    void theReadOnlyClaimCheckAlsoRefusesATakeoverThatCommittedBeforeTheWrite() {
        KeyedStateStore store = open("lease-read", 60_000);
        store.saveAll("ns", keys("a", 10));
        long before = stored();
        onClaim(new Document("$inc", new Document("claimGeneration", 1L)));
        refusedAsClaimFenced(() -> store.saveAll("ns", keys("b", 200)));
        assertThat(stored()).isEqualTo(before);
    }

    @Test
    void theStateFenceVariantRefusesATakeoverToo() {
        KeyedStateStore store = open("lease-state", 60_000);
        store.saveAll("ns", keys("a", 10));
        long before = stored();
        onClaim(new Document("$inc", new Document("claimGeneration", 1L)));
        refusedAsClaimFenced(() -> store.saveAll("ns", keys("b", 200)));
        assertThat(stored()).isEqualTo(before);
    }

    @Test
    void anExpiredLeaseRefusesTheNextCommitEvenBeforeTheLocalLeaseRunsOut() {
        KeyedStateStore store = open("lease", 60_000);
        store.saveAll("ns", keys("a", 10));
        long before = stored();
        onClaim(new Document("$set", new Document("leaseUntil", new java.util.Date(System.currentTimeMillis() - 1_000))));
        refusedAsClaimFenced(() -> store.saveAll("ns", keys("b", 10)));
        assertThat(stored()).isEqualTo(before);
    }

    @Test
    void aGenerationSwitchRefusesTheNextCommit() {
        KeyedStateStore store = open("lease", 60_000);
        store.saveAll("ns", keys("a", 10));
        long before = stored();
        onClaim(new Document("$inc", new Document("pipelineScope.stateGeneration", 1L)));
        refusedAsClaimFenced(() -> store.saveAll("ns", keys("b", 10)));
        assertThat(stored()).isEqualTo(before);
    }

    @Test
    void aReadWithinTheLeaseAfterATakeoverIsServedAndOnlyTheWriteIsRefused() {
        KeyedStateStore store = open("lease", 60_000);
        store.saveAll("ns", keys("a", 10));
        onClaim(new Document("$inc", new Document("claimGeneration", 1L)));
        // Reads are not fenced within a lease: a stale owner may read, it cannot write.
        assertThat(store.loadAll("ns", List.of("a1", "a2"))).hasSize(2);
        refusedAsClaimFenced(() -> store.save("ns", "b", new byte[] {1}));
    }

    @Test
    void aStructureChangeIsSeenOnlyWhenTheLeaseIsProvedAgain() throws Exception {
        KeyedStateStore store = open("lease", 1_500);
        store.saveAll("ns", keys("a", 10));
        SystemCollections.CACHE_NEST_STATE.on(cache.database()).createIndex(new Document("ttl", 1),
                new com.mongodb.client.model.IndexOptions().expireAfter(3600L, java.util.concurrent.TimeUnit.SECONDS));
        store.saveAll("ns", keys("b", 10));
        assertThat(stored()).describedAs("within the lease the change is not looked for").isEqualTo(20);
        Thread.sleep(1_700);
        assertThatThrownBy(() -> store.saveAll("ns", keys("c", 10))).isInstanceOfSatisfying(TapstateException.class,
                refused -> assertThat(refused.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
        assertThat(stored()).isEqualTo(20);
    }

    @Test
    void perTransactionValidationSeesTheSameStructureChangeAtOnce() {
        KeyedStateStore store = open("transaction", 60_000);
        store.saveAll("ns", keys("a", 10));
        SystemCollections.CACHE_NEST_STATE.on(cache.database()).createIndex(new Document("ttl", 1),
                new com.mongodb.client.model.IndexOptions().expireAfter(3600L, java.util.concurrent.TimeUnit.SECONDS));
        assertThatThrownBy(() -> store.saveAll("ns", keys("b", 10))).isInstanceOfSatisfying(TapstateException.class,
                refused -> assertThat(refused.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
    }
}
