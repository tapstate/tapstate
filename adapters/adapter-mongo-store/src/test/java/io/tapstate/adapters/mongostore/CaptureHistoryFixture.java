package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoDatabase;
import io.tapstate.spi.store.*;
import java.time.Duration;
import java.util.UUID;
import org.bson.Document;
import static org.assertj.core.api.Assertions.assertThat;

/** Known-fresh protocol fixtures only. Production birth requires the committed CONFIG grant. */
final class CaptureHistoryFixture {
    private CaptureHistoryFixture() {}
    static WorkloadClaimFence expired(MongoDatabase database,CacheBusinessScope scope,String capture,String chain,WorkloadOwner owner,long topology,Duration ttl) {
        var store=new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database));
        var claim=store.acquire(new WorkloadClaimKey(scope.clusterId(),WorkloadClaimType.CAPTURE,capture),owner,topology,ttl).claim();
        assertThat(store.release(claim)).isTrue();
        marker(database,scope,WorkloadClaimFence.from(claim),chain,CapturePreparation.Mode.DIRECT);
        return WorkloadClaimFence.from(claim);
    }
    static void shared(MongoDatabase database,CacheBusinessScope scope,WorkloadClaimFence fence,String chain,String retention) {
        marker(database,scope,fence,chain,CapturePreparation.Mode.SHARED_SRS);
        SystemCollections.SRS_CHAINS.on(database).insertOne(MongoSrsChainStore.initialRow(scope,chain,retention,fence));
    }
    private static void marker(MongoDatabase database,CacheBusinessScope scope,WorkloadClaimFence fence,String chain,CapturePreparation.Mode mode) {
        var value=new CapturePreparation(scope,mode,chain,fence.key().resourceId(),"mc-"+"a".repeat(64),UUID.randomUUID().toString());
        assertThat(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).updateOne(new Document("_id",MongoWorkloadClaimStore.id(fence.key())),
                new Document("$set",new Document("capturePreparation",MongoCapturePreparationFacts.row(value)).append("fencedChainWrites",1L))).getMatchedCount()).isEqualTo(1);
    }
}
