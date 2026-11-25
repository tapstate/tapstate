package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.mongodb.MongoException;
import com.mongodb.client.MongoClients;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.testsupport.RequiresDocker;
import java.time.Instant;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;
import org.bson.Document;
import org.junit.jupiter.api.Test;

@RequiresDocker
class NativeHeldTransactionIT {
    @Test void anActualAcquisitionConflictDoesNotAbandonTheHeldTransactionProof() {
        try (var storage = PreparedNativeStore.create(); var client = MongoClients.create(storage.cacheUri());
                var competing = client.startSession()) {
            var claims = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(client.getDatabase(storage.cacheDatabase()));
            var fence = fence(storage);
            var key = key(fence);
            claims.insertOne(row(fence, key));
            competing.startTransaction();
            claims.updateOne(competing, new Document("_id", key),
                    new Document("$inc", new Document("fencedAppends", 1L)));
            var conflicts = new AtomicInteger();
            var observer = new CommandListener() {
                @Override public void commandFailed(CommandFailedEvent event) {
                    if (event.getCommandName().equals("update") && event.getThrowable() instanceof MongoException failure
                            && failure.getCode() == 112 && conflicts.incrementAndGet() == 1) {
                        competing.abortTransaction();
                    }
                }
            };
            assertThatCode(() -> {
                try (var held = new NativeHeldTransaction(storage, fence, "original-held-boot", observer)) {
                    assertThat(conflicts.get()).isEqualTo(1);
                    assertThat(held.stillHeld()).isTrue();
                    assertThat(claims.find(new Document("_id", key)).first().getLong("fencedAppends")).isZero();
                }
            }).doesNotThrowAnyException();
            assertThat(claims.find(new Document("_id", key)).first().getLong("fencedAppends")).isZero();
        }
    }

    @Test void aChangedOriginalFenceIsRefusedWithoutRetryingAnotherIdentity() {
        try (var storage = PreparedNativeStore.create(); var client = MongoClients.create(storage.cacheUri())) {
            var claims = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(client.getDatabase(storage.cacheDatabase()));
            var fence = fence(storage);
            var key = key(fence);
            claims.insertOne(row(fence, key).append("claimGeneration", 2L));
            var updates = new AtomicInteger();
            var observer = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent event) {
                    if (event.getCommandName().equals("update")) updates.incrementAndGet();
                }
            };
            assertThatThrownBy(() -> new NativeHeldTransaction(storage, fence, "original-held-boot", observer))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("original live capture fence");
            assertThat(updates.get()).isEqualTo(1);
            assertThat(claims.find(new Document("_id", key)).first().getLong("fencedAppends")).isZero();
        }
    }

    private static Document fence(PreparedNativeStore storage) {
        return new Document("clusterId", storage.clusterId()).append("resourceType", "CAPTURE")
                .append("resourceId", "original-capture").append("ownerNodeId", "original-node")
                .append("ownerBootId", "original-boot").append("claimGeneration", 1L)
                .append("executionGeneration", 1L).append("topologyRevision", 1L);
    }

    private static Document key(Document fence) {
        return new Document("clusterId", fence.getString("clusterId")).append("resourceType", "CAPTURE")
                .append("resourceId", fence.getString("resourceId"));
    }

    private static Document row(Document fence, Document key) {
        return new Document(fence).append("_id", key).append("fencedAppends", 0L)
                .append("leaseUntil", Date.from(Instant.now().plusSeconds(60)));
    }
}
