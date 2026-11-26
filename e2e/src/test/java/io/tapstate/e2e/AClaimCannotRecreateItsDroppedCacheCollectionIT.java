package io.tapstate.e2e;

import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.MongoCacheDomain;
import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.adapters.mongostore.MongoConnectionSettings;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RequiresDocker
class AClaimCannotRecreateItsDroppedCacheCollectionIT {
    @Test
    void aPreparedCacheRefusesAnotherClaimAfterItsCollectionIsDropped() {
        var timeout = Duration.ofSeconds(5);
        var ttl = Duration.ofSeconds(30);
        try (var installed = PreparedNativeStore.create();
             var config = MongoConfigurationDomain.open(
                     new MongoConnectionSettings(installed.configurationUri(), null, timeout), installed.clusterId());
             var cache = MongoCacheDomain.connect(config.bindings().load().orElseThrow().cache(), timeout, "claim-witness");
             var setupClient = MongoClients.create(installed.cacheUri())) {
            var scope = config.bindings().cachePreparation().orElseThrow().businessScope();
            var claims = cache.workloadClaims(scope);
            var owner = new WorkloadOwner("scope-node", "boot");
            var key = new WorkloadClaimKey(scope.clusterId(), WorkloadClaimType.NODE_SESSION, owner.nodeId());
            assertThat(claims.acquire(key, owner, 1, ttl).acquired()).isTrue();

            var database = setupClient.getDatabase(installed.cacheDatabase());
            var table = SystemCollections.CACHE_WORKLOAD_CLAIMS;
            table.on(database).drop();
            var identity = SystemCollections.CACHE_IDENTITY.on(database);
            var before = identity.find().first();
            assertThatThrownBy(() -> claims.acquire(key, owner, 1, ttl))
                    .isInstanceOfSatisfying(TapstateException.class,
                            failure -> assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
            assertThat(database.listCollectionNames()).doesNotContain(table.collectionName());
            assertThat(identity.find().first()).isEqualTo(before);
        }
    }
}
