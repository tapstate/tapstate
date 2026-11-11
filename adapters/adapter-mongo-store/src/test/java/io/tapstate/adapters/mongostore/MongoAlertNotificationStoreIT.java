package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.alerting.AlertChannelPolicy.Deployment;
import io.tapstate.core.alerting.AlertTargetLimit;
import io.tapstate.core.alerting.AlertTestQuota;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import io.tapstate.testsupport.RequiresDocker;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import static io.tapstate.adapters.mongostore.MongoAlertNotificationCodecTest.*;
import static org.assertj.core.api.Assertions.*;

@RequiresDocker
class MongoAlertNotificationStoreIT {
    @Container static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
            .withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1");

    @Test void readsAreLoadOnlyAndOriginalQuotaAndRestrictionSurviveAnotherClient() {
        withConfiguration(f -> {
            var noQuota = transaction(f, s -> f.store.quota(s, MINUTE));
            var noLimit = transaction(f, s -> f.store.limit(s, DOMAIN));
            assertThat(noQuota).isEmpty();
            assertThat(noLimit).isEmpty();
            assertThat(SystemCollections.ALERT_TEST_QUOTAS.on(f.database).countDocuments()).isZero();
            var bucket = AlertTestQuota.Bucket.empty(SCOPE, MINUTE).accepted("actor-a");
            var limit = new AlertTargetLimit(DOMAIN, MINUTE.plusSeconds(48 * 3600).plusNanos(1));
            assertThat(MongoAlertNotificationStoreIT.<MongoAlertNotificationStore.Stage>transaction(f, s -> f.store.stageQuota(s, null, bucket)))
                    .isEqualTo(MongoAlertNotificationStore.Stage.STAGED);
            transaction(f, s -> f.store.stageLimit(s, null, limit));
            try (var client = MongoClients.create(MONGO.getReplicaSetUrl())) {
                var reopened = new MongoAlertNotificationStore(client.getDatabase(f.database.getName()), SCOPE, Deployment.ON_PREM, () -> {});
                try (var session = client.startSession()) {
                    session.withTransaction(() -> {
                        assertThat(reopened.quota(session, MINUTE).orElseThrow().value()).isEqualTo(bucket);
                        assertThat(reopened.requiredLimit(session, DOMAIN, 1).value()).isEqualTo(limit);
                        return null;
                    });
                }
            }
            assertThat(limit.nextPermitAt(MINUTE, MINUTE.plusSeconds(300))).isEmpty();
        });
    }

    @Test void quotaAndRestrictionFragmentsRollBackWithTheOriginalConfigurationContentionWrite() {
        withConfiguration(f -> {
            assertThatThrownBy(() -> transaction(f, s -> {
                f.store.stageQuota(s, null, AlertTestQuota.Bucket.empty(SCOPE, MINUTE).accepted("actor-a"));
                f.store.stageLimit(s, null, new AlertTargetLimit(DOMAIN, MINUTE.plusSeconds(120)));
                ConfigurationVersion.advanceAlert(s, SystemCollections.CONFIGURATION_VERSION.on(f.database));
                throw new AbortProbe();
            })).isInstanceOf(AbortProbe.class);
            assertThat(SystemCollections.ALERT_TEST_QUOTAS.on(f.database).countDocuments()).isZero();
            assertThat(SystemCollections.ALERT_TARGET_LIMITS.on(f.database).countDocuments()).isZero();
            assertThat(SystemCollections.CONFIGURATION_VERSION.on(f.database).find().first().getLong(ConfigurationVersion.ALERT_FENCE)).isZero();
        });
    }

    @Test void differentActorsContendOnOneScopeHeadAndCannotExceedTenAcrossRetries() {
        withConfiguration(f -> {
            transaction(f, s -> {
                f.store.stageQuota(s, null, AlertTestQuota.Bucket.empty(SCOPE, MINUTE).accepted("seed"));
                return ConfigurationVersion.advanceAlert(s, SystemCollections.CONFIGURATION_VERSION.on(f.database));
            });
            CountDownLatch bothRead = new CountDownLatch(2);
            try (var executor = Executors.newFixedThreadPool(4)) {
                var futures = new ArrayList<java.util.concurrent.Future<Boolean>>();
                for (int index = 0; index < 12; index++) {
                    final int actor = index; AtomicBoolean first = new AtomicBoolean(true);
                    futures.add(executor.submit(() -> transaction(f, s -> {
                        var current = f.store.quota(s, MINUTE).orElseThrow();
                        if (actor < 2 && first.compareAndSet(true, false)) { bothRead.countDown(); await(bothRead); }
                        if (current.value().scopeAccepted() == 10) { return false; }
                        assertThat(f.store.stageQuota(s, current, current.value().accepted("actor-" + actor)))
                                .isEqualTo(MongoAlertNotificationStore.Stage.STAGED);
                        ConfigurationVersion.advanceAlert(s, SystemCollections.CONFIGURATION_VERSION.on(f.database));
                        return true;
                    })));
                }
                int admitted = 0;
                for (var future : futures) { if (future.get(20, TimeUnit.SECONDS)) { admitted++; } }
                assertThat(admitted).isEqualTo(9);
                var actual = transaction(f, s -> f.store.quota(s, MINUTE).orElseThrow());
                assertThat(actual.value().scopeAccepted()).isEqualTo(10);
                assertThat(actual.recordRevision()).isEqualTo(10);
                assertThat(SystemCollections.ALERT_TEST_QUOTAS.on(f.database).countDocuments()).isOne();
                assertThat(SystemCollections.CONFIGURATION_VERSION.on(f.database).find().first().getLong("revision")).isZero();
            } catch (Exception failure) { throw new AssertionError(failure); }
        });
    }

    @Test void equalBucketCountsCannotPretendThatADifferentCommandWasAlreadyAccepted() {
        withConfiguration(f -> {
            var first = AlertTestQuota.Bucket.empty(SCOPE, MINUTE).accepted("actor-a");
            transaction(f, s -> f.store.stageQuota(s, null, first));
            assertThat(MongoAlertNotificationStoreIT.<MongoAlertNotificationStore.Stage>transaction(f, s -> f.store.stageQuota(s, null, first)))
                    .isEqualTo(MongoAlertNotificationStore.Stage.CONFLICT);
            var actual = transaction(f, s -> f.store.quota(s, MINUTE).orElseThrow());
            assertThatThrownBy(() -> transaction(f, s -> f.store.stageQuota(s, actual, first))).isInstanceOf(IllegalArgumentException.class);
            assertThat(actual.value().window("actor-a").actorAccepted()).isOne();
        });
    }

    @Test void monotonicTargetCasRejectsShrinkingAndStaleUpdatesAndMissingReferencedHeads() {
        withConfiguration(f -> {
            var first = new AlertTargetLimit(DOMAIN, MINUTE.plusSeconds(120));
            transaction(f, s -> f.store.stageLimit(s, null, first));
            var before = transaction(f, s -> f.store.requiredLimit(s, DOMAIN, 1));
            assertThat(MongoAlertNotificationStoreIT.<MongoAlertNotificationStore.Stage>transaction(f, s -> f.store.stageLimit(s, before, first)))
                    .isEqualTo(MongoAlertNotificationStore.Stage.UNCHANGED);
            assertThatThrownBy(() -> transaction(f, s -> f.store.stageLimit(s, before, new AlertTargetLimit(DOMAIN, MINUTE))))
                    .isInstanceOf(IllegalArgumentException.class);
            transaction(f, s -> f.store.stageLimit(s, before, first.observe(DOMAIN, MINUTE.plusSeconds(3600))));
            assertThat(MongoAlertNotificationStoreIT.<MongoAlertNotificationStore.Stage>transaction(f, s -> f.store.stageLimit(s, before, first)))
                    .isEqualTo(MongoAlertNotificationStore.Stage.CONFLICT);
            SystemCollections.ALERT_TARGET_LIMITS.on(f.database).deleteMany(new Document());
            assertUnreadable(() -> transaction(f, s -> f.store.requiredLimit(s, DOMAIN, 1)));
            assertThat(SystemCollections.ALERT_TARGET_LIMITS.on(f.database).countDocuments()).isZero();
        });
    }

    @Test void corruptQuotaCannotReopenAsEmptyOrBeReplacedThroughAFreshCandidate() {
        withConfiguration(f -> {
            transaction(f, s -> f.store.stageQuota(s, null, AlertTestQuota.Bucket.empty(SCOPE, MINUTE).accepted("actor-a")));
            SystemCollections.ALERT_TEST_QUOTAS.on(f.database).updateOne(new Document(), new Document("$set", new Document("recordRevision", 1)));
            assertUnreadable(() -> transaction(f, s -> f.store.quota(s, MINUTE)));
            assertUnreadable(() -> transaction(f, s -> f.store.stageQuota(s, null, AlertTestQuota.Bucket.empty(SCOPE, MINUTE).accepted("actor-b"))));
            assertThat(SystemCollections.ALERT_TEST_QUOTAS.on(f.database).find().first().get("recordRevision")).isInstanceOf(Integer.class);
        });
    }

    @Test void aLostCommitReplyDoesNotReexecuteTheCommittedQuotaMutation() {
        withConfiguration(f -> {
            f.client.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", new Document("times", 1))
                    .append("data", new Document("failCommands", List.of("commitTransaction"))
                            .append("writeConcernError", new Document("code", 64).append("errmsg", "test commit reply lost"))
                            .append("errorLabels", List.of("UnknownTransactionCommitResult"))));
            try {
                transaction(f, s -> {
                    f.store.stageQuota(s, null, AlertTestQuota.Bucket.empty(SCOPE, MINUTE).accepted("actor-a"));
                    return ConfigurationVersion.advanceAlert(s, SystemCollections.CONFIGURATION_VERSION.on(f.database));
                });
            } finally { f.client.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off")); }
            assertThat(MongoAlertNotificationStoreIT.<MongoAlertNotificationCodec.Quota>transaction(f, s -> f.store.quota(s, MINUTE).orElseThrow())
                    .value().scopeAccepted()).isOne();
            assertThat(SystemCollections.CONFIGURATION_VERSION.on(f.database).find().first().getLong(ConfigurationVersion.ALERT_FENCE)).isOne();
        });
    }

    @Test void foreignScopeAndCloudCannotCreateExternalQuotaOrRestrictionFacts() {
        withConfiguration(f -> {
            var foreign = new io.tapstate.core.alerting.AlarmKey.Scope("foreign", SCOPE.clusterId());
            assertUnreadable(() -> transaction(f, s -> f.store.stageQuota(s, null, AlertTestQuota.Bucket.empty(foreign, MINUTE).accepted("actor"))));
            var cloud = new MongoAlertNotificationStore(f.database, SCOPE, Deployment.CLOUD_MANAGED, () -> {});
            assertThatThrownBy(() -> transaction(f, s -> cloud.stageQuota(s, null, AlertTestQuota.Bucket.empty(SCOPE, MINUTE).accepted("actor"))))
                    .isInstanceOf(TapstateException.class);
            assertThatThrownBy(() -> transaction(f, s -> cloud.stageLimit(s, null, new AlertTargetLimit(DOMAIN, MINUTE))))
                    .isInstanceOf(TapstateException.class);
            assertThat(SystemCollections.ALERT_TEST_QUOTAS.on(f.database).countDocuments()).isZero();
            assertThat(SystemCollections.ALERT_TARGET_LIMITS.on(f.database).countDocuments()).isZero();
        });
    }

    private record Fixture(MongoClient client, MongoDatabase database, MongoAlertNotificationStore store) { }
    private static void withConfiguration(Consumer<Fixture> action) {
        try (var client = MongoClients.create(MONGO.getReplicaSetUrl())) {
            var db = client.getDatabase("alert_controls_" + UUID.randomUUID().toString().replace("-", ""));
            var settings = new MongoConnectionSettings(MONGO.getReplicaSetUrl(db.getName()), null, Duration.ofSeconds(5));
            try (var domain = MongoConfigurationBootstrap.prepare(settings, SCOPE.clusterId(), "installation-a", "setup-a", 1)) {
                assertThat(domain.alertSchemaVersion()).isEqualTo(MongoAlertStorageSchema.VERSION);
                action.accept(new Fixture(client, db, new MongoAlertNotificationStore(db, SCOPE, Deployment.ON_PREM, () -> {})));
            } finally { db.drop(); }
        }
    }
    private static <T> T transaction(Fixture f, Function<ClientSession, T> body) {
        return new MongoReadBudget(Duration.ofSeconds(30)).transaction(f.client, body);
    }
    private static void await(CountDownLatch latch) {
        try { if (!latch.await(10, TimeUnit.SECONDS)) { throw new AssertionError("transaction barrier timed out"); } }
        catch (InterruptedException failure) { Thread.currentThread().interrupt(); throw new AssertionError(failure); }
    }
    private static void assertUnreadable(org.assertj.core.api.ThrowableAssert.ThrowingCallable action) {
        assertThatThrownBy(action).isInstanceOfSatisfying(TapstateException.class, e -> assertThat(e.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
    }
    private static final class AbortProbe extends RuntimeException { }
}
