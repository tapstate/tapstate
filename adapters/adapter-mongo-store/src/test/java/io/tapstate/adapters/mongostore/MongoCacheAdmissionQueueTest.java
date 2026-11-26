package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterType;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.IoError;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MongoCacheAdmissionQueueTest {
    @Test
    void interruptedWaitKeepsItsCauseAndLeavesAdmissionAvailable() {
        MongoClient client = mock(MongoClient.class);
        when(client.getClusterDescription()).thenReturn(
                new ClusterDescription(ClusterConnectionMode.MULTIPLE, ClusterType.UNKNOWN, List.of()));
        var operations = new MongoClaimOperations(client);
        var scope = new CacheBusinessScope("cluster", 1, "cache");
        String database = "cache";
        MongoCacheAdmissionQueue.bind(client, database, scope, operations);
        try {
            MongoCacheAdmissionQueue.first(client, database, scope, () -> {}, () -> {
                var result = new CompletableFuture<InterruptedAdmission>();
                Thread.ofVirtual().start(() -> {
                    Thread.currentThread().interrupt();
                    try {
                        MongoCacheAdmissionQueue.first(client, database, scope, () -> {}, () -> {
                            throw new AssertionError("Interrupted waiter reached the operation");
                        });
                        result.completeExceptionally(new AssertionError("Interrupted waiter entered"));
                    } catch (TapstateException failure) {
                        result.complete(new InterruptedAdmission(failure, Thread.currentThread().isInterrupted()));
                    } catch (RuntimeException | Error failure) {
                        result.completeExceptionally(failure);
                    }
                });
                var interrupted = result.orTimeout(5, TimeUnit.SECONDS).join();
                assertThat(interrupted.failure().code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED);
                assertThat(interrupted.failure()).hasCauseInstanceOf(InterruptedException.class);
                assertThat(interrupted.interrupted()).isTrue();
                assertThat(MongoCacheAdmissionQueue.waiting(client, database, scope, true)).isZero();
                return null;
            });
            assertThat(MongoCacheAdmissionQueue.first(client, database, scope, () -> {}, () -> "next"))
                    .isEqualTo("next");
        } finally {
            MongoCacheAdmissionQueue.unbind(client);
            operations.closeIfIdle();
        }
    }

    private record InterruptedAdmission(TapstateException failure, boolean interrupted) {}
}
