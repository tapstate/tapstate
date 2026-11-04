package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoDatabase;
import com.mongodb.MongoCommandException;
import com.mongodb.ServerAddress;
import io.tapstate.core.common.TapstateException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

class MongoClaimOperationsTest {
    @Test void acknowledgedTransactionRetainsItsSessionUntilLocalCloseSucceeds() {
        var client=client();var session=mock(ClientSession.class);
        var operations=new MongoClaimOperations(client);var transaction=operations.own(session);
        doThrow(new IllegalStateException("local session close failed")).doNothing().when(session).close();
        assertThatThrownBy(()->operations.committed(transaction)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(operations::closeIfIdle).isInstanceOf(TapstateException.class);
        operations.await(deadline(2000));operations.closeIfIdle();
        verify(session,times(2)).close();verify(session,never()).abortTransaction();verify(client,never()).getDatabase(anyString());
    }

    @Test void terminalAbortWithFailedWriteConcernRetainsTheOriginalCleanupOwner() {
        var client=client();var database=mock(MongoDatabase.class);var session=mock(ClientSession.class);
        var server=mock(com.mongodb.session.ServerSession.class);
        var number=new java.util.concurrent.atomic.AtomicLong(42);
        var originalToken=org.bson.BsonDocument.parse("{recoveryShardId:'original-shard',opaque:{version:7}}");
        var token=new AtomicReference<>(originalToken);
        when(session.getServerSession()).thenReturn(server);when(server.getTransactionNumber()).thenAnswer(call->number.get());
        when(session.getRecoveryToken()).thenAnswer(call->token.get());
        doAnswer(call->{number.incrementAndGet();token.set(null);return null;}).when(session).startTransaction(any(com.mongodb.TransactionOptions.class));
        when(client.getDatabase("admin")).thenReturn(database);
        when(database.withReadPreference(any())).thenReturn(database);
        when(database.withTimeout(anyLong(),any())).thenReturn(database);
        var response=org.bson.BsonDocument.parse("{ok:0,code:251,errmsg:'no transaction',writeConcernError:{code:91,errmsg:'not acknowledged'}}");
        when(database.runCommand(eq(session),any(org.bson.Document.class)))
                .thenThrow(new MongoCommandException(response,new ServerAddress()))
                .thenThrow(new MongoCommandException(org.bson.BsonDocument.parse("{ok:0,code:251,errmsg:'no transaction'}"),new ServerAddress()));
        var operations=new MongoClaimOperations(client);var transaction=operations.own(session);
        assertThatThrownBy(()->operations.abort(transaction,new MongoReadBudget(Duration.ofSeconds(2))))
                .isInstanceOf(MongoCommandException.class);
        verify(session,never()).close();
        assertThatThrownBy(operations::closeIfIdle).isInstanceOf(TapstateException.class);
        operations.await(deadline(2000));operations.closeIfIdle();verify(session).close();
        var commands=org.mockito.ArgumentCaptor.forClass(org.bson.Document.class);
        verify(database,times(2)).runCommand(eq(session),commands.capture());
        assertThat(commands.getAllValues()).allSatisfy(c->{
            assertThat(c.getLong("txnNumber")).isEqualTo(42L);
            assertThat(c.get("recoveryToken")).isEqualTo(originalToken);
        });
    }

    @Test void aRouterWithoutARecoveryTokenCannotReleaseAnUnconfirmedTransaction() {
        var client=client();var database=mock(MongoDatabase.class);var session=mock(ClientSession.class);
        var topology=mock(com.mongodb.connection.ClusterDescription.class);
        when(topology.getType()).thenReturn(com.mongodb.connection.ClusterType.SHARDED);
        when(client.getClusterDescription()).thenReturn(topology);
        var server=mock(com.mongodb.session.ServerSession.class);
        when(session.getServerSession()).thenReturn(server);when(server.getTransactionNumber()).thenReturn(42L);
        when(client.getDatabase("admin")).thenReturn(database);
        when(database.withReadPreference(any())).thenReturn(database);when(database.withTimeout(anyLong(),any())).thenReturn(database);
        when(database.runCommand(eq(session),any(org.bson.Document.class)))
                .thenThrow(new MongoCommandException(org.bson.BsonDocument.parse("{ok:0,code:251,errmsg:'not known here'}"),new ServerAddress()))
                .thenReturn(new org.bson.Document("ok",1.0));
        var operations=new MongoClaimOperations(client);var transaction=operations.own(session);
        assertThatThrownBy(()->operations.abort(transaction,new MongoReadBudget(Duration.ofSeconds(2))))
                .isInstanceOf(MongoCommandException.class);
        verify(session,never()).close();assertThatThrownBy(operations::closeIfIdle).isInstanceOf(TapstateException.class);
        operations.await(deadline(2000));operations.closeIfIdle();verify(session).close();
    }

    @Test void callerExpiryRetainsTheActualUninterruptedWorkerForCleanupRetry() throws Exception {
        var operations=new MongoClaimOperations(client());
        var entered=new CountDownLatch(1);var release=new CountDownLatch(1);var interrupted=new AtomicBoolean();
        try(var caller=Executors.newSingleThreadExecutor()) {
            var result=caller.submit(()->operations.call(deadline(300),()->{
                entered.countDown();
                try {if(!release.await(3,TimeUnit.SECONDS))throw new AssertionError("IO was not released");}
                catch(InterruptedException failure){interrupted.set(true);throw new AssertionError(failure);}
                return true;
            }));
            assertThat(entered.await(2,TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(()->result.get(1,TimeUnit.SECONDS)).hasCauseInstanceOf(TapstateException.class);
            assertThatThrownBy(operations::closeIfIdle).isInstanceOf(TapstateException.class);
            assertThatThrownBy(()->operations.await(deadline(30))).isInstanceOf(TapstateException.class);
            assertThat(interrupted).isFalse();
            release.countDown();operations.await(deadline(2000));operations.closeIfIdle();
            assertThat(interrupted).isFalse();
        }finally {release.countDown();}
    }

    @Test void pendingExpiryNeverStartsTheOriginalOperationAfterWorkersAreReleased() throws Exception {
        var operations=new MongoClaimOperations(client());
        var entered=new CountDownLatch(4);var release=new CountDownLatch(1);var pendingCalls=new AtomicInteger();
        try(var callers=Executors.newFixedThreadPool(4)) {
            var originals=new ArrayList<java.util.concurrent.Future<Boolean>>();
            for(int i=0;i<4;i++)originals.add(callers.submit(()->operations.call(deadline(3000),()->{
                entered.countDown();
                try {if(!release.await(3,TimeUnit.SECONDS))throw new AssertionError("IO was not released");}
                catch(InterruptedException failure){throw new AssertionError(failure);}
                return true;
            })));
            assertThat(entered.await(2,TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(()->operations.call(deadline(30),pendingCalls::incrementAndGet)).isInstanceOf(TapstateException.class);
            release.countDown();for(var original:originals)assertThat(original.get(2,TimeUnit.SECONDS)).isTrue();
            operations.await(deadline(2000));operations.closeIfIdle();
            assertThat(pendingCalls).hasValue(0);
        }finally {release.countDown();}
    }

    @Test void interruptedCallerRetainsItsFlagAndDoesNotInterruptTheOwnedIo() throws Exception {
        var operations=new MongoClaimOperations(client());
        var caller=new AtomicReference<Thread>();var entered=new CountDownLatch(1);var release=new CountDownLatch(1);
        var ownedInterrupted=new AtomicBoolean();var callerInterrupted=new AtomicBoolean();
        try(var executor=Executors.newSingleThreadExecutor()) {
            var result=executor.submit(()->{
                caller.set(Thread.currentThread());
                try {operations.call(deadline(3000),()->{
                    entered.countDown();
                    try {if(!release.await(3,TimeUnit.SECONDS))throw new AssertionError("IO was not released");}
                    catch(InterruptedException failure){ownedInterrupted.set(true);throw new AssertionError(failure);}
                    return true;
                });throw new AssertionError("the interrupted caller was accepted");}
                catch(TapstateException refused){callerInterrupted.set(Thread.currentThread().isInterrupted());}
            });
            assertThat(entered.await(2,TimeUnit.SECONDS)).isTrue();caller.get().interrupt();result.get(1,TimeUnit.SECONDS);
            assertThat(callerInterrupted).isTrue();assertThat(ownedInterrupted).isFalse();
            assertThatThrownBy(operations::closeIfIdle).isInstanceOf(TapstateException.class);
            release.countDown();operations.await(deadline(2000));operations.closeIfIdle();
            assertThat(ownedInterrupted).isFalse();
        }finally {release.countDown();}
    }

    private static MongoClient client() {
        var client = mock(MongoClient.class);
        var topology = new com.mongodb.connection.ClusterDescription(
                com.mongodb.connection.ClusterConnectionMode.MULTIPLE,
                com.mongodb.connection.ClusterType.UNKNOWN, java.util.List.of());
        when(client.getClusterDescription()).thenReturn(topology);
        return client;
    }

    private static long deadline(long millis) {return System.nanoTime()+Duration.ofMillis(millis).toNanos();}
}
