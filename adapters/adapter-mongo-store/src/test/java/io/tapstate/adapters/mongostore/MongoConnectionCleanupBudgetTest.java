package io.tapstate.adapters.mongostore;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class MongoConnectionCleanupBudgetTest {
    @Test @SuppressWarnings("unchecked")
    void timeSpentDiscoveringSessionsCannotGiveTheKillCommandANewAllowance() throws Exception {
        var client=mock(MongoClient.class);var admin=mock(MongoDatabase.class);
        AggregateIterable<Document> rows=mock(AggregateIterable.class);
        when(client.getDatabase("admin")).thenReturn(admin);
        when(admin.withTimeout(anyLong(),any())).thenReturn(admin);
        when(admin.aggregate(anyList())).thenReturn(rows);
        var entered=new CountDownLatch(1);var release=new CountDownLatch(1);
        doAnswer(call->{
            entered.countDown();
            if(!release.await(3,TimeUnit.SECONDS))throw new AssertionError("discovery was not released");
            Consumer<Document> consumer=call.getArgument(0);
            consumer.accept(new Document("lsid",new Document("id",java.util.UUID.randomUUID())));
            return null;
        }).when(rows).forEach(any());
        try(var connection=new MongoConnection(new MongoConnectionSettings("mongodb://localhost/budget",null,Duration.ofSeconds(1)));
                var executor=Executors.newSingleThreadExecutor();var timer=Executors.newSingleThreadScheduledExecutor()){
            var field=MongoConnection.class.getDeclaredField("client");field.setAccessible(true);field.set(connection,client);
            var cleanup=executor.submit(()->connection.endTransactionsLeftOpenBy("gone",Duration.ofSeconds(1)));
            try{
                assertThat(entered.await(3,TimeUnit.SECONDS)).isTrue();
                timer.schedule(release::countDown,1,TimeUnit.SECONDS);
                assertThatThrownBy(()->cleanup.get(3,TimeUnit.SECONDS)).hasCauseInstanceOf(TapstateException.class);
                verify(admin,never()).runCommand(any(org.bson.conversions.Bson.class));
            }finally{release.countDown();}
        }
    }
}
