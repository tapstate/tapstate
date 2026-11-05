package io.tapstate.app;

import com.mongodb.client.MongoClients;
import io.tapstate.core.alerting.BuiltinAlarmTypes;
import io.tapstate.spi.store.AlertStore;
import io.tapstate.testsupport.RequiresDocker;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.bson.Document;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.mock;

@RequiresDocker
class AlertSiblingQuiescenceIT {
    @Container static final MongoDBContainer MONGO=AlertEvaluationHostIT.MONGO;

    @ParameterizedTest @ValueSource(strings={"planning","delivery","maintenance"})
    void siblingTransactionCompletesBeforeItsConfigurationClientCloses(String kind) {
        AlertEvaluationHostIT.withFixture(f->{
            try(var observer=MongoClients.create(f.settings().uri())){
                var admin=observer.getDatabase("admin");
                var entered=new CountDownLatch(1);var release=new CountDownLatch(1);
                var interrupted=new AtomicBoolean();var transaction=new AtomicReference<Document>();
                Runnable active=()->{
                    if(!Thread.currentThread().getName().startsWith("alert-") || transaction.get()!=null)return;
                    var open=transactions(admin);
                    if(open.isEmpty() || !transaction.compareAndSet(null,open.getFirst()))return;
                    entered.countDown();
                    try {if(!release.await(5,TimeUnit.SECONDS))throw new AssertionError("transaction was not released");}
                    catch(InterruptedException failure){interrupted.set(true);Thread.currentThread().interrupt();}
                };
                var original=f.storeHost();
                var host=new AlertStore.Host(original.scope(),original.setup(),original.deployment(),active);
                var store=f.configuration().alerts(host,BuiltinAlarmTypes.catalog(),f.clock());
                Runnable start,stop;java.util.function.LongConsumer drain;
                if(kind.equals("planning")){
                    var worker=new AlertPlanningRuntime(new AlertNotificationPlanner(store,f.clock(),"planner",
                            java.net.URI.create("https://example.invalid"),active));
                    start=worker::start;stop=worker::close;drain=worker::awaitStopped;
                }else if(kind.equals("delivery")){
                    var worker=new AlertDeliveryRuntime(store,f.clock(),"delivery",active,
                            mock(AlertDeliveryRuntime.Channels.class),ignored->null);
                    start=worker::start;stop=worker::close;drain=worker::awaitStopped;
                }else{
                    var worker=new AlertMaintenanceRuntime(store.maintenance().orElseThrow(),f.clock(),active);
                    start=worker::start;stop=worker::close;drain=worker::awaitStopped;
                }
                try {
                    start.run();assertThat(entered.await(5,TimeUnit.SECONDS)).as("active sibling CONFIG transaction").isTrue();
                    stop.run();assertThat(interrupted).isFalse();
                    assertThatThrownBy(()->drain.accept(System.nanoTime()+Duration.ofMillis(40).toNanos()))
                            .isInstanceOf(io.tapstate.core.common.TapstateException.class);
                    // A failed drain has not revoked the original client; it still owns cleanup.
                    assertThat(f.configuration().schemaVersion()).isPositive();
                    release.countDown();drain.accept(System.nanoTime()+Duration.ofSeconds(5).toNanos());
                    f.configuration().close();
                    var session=transaction.get().get("lsid");
                    assertThat(transactions(admin).stream().filter(op->session.equals(op.get("lsid"))).toList()).isEmpty();
                    assertThat(interrupted).isFalse();
                }finally{release.countDown();stop.run();}
            }catch(Exception failure){throw new AssertionError(failure);}
        });
    }

    private static List<Document> transactions(com.mongodb.client.MongoDatabase admin){
        return admin.aggregate(List.of(new Document("$currentOp",new Document("allUsers",true).append("idleSessions",true)),
                new Document("$match",new Document("transaction",new Document("$exists",true))
                        .append("locks.Database",new Document("$exists",true))))).into(new java.util.ArrayList<>());
    }
}
