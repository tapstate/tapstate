package io.tapstate.app;

import io.tapstate.core.alerting.*;
import java.net.InetAddress;
import java.time.Clock;
import java.util.List;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.*;

class AlertDnsCapacityTest {
    @Test void completedLookupsDoNotRejectTheNextResolutionBeforeTheirWorkersReturnIdle() throws Exception {
        var capability=new AlertChannelPolicy.Capability("email",1,AlertChannelPolicy.Family.OP_EXTERNAL);
        var target=mock(AlertIntent.ReceiverTarget.class);
        when(target.transport()).thenReturn(new AlertReceiver.Email("localhost",465,AlertReceiver.Encryption.IMPLICIT_TLS,
                AlertReceiver.Authentication.NONE,null,"alerts@example.test",List.of("one@example.test"),AlertReceiver.Trust.SYSTEM));
        var frozen=mock(AlertIntent.Frozen.class);
        when(frozen.channel()).thenReturn(capability);when(frozen.target()).thenReturn(target);
        when(frozen.retention()).thenReturn(AlertIntent.Policy.capture(AlertRetentionPolicy.defaults(AlertChannelPolicy.Deployment.ON_PREM)));
        var entered=new CountDownLatch(2);var release=new CountDownLatch(1);var caller=Executors.newFixedThreadPool(4);
        try(var channels=new AlertChannels(AlertChannelPolicy.Deployment.ON_PREM,CloudRuntimeSettings.resolve(new CloudProperties()),
                Clock.systemUTC(),()->{},null,host->new InetAddress[]{InetAddress.getLoopbackAddress()})) {
            var pool=(ThreadPoolExecutor)org.springframework.test.util.ReflectionTestUtils.getField(channels,"dns");
            var completed=new java.util.ArrayList<FutureTask<Void>>();
            for(int i=0;i<2;i++) {
                var lookup=new FutureTask<Void>(()->null) {
                    @Override protected void done(){entered.countDown();try{release.await(3,TimeUnit.SECONDS);}catch(InterruptedException stopped){Thread.currentThread().interrupt();}}
                };
                completed.add(lookup);pool.execute(lookup);
            }
            assertThat(entered.await(1,TimeUnit.SECONDS)).isTrue();assertThat(completed).allMatch(Future::isDone);
            var next=new java.util.ArrayList<Future<Object>>();var ready=new CountDownLatch(4);var begin=new CountDownLatch(1);
            for(int i=0;i<4;i++)next.add(caller.submit(()->{ready.countDown();begin.await();return channels.prepare(frozen);}));
            assertThat(ready.await(1,TimeUnit.SECONDS)).isTrue();begin.countDown();
            await().atMost(java.time.Duration.ofSeconds(1)).until(()->next.stream().filter(Future::isDone).count()+pool.getQueue().size()==4);
            release.countDown();
            for(var reply:next)assertThat(reply.get(1,TimeUnit.SECONDS)).isInstanceOf(InetAddress[].class);
            assertThat(pool.getMaximumPoolSize()).isEqualTo(2);
        }finally{release.countDown();caller.shutdownNow();assertThat(caller.awaitTermination(2,TimeUnit.SECONDS)).isTrue();}
    }
}
