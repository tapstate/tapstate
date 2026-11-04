package io.tapstate.app;

import java.time.Clock;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.spi.store.StorageBinding;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class StorageRootShutdownTest {
    @Test void closingCannotPublishAnInstallerSuccessorWhichWasStillBeingConstructed() throws Exception {
        var mongo=new MongoProperties();mongo.setUri(null);
        var root=new StorageRoot(mongo,CloudRuntimeSettings.resolve(new CloudProperties()),null,BootId.fresh(),
                new MetricsHistoryProperties(),new RandomTokenSecrets(),Clock.systemUTC(),new byte[32]);
        var entered=new CountDownLatch(1);var release=new CountDownLatch(1);
        var next=mock(StorageRuntime.class);var authentication=mock(StorageAuthentication.class);
        Function<MongoConfigurationDomain,StorageRuntime> runtimes=configuration->{
            entered.countDown();try{if(!release.await(3,TimeUnit.SECONDS))throw new AssertionError("replacement was not released");}
            catch(InterruptedException failure){throw new AssertionError(failure);}return next;
        };
        ReflectionTestUtils.setField(root,"runtimes",runtimes);
        ReflectionTestUtils.setField(root,"authentications",(Function<MongoConfigurationDomain,StorageAuthentication>)configuration->authentication);
        var configuration=mock(MongoConfigurationDomain.class);
        var connection=new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB,"mongodb://fixture/config","config",null);
        try(var executor=Executors.newFixedThreadPool(2)){
            var install=executor.submit(()->root.installOnPremConfiguration(configuration,connection));
            assertThat(entered.await(3,TimeUnit.SECONDS)).isTrue();
            var close=executor.submit(root::close);
            org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(1))
                    .until(()->Boolean.TRUE.equals(ReflectionTestUtils.getField(root,"closeRequested")));
            assertThat(close.isDone()).isFalse();release.countDown();
            assertThatThrownBy(()->install.get(3,TimeUnit.SECONDS)).hasCauseInstanceOf(io.tapstate.core.common.TapstateException.class);
            close.get(3,TimeUnit.SECONDS);verify(next).close();verify(authentication).close();
            assertThat(ReflectionTestUtils.getField(root,"runtime")).isNotSameAs(next);
        }finally{release.countDown();root.close();}
    }
}
