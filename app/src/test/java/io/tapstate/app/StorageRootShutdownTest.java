package io.tapstate.app;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.spi.store.StorageBinding;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.slf4j.LoggerFactory;
import org.springframework.test.util.ReflectionTestUtils;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class StorageRootShutdownTest {
    private final Logger logger = (Logger) LoggerFactory.getLogger(ShutdownAwait.class);
    private final ListAppender<ILoggingEvent> shutdownLog = new ListAppender<>();

    @BeforeEach void captureLog() {
        shutdownLog.start();
        logger.addAppender(shutdownLog);
    }

    @AfterEach void releaseLog() {
        logger.detachAppender(shutdownLog);
        shutdownLog.stop();
    }

    @Test void blockedAuthenticationWorkerStillAllowsMemberStopWithoutClosingItsResources() throws Exception {
        var root = root();
        var runtime = mock(StorageRuntime.class);
        var authentication = mock(StorageAuthentication.class);
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var worker = Executors.newSingleThreadExecutor();
        ReflectionTestUtils.setField(root, "runtime", runtime);
        ReflectionTestUtils.setField(root, "authentication", authentication);
        ReflectionTestUtils.setField(root, "authenticationWorker", worker);
        worker.submit(() -> { entered.countDown(); release.await(); return null; });
        try {
            assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(catchThrowable(() -> ShutdownAwait.within(Duration.ofMillis(200), root::close)))
                    .isInstanceOf(io.tapstate.core.common.TapstateException.class);
            verify(runtime).stopPublishedMemberAfterFailedClose();
            verify(runtime, never()).close(anyLong());
            verify(authentication, never()).close();
            assertThat(shutdownLog.list).anySatisfy(event -> {
                assertThat(event.getFormattedMessage()).contains("storage coordinator drain");
                assertThat(event.getThrowableProxy().getMessage()).contains("TS0210030");
            });
        } finally {
            release.countDown();
            worker.shutdownNow();
        }
        assertThat(worker.awaitTermination(2, TimeUnit.SECONDS)).isTrue();
        root.close();
        verify(authentication).close();
        verify(runtime).close(anyLong());
    }

    @Test void anAlreadyExpiredDeadlineStillAttemptsMemberStop() {
        var root = root();
        var runtime = mock(StorageRuntime.class);
        var authentication = mock(StorageAuthentication.class);
        ReflectionTestUtils.setField(root, "runtime", runtime);
        ReflectionTestUtils.setField(root, "authentication", authentication);

        assertThat(catchThrowable(() -> ShutdownAwait.within(Duration.ZERO, root::close)))
                .isInstanceOf(io.tapstate.core.common.TapstateException.class);
        verify(runtime).stopPublishedMemberAfterFailedClose();
        verify(runtime, never()).close(anyLong());
        verify(authentication, never()).close();

        root.close();
    }

    @Test void authenticationCloseFailureStillClosesTheRuntime() {
        var root = root();
        var runtime = mock(StorageRuntime.class);
        var authentication = mock(StorageAuthentication.class);
        ReflectionTestUtils.setField(root, "runtime", runtime);
        ReflectionTestUtils.setField(root, "authentication", authentication);
        var failure = new IllegalStateException("authentication close failed");
        doThrow(failure).when(authentication).close();

        assertThat(catchThrowable(root::close)).isSameAs(failure);
        verify(runtime).close(anyLong());
        assertThat(shutdownLog.list).anySatisfy(event -> {
            assertThat(event.getFormattedMessage()).contains("authentication close");
            assertThat(event.getThrowableProxy().getMessage()).contains("authentication close failed");
        });
    }

    @Test void unpublishedCloseFailureStillClosesPublishedOwnersAndCanBeRetried() {
        var root = root();
        var runtime = mock(StorageRuntime.class);
        var authentication = mock(StorageAuthentication.class);
        var candidate = mock(StorageRuntime.class);
        var candidateAuthentication = mock(StorageAuthentication.class);
        ReflectionTestUtils.setField(root, "runtime", runtime);
        ReflectionTestUtils.setField(root, "authentication", authentication);
        ReflectionTestUtils.setField(root, "runtimes", (Function<MongoConfigurationDomain, StorageRuntime>) ignored -> candidate);
        ReflectionTestUtils.setField(root, "authentications", (Function<MongoConfigurationDomain, StorageAuthentication>) ignored -> candidateAuthentication);
        var publicationFailure = new IllegalStateException("before replacement publication");
        var candidateFailure = new IllegalStateException("unpublished runtime close failed");
        doThrow(publicationFailure).when(runtime).close();
        doThrow(candidateFailure).when(candidate).close();
        var endpoint = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB,
                "mongodb://fixture/config", "config", null);

        assertThat(catchThrowable(() -> root.installOnPremConfiguration(mock(MongoConfigurationDomain.class), endpoint)))
                .isSameAs(publicationFailure).hasSuppressedException(candidateFailure);
        clearInvocations(authentication, runtime);

        assertThat(catchThrowable(root::close)).hasSuppressedException(candidateFailure);
        verify(authentication).close();
        verify(runtime).close(anyLong());
        assertThat(shutdownLog.list).anySatisfy(event -> {
            assertThat(event.getFormattedMessage()).contains("unpublished owner close");
            assertThat(event.getThrowableProxy().getSuppressed())
                    .anySatisfy(failure -> assertThat(failure.getMessage()).isEqualTo(candidateFailure.getMessage()));
        });

        doNothing().when(candidate).close();
        root.close();
        verify(candidate, times(3)).close();
        verify(candidateAuthentication).close();
        root.close();
        verifyNoMoreInteractions(candidate, candidateAuthentication);
    }

    @Test void closingStopsRecoveryWorkerPublishedDuringCoordinatorDrain() throws Exception {
        var root = root();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (var recovery = Executors.newSingleThreadExecutor();
                var coordinator = Executors.newSingleThreadExecutor()) {
            var admitted = coordinator.submit(() -> root.coordinateInstallation(() -> {
                entered.countDown();
                try {
                    assertThat(release.await(5, TimeUnit.SECONDS)).isTrue();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(interrupted);
                }
                ReflectionTestUtils.setField(root, "recoveryWorker", recovery);
                return null;
            }));
            try {
                assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
                var closing = new java.util.concurrent.FutureTask<Void>(() -> {
                    ShutdownAwait.within(Duration.ofSeconds(3), root::close);
                    return null;
                });
                var caller = Thread.ofPlatform().daemon().start(closing);
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(1))
                        .until(() -> caller.getState() == Thread.State.TIMED_WAITING);
                release.countDown();
                admitted.get(2, TimeUnit.SECONDS);
                closing.get(4, TimeUnit.SECONDS);
                assertThat(recovery.isTerminated()).isTrue();
            } finally {
                release.countDown();
            }
        } finally {
            root.close();
        }
    }

    private static StorageRoot root() {
        var mongo = new MongoProperties();
        mongo.setUri(null);
        return new StorageRoot(mongo, CloudRuntimeSettings.resolve(new CloudProperties()), null, BootId.fresh(),
                new MetricsHistoryProperties(), new RandomTokenSecrets(), Clock.systemUTC(), new byte[32]);
    }

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
