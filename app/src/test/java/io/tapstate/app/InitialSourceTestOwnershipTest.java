package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.control.core.ConnectionTestService;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.*;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.test.util.ReflectionTestUtils;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class InitialSourceTestOwnershipTest {
    @Test void initialReadySchedulesTheSavedSourceOnceAndClosingTheOwnerInterruptsThatCall() throws Exception {
        var setup = new StorageSetupScope("cluster", "installation", "setup", 1);
        var binding = new StorageBinding(setup, StorageBinding.Mode.OP, 1, "views",
                new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, "mongodb://cache/cache", "cache", null));
        var source = new SourceResource("views", null, "mongodb", Map.of("uri", "mongodb://target/master"), null, null, null, null);
        var root = mock(StorageRoot.class); var runtime = mock(StorageRuntime.class); var bound = mock(StorageRuntime.Bound.class);
        var configuration = mock(MongoConfigurationDomain.class); var bindings = mock(StorageBindingStore.class); var artifacts = mock(ArtifactStore.class);
        var tests = mock(ConnectionTestService.class);
        when(root.runtime()).thenReturn(runtime); when(runtime.requireConfiguration()).thenReturn(configuration);
        when(configuration.bindings()).thenReturn(bindings); when(configuration.artifacts()).thenReturn(artifacts);
        when(bound.binding()).thenReturn(binding); when(bindings.load()).thenReturn(Optional.of(binding));
        when(artifacts.get("views")).thenReturn(Optional.of(source));
        var entered = new CountDownLatch(1); var interrupted = new CountDownLatch(1); var release = new CountDownLatch(1);
        doAnswer(ignored -> {
            entered.countDown();
            try { if (!release.await(5, TimeUnit.SECONDS)) throw new AssertionError("owner did not close its call"); }
            catch (InterruptedException cancelled) { interrupted.countDown(); Thread.currentThread().interrupt(); }
            return new ConnectionTestResult("views", "mongodb", ConnectionTestResult.Outcome.PASSED, List.of(), 1);
        }).when(tests).test("views", "mongodb", source.config(), "installation:installation");
        try (var parent = new GenericApplicationContext(); var child = new GenericApplicationContext()) {
            parent.refresh(); child.registerBean(ConnectionTestService.class, () -> tests); child.refresh();
            var business = new NativeBusinessContext(root, bound, parent);
            ReflectionTestUtils.setField(business, "context", child);
            try {
                business.afterInitialReady(); business.afterInitialReady();
                assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
                business.close();
                assertThat(interrupted.await(2, TimeUnit.SECONDS)).isTrue();
                verify(tests).test("views", "mongodb", source.config(), "installation:installation");
                verify(bound).requireBusiness();
                assertThat(child.isActive()).isFalse();
            } finally { release.countDown(); business.close(); }
        }
    }
}
