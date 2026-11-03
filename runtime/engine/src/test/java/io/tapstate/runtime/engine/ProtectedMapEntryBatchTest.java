package io.tapstate.runtime.engine;

import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;
import java.lang.reflect.Proxy;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class ProtectedMapEntryBatchTest {
    @SuppressWarnings("unchecked")
    @Test void anOrdinaryOrFatalFailureOutranksARefusalAndPreventsAnyRetry() {
        for (Throwable decisive : java.util.List.of(new IllegalStateException("ordinary"), new AssertionError("fatal"))) {
            var calls = new java.util.concurrent.atomic.AtomicInteger();
            IMap<String, String> raw = (IMap<String, String>) Proxy.newProxyInstance(IMap.class.getClassLoader(),
                    new Class<?>[]{IMap.class}, (proxy, method, args) -> {
                        if (method.getName().equals("submitToKey")) {
                            calls.incrementAndGet();
                            return CompletableFuture.failedFuture(args[0].equals("first")
                                    ? new com.hazelcast.splitbrainprotection.SplitBrainProtectionException("refused") : decisive);
                        }
                        throw new AssertionError("unexpected map operation " + method.getName());
                    });
            Map<String, EntryProcessor<String, String, Void>> operations = new LinkedHashMap<>();
            operations.put("first", entry -> null); operations.put("second", entry -> null);
            assertThatThrownBy(() -> ProtectedMap.of(raw).executeOnKeys(operations)).isSameAs(decisive);
            assertThat(calls).hasValue(2);
        }
    }

    @SuppressWarnings("unchecked")
    @Test void synchronousSubmissionFailureStillWaitsForAlreadySubmittedWrites() throws Exception {
        var first = new CompletableFuture<Void>(); var failedSubmission = new CountDownLatch(1);
        var failure = new IllegalStateException("submission failed");
        IMap<String, String> raw = (IMap<String, String>) Proxy.newProxyInstance(IMap.class.getClassLoader(),
                new Class<?>[]{IMap.class}, (proxy, method, args) -> {
                    if (method.getName().equals("submitToKey")) {
                        if (args[0].equals("first")) return first;
                        failedSubmission.countDown(); throw failure;
                    }
                    throw new AssertionError("unexpected map operation " + method.getName());
                });
        Map<String, EntryProcessor<String, String, Void>> operations = new LinkedHashMap<>();
        operations.put("first", entry -> null); operations.put("second", entry -> null);
        try (var worker = Executors.newSingleThreadExecutor()) {
            var call = worker.submit(() -> ProtectedMap.of(raw).executeOnKeys(operations));
            try {
                assertThat(failedSubmission.await(5, TimeUnit.SECONDS)).isTrue();
                assertThatThrownBy(() -> call.get(100, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
                first.complete(null);
                assertThatThrownBy(() -> call.get(5, TimeUnit.SECONDS)).hasCause(failure);
            } finally { first.complete(null); }
        }
    }

    @SuppressWarnings("unchecked")
    @Test void aFailedEntryDoesNotReturnWhileAnotherSubmittedMutationIsStillRunning() throws Exception {
        var first = new CompletableFuture<Void>(); var second = new CompletableFuture<Void>();
        var submitted = new CountDownLatch(2);
        IMap<String, String> raw = (IMap<String, String>) Proxy.newProxyInstance(IMap.class.getClassLoader(),
                new Class<?>[]{IMap.class}, (proxy, method, args) -> {
                    if (method.getName().equals("submitToKey")) { submitted.countDown(); return args[0].equals("first") ? first : second; }
                    throw new AssertionError("unexpected map operation " + method.getName());
                });
        var map = ProtectedMap.of(raw);
        Map<String, EntryProcessor<String, String, Void>> operations = new LinkedHashMap<>();
        operations.put("first", entry -> null); operations.put("second", entry -> null);
        var failed = new IllegalStateException("first entry failed");
        try (var worker = Executors.newSingleThreadExecutor()) {
            var call = worker.submit(() -> map.executeOnKeys(operations));
            try {
                assertThat(submitted.await(5, TimeUnit.SECONDS)).isTrue(); first.completeExceptionally(failed);
                assertThatThrownBy(() -> call.get(100, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
                second.complete(null);
                assertThatThrownBy(() -> call.get(5, TimeUnit.SECONDS)).hasCause(failed);
            } finally { first.completeExceptionally(failed); second.complete(null); }
        }
    }
}
