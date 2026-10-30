package io.tapstate.adapters.pdk;

import io.tapdata.entity.utils.ClassFactory;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.assertj.core.api.Assertions.assertThat;

class PdkPluginBootstrapTest {
    @Test
    void normalOpenPassesTheSharedFactoryGateBeforePluginWork(@TempDir Path dir) throws Exception {
        Path jar = Synthetic.discoverableSource(dir);
        throughGate(() -> {
            try (var connector = PdkConnector.open("demo",
                    new ConnectorRef(List.of(jar), "synthetic.Discoverable", "2.0.8", null), Map.of())) {
                assertThat(connector.connector()).isNotNull();
            }
        });
    }

    @Test
    void offlineCapabilityProbePassesTheSameFactoryGate(@TempDir Path dir) throws Exception {
        Path jar = Synthetic.discoverableSource(dir);
        try (var loader = ConnectorClassLoader.open(List.of(jar))) {
            throughGate(() -> assertThat(CapabilityHarness.deriveCapabilities(loader, "synthetic.Discoverable"))
                    .isNotNull());
        }
    }

    @Test
    void aHostWithoutTheOptionalRuntimeDoesNotResolveTheJsonProvider() throws Exception {
        String providerName = PdkJsonProvider.class.getName();
        try (var host = new URLClassLoader(new java.net.URL[]{
                PdkJsonProvider.class.getProtectionDomain().getCodeSource().getLocation()},
                PdkJsonProvider.class.getClassLoader()) {
            @Override protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
                if (name.equals("io.tapdata.pdk.core.runtime.TapRuntime")) throw new ClassNotFoundException(name);
                if (!name.equals(providerName)) return super.loadClass(name, resolve);
                synchronized (getClassLoadingLock(name)) {
                    Class<?> loaded = findLoadedClass(name);
                    if (loaded == null) loaded = findClass(name);
                    if (resolve) resolveClass(loaded);
                    return loaded;
                }
            }
        }) {
            Class<?> provider = host.loadClass(providerName);
            var prepare = provider.getDeclaredMethod("prepareRuntime");
            prepare.setAccessible(true);
            prepare.invoke(null);
            var shared = provider.getDeclaredField("SHARED");
            shared.setAccessible(true);
            var parser = provider.getDeclaredField("parser");
            parser.setAccessible(true);
            assertThat(parser.get(shared.get(null))).isNull();
        }
    }

    private static void throughGate(Runnable pluginWork) throws Exception {
        AtomicReference<Thread> worker = new AtomicReference<>();
        CompletableFuture<Void> invocation;
        synchronized (ClassFactory.class) {
            invocation = CompletableFuture.runAsync(() -> {
                worker.set(Thread.currentThread());
                pluginWork.run();
            });
            long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
            while (!invocation.isDone() && (worker.get() == null
                    || worker.get().getState() != Thread.State.BLOCKED) && System.nanoTime() < deadline) {
                Thread.onSpinWait();
            }
            assertThat(invocation).as("plugin entry waits for the shared factory initialization gate").isNotDone();
            assertThat(worker.get().getState()).isEqualTo(Thread.State.BLOCKED);
        }
        invocation.get(10, TimeUnit.SECONDS);
    }
}
