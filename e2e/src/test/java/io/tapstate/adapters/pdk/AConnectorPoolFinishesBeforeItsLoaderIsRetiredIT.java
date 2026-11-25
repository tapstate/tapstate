package io.tapstate.adapters.pdk;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Exercises the driver's late pool-event linkage through the original connector loader. */
class AConnectorPoolFinishesBeforeItsLoaderIsRetiredIT {
    @Test
    void poolInvalidationCanFinishBeforeItsLoaderIsRetired() throws Exception {
        String configured = System.getProperty("tapstate.pdk.it.mongodb-jar",
                System.getProperty("tapstate.e2e.managed-mongo-jar"));
        String directory = System.getProperty("tapstate.e2e.connectors-dir");
        String release = System.getProperty("tapstate.test.cloud-release-dir", System.getenv("TAPSTATE_TEST_CLOUD_RELEASE_DIR"));
        if (configured == null && directory == null && release != null) {
            configured = Path.of(release, "connectors", "mongodb-connector.jar").toString();
        }
        assumeTrue(configured != null || directory != null, "the real MongoDB connector jar is required");
        Path jar;
        if (configured != null) {
            jar = Path.of(configured);
        } else {
            try (var files = java.nio.file.Files.list(Path.of(directory))) {
                var matches = files.filter(file -> file.getFileName().toString().startsWith("mongodb-connector"))
                        .filter(file -> file.getFileName().toString().endsWith(".jar")).toList();
                assertThat(matches).hasSize(1);
                jar = matches.getFirst();
            }
        }
        var entered = new CountDownLatch(1);
        var invalidate = new CountDownLatch(1);
        var failure = new AtomicReference<Throwable>();
        Object pool;
        Thread monitor;
        boolean retired;
        try (var use = ConnectorClassLoader.acquire(List.of(jar))) {
            var loader = use.loader();
            Class<?> poolType = loader.load("com.mongodb.internal.connection.DefaultConnectionPool");
            loader.load("com.mongodb.event.ConnectionPoolClosedEvent");
            ClassLoader isolated = poolType.getClassLoader();
            Class<?> clusterType = loader.load("com.mongodb.connection.ClusterId");
            Class<?> addressType = loader.load("com.mongodb.ServerAddress");
            Class<?> serverType = loader.load("com.mongodb.connection.ServerId");
            Object server = serverType.getConstructor(clusterType, addressType).newInstance(
                    clusterType.getConstructor().newInstance(), addressType.getConstructor().newInstance());
            Class<?> settingsType = loader.load("com.mongodb.connection.ConnectionPoolSettings");
            Object builder = settingsType.getMethod("builder").invoke(null);
            Object settings = builder.getClass().getMethod("build").invoke(builder);
            Class<?> factoryType = loader.load("com.mongodb.internal.connection.InternalConnectionFactory");
            Object factory = Proxy.newProxyInstance(isolated, new Class<?>[] {factoryType},
                    (proxy, method, args) -> { throw new AssertionError("this pool never opens a connection"); });
            Class<?> providerType = loader.load("com.mongodb.internal.inject.OptionalProvider");
            Class<?> descriptionType = loader.load("com.mongodb.internal.connection.SdamServerDescriptionManager");
            Object description = Proxy.newProxyInstance(isolated, new Class<?>[] {descriptionType},
                    (proxy, method, args) -> null);
            Object provider = Proxy.newProxyInstance(isolated, new Class<?>[] {providerType},
                    (proxy, method, args) -> Optional.of(description));
            var constructor = poolType.getDeclaredConstructor(serverType, factoryType, settingsType, providerType);
            constructor.setAccessible(true);
            pool = constructor.newInstance(server, factory, settings, provider);
            var ready = poolType.getDeclaredMethod("ready");
            ready.setAccessible(true);
            ready.invoke(pool);
            var clear = poolType.getDeclaredMethod("invalidate", Throwable.class);
            clear.setAccessible(true);
            monitor = new Thread(() -> {
                entered.countDown();
                try {
                    if (!invalidate.await(5, TimeUnit.SECONDS)) throw new AssertionError("pool was not released");
                    clear.invoke(pool, new IllegalStateException("connection reset witness"));
                } catch (Throwable caught) {
                    failure.set(caught instanceof InvocationTargetException invocation ? invocation.getCause() : caught);
                }
            }, "mongo-pool-retirement-witness");
            monitor.setContextClassLoader(isolated);
            monitor.start();
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
        }
        try {
            retired = ConnectorClassLoader.retireUnused(jar);
        } finally {
            invalidate.countDown();
            monitor.join(5000);
        }
        try {
            assertThat(monitor.isAlive()).isFalse();
            assertThat(failure.get()).isNull();
            assertThat(retired).isFalse();
        } finally {
            var close = pool.getClass().getDeclaredMethod("close");
            close.setAccessible(true);
            close.invoke(pool);
        }
        assertThat(ConnectorClassLoader.retireUnused(jar)).isTrue();
    }

}
