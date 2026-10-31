package io.tapstate.adapters.pdk;

import io.tapstate.core.catalog.ConnectorCatalogEntry;
import io.tapstate.spi.store.*;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SeedSweepConcurrencyTest {
    @TempDir Path work;

    @Test
    void boundsInflightTransfersAndWaitsForEveryOutcome() throws Exception {
        Path seeds = seeds(7);
        var entered = new CountDownLatch(2);
        var release = new CountDownLatch(1);
        var active = new AtomicInteger();
        var maximum = new AtomicInteger();
        var calls = new AtomicInteger();
        var sweep = sweep((id, version, source, hash, bytes) -> {
            calls.incrementAndGet();
            int count = active.incrementAndGet(); maximum.accumulateAndGet(count, Math::max);
            entered.countDown();
            try { await(release); return registered(id, version, source, hash, bytes); }
            finally { active.decrementAndGet(); }
        });
        try (var caller = java.util.concurrent.Executors.newSingleThreadExecutor()) {
            var result = caller.submit(() -> sweep.sweep(seeds));
            try {
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(calls.get()).isEqualTo(2);
                assertThat(result.isDone()).isFalse();
            } finally { release.countDown(); }
            assertThat(result.get(10, TimeUnit.SECONDS)).hasSize(7);
        }
        assertThat(maximum.get()).isEqualTo(2);
        assertThat(active.get()).isZero();
    }

    @Test
    void callerInterruptionCancelsInflightWorkAndWaitsForCleanup() throws Exception {
        Path seeds = seeds(7);
        var entered = new CountDownLatch(2);
        var neverReleased = new CountDownLatch(1);
        var active = new AtomicInteger();
        var calls = new AtomicInteger();
        var finished = new CountDownLatch(1);
        var restored = new AtomicBoolean();
        var error = new AtomicReference<Throwable>();
        var sweep = sweep((id, version, source, hash, bytes) -> {
            active.incrementAndGet(); calls.incrementAndGet(); entered.countDown();
            try { await(neverReleased); return registered(id, version, source, hash, bytes); }
            finally { active.decrementAndGet(); }
        });
        Thread caller = new Thread(() -> {
            try { sweep.sweep(seeds); error.set(new AssertionError("Interrupted sweep returned success")); }
            catch (UncheckedIOException expected) { restored.set(Thread.currentThread().isInterrupted()); }
            catch (Throwable failure) { error.set(failure); }
            finally { finished.countDown(); }
        });
        caller.start();
        try {
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            caller.interrupt();
            assertThat(finished.await(10, TimeUnit.SECONDS)).as("cleanup completes before sweep returns").isTrue();
            assertThat(error.get()).isNull();
            assertThat(restored.get()).isTrue();
            assertThat(active.get()).isZero();
            assertThat(calls.get()).isEqualTo(2);
        } finally { neverReleased.countDown(); caller.interrupt(); caller.join(10000); }
    }

    @Test
    void anUnexpectedWorkerFailureCancelsOtherWorkWithoutHidingTheDefect() throws Exception {
        Path seeds = seeds(2);
        var entered = new CountDownLatch(2);
        var active = new AtomicInteger();
        var calls = new AtomicInteger();
        var failure = new IllegalStateException("worker invariant");
        var sweep = sweep((id, version, source, hash, bytes) -> {
            int call = calls.incrementAndGet(); active.incrementAndGet(); entered.countDown();
            try {
                await(entered);
                if (call == 1) throw failure;
                await(new CountDownLatch(1));
                return registered(id, version, source, hash, bytes);
            } finally { active.decrementAndGet(); }
        });
        assertThatThrownBy(() -> sweep.sweep(seeds)).isSameAs(failure);
        assertThat(active.get()).isZero();
    }

    private Path seeds(int count) throws IOException {
        Path seeds = Files.createDirectories(work.resolve("seeds"));
        Path mysql = Synthetic.seedableMysqlConnector(work.resolve("mysql-fixture"));
        Path mongo = Synthetic.seedableMongodbConnector(work.resolve("mongo-fixture"));
        // Independent transfers use distinct logical IDs; current versions of one ID serialize.
        for (int i = 0; i < count; i++) Files.copy(i % 2 == 0 ? mysql : mongo, seeds.resolve(i + ".jar"));
        return seeds;
    }

    private static void await(CountDownLatch latch) throws IOException {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) throw new IOException("Fixture barrier timed out");
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt(); throw new InterruptedIOException("Fixture transfer interrupted");
        }
    }

    private static RegistrationOutcome registered(String id, String version, RegistrationSource source, String hash,
            InputStream bytes) throws IOException {
        assertThat(ContentHash.of(bytes)).isEqualTo(hash);
        return new RegistrationOutcome(new ConnectorRegistration(id, hash, version, source), true);
    }

    @FunctionalInterface
    interface Upload {
        RegistrationOutcome run(String id, String version, RegistrationSource source, String hash, InputStream bytes) throws IOException;
    }

    private static SeedConnectorSweep sweep(Upload upload) {
        var registry = new ConnectorRegistry() {
            @Override public RegistrationOutcome register(String id, String version, RegistrationSource source, byte[] bytes) {
                throw new AssertionError("Transfer must stream");
            }
            @Override public RegistrationOutcome register(String id, String version, RegistrationSource source,
                    String hash, long length, InputStream bytes) throws IOException { return upload.run(id, version, source, hash, bytes); }
            @Override public List<ConnectorRegistration> list() { return List.of(); }
            @Override public Optional<byte[]> artifact(String hash) { throw new AssertionError("Download must stream"); }
            @Override public boolean hasArtifact(String hash) { return false; }
        };
        var catalog = new ConnectorCatalogStore() {
            @Override public void upsert(ConnectorCatalogEntry row) { }
            @Override public Optional<ConnectorCatalogEntry> get(String id) { return Optional.empty(); }
            @Override public List<ConnectorCatalogEntry> list() { return List.of(); }
        };
        var specs = new ConnectorSpecStore() {
            @Override public void put(String hash, byte[] bytes) { }
            @Override public Optional<byte[]> get(String hash) { return Optional.empty(); }
        };
        return new SeedConnectorSweep(new ConnectorArtifactRegistrar(registry, new ConnectorIntrospector(),
                id -> new ConnectorCapabilities(Set.of()), catalog, specs), 2);
    }
}
