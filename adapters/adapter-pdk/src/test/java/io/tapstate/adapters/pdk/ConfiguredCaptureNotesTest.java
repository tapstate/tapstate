package io.tapstate.adapters.pdk;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.capture.CaptureBatch;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.store.RequiredNoteOwner;
import io.tapstate.spi.store.RequiredNoteScope;
import io.tapstate.spi.store.RequiredNoteStore;
import io.tapstate.spi.store.RequiredNoteValue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/** Configured source and release paths consume CONFIG notes without a generic operator-state store. */
class ConfiguredCaptureNotesTest {
    private static final PipelineNode NODE = new PipelineNode("p1", "src");

    @Test
    void aSnapshotReadsItsRecordedIdentityWithoutAStoreCache(@TempDir Path dir) {
        try (Fixture fixture = fixture(dir, false)) {
            var port = PdkCapturePort.withRequiredNotes(id -> fixture.ref, null, fixture.scopes);
            List<Envelope> rows = new ArrayList<>();
            try (CaptureBatch batch = port.snapshot(config())) {
                while (batch.hasNext()) rows.add(batch.next());
            }
            assertThat(rows).hasSize(1);
            assertThat(rows.getFirst().after()).containsEntry("slot", "recorded-slot");
            assertThat(fixture.opened).containsExactly("pdk.state.p1.capture.src", "pdk.global-state");
        }
    }

    @Test
    void aReleaseStillRunsWithConfigurationNotesWhenThereIsNoGenericCacheStore(@TempDir Path dir) {
        try (Fixture fixture = fixture(dir, false)) {
            var port = PdkCapturePort.withRequiredNotes(id -> fixture.ref, null, fixture.scopes);
            assertThat(port.release(config())).isEmpty();
            assertThat(fixture.released).containsExactly("recorded-slot");
            assertThat(fixture.opened).containsExactly("pdk.state.p1.capture.src", "pdk.global-state");
        }
    }

    @Test
    void releaseUsesTheRetainedConnectorArtifactAndSettingsAfterTheCurrentSourceChanges(@TempDir Path dir) {
        try (Fixture fixture = fixture(dir, false, Map.of("uri", "database-x-sentinel", "tlsCA", "original-ca"))) {
            List<String> resolved = new ArrayList<>();
            ConnectorProvisioner provisioner = new ConnectorProvisioner() {
                @Override public ConnectorRef resolve(String id) { throw new AssertionError("current registration was used"); }
                @Override public ConnectorRef resolve(String id, String hash) {
                    resolved.add(id + ":" + hash);
                    return fixture.ref;
                }
            };
            var port = PdkCapturePort.withRequiredNotes(provisioner, null, fixture.scopes);
            var changed = new CaptureConfig("another-connector", Map.of("uri", "database-y-sentinel"), List.of("t1")).at(NODE);
            assertThat(port.release(changed)).isEmpty();
            assertThat(resolved).containsExactly("postgres:" + "a".repeat(64));
            assertThat(fixture.released).containsExactly("recorded-slot");
            assertThat(fixture.channel.get("releaseSettings")).isEqualTo(Map.of("uri", "database-x-sentinel", "tlsCA", "original-ca"));
        }
    }

    @Test
    void aReleaseKeepsTheExactScopeCapturedForItsOriginInsteadOfResolvingItAgain(@TempDir Path dir) {
        try (Fixture fixture = fixture(dir, false)) {
            AtomicInteger privateOpens = new AtomicInteger();
            var fixed = ConnectorNoteScopes.of(fixture.scopes.store(), namespace -> {
                if (namespace.equals("pdk.state.p1.capture.src") && privateOpens.incrementAndGet() > 1) {
                    throw new TapstateException(io.tapstate.spi.store.IoError.NOTE_SCOPE_UNAVAILABLE, Map.of(), null);
                }
                return fixture.scopes.open(namespace);
            }, () -> {});
            var port = PdkCapturePort.withRequiredNotes(id -> fixture.ref, null, fixed);
            assertThat(port.release(config())).isEmpty();
            assertThat(privateOpens.get()).isEqualTo(1);
            assertThat(fixture.released).containsExactly("recorded-slot");
        }
    }

    @Test
    void aTimedOutReleaseCannotReadOrWriteNotesAfterItsInterrupt(@TempDir Path dir) throws Exception {
        try (Fixture fixture = fixture(dir, true)) {
            var port = new PdkCapturePort(id -> fixture.ref, null, PdkCapturePort.DEFAULT_PREFLIGHT_TIMEOUT,
                    Duration.ofSeconds(5), System::nanoTime, Duration.ofMillis(300), fixture.scopes);
            assertThat(port.release(config())).hasValueSatisfying(error ->
                    assertThat(error.code()).isEqualTo(ConnectorError.RELEASE_FAILED));
            assertThat(fixture.lateAttempt.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(fixture.channel).containsEntry("lateWrite", "refused");
            assertThat(fixture.lateReads.get()).isZero();
        }
    }

    @Test
    void anInterruptedCallerDisablesReleaseNotesBeforeInterruptingTheConnector(@TempDir Path dir) throws Exception {
        try (Fixture fixture = fixture(dir, true)) {
            var port = PdkCapturePort.withRequiredNotes(id -> fixture.ref, null, fixture.scopes);
            AtomicReference<Optional<TapstateException>> result = new AtomicReference<>();
            AtomicBoolean interrupted = new AtomicBoolean();
            Thread caller = new Thread(() -> {
                result.set(port.release(config()));
                interrupted.set(Thread.currentThread().isInterrupted());
            }, "configured-notes-release-caller");
            caller.start();
            try {
                assertThat(fixture.releaseEntered.await(10, TimeUnit.SECONDS)).isTrue();
                caller.interrupt();
                caller.join(10_000);
                assertThat(caller.isAlive()).isFalse();
                assertThat(result.get()).hasValueSatisfying(error ->
                        assertThat(error.code()).isEqualTo(ConnectorError.RELEASE_FAILED));
                assertThat(interrupted.get()).isTrue();
                assertThat(fixture.lateAttempt.await(10, TimeUnit.SECONDS)).isTrue();
                assertThat(fixture.channel).containsEntry("lateWrite", "refused");
                assertThat(fixture.lateReads.get()).isZero();
            } finally {
                caller.interrupt();
                caller.join(10_000);
            }
        }
    }

    @Test
    void unscopedConnectionAndDiscoveryNeverResolveConfigurationNotes(@TempDir Path dir) {
        ConnectorRef ref = new ConnectorRef(List.of(Synthetic.emittingSource(dir)),
                "synthetic.EmittingSource", "2.0.8", null);
        ConnectorNoteScopes forbidden = new ConnectorNoteScopes() {
            @Override public RequiredNoteStore store() { throw new AssertionError("unscoped store"); }
            @Override public RequiredNoteScope open(String namespace) { throw new AssertionError("unscoped open"); }
            @Override public void requireActive() { throw new AssertionError("unscoped guard"); }
        };
        var port = PdkCapturePort.withRequiredNotes(id -> ref, null, forbidden);
        assertThat(port.testConnection(config()).sample()).isNotEmpty();
        assertThat(port.discoverSchema(config()).tables()).extracting("name").contains("t1");
    }

    private static CaptureConfig config() {
        return new CaptureConfig("postgres", Map.of(), List.of("t1")).at(NODE);
    }

    private static Fixture fixture(Path dir, boolean hang) {
        return fixture(dir, hang, Map.of());
    }

    private static Fixture fixture(Path dir, boolean hang, Map<String, Object> originalSettings) {
        String channelKey = "tapstate.test.configured-notes." + dir.getFileName();
        CountDownLatch lateAttempt = new CountDownLatch(1);
        Map<String, Object> channel = new ConcurrentHashMap<>() {
            @Override public Object put(String key, Object value) {
                Object previous = super.put(key, value);
                if (key.equals("lateWrite")) lateAttempt.countDown();
                return previous;
            }
        };
        CountDownLatch releaseEntered = new CountDownLatch(1);
        List<Object> released = new CopyOnWriteArrayList<>() {
            @Override public boolean add(Object value) {
                boolean added = super.add(value);
                releaseEntered.countDown();
                return added;
            }
        };
        channel.put("released", released);
        if (hang) {
            channel.put("hang", true);
            channel.put("writeAfterInterrupt", true);
        }
        System.getProperties().put(channelKey, channel);
        try {
            ConnectorRef ref = new ConnectorRef(List.of(Synthetic.slotKeepingSource(dir, channelKey)),
                    "synthetic.SlotKeepingSource", "2.0.8", null, null, "a".repeat(64), false);
            List<String> opened = new CopyOnWriteArrayList<>();
            AtomicInteger lateReads = new AtomicInteger();
            RequiredNoteStore store = (RequiredNoteStore) Proxy.newProxyInstance(RequiredNoteStore.class.getClassLoader(),
                    new Class<?>[]{RequiredNoteStore.class}, (proxy, method, args) -> {
                        if (method.getName().equals("guardedBy")) return proxy;
                        if (method.getName().equals("origin")) return ConnectorNoteOrigin.capture("postgres", ref.contentHash(), originalSettings);
                        if (method.getName().equals("read")) {
                            if (args[1].equals("late")) lateReads.incrementAndGet();
                            return args[1].equals("tapdata_pg_slot")
                                    ? new RequiredNoteValue(1, ConnectorStateCodec.encode("recorded-slot"))
                                    : new RequiredNoteValue(0, null);
                        }
                        throw new AssertionError("unexpected configured note operation: " + method.getName());
                    });
            ConnectorNoteScopes scopes = ConnectorNoteScopes.of(store, namespace -> {
                opened.add(namespace);
                RequiredNoteOwner owner = namespace.equals("pdk.global-state")
                        ? new RequiredNoteOwner(1, namespace, RequiredNoteOwner.Kind.GLOBAL, "cluster-a", 0)
                        : new RequiredNoteOwner(1, namespace, RequiredNoteOwner.Kind.PIPELINE, "p1", 1);
                return new RequiredNoteScope(owner, 1);
            }, () -> {});
            return new Fixture(channelKey, ref, scopes, opened, released, channel, lateAttempt, lateReads, releaseEntered);
        } catch (RuntimeException | Error failure) {
            System.getProperties().remove(channelKey);
            throw failure;
        }
    }

    private record Fixture(String channelKey, ConnectorRef ref, ConnectorNoteScopes scopes, List<String> opened,
            List<Object> released, Map<String, Object> channel, CountDownLatch lateAttempt, AtomicInteger lateReads, CountDownLatch releaseEntered)
            implements AutoCloseable {
        @Override public void close() { System.getProperties().remove(channelKey); }
    }
}
