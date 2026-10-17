package io.tapstate.adapters.pdk;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.sink.DdlPolicy;
import io.tapstate.spi.sink.SinkConfig;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.WriteMode;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.RequiredNoteOrigin;
import io.tapstate.spi.store.RequiredNoteOwner;
import io.tapstate.spi.store.RequiredNoteScope;
import io.tapstate.spi.store.RequiredNoteStore;
import io.tapstate.spi.store.RequiredNoteValue;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.params.provider.EnumSource;

import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Configured writers keep exact CONFIG notes and share only within the same captured run and scopes. */
class ConfiguredSinkNotesTest {
    private static final PipelineNode NODE = new PipelineNode("p1", "sink");
    private static final Map<String, Object> SETTINGS = Map.of("uri", "original-database");
    private static final SinkConfig CONFIG = new SinkConfig("demo", SETTINGS, WriteMode.UPSERT, DdlPolicy.FAIL, null, NODE);

    @Test
    void aSinkReadsAndPersistsConfigurationNotesWithoutAGenericStateCache(@TempDir Path directory) throws Exception {
        Fixture f = new Fixture(directory);
        var port = f.port(1, new AtomicBoolean(true));
        try (SinkWriter writer = port.open(CONFIG)) {
            var connector = ((PdkSinkWriter) writer).connector();
            assertThat(connector.context().getStateMap().get("identity")).isEqualTo("recorded-private");
            assertThat(connector.context().getGlobalStateMap().get("identity")).isEqualTo("recorded-global");
            connector.context().getStateMap().put("identity", "updated-private");
        }
        try (SinkWriter writer = port.open(CONFIG)) {
            assertThat(((PdkSinkWriter) writer).connector().context().getStateMap().get("identity")).isEqualTo("updated-private");
        }
        assertThat(f.reads).hasPositiveValue();
    }

    @ParameterizedTest
    @EnumSource(value = PipelineNode.Purpose.class, names = {"VIEW", "SYNC"})
    void configuredWritersKeepTheirActualSinkPurpose(PipelineNode.Purpose purpose, @TempDir Path directory) throws Exception {
        Fixture f = new Fixture(directory, purpose);
        try (SinkWriter writer = f.port(1, new AtomicBoolean(true)).open(CONFIG)) {
            var connector = ((PdkSinkWriter) writer).connector();
            assertThat(connector.stateNamespace()).isEqualTo(ConnectorStateNamespace.of(NODE, purpose));
            assertThat(connector.context().getStateMap().get("identity")).isEqualTo("recorded-private");
        }
    }

    @Test
    void preparationUsesTheSameConfiguredScopesBeforePluginInit(@TempDir Path directory) throws Exception {
        Fixture f = new Fixture(directory);
        f.port(1, new AtomicBoolean(true)).prepare(CONFIG, Map.of());
        assertThat(f.opens).hasPositiveValue();
        assertThat(Files.readAllLines(f.trace)).containsExactly("init", "stop");
    }

    @Test
    void closingTheWriterRefusesLateNotesBeforeTheStoreIsTouched(@TempDir Path directory) throws Exception {
        Fixture f = new Fixture(directory);
        SinkWriter writer = f.port(1, new AtomicBoolean(true)).open(CONFIG);
        var map = ((PdkSinkWriter) writer).connector().context().getStateMap();
        writer.close();
        int reads = f.reads.get();
        assertThatThrownBy(() -> map.get("identity")).satisfies(ConfiguredSinkNotesTest::unavailable);
        assertThatThrownBy(() -> map.put("identity", "late")).satisfies(ConfiguredSinkNotesTest::unavailable);
        assertThat(f.reads).hasValue(reads);
    }

    @Test
    void sameRunAndScopeStillShareTheCertifiedConnector(@TempDir Path directory) throws Exception {
        Fixture f = new Fixture(directory);
        var port = f.port(1, new AtomicBoolean(true));
        try (SinkWriter first = port.openPrepared(CONFIG, Map.of()); SinkWriter second = port.openPrepared(CONFIG, Map.of())) {
            assertThat(((PdkSinkWriter) first).connector()).isSameAs(((PdkSinkWriter) second).connector());
            assertThat(Files.readAllLines(f.trace)).containsExactly("init");
        }
        assertThat(Files.readAllLines(f.trace)).containsExactly("init", "stop");
    }

    @Test
    void aNewRunCannotReuseTheOldRunsHandleOrGuard(@TempDir Path directory) throws Exception {
        Fixture f = new Fixture(directory);
        AtomicBoolean oldActive = new AtomicBoolean(true);
        SinkWriter first = f.port(1, oldActive).openPrepared(CONFIG, Map.of());
        try (SinkWriter second = f.port(2, new AtomicBoolean(true)).openPrepared(CONFIG, Map.of())) {
            var oldConnector = ((PdkSinkWriter) first).connector();
            var newConnector = ((PdkSinkWriter) second).connector();
            assertThat(newConnector).isNotSameAs(oldConnector);
            oldActive.set(false);
            assertThatThrownBy(() -> oldConnector.context().getStateMap().get("identity")).satisfies(ConfiguredSinkNotesTest::unavailable);
            assertThat(newConnector.context().getStateMap().get("identity")).isEqualTo("recorded-private");
            first.close();
            second.write(List.of(Envelope.insert(1, "orders", Map.of("id", 1), null))).toCompletableFuture().get(10, TimeUnit.SECONDS);
            assertThat(Files.readAllLines(f.trace)).containsExactly("init", "init", "stop", "write");
        } finally { first.close(); }
        assertThat(Files.readAllLines(f.trace)).endsWith("write", "stop");
    }

    @Test
    void aNewScopeGenerationCannotBorrowTheOldGenerationConnector(@TempDir Path directory) throws Exception {
        Fixture f = new Fixture(directory);
        var port = f.port(1, new AtomicBoolean(true));
        try (SinkWriter first = port.openPrepared(CONFIG, Map.of())) {
            f.generation.incrementAndGet();
            try (SinkWriter second = port.openPrepared(CONFIG, Map.of())) {
                assertThat(((PdkSinkWriter) second).connector()).isNotSameAs(((PdkSinkWriter) first).connector());
                assertThatThrownBy(() -> ((PdkSinkWriter) first).connector().context().getStateMap().get("identity"))
                        .satisfies(ConfiguredSinkNotesTest::unavailable);
                assertThat(((PdkSinkWriter) second).connector().context().getStateMap().get("identity")).isEqualTo("recorded-private");
            }
        }
    }

    @Test
    void reuseCannotBypassMissingScopesOrAnOriginMismatch(@TempDir Path directory) throws Exception {
        Fixture f = new Fixture(directory);
        var port = f.port(1, new AtomicBoolean(true));
        try (SinkWriter first = port.openPrepared(CONFIG, Map.of())) {
            f.globalPresent.set(false);
            assertThatThrownBy(() -> port.openPrepared(CONFIG, Map.of())).satisfies(ConfiguredSinkNotesTest::unavailable);
            f.globalPresent.set(true);
            f.privatePresent.set(false);
            assertThatThrownBy(() -> port.openPrepared(CONFIG, Map.of())).satisfies(ConfiguredSinkNotesTest::unavailable);
            f.privatePresent.set(true);
            SinkConfig changed = new SinkConfig("demo", Map.of("uri", "another-database"), WriteMode.UPSERT, DdlPolicy.FAIL, null, NODE);
            assertThatThrownBy(() -> port.openPrepared(changed, Map.of())).isInstanceOfSatisfying(TapstateException.class,
                    error -> assertThat(error.code()).isEqualTo(IoError.NOTE_ORIGIN_MISMATCH));
            assertThat(Files.readAllLines(f.trace)).containsExactly("init");
        }
    }

    @Test
    void activationThatLosesAuthorityClosesItsOriginalConnectorBeforeRefusing(@TempDir Path directory) throws Exception {
        Fixture f = new Fixture(directory);
        AtomicBoolean active = new AtomicBoolean(true);
        String channel = "configured-sink-activation-" + java.util.UUID.randomUUID();
        System.getProperties().put(channel, active);
        try {
            Path jar = Synthetic.lifecycleSinkThatLosesAuthorityOnActivation(directory, f.trace, channel);
            var ref = new ConnectorRef(List.of(jar), "synthetic.ActivationLosingSink", "2.0.8", null, null,
                    "a".repeat(64), false);
            var port = PdkSinkPort.withRequiredNotes(id -> ref, null, null, PipelineNode.Purpose.SYNC,
                    f.scopes(active), fence(1));
            assertThatThrownBy(() -> port.open(CONFIG)).satisfies(ConfiguredSinkNotesTest::unavailable);
            assertThat(Files.readAllLines(f.trace)).as("the constructed connector is disconnected without calling init")
                    .containsExactly("stop");
        } finally { System.getProperties().remove(channel); }
    }

    @Test
    void aQueuedWriteRechecksTheDriveBeforeItCallsTheConnector(@TempDir Path directory) throws Exception {
        Fixture f = new Fixture(directory);
        AtomicBoolean active = new AtomicBoolean(true);
        try (SinkWriter writer = f.port(1, active).open(CONFIG)) {
            java.util.concurrent.CompletableFuture<io.tapstate.spi.sink.WriteResult> queued;
            synchronized (writer) {
                queued = writer.write(List.of(Envelope.insert(1, "orders", Map.of("id", 1), null))).toCompletableFuture();
                active.set(false);
            }
            assertThatThrownBy(queued::join).isInstanceOf(java.util.concurrent.CompletionException.class)
                    .satisfies(error -> unavailable(error.getCause()));
            assertThat(Files.readAllLines(f.trace)).as("the queued request must not call the target after losing authority")
                    .containsExactly("init");
        }
        assertThat(Files.readAllLines(f.trace)).as("disconnect cleanup still runs for the original connector")
                .containsExactly("init", "stop");
    }

    @Test
    void anExpiredDriveIsRefusedBeforeResolvingOrLoadingAConnector(@TempDir Path directory) throws Exception {
        Fixture f = new Fixture(directory);
        AtomicInteger resolved = new AtomicInteger();
        var port = PdkSinkPort.withRequiredNotes(id -> { resolved.incrementAndGet(); return f.ref; }, null, f.sharing,
                PipelineNode.Purpose.SYNC, f.scopes(new AtomicBoolean(false)), fence(1));
        assertThatThrownBy(() -> port.open(CONFIG)).satisfies(ConfiguredSinkNotesTest::unavailable);
        assertThat(resolved).hasValue(0);
        assertThat(Files.readAllLines(f.trace)).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {"open", "prepare", "prepared", "prepared-private"})
    void aDriveResolvesEachScopeOnlyOnce(String mode, @TempDir Path directory) throws Exception {
        Fixture f = new Fixture(directory);
        var port = PdkSinkPort.withRequiredNotes(id -> f.ref, null, mode.equals("prepared-private") ? null : f.sharing,
                PipelineNode.Purpose.SYNC, f.scopes(new AtomicBoolean(true)), fence(1));
        drive(port, mode);
        assertThat(f.namespaceOpens).containsOnly(
                Map.entry(ConnectorStateNamespace.of(NODE, PipelineNode.Purpose.SYNC), 1),
                Map.entry("pdk.global-state", 1));
    }

    @ParameterizedTest
    @ValueSource(strings = {"open", "prepare", "prepared", "prepared-private"})
    void aScopeAdvancedAfterPreflightIsRefusedInsteadOfFollowingItsNewToken(String mode, @TempDir Path directory)
            throws Exception {
        Fixture f = new Fixture(directory);
        f.advanceAfterOrigin.set(true);
        var port = PdkSinkPort.withRequiredNotes(id -> f.ref, null, mode.equals("prepared-private") ? null : f.sharing,
                PipelineNode.Purpose.SYNC, f.scopes(new AtomicBoolean(true)), fence(1));
        assertThatThrownBy(() -> drive(port, mode)).satisfies(ConfiguredSinkNotesTest::unavailable);
        assertThat(Files.readAllLines(f.trace)).as("the old open cannot init a plugin with a newer scope").isEmpty();
    }

    private static void drive(PdkSinkPort port, String mode) {
        if (mode.equals("prepare")) { port.prepare(CONFIG, Map.of()); return; }
        try (SinkWriter ignored = mode.equals("open") ? port.open(CONFIG) : port.openPrepared(CONFIG, Map.of())) { }
    }

    private static WorkloadClaimFence fence(long execution) {
        return new WorkloadClaimFence(new WorkloadClaimKey("cluster-a", WorkloadClaimType.PIPELINE_ACTUATION, "p1"),
                new WorkloadOwner("node-a", "boot-a"), 1, execution, 1);
    }

    private static void unavailable(Throwable error) {
        assertThat(error).isInstanceOfSatisfying(TapstateException.class,
                coded -> assertThat(coded.code()).isEqualTo(IoError.NOTE_SCOPE_UNAVAILABLE));
    }

    private static final class Fixture {
        final PipelineNode.Purpose purpose;
        final Path trace;
        final ConnectorRef ref;
        final SharedSinkConnectors sharing = new SharedSinkConnectors();
        final AtomicLong generation = new AtomicLong(1);
        final AtomicBoolean privatePresent = new AtomicBoolean(true);
        final AtomicBoolean globalPresent = new AtomicBoolean(true);
        final AtomicInteger reads = new AtomicInteger();
        final AtomicInteger opens = new AtomicInteger();
        final AtomicBoolean advanceAfterOrigin = new AtomicBoolean();
        final Map<String, Integer> namespaceOpens = new ConcurrentHashMap<>();
        final Map<String, RequiredNoteValue> values = new ConcurrentHashMap<>();
        final RequiredNoteStore store;

        Fixture(Path directory) throws Exception { this(directory, PipelineNode.Purpose.SYNC); }

        Fixture(Path directory, PipelineNode.Purpose purpose) throws Exception {
            this.purpose = purpose;
            trace = Files.createFile(directory.resolve("trace"));
            ref = new ConnectorRef(List.of(Synthetic.lifecycleSink(directory, trace)), "synthetic.LifecycleSink", "2.0.8",
                    null, null, "a".repeat(64), true);
            store = (RequiredNoteStore) Proxy.newProxyInstance(RequiredNoteStore.class.getClassLoader(),
                    new Class<?>[]{RequiredNoteStore.class}, (proxy, method, args) -> {
                        if (method.getName().equals("guardedBy")) return proxy;
                        RequiredNoteScope scope = (RequiredNoteScope) args[0];
                        boolean global = scope.owner().kind() == RequiredNoteOwner.Kind.GLOBAL;
                        if (!(global ? globalPresent.get() && scope.generation() == 1
                                : privatePresent.get() && scope.generation() == generation.get())) {
                            throw new TapstateException(IoError.NOTE_SCOPE_UNAVAILABLE, Map.of(), null);
                        }
                        if (method.getName().equals("origin")) {
                            var origin = ConnectorNoteOrigin.capture("demo", ref.contentHash(), SETTINGS);
                            if (advanceAfterOrigin.compareAndSet(true, false)) generation.incrementAndGet();
                            return origin;
                        }
                        String key = scope.owner().namespace() + ":" + scope.generation() + ":" + args[1];
                        if (method.getName().equals("read")) {
                            reads.incrementAndGet();
                            return values.getOrDefault(key, args[1].equals("identity")
                                    ? new RequiredNoteValue(1, ConnectorStateCodec.encode(global ? "recorded-global" : "recorded-private"))
                                    : new RequiredNoteValue(0, null));
                        }
                        if (method.getName().equals("compareAndSet")) {
                            RequiredNoteValue old = values.getOrDefault(key, args[1].equals("identity")
                                    ? new RequiredNoteValue(1, ConnectorStateCodec.encode("recorded-private")) : new RequiredNoteValue(0, null));
                            if (old.revision() != (long) args[2]) return false;
                            values.put(key, new RequiredNoteValue(old.revision() + 1, (byte[]) args[3]));
                            return true;
                        }
                        throw new AssertionError("unexpected configured sink note operation: " + method.getName());
                    });
        }

        ConnectorNoteScopes scopes(AtomicBoolean active) {
            return ConnectorNoteScopes.of(store, namespace -> {
                opens.incrementAndGet();
                namespaceOpens.merge(namespace, 1, Integer::sum);
                if (namespace.equals("pdk.global-state") && globalPresent.get()) {
                    return new RequiredNoteScope(new RequiredNoteOwner(1, namespace, RequiredNoteOwner.Kind.GLOBAL, "cluster-a", 0), 1);
                }
                if (namespace.equals(ConnectorStateNamespace.of(NODE, purpose)) && privatePresent.get()) {
                    return new RequiredNoteScope(RequiredNoteOwner.forNode(1, NODE, purpose, 1), generation.get());
                }
                throw new TapstateException(IoError.NOTE_SCOPE_UNAVAILABLE, Map.of(), null);
            }, () -> {
                if (!active.get()) throw new TapstateException(IoError.NOTE_SCOPE_UNAVAILABLE, Map.of(), null);
            });
        }

        PdkSinkPort port(long execution, AtomicBoolean active) {
            return PdkSinkPort.withRequiredNotes(id -> ref, null, sharing, purpose, scopes(active), fence(execution));
        }
    }
}
