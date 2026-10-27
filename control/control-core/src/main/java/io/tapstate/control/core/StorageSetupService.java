package io.tapstate.control.core;

import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.canonical.CanonicalWriter;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.spi.store.StorageBindingCommit;
import io.tapstate.spi.store.StorageBindingStore;
import io.tapstate.spi.store.StorageConnectionProbe;
import io.tapstate.spi.store.StorageSetupScope;
import io.tapstate.spi.store.StorageSetupStateStore;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;

/** One installation's tests and original binding publication, independent from business assembly. */
public final class StorageSetupService {
    public enum Layer { CONFIG, CACHE, MASTER }
    public static final String CA_BUNDLE_FIELD = "tlsCaPem";
    public static final Duration TEST_LIFETIME = Duration.ofMinutes(10);
    public record Candidate(StorageBinding.Connection configuration, StorageBinding.Connection cache,
            StorageBinding.Connection master) {
        public Candidate {
            Objects.requireNonNull(cache, "cache");
            Objects.requireNonNull(master, "master");
        }
        StorageBinding.Connection connection(Layer layer) {
            return switch (layer) { case CONFIG -> configuration; case CACHE -> cache; case MASTER -> master; };
        }
        @Override public String toString() { return "StorageCandidate[redacted]"; }
    }
    public record ConnectorIdentity(String artifactHash, String specHash) {
        public ConnectorIdentity {
            if (artifactHash == null || !artifactHash.matches("[0-9a-f]{64}") || specHash == null || !specHash.matches("[0-9a-f]{64}"))
                throw new IllegalArgumentException("a formal connector identity requires artifact and spec hashes");
        }
    }
    /** Uses the registered connector and the complete normalized TLS input, with request-owned probe resources. */
    public interface DefaultTarget {
        ConnectorIdentity identity(StorageBinding.Connection master);
        SourceResource definition(StorageBinding.Connection master, String sourceId);
        void test(StorageBinding.Connection master, SourceResource source, ConnectorIdentity original);
    }
    /** The installation owner hands CONFIG over only after the original three layer proofs pass. */
    public interface ConfigurationPreparation {
        void prepare(StorageSetupScope expected, Candidate tested, Runnable requireCurrent);
    }
    public record TestResult(Layer layer, String testStamp, Instant expiresAt, List<String> checks) {
        public TestResult { checks = List.copyOf(checks); }
    }
    public record Status(StorageBinding.Mode mode, String clusterId, String operationId, long generation,
            StorageSetupStateStore.Phase setupPhase, long bindingVersion) {}
    private record Prepared(Candidate candidate, SourceResource target, ConnectorIdentity connector, String hash) {}
    private record Tested(StorageSetupScope scope, String hash, String token, Instant expiresAt) {}

    private final StorageBinding.Mode mode;
    private final StorageBindingStore bindings;
    private final StorageSetupStateStore states;
    private final StorageConnectionProbe probe;
    private final DefaultTarget target;
    private final Supplier<Optional<StorageBinding.Connection>> configuration;
    private final AuditGate audit;
    private final Clock clock;
    private final Runnable operationActive;
    private final java.util.function.Consumer<StorageBindingCommit> continuation;
    // Bounded to the one original installation operation and three layer proofs. No credential is persisted here.
    private final EnumMap<Layer,Tested> tests = new EnumMap<>(Layer.class);
    private StorageBindingCommit pending;
    private final java.util.function.Function<Supplier<Status>,Status> commitOwner;
    private final ConfigurationPreparation configurationPreparation;
    private final StorageInitializationProgress progress;

    public StorageSetupService(StorageBinding.Mode mode, StorageBindingStore bindings, StorageSetupStateStore states,
            StorageConnectionProbe probe, DefaultTarget target, Supplier<Optional<StorageBinding.Connection>> configuration,
            AuditGate audit, Clock clock, Runnable operationActive) {
        this(mode, bindings, states, probe, target, configuration, audit, clock, operationActive, committed -> {});
    }

    public StorageSetupService(StorageBinding.Mode mode, StorageBindingStore bindings, StorageSetupStateStore states,
            StorageConnectionProbe probe, DefaultTarget target, Supplier<Optional<StorageBinding.Connection>> configuration,
            AuditGate audit, Clock clock, Runnable operationActive,
            java.util.function.Consumer<StorageBindingCommit> continuation) {
        this(mode, bindings, states, probe, target, configuration, audit, clock, operationActive, continuation, Supplier::get);
    }

    public StorageSetupService(StorageBinding.Mode mode, StorageBindingStore bindings, StorageSetupStateStore states,
            StorageConnectionProbe probe, DefaultTarget target, Supplier<Optional<StorageBinding.Connection>> configuration,
            AuditGate audit, Clock clock, Runnable operationActive,
            java.util.function.Consumer<StorageBindingCommit> continuation, java.util.function.Function<Supplier<Status>,Status> commitOwner) {
        this(mode, bindings, states, probe, target, configuration, audit, clock, operationActive, continuation, commitOwner,
                (expected, tested, active) -> {});
    }

    public StorageSetupService(StorageBinding.Mode mode, StorageBindingStore bindings, StorageSetupStateStore states,
            StorageConnectionProbe probe, DefaultTarget target, Supplier<Optional<StorageBinding.Connection>> configuration,
            AuditGate audit, Clock clock, Runnable operationActive, java.util.function.Consumer<StorageBindingCommit> continuation,
            java.util.function.Function<Supplier<Status>,Status> commitOwner, ConfigurationPreparation configurationPreparation) {
        this(mode, bindings, states, probe, target, configuration, audit, clock, operationActive, continuation,
                commitOwner, configurationPreparation, null);
    }

    public StorageSetupService(StorageBinding.Mode mode, StorageBindingStore bindings, StorageSetupStateStore states,
            StorageConnectionProbe probe, DefaultTarget target, Supplier<Optional<StorageBinding.Connection>> configuration,
            AuditGate audit, Clock clock, Runnable operationActive, java.util.function.Consumer<StorageBindingCommit> continuation,
            java.util.function.Function<Supplier<Status>,Status> commitOwner, ConfigurationPreparation configurationPreparation,
            StorageInitializationProgress progress) {
        this.mode = Objects.requireNonNull(mode, "mode"); this.bindings = Objects.requireNonNull(bindings, "bindings");
        this.states = Objects.requireNonNull(states, "states"); this.probe = Objects.requireNonNull(probe, "probe");
        this.target = Objects.requireNonNull(target, "target"); this.configuration = Objects.requireNonNull(configuration, "configuration");
        this.audit = Objects.requireNonNull(audit, "audit"); this.clock = Objects.requireNonNull(clock, "clock");
        this.operationActive = Objects.requireNonNull(operationActive, "operationActive");
        this.continuation = Objects.requireNonNull(continuation, "continuation");
        this.commitOwner = Objects.requireNonNull(commitOwner, "commitOwner");
        this.configurationPreparation = Objects.requireNonNull(configurationPreparation, "configurationPreparation");
        this.progress = progress;
    }

    /** The caller guard is established by the server's existing Cloud identity or OP management capability. */
    public synchronized TestResult test(StorageSetupScope expected, Runnable active, Candidate candidate, Layer layer) {
        requireSetup(expected, active);
        Prepared prepared = prepare(candidate);
        try {
            probe.test(prepared.candidate().connection(layer));
            if (layer == Layer.MASTER) target.test(prepared.candidate().master(), prepared.target(), prepared.connector());
        } catch (TapstateException refused) {
            throw new TapstateException(ControlError.STORAGE_TEST_FAILED,
                    Map.of("layer", layer.name(), "causeCode", refused.code().code()), null);
        }
        requireSetup(expected, active);
        if (!prepared.connector().equals(target.identity(prepared.candidate().master()))) throw testsRequired();
        if (tests.values().stream().anyMatch(test -> !test.scope().equals(expected) || !test.hash().equals(prepared.hash()))) tests.clear();
        String stamp = UUID.randomUUID().toString();
        Instant expires = clock.instant().plus(TEST_LIFETIME);
        tests.put(layer, new Tested(expected, prepared.hash(), stamp, expires));
        return new TestResult(layer, stamp, expires, layer == Layer.MASTER
                ? List.of("transactional-crud", "index-permissions", "registered-connector")
                : List.of("transactional-crud", "index-permissions"));
    }

    /** Binding is durable before runtime activation. A lost HTTP response never discards its original first-cache invocation. */
    public synchronized Status commit(String subject, StorageSetupScope expected, Runnable active,
            Candidate candidate, Map<Layer,String> stamps) {
        return commitOwner.apply(() -> {
            try { return commitOwned(subject, expected, active, candidate, stamps); }
            catch (RuntimeException | Error failure) {
                if (progress != null) progress.failed(expected, failure);
                throw failure;
            }
        });
    }

    private Status commitOwned(String subject, StorageSetupScope expected, Runnable active, Candidate candidate, Map<Layer,String> stamps) {
        active.run();
        Prepared prepared = prepare(candidate);
        if (mode == StorageBinding.Mode.OP && states.load().phase() == StorageSetupStateStore.Phase.LOCAL_SETUP) {
            requireSetup(expected, active);
            requireTests(expected, prepared, stamps);
            for (Layer layer : Layer.values()) probe.test(prepared.candidate().connection(layer));
            requireSetup(expected, active);
            configurationPreparation.prepare(expected, prepared.candidate(), active);
        }
        requireInstalledConfiguration(prepared.candidate().configuration());
        var guarded = bindings.guardedBy(active);
        Optional<StorageBinding> previous = guarded.load();
        if (progress != null) progress.begin(expected, previous.isPresent() ? 1 : 0, false);
        if (previous.isEmpty()) {
            requireSetup(expected, active);
            requireTests(expected, prepared, stamps);
            for (Layer layer : Layer.values()) {
                try {
                    probe.test(prepared.candidate().connection(layer));
                    if (progress != null) progress.checked(expected, layer);
                }
                catch (TapstateException refused) {
                    throw new TapstateException(ControlError.STORAGE_TEST_FAILED,
                            Map.of("layer", layer.name(), "causeCode", refused.code().code()), null);
                }
            }
        }
        Runnable writeGuard = previous.isPresent() ? active : () -> {
            requireSetup(expected, active);
            requireTests(expected, prepared, stamps);
            if (!prepared.connector().equals(target.identity(prepared.candidate().master()))) throw testsRequired();
        };
        audit.dispatch(ControlOperations.STORAGE_SETUP_COMMIT, new AuditContext(subject, expected.operationId()), () -> {
            StorageBindingCommit result = bindings.guardedBy(operationActive).commitWithFirstCache(expected, mode,
                    prepared.candidate().cache(), prepared.target(), writeGuard);
            if (pending == null || result.firstCreation().isPresent() || !pending.binding().equals(result.binding())
                    || !pending.preparation().equals(result.preparation())) pending = result;
            return result.binding();
        });
        if (progress != null) progress.completed(expected, StorageInitializationProgress.Stage.SAVE_SETTINGS);
        // The committed operation owns continuation even if the HTTP request or its test stamps expire.
        continuation.accept(pending);
        Status result = status(expected, operationActive);
        // Completing Cloud setup deliberately retires this request's temporary credential. Only its
        // secret-free completion receipt may return; no later request inherits that authorization.
        if (result.setupPhase() != StorageSetupStateStore.Phase.COMPLETE) active.run();
        return result;
    }

    /** Internal operation ownership only; no frontend serializes this grant or takes ownership per HTTP request. */
    public synchronized Optional<StorageBindingCommit> pending(StorageSetupScope original, Runnable active) {
        active.run();
        if (pending != null && !sameOperation(original, pending.binding().setup())) throw conflict();
        return Optional.ofNullable(pending);
    }

    /** Safe load-only status; configured but unavailable never becomes a new installation. */
    public Status status(StorageSetupScope original, Runnable active) {
        active.run();
        var state = states.guardedBy(active).load();
        if (!sameOperation(original, state.scope())) throw conflict();
        return new Status(mode, state.scope().clusterId(), state.scope().operationId(), state.scope().generation(),
                state.phase(), state.bindingVersion());
    }

    private Prepared prepare(Candidate input) {
        Objects.requireNonNull(input, "input");
        var installed = configuration.get().map(probe::normalize);
        StorageBinding.Connection config;
        if (mode == StorageBinding.Mode.CLOUD) {
            if (input.configuration() != null) throw conflict();
            config = installed.orElseThrow(StorageSetupService::unavailable);
        } else {
            if (input.configuration() == null) throw conflict();
            config = probe.normalize(input.configuration());
            if (installed.isPresent() && !sameConfigurationEndpoint(config, installed.get())) throw conflict();
        }
        Candidate normalized = new Candidate(config, probe.normalize(input.cache()), probe.normalize(input.master()));
        if (java.util.stream.Stream.of(normalized.configuration(), normalized.cache(), normalized.master())
                .map(StorageBinding.Connection::database).distinct().count() != Layer.values().length)
            throw new TapstateException(ControlError.STORAGE_DATABASES_OVERLAP, Map.of(), null);
        if (mode == StorageBinding.Mode.CLOUD && (normalized.cache().kind() != StorageBinding.Connection.Kind.ATLAS
                || normalized.master().kind() != StorageBinding.Connection.Kind.ATLAS)) throw conflict();
        ConnectorIdentity connector = target.identity(normalized.master());
        SourceResource source = (SourceResource) new DslParser().parse(new CanonicalWriter().write(
                target.definition(normalized.master(), StorageBinding.defaultSourceId(mode))));
        if (!source.id().equals(StorageBinding.defaultSourceId(mode)) || source.mode() != null || source.tables() != null
                || source.srs() != null || source.execution() != null || !normalized.master().uri().equals(source.config().get("uri"))
                || !Objects.equals(normalized.master().caPem(), source.config().get(CA_BUNDLE_FIELD))
                || !source.connector().equals(normalized.master().kind() == StorageBinding.Connection.Kind.ATLAS ? "mongodb-atlas" : "mongodb")) throw conflict();
        String hash = CanonicalHash.ofText(JsonWriter.write(List.of(mode.name(), connectionTree(normalized.configuration()),
                connectionTree(normalized.cache()), connectionTree(normalized.master()), CanonicalHash.of(source),
                connector.artifactHash(), connector.specHash())));
        return new Prepared(normalized, source, connector, hash);
    }

    private void requireInstalledConfiguration(StorageBinding.Connection expected) {
        var installed = configuration.get().map(probe::normalize).orElseThrow(StorageSetupService::unavailable);
        if (!sameConfigurationEndpoint(expected, installed)) throw conflict();
        if (states.load().phase() == StorageSetupStateStore.Phase.LOCAL_SETUP) throw unavailable();
    }
    private static TapstateException unavailable() {
        return new TapstateException(ControlError.STORAGE_UNAVAILABLE, Map.of(), null);
    }
    private void requireTests(StorageSetupScope expected, Prepared prepared, Map<Layer,String> stamps) {
        if (stamps == null || stamps.size() != Layer.values().length) throw testsRequired();
        for (Layer layer : Layer.values()) {
            Tested tested = tests.get(layer);
            if (tested == null || !tested.scope().equals(expected) || !tested.hash().equals(prepared.hash())
                    || !tested.token().equals(stamps.get(layer)) || !tested.expiresAt().isAfter(clock.instant())) throw testsRequired();
        }

    }
    private void requireSetup(StorageSetupScope expected, Runnable active) {
        operationActive.run(); active.run(); states.guardedBy(active).requireCurrent(expected, StorageSetupStateStore.Purpose.SETUP);
    }
    private static Object connectionTree(StorageBinding.Connection connection) {
        return List.of(connection.kind().name(), connection.uri(), connection.database(), connection.caPem() == null ? "" : connection.caPem());
    }
    private static boolean sameOperation(StorageSetupScope a, StorageSetupScope b) {
        return a.clusterId().equals(b.clusterId()) && a.installationId().equals(b.installationId()) && a.operationId().equals(b.operationId());
    }
    private static boolean sameConfigurationEndpoint(StorageBinding.Connection a, StorageBinding.Connection b) {
        // The owner file retains the normalized driver URI and trust input. Its local policy tag is
        // not a second persisted connection setting; both inputs already passed their own policy.
        return a.uri().equals(b.uri()) && a.database().equals(b.database()) && Objects.equals(a.caPem(), b.caPem());
    }
    private static TapstateException conflict() { return new TapstateException(ControlError.STORAGE_SCOPE_CONFLICT, Map.of(), null); }
    private static TapstateException testsRequired() { return new TapstateException(ControlError.STORAGE_TEST_REQUIRED, Map.of(), null); }
}
