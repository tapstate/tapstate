package io.tapstate.app;

import io.tapstate.adapters.mongostore.*;
import io.tapstate.control.core.*;
import io.tapstate.control.restapi.InstallationHttpAccess;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.*;
import jakarta.servlet.http.HttpServletRequest;
import java.time.Clock;
import java.util.*;
import org.apache.catalina.connector.Connector;

/** One installation owner, using the same storage commands and the native first-administrator transaction. */
final class OnPremInstallation implements InstallationHttpAccess, AutoCloseable {
    static final String DIRECTORY_PROPERTY = "tapstate.installation.directory";
    static final String CAPABILITY_HEADER = "X-Tapstate-Setup-Capability";
    static final String CONNECTOR_ATTRIBUTE = OnPremInstallation.class.getName() + ".connector";
    private static final Set<String> STORAGE_PATHS = Set.of("/api/storage/status", "/api/storage/test", "/api/storage/setup/commit", "/api/storage/setup/status",
            "/api/storage/recovery/plan", "/api/storage/recovery/confirm", "/api/storage/recovery/status");
    private final Object connectorIdentity = new Object();
    private final StorageRoot root;
    private final InstallationFiles files;
    private final MongoProperties mongo;
    private final StorageSetupService.DefaultTarget target;
    private final InstallationSetupStates states;
    private final SetupCapabilityService capabilities;
    private final StorageSetupService setups;
    private final Clock clock;
    private SetupCapabilityService.Issued restored;
    private Connector connector;
    private volatile boolean closed;
    private final boolean completedBeforeBoot;
    private MongoConfigurationDomain.InitialOpRecovery recoveryInvocation;
    private Runnable restartRequired = () -> {};
    private Runnable startupCompleted = () -> {};
    void onStartupCompleted(Runnable action) { startupCompleted = Objects.requireNonNull(action); }
    void onRestartRequired(Runnable action) { restartRequired = Objects.requireNonNull(action); }
    @Override public Map<String,Object> initializationProgress() {
        var stage = files.stage();
        return root.initializationProgress().snapshot().filter(value -> value.operationId().equals(stage.scope().operationId()))
                .map(StorageControlDispatcher::progress).orElse(Map.of());
    }

    OnPremInstallation(StorageRoot root, InstallationFiles files, MongoProperties mongo, StorageSetupService.DefaultTarget target, Clock clock) {
        this.root = root; this.files = files; this.mongo = mongo; this.target = target; this.clock = clock;
        if (root.mode() != StorageBinding.Mode.OP) throw InstallationFiles.refused();
        states = new InstallationSetupStates(files, () -> root.runtime().configuration().map(MongoConfigurationDomain::setupStates));
        var retained = root.runtime().configuration().isPresent() ? states.load() : null;
        this.completedBeforeBoot = files.stage().handoff() == InstallationFiles.Handoff.RESTART_REQUIRED
                || files.stage().handoff() == InstallationFiles.Handoff.RESTARTED
                || retained != null && retained.phase() == StorageSetupStateStore.Phase.COMPLETE;
        capabilities = new SetupCapabilityService(states, clock, files::requireOwner,
                () -> requireManagementRequest(currentRequest()), this::requireFirstAdministratorReady);
        StorageBindingStore bindings = new StorageBindingStore() {
            private StorageBindingStore current() { return root.runtime().requireConfiguration().bindings(); }
            @Override public StorageBindingStore guardedBy(Runnable active) { return current().guardedBy(active); }
            @Override public Optional<StorageBinding> load() { return current().load(); }
            @Override public Optional<CachePreparation> cachePreparation() { return current().cachePreparation(); }
            @Override public StorageBinding commit(StorageSetupScope scope, StorageBinding.Mode mode, StorageBinding.Connection cache, SourceResource source) {
                return current().commit(scope, mode, cache, source);
            }
            @Override public StorageBindingCommit commitWithFirstCache(StorageSetupScope scope, StorageBinding.Mode mode,
                    StorageBinding.Connection cache, SourceResource source, Runnable active) { return current().commitWithFirstCache(scope, mode, cache, source, active); }
            @Override public StorageBinding complete(StorageSetupScope scope) { return current().complete(scope); }
        };
        setups = new StorageSetupService(StorageBinding.Mode.OP, bindings, states,
                new MongoStorageConnectionProbe(mongo.getServerSelectionTimeout()), target, root::configurationEndpoint,
                new AuditGate(record -> root.runtime().requireConfiguration().audit().record(record), clock), clock, root::requireActive,
                this::continueSetup, root::coordinateSetup, this::prepareConfiguration, root.initializationProgress());
    }

    InstallationFiles files() { return files; }
    StorageSetupService setups() { return setups; }
    boolean normalStartupAllowed() { return completedBeforeBoot; }

    void attach(Connector connector) {
        if (this.connector != null) throw new IllegalStateException("installation connector is already attached");
        this.connector = connector;
    }
    void markRequest(org.apache.catalina.connector.Request request) {
        request.removeAttribute(CONNECTOR_ATTRIBUTE);
        if (request.getConnector() == connector) request.setAttribute(CONNECTOR_ATTRIBUTE, connectorIdentity);
    }
    void publishAddress() {
        if (connector == null || connector.getLocalPort() < 1) throw StorageRuntime.unavailable();
        files.locked(() -> { files.saveStage(files.stage().withManagementUrl("http://127.0.0.1:" + connector.getLocalPort())); return null; });
    }
    @Override public boolean isManagementRequest(HttpServletRequest request) { return request.getAttribute(CONNECTOR_ATTRIBUTE) == connectorIdentity; }
    @Override public void requireManagementRequest(HttpServletRequest request) {
        boolean shellForward = request.getDispatcherType() == jakarta.servlet.DispatcherType.FORWARD && "/index.html".equals(request.getRequestURI());
        if (closed || connector == null || !isManagementRequest(request) || request.getDispatcherType() != jakarta.servlet.DispatcherType.REQUEST && !shellForward
                || request.getLocalPort() != connector.getLocalPort() || !"127.0.0.1".equals(request.getLocalAddr())) throw InstallationFiles.refused();
        String host = "127.0.0.1:" + connector.getLocalPort();
        if (!host.equals(request.getHeader("Host")) || request.getHeader("Origin") != null && !("http://" + host).equals(request.getHeader("Origin"))
                || request.getHeader("Sec-Fetch-Site") != null && !Set.of("same-origin", "none").contains(request.getHeader("Sec-Fetch-Site"))) throw InstallationFiles.refused();
        if (request.getHeader("Forwarded") != null || request.getHeader("X-Forwarded-For") != null || request.getHeader("X-Forwarded-Host") != null) throw InstallationFiles.refused();
        String path = request.getRequestURI();
        boolean write = "POST".equals(request.getMethod()) && (STORAGE_PATHS.contains(path) || path.equals("/installation/first-admin"));
        boolean read = Set.of("GET", "HEAD").contains(request.getMethod()) && (path.equals("/healthz")
                || path.equals("/auth/storage-entry") || path.equals("/index.html") || path.equals("/installation") || path.equals("/overview")
                || path.equals("/") || path.startsWith("/assets/"));
        if (!write && !read) throw InstallationFiles.refused();
    }
    @Override public StorageControlAccess caller(HttpServletRequest request) {
        var permission = permission(request);
        if (permission.purpose() == StorageSetupStateStore.Purpose.SETUP && states.load().phase() == StorageSetupStateStore.Phase.STORAGE_BOUND) {
            var scope = root.runtime().requireConfiguration().bindings().cachePreparation().orElseThrow(StorageRuntime::unavailable).authScope();
            return StorageControlAccess.initialInstallationRecovery(permission, scope);
        }
        return StorageControlAccess.management(permission);
    }

    private synchronized SetupCapabilityService.Permission permission(HttpServletRequest request) {
        requireManagementRequest(request);
        var issued = files.capability().orElseThrow(InstallationFiles::refused);
        if (!issued.equals(restored)) { capabilities.restore(issued); restored = issued; }
        return capabilities.authorize(request.getHeader(CAPABILITY_HEADER), issued.scope(), issued.purpose());
    }

    @Override public void createAdministrator(HttpServletRequest request, String username, String password) {
        var permission = permission(request);
        if (permission.purpose() != StorageSetupStateStore.Purpose.FIRST_ADMIN) throw InstallationFiles.refused();
        var configuration = root.runtime().requireConfiguration();
        var service = new FirstAdministratorService(configuration.firstAdmin(), new BCryptPasswordHasher(), new AuditGate(configuration.audit(), clock));
        service.create(permission, username, password);
        capabilities.close(); restored = null; files.removeCapability();
    }

    private void prepareConfiguration(StorageSetupScope expected, StorageSetupService.Candidate candidate, Runnable active) {
        files.locked(() -> {
            active.run(); var stage = files.stage();
            if (!stage.scope().equals(expected) || stage.handoff().ordinal() >= InstallationFiles.Handoff.CREATE_STARTED.ordinal()) throw InstallationFiles.refused();
            var connection = candidate.configuration(); String hash = configurationHash(connection);
            if (stage.configurationHash() != null && !stage.configurationHash().equals(hash)) throw InstallationFiles.refused();
            files.saveStage(stage.withHandoff(InstallationFiles.Handoff.INTENT, hash));
            files.saveConnection(connection.uri(), connection.caPem());
            files.saveStage(files.stage().withHandoff(InstallationFiles.Handoff.URI_WRITTEN, hash));
            active.run();
            files.saveStage(files.stage().withHandoff(InstallationFiles.Handoff.CREATE_STARTED, hash));
            MongoConfigurationDomain prepared = MongoConfigurationBootstrap.prepare(settings(files, mongo), expected.clusterId(), expected.installationId(), expected.operationId(), expected.generation());
            adopt(prepared, connection);
            files.saveStage(files.stage().withHandoff(InstallationFiles.Handoff.CONFIGURED, hash));
            return null;
        });
    }

    private void adopt(MongoConfigurationDomain prepared, StorageBinding.Connection connection) {
        try { root.installOnPremConfiguration(prepared, connection); }
        catch (RuntimeException | Error failure) {
            try { prepared.close(); } catch (RuntimeException | Error cleanup) { if (failure != cleanup) failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    /** Reconcile only retained authority; a consumed creation attempt never gets a new empty-store grant. */
    void resumePreparation() {
        var stage = files.stage();
        if (stage.handoff().ordinal() < InstallationFiles.Handoff.CREATE_STARTED.ordinal()) return;
        var connection = connection(files);
        if (!configurationHash(connection).equals(stage.configurationHash())) throw StorageRuntime.unavailable();
        if (root.runtime().configuration().isEmpty()) {
            if (stage.handoff() != InstallationFiles.Handoff.CREATE_STARTED) throw StorageRuntime.unavailable();
            var expected = stage.scope();
            adopt(MongoConfigurationBootstrap.resume(settings(files, mongo), expected.clusterId(), expected.installationId(), expected.operationId(), expected.generation()), connection);
        }
        var status = states.load();
        if (status.phase() != StorageSetupStateStore.Phase.COMPLETE && !completedBeforeBoot) {
            files.locked(() -> { files.saveStage(files.stage().withScope(status.scope()).withHandoff(InstallationFiles.Handoff.CONFIGURED, stage.configurationHash())); return null; });
            var configuration = root.runtime().requireConfiguration();
            var preparation = configuration.bindings().cachePreparation();
            if (preparation.isPresent()) {
                var pending = configuration.recoveryCommands().current(preparation.orElseThrow().authScope());
                if (pending.isPresent()) {
                    if (pending.orElseThrow().phase() != StorageRecoveryCommands.Phase.PLANNED)
                        root.coordinateInstallation(() -> finishRecovery(pending.orElseThrow()));
                    return;
                }
            }
            configuration.bindings().load().ifPresent(binding -> continueSetup(new StorageBindingCommit(binding,
                    configuration.bindings().cachePreparation().orElseThrow(StorageRuntime::unavailable), Optional.empty())));
        } else if (status.phase() != StorageSetupStateStore.Phase.COMPLETE) throw StorageRuntime.unavailable();
        else if (!completedBeforeBoot) markRestartRequired();
    }

    private void continueSetup(StorageBindingCommit committed) {
        root.continueOnPremSetup(committed, target); markRestartRequired();
    }
    private void markRestartRequired() {
        capabilities.close(); restored = null;
        files.locked(() -> {
            var current = files.stage();
            files.saveStage(current.withScope(states.load().scope()).withHandoff(InstallationFiles.Handoff.RESTART_REQUIRED, current.configurationHash()));
            files.removeCapability(); return null;
        });
        restartRequired.run();
    }
    void afterNormalStartup() {
        if (!completedBeforeBoot) return;
        var status = states.load();
        if (status.phase() != StorageSetupStateStore.Phase.COMPLETE || root.phase() != StorageRoot.Phase.BUSINESS_READY) throw StorageRuntime.unavailable();
        files.locked(() -> {
            var stage = files.stage(); files.saveStage(stage.withScope(status.scope()).withHandoff(InstallationFiles.Handoff.RESTARTED, stage.configurationHash()));
            if (files.capability().filter(value -> value.purpose() == StorageSetupStateStore.Purpose.SETUP).isPresent()) files.removeCapability();
            return null;
        });
        startupCompleted.run();
    }

    StorageRecoveryService recoveryService() {
        var configuration = root.runtime().requireConfiguration();
        return StorageRecoveryService.ownedPublication(configuration.recoveryCommands(), new StorageRecoveryService.Inspector() {
            @Override public StorageRecoveryCommands.Manifest inspect(CacheAuthScope scope, StorageRecoveryCommands.Selection selection) {
                return root.coordinateInstallation(() -> recoveryManifest(scope, selection));
            }
            @Override public void requireCurrent(CacheAuthScope scope, StorageRecoveryCommands.Manifest original) {
                root.coordinateInstallation(() -> {
                    var actual = recoveryManifest(scope, original.selection());
                    if (!actual.definitionHashes().equals(original.definitionHashes()) || !actual.dependencyHash().equals(original.dependencyHash())) throw StorageRuntime.unavailable();
                    return null;
                });
            }
        }, new AuditGate(configuration.audit(), clock), original -> root.coordinateInstallation(() -> finishRecovery(original)));
    }

    private StorageRecoveryCommands.Manifest recoveryManifest(CacheAuthScope scope, StorageRecoveryCommands.Selection selection) {
        if (selection.extent() != StorageRecoveryCommands.Extent.CLUSTER || !selection.pipelineIds().isEmpty()
                || root.originalSetupCreationAvailable()) throw StorageRuntime.unavailable();
        var original = recoveryOwner(scope); var configuration = root.runtime().requireConfiguration();
        var source = configuration.initialOpRecoverySource(scope, false);
        original.cache().verifyEmptyInitialRecovery();
        var identity = target.identity(master(source));
        String dependency = recoveryDependency(original.binding(), scope, identity);
        return new StorageRecoveryCommands.Manifest(selection, Set.of(), Map.of(source.id(), CanonicalHash.of(source)), dependency, configuration.configurationRevision());
    }

    private StorageRuntime.Bound recoveryOwner(CacheAuthScope scope) {
        root.requireActive();
        if (closed || root.runtime().cleanupRequired() || root.runtime().business().isPresent() || scope.mode() != StorageBinding.Mode.OP || scope.authGeneration() != 1)
            throw StorageRuntime.unavailable();
        var original = root.runtime().loadBound().orElseThrow(StorageRuntime::unavailable);
        original.requireActive();
        if (!original.preparation().authScope().equals(scope)) throw StorageRuntime.unavailable();
        return original;
    }

    private StorageRecoveryCommands.Plan finishRecovery(StorageRecoveryCommands.Plan requested) {
        var original = recoveryOwner(requested.originalScope());
        var configuration = root.runtime().requireConfiguration(); var commands = configuration.recoveryCommands();
        var current = commands.find(requested.originalScope(), requested.operationId()).orElseThrow(StorageRuntime::unavailable);
        if (current.phase() == StorageRecoveryCommands.Phase.PLANNED) throw StorageRuntime.unavailable();
        var source = configuration.initialOpRecoverySource(current.originalScope(), true); var master = master(source);
        if (!current.manifest().definitionHashes().equals(Map.of(source.id(), CanonicalHash.of(source)))
                || !current.manifest().dependencyHash().equals(recoveryDependency(original.binding(), current.originalScope(), target.identity(master))))
            throw StorageRuntime.unavailable();
        target.test(master, source, target.identity(master));
        Runnable owner = () -> {
            root.requireActive(); original.requireActive();
            if (closed || root.runtime().cleanupRequired() || root.runtime().current().orElseThrow(StorageRuntime::unavailable) != original) throw StorageRuntime.unavailable();
        };
        if (current.phase() == StorageRecoveryCommands.Phase.CONFIRMED) {
            original.cache().verifyEmptyInitialRecovery(); original.cache().verifyInitialRecoveryPermissions(); owner.run();
        }
        if (recoveryInvocation == null || !recoveryInvocation.plan().operationId().equals(current.operationId())
                || recoveryInvocation.plan().phase() != current.phase()) recoveryInvocation = configuration.startInitialOpRecovery(current, owner);
        var running = recoveryInvocation.plan();
        configuration.prepareInitialOpRecovery(original.cache(), recoveryInvocation, owner);
        original.verifyAuthentication(); original.verifyBusiness(); target.test(master, source, target.identity(master)); owner.run();
        var completed = running.phase() == StorageRecoveryCommands.Phase.COMPLETE ? running : commands.complete(running);
        Runnable completedOwner = () -> {
            owner.run();
            if (commands.find(completed.originalScope(), completed.operationId()).filter(completed::equals).isEmpty()) throw StorageRuntime.unavailable();
            configuration.initialOpRecoverySource(completed.originalScope(), true);
        };
        completedOwner.run();
        configuration.bindings().guardedBy(completedOwner).complete(states.load().scope());
        markRestartRequired(); return completed;
    }
    static String recoveryDependency(StorageBinding binding, CacheAuthScope scope, StorageSetupService.ConnectorIdentity identity) {
        var setup = binding.setup();
        return CanonicalHash.ofText(JsonWriter.write(List.of("initial-op-installation", setup.clusterId(), setup.installationId(), setup.operationId(),
                scope.bindingVersion(), scope.cacheIncarnation(), scope.authGeneration(), identity.artifactHash(), identity.specHash())));
    }
    private void requireFirstAdministratorReady() {
        if (!completedBeforeBoot || files.stage().handoff() != InstallationFiles.Handoff.RESTARTED
                || root.phase() != StorageRoot.Phase.BUSINESS_READY || !root.runtime().businessStarted()) throw StorageRuntime.unavailable();
        var configuration = root.runtime().requireConfiguration();
        var current = configuration.firstAdmin().load().orElseThrow(StorageRuntime::unavailable);
        if (current.phase() != FirstAdminStore.Phase.PENDING) throw InstallationFiles.refused();
        var original = root.runtime().current().orElseThrow(StorageRuntime::unavailable);
        original.verifyAuthentication(); original.verifyBusiness();
        var source = (SourceResource)configuration.artifacts().get(original.binding().defaultSourceId()).orElseThrow(StorageRuntime::unavailable);
        var master = master(source); target.test(master, source, target.identity(master));
    }
    StorageControlDispatcher.Snapshot snapshot(StorageControlAccess caller, Runnable active) {
        active.run(); var state = states.load();
        if (state.phase() != StorageSetupStateStore.Phase.LOCAL_SETUP) {
            var original = root.snapshot(caller, active);
            boolean complete = state.phase() == StorageSetupStateStore.Phase.COMPLETE;
            boolean restart = complete && !completedBeforeBoot;
            if (caller.initialInstallationRecovery()) {
                var current = root.runtime().requireConfiguration().recoveryCommands().guardedBy(active).current(caller.cache());
                return new StorageControlDispatcher.Snapshot(original.mode(), original.clusterId(), current.map(StorageRecoveryCommands.Plan::operationId).orElse(state.scope().operationId()),
                        original.setupGeneration(), original.bindingVersion(), original.authGeneration(), original.stateVersion(), original.setupPhase(),
                        StorageControlDispatcher.BusinessPhase.AWAITING_REBUILD_CONFIRMATION, original.health(),
                        List.of("storage.status", "storage.setup.status", "storage.test", "storage.setup.commit", "storage.recovery.plan", "storage.recovery.confirm", "storage.recovery.status"), false);
            }
            return new StorageControlDispatcher.Snapshot(original.mode(), original.clusterId(), state.scope().operationId(),
                    original.setupGeneration(), original.bindingVersion(), original.authGeneration(), original.stateVersion(), original.setupPhase(),
                    restart ? StorageControlDispatcher.BusinessPhase.RESTART_REQUIRED : complete ? StorageControlDispatcher.BusinessPhase.FIRST_ADMIN_REQUIRED : original.businessPhase(),
                    original.health(), complete ? List.of("storage.status", "storage.setup.status", restart ? "restart" : "first-admin")
                            : List.of("storage.status", "storage.test", "storage.setup.commit", "storage.setup.status"), restart);
        }
        return new StorageControlDispatcher.Snapshot(StorageBinding.Mode.OP, state.scope().clusterId(), state.scope().operationId(),
                state.scope().generation(), 0, 0, 0, state.phase().name(), StorageControlDispatcher.BusinessPhase.NOT_CONFIGURED,
                Map.of(StorageSetupService.Layer.CONFIG, StorageControlDispatcher.Health.UNKNOWN, StorageSetupService.Layer.CACHE, StorageControlDispatcher.Health.UNKNOWN,
                        StorageSetupService.Layer.MASTER, StorageControlDispatcher.Health.UNKNOWN), List.of("storage.status", "storage.test", "storage.setup.commit", "storage.setup.status"), false);
    }
    static MongoConnectionSettings settings(InstallationFiles files, MongoProperties mongo) {
        return new MongoConnectionSettings(files.configurationUri().orElseThrow(StorageRuntime::unavailable),
                files.configurationCa().isPresent() ? files.caFile().toString() : null, mongo.getServerSelectionTimeout());
    }
    static StorageBinding.Connection connection(InstallationFiles files) {
        String uri = files.configurationUri().orElseThrow(StorageRuntime::unavailable);
        return new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, uri, MongoDatabaseNames.fromUri(uri), files.configurationCa().orElse(null));
    }
    static String configurationHash(StorageBinding.Connection connection) {
        return CanonicalHash.ofText(JsonWriter.write(List.of(connection.uri(), connection.database(), connection.caPem() == null ? "" : connection.caPem())));
    }
    private static StorageBinding.Connection master(SourceResource source) {
        if (!(source.config().get("uri") instanceof String uri)) throw StorageRuntime.unavailable();
        return new StorageBinding.Connection(source.connector().equals("mongodb-atlas") ? StorageBinding.Connection.Kind.ATLAS : StorageBinding.Connection.Kind.MONGODB,
                uri, MongoDatabaseNames.fromUri(uri), source.config().get(StorageSetupService.CA_BUNDLE_FIELD) instanceof String ca ? ca : null);
    }
    private static HttpServletRequest currentRequest() {
        var attributes = org.springframework.web.context.request.RequestContextHolder.getRequestAttributes();
        if (!(attributes instanceof org.springframework.web.context.request.ServletRequestAttributes servlet)) throw InstallationFiles.refused();
        return servlet.getRequest();
    }
    @Override public void close() { closed = true; capabilities.close(); }
}
