package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.adapters.pdk.ConnectorProvisioner;
import io.tapstate.control.core.*;
import io.tapstate.control.restapi.RuntimeHttpHandlers;
import io.tapstate.core.lifecycle.*;
import io.tapstate.core.model.*;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.runtime.engine.nest.NestSettings;
import io.tapstate.runtime.scheduler.*;
import io.tapstate.runtime.srs.NativeSrsCaptureRunUnit;
import io.tapstate.runtime.srs.SnapshotBuffer;
import io.tapstate.runtime.srs.SourcePlacement;
import io.tapstate.spi.metrics.MetricsExport;
import io.tapstate.spi.store.*;
import jakarta.servlet.http.HttpServletRequest;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.boot.DefaultApplicationArguments;
import org.springframework.context.*;
import org.springframework.context.annotation.*;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.core.annotation.AnnotationAwareOrderComparator;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.GenericWebApplicationContext;
import org.springframework.web.method.HandlerTypePredicate;
import org.springframework.web.servlet.*;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;

/** One admitted runtime's workers, member providers and existing HTTP controllers share one immutable CACHE owner. */
final class NativeBusinessContext implements StorageRuntime.Business, RuntimeHttpHandlers {
    private final StorageRoot root;
    private final StorageRuntime.Bound original;
    private final ConfigurableApplicationContext parent;
    private final MongoConfigurationDomain configuration;
    private final AtomicBoolean active = new AtomicBoolean();
    private GenericApplicationContext context;
    private org.springframework.boot.availability.ApplicationAvailability availability;
    private ThreeLayerStorePort port;
    private NativeMemberStorage memberStorage;
    private RetiredPipelinePayloadCleanup payloadCleanup;
    private List<HandlerMapping> handlers = List.of();

    NativeBusinessContext(StorageRoot root, StorageRuntime.Bound original, ConfigurableApplicationContext parent) {
        this.root = root; this.original = original; this.parent = parent;
        this.configuration = root.runtime().requireConfiguration();
    }

    @Override public synchronized void start() {
        original.requireBusiness();
        if (context != null) throw ThreeLayerStorePort.refused();
        if (parent instanceof WebApplicationContext web) {
            var child = new GenericWebApplicationContext(); child.setServletContext(web.getServletContext()); context = child;
        } else context = new GenericApplicationContext();
        context.setParent(parent); context.setEnvironment(parent.getEnvironment());
        var beans = context.getDefaultListableBeanFactory();
        beans.setConversionService(org.springframework.boot.convert.ApplicationConversionService.getSharedInstance());
        beans.registerSingleton("nativeBusinessContext", this);
        port = new ThreeLayerStorePort(configuration, original, this::preparePipeline);
        beans.registerSingleton("storePort", port);
        beans.registerSingleton("storageBindingStore", configuration.bindings());
        beans.registerSingleton("defaultSourceVerification", new DefaultSourceVerification(() -> {
            original.requireBusiness();
            var binding = configuration.bindings().load().orElseThrow(StorageRuntime::unavailable);
            if (!binding.equals(original.binding())) return Optional.empty();
            return configuration.artifacts().get(binding.defaultSourceId())
                    .filter(SourceResource.class::isInstance).map(SourceResource.class::cast);
        }, port.connectionTestResults(), parent.getBean(Clock.class)));
        var overrides = new LinkedHashMap<String,Object>();
        overrides.put("tokenStore", configuration.tokens());
        overrides.put("auditStore", configuration.audit());
        overrides.put("clusterIdentityStore", configuration.clusterIdentity());
        overrides.put("clusterIdentityService", parent.getBean(ClusterIdentityService.class));
        overrides.put("credentialAuthenticator", parent.getBean(CredentialAuthenticator.class));
        for (String name : List.of("clock", "tokenSecrets", "signingSecret", "cloudRuntimeSettings", "bootId"))
            overrides.put(name, parent.getBean(name));
        context.addBeanFactoryPostProcessor(factory -> {
            var definitions = (BeanDefinitionRegistry)factory;
            for (String name : List.of("authStores", "userStore", "sessionStore", "cloudSessionStore", "loginService", "sessionService",
                    "bootstrapService", "accessTokenService", "tokenSigner", "viewStoreSeedRunner", "derivedSchemas"))
                if (definitions.containsBeanDefinition(name)) definitions.removeBeanDefinition(name);
            overrides.forEach((name, value) -> {
                if (definitions.containsBeanDefinition(name)) definitions.removeBeanDefinition(name);
                factory.registerSingleton(name, value);
            });
        });
        new AnnotatedBeanDefinitionReader(context).register(ControlPlaneConfiguration.class, HazelcastConfiguration.class, Wiring.class);
        try { context.refresh(); }
        catch (org.springframework.beans.BeansException failed) {
            for (Throwable cause = failed; cause != null; cause = cause.getCause())
                if (cause instanceof io.tapstate.core.common.TapstateException coded) throw coded;
            throw failed;
        }
        availability = context.getBean(EngineAvailability.class);
        memberStorage.admitted(context.getBean(NodeSessionLease.class));
        context.getBean(SeedSweepRunner.class).run(new DefaultApplicationArguments(new String[0]));
        var ordered = new ArrayList<>(context.getBeansOfType(HandlerMapping.class).values());
        AnnotationAwareOrderComparator.sort(ordered); handlers = List.copyOf(ordered);
        original.requireBusiness(); active.set(true);
        // The first-admin page uses the same formal result and observer as ordinary Source tests.
        if (original.preparation().authScope().mode() == StorageBinding.Mode.OP) {
            try {
                if (configuration.firstAdmin().load().orElseThrow(StorageRuntime::unavailable).phase() == FirstAdminStore.Phase.PENDING) {
                    var binding = configuration.bindings().load().filter(original.binding()::equals).orElseThrow(StorageRuntime::unavailable);
                    var source = configuration.artifacts().get(binding.defaultSourceId()).filter(SourceResource.class::isInstance)
                            .map(SourceResource.class::cast).orElseThrow(StorageRuntime::unavailable);
                    context.getBean(ConnectionTestService.class).test(source.id(), source.connector(), source.config(),
                            "installation:" + binding.setup().installationId());
                }
            } catch (io.tapstate.core.common.TapstateException unavailable) {
                org.slf4j.LoggerFactory.getLogger(NativeBusinessContext.class)
                        .warn("Initial application-data verification unavailable [{}]", unavailable.code().code());
            }
        }
    }

    Optional<org.springframework.boot.availability.ApplicationAvailability> availability() {
        return active.get() ? Optional.ofNullable(availability) : Optional.empty();
    }

    private void preparePipeline(PipelineIntentScope scope) {
        original.requireBusiness();
        var snapshot = ReadOnlyArtifactSnapshot.capture(context.getBean(ArtifactStore.class));
        var authored = StoredArtifacts.requirePipeline(snapshot, scope.pipelineId());
        var expanded = PipelineInlining.inline(authored, snapshot);
        var sources = new LinkedHashMap<String,SourceResource>();
        expanded.sourceIds().forEach(id -> sources.put(id, StoredArtifacts.requireSource(snapshot, id)));
        var models = SourceModelSnapshot.capture(scope.pipelineId(), sources.values(), port.schemas());
        var inputs = models.preparationInputs(scope, authored, expanded, sources);
        context.getBean(PipelineFirstStatePreparer.class).prepare(scope, inputs);
    }
    private StoreBackedDerivedSchemas.Command modelCommand(String pipeline, PipelineActuationOwnership ownership) {
        var live = new LivePipelines(port.desired(), port.state());
        Runnable resting = () -> {
            memberStorage.requireNode();
            if (!live.isAtRest(pipeline)) throw new io.tapstate.core.common.TapstateException(ActuationError.SCHEMA_SYNC_WHILE_RUNNING,
                    Map.of("pipeline", pipeline, "state", live.actualStateOf(pipeline).name(), "desired", live.intentOf(pipeline).name()), null);
        };
        resting.run(); var scope = port.scope(pipeline);
        var preparation = configuration.preparations().read(pipeline).orElseThrow(ThreeLayerStorePort::refused);
        if (preparation.phase() == PipelinePreparation.Phase.UNASSIGNED) {
            var models = configuration.authoringModels(scope);
            var node = context.getBean(NodeSessionLease.class).currentFence().orElseThrow(ThreeLayerStorePort::refused);
            var command = ownership.unpreparedControl(scope);
            Runnable guard = () -> {
                resting.run(); command.requireActive();
                if (!scope.equals(port.scope(pipeline)) || configuration.preparations().read(pipeline).orElseThrow(ThreeLayerStorePort::refused).phase()
                        != PipelinePreparation.Phase.UNASSIGNED) throw ThreeLayerStorePort.refused();
            };
            try { return new StoreBackedDerivedSchemas.Command(port.forUnpreparedControl(models, node, guard), command::close); }
            catch (RuntimeException | Error failed) { command.close(); throw failed; }
        }
        if (preparation.phase() != PipelinePreparation.Phase.COMPLETE) throw ThreeLayerStorePort.refused();
        var command = ownership.control(scope);
        var claim = command.claim();
        Runnable guard = () -> {
            resting.run();
            if (!scope.equals(port.scope(pipeline))) throw ThreeLayerStorePort.refused();
            command.requireActive();
        };
        try { return new StoreBackedDerivedSchemas.Command(port.forControl(scope, claim, guard), command::close); }
        catch (RuntimeException | Error failed) { command.close(); throw failed; }
    }
    @Override public boolean available() {
        return active.get() && memberStorage != null && memberStorage.nodeActive()
                && context.getBean(ClusterMembershipGate.class).businessEligible();
    }
    @Override public void awaitAvailability() {
        long deadline=System.nanoTime()+Duration.ofSeconds(30).toNanos();
        while(!available()) {
            original.requireBusiness();memberStorage.requireNode();
            if(System.nanoTime()-deadline>=0 || Thread.currentThread().isInterrupted())throw StorageRuntime.unavailable();
            java.util.concurrent.locks.LockSupport.parkNanos(Duration.ofMillis(25).toNanos());
        }
        original.requireBusiness();memberStorage.requireNode();
    }
    void refreshImportedSecrets(Set<String> ids) {
        if (!active.get()) return;
        var artifacts = context.getBean(ArtifactStore.class);
        if (!(artifacts instanceof SecretTrackingArtifactStore tracked)) throw new IllegalStateException("native artifact tracking is not installed");
        tracked.refreshLoaded(ids);
    }
    ApplyService importValidation() {
        if (!available()) throw StorageRuntime.unavailable();
        return context.getBean(ApplyService.class);
    }
    NativePipelineRecovery pipelineRecovery() {
        if(!available())throw StorageRuntime.unavailable();return context.getBean(NativePipelineRecovery.class);
    }
    NativeSharedSrsRecovery sharedRecovery() {
        if(!available())throw StorageRuntime.unavailable();return context.getBean(NativeSharedSrsRecovery.class);
    }
    StorePort statusStore() { if (!available()) throw StorageRuntime.unavailable(); return port; }
    DefaultSourceVerification.Verification applicationDataVerification() {
        return available() ? context.getBean(DefaultSourceVerification.class).snapshot()
                : DefaultSourceVerification.Verification.unknown();
    }
    private boolean recoveryPinned(PipelineIntentScope scope) {
        original.requireBusiness();return configuration.cacheRecoveries().read(scope.pipelineId()).filter(value -> value.targetScope().equals(scope)).isPresent();
    }
    @Override public HandlerExecutionChain handlerFor(HttpServletRequest request) throws Exception {
        if (!active.get()) return null;
        for (var mapping : handlers) {
            var handler = mapping.getHandler(request); if (handler != null) return handler;
        }
        return null;
    }
    @Override public synchronized void close() {
        active.set(false); handlers = List.of();
        Object member = context == null ? null : context.getBeanFactory().getSingleton("hazelcastMember");
        Throwable failure = null;
        try { if (payloadCleanup != null) { payloadCleanup.close(); payloadCleanup = null; } }
        catch (RuntimeException | Error cleanup) { failure = cleanup; }
        try { if (memberStorage != null) memberStorage.close(); } catch (RuntimeException | Error cleanup) {
            if (failure == null) failure = cleanup; else if (failure != cleanup) failure.addSuppressed(cleanup);
        }
        try { if (context != null) context.close(); } catch (RuntimeException | Error cleanup) {
            if (failure == null) failure = cleanup; else if (failure != cleanup) failure.addSuppressed(cleanup);
        }
        try {
            if (member instanceof HazelcastInstance actual && actual.getLifecycleService().isRunning()) throw StorageRuntime.unavailable();
        } catch (RuntimeException | Error cleanup) {
            if (failure == null) failure = cleanup; else if (failure != cleanup) failure.addSuppressed(cleanup);
        }
        if (failure instanceof Error error) throw error;
        if (failure instanceof RuntimeException error) throw error;
    }

    static final class ChildOnly implements Condition {
        @Override public boolean matches(ConditionContext context, org.springframework.core.type.AnnotatedTypeMetadata metadata) {
            return context.getBeanFactory() != null && context.getBeanFactory().containsSingleton("nativeBusinessContext");
        }
    }

    @Configuration(proxyBeanMethods = false)
    @Conditional(ChildOnly.class)
    @EnableScheduling
    @org.springframework.boot.context.properties.EnableConfigurationProperties({MetricsExportProperties.class})
    static class Wiring {
        @Bean StoreBackedDerivedSchemas nativeDerivedSchemas(NativeBusinessContext owner, AuditGate audit,
                PipelineActuationOwnership ownership) {
            return new StoreBackedDerivedSchemas(owner.port, audit, pipeline -> owner.modelCommand(pipeline, ownership));
        }
        @Bean SrsStores nativeSrsStores(NativeBusinessContext owner) {
            return owner.original.cache().srs(owner.original.preparation().businessScope(), owner.original::requireBusiness);
        }
        @Bean SrsLogStore srsLogStore(SrsStores stores) { return stores.log(); }
        @Bean WorkloadClaimStore workloadClaimStore(NativeBusinessContext owner) { return owner.port.workloadClaims(); }
        @Bean MemberStorage memberStorage(NativeBusinessContext owner, ClusterProperties cluster, WorkloadClaimStore claims, SrsStores sources) {
            owner.memberStorage = new NativeMemberStorage(owner.original, owner.configuration, cluster, claims, sources);
            return owner.memberStorage;
        }
        @Bean ClusterMembershipStore clusterMembershipStore(NativeBusinessContext owner) {
            var read = owner.port.clusterMembership();
            return new ClusterMembershipStore() {
                private ClusterMembershipStore writer() {
                    owner.memberStorage.requireNode();
                    var node = owner.context.getBean(NodeSessionLease.class).currentFence().orElseThrow(ThreeLayerStorePort::refused);
                    return owner.original.cache().clusterMembership(owner.original.preparation().businessScope(), node, owner.memberStorage::requireNode);
                }
                @Override public Optional<ClusterMembership> read(String id) { return read.read(id); }
                @Override public ClusterMembership createIfAbsent(String id, Set<String> nodes) { return writer().createIfAbsent(id, nodes); }
                @Override public Optional<ClusterMembership> compareAndSet(String id, long revision, Set<String> nodes) { return writer().compareAndSet(id, revision, nodes); }
            };
        }
        @Bean ClusterWorkloadClaims clusterWorkloadClaims(WorkloadClaimStore claims, ClusterMembershipGate membership) {
            return new ClusterWorkloadClaims(claims, membership);
        }
        @Bean SnapshotBuffer snapshotBuffer() { return new SnapshotBuffer(); }
        @Bean Engine engine(HazelcastInstance member) { return new Engine(member); }
        @Bean(destroyMethod = "close") ExecutionAuthorization executionAuthorization(HazelcastInstance member, NativeBusinessContext owner) {
            return owner.memberStorage.authorization();
        }
        @Bean(destroyMethod = "close") SnapshotLoadReceipts snapshotLoadReceipts(HazelcastInstance member,
                NativeBusinessContext owner,ExecutionAuthorization authorization) {
            return new SnapshotLoadReceipts(member,owner.memberStorage::requireNode,authorization);
        }
        @Bean PipelineActuationOwnership pipelineActuationOwnership(HazelcastInstance member,
                NativeBusinessContext owner, ClusterProperties cluster, ClusterMembershipGate membership, ClusterWorkloadClaims claims) {
            Object held = member.getUserContext().get(HazelcastConfiguration.NODE_SESSION_CONTEXT_KEY);
            if (!(held instanceof WorkloadClaim node)) throw ThreeLayerStorePort.refused();
            return PipelineActuationOwnership.scoped(owner.original.preparation().businessScope(), node.owner(), membership, claims,
                    cluster.getWorkloadClaimTtl(), cluster.getWorkloadClaimRenewInterval(), System::nanoTime);
        }
        @Bean(destroyMethod = "close") ActuationClaimRenewer actuationClaimRenewer(PipelineActuationOwnership ownership, ClusterProperties cluster) {
            return new ActuationClaimRenewer(ownership, cluster.getWorkloadClaimRenewInterval());
        }
        @Bean CaptureOwnership captureOwnership(HazelcastInstance member, ClusterProperties cluster,
                ClusterMembershipGate membership, ClusterWorkloadClaims claims) {
            Object held = member.getUserContext().get(HazelcastConfiguration.NODE_SESSION_CONTEXT_KEY);
            if (!(held instanceof WorkloadClaim node)) throw ThreeLayerStorePort.refused();
            return new CaptureOwnership(node.key().clusterId(), node.owner(), membership, claims, cluster.getWorkloadClaimTtl());
        }
        @Bean PipelineFirstStatePreparer pipelineFirstStatePreparer(NativeBusinessContext owner, NodeSessionLease node,
                ClusterProperties cluster, Clock clock) {
            var scope = owner.original.preparation().businessScope();
            return new PipelineFirstStatePreparer(scope, owner.configuration.preparations(),
                    owner.original.cache().pipelinePreparations(scope, owner.memberStorage::requireNode), node::currentFence,
                    owner.memberStorage::requireNode, clock, cluster.getWorkloadClaimTtl());
        }
        @Bean(destroyMethod = "close") NativeSrsCaptureRunUnit nativeSrsCaptureRunUnit(NativeBusinessContext owner, SrsStores sources,
                HazelcastInstance member) {
            return new NativeSrsCaptureRunUnit(owner.original.preparation().businessScope(), sources, member);
        }
        @Bean(destroyMethod = "close") PipelineCaptureCoordinator pipelineCaptureCoordinator(NativeBusinessContext owner,
                NodeSessionLease node, ExecutionAuthorization authorization, CaptureOwnership ownership, ClusterProperties cluster,
                NativeSrsCaptureRunUnit runtime, SnapshotBuffer buffer, ConnectorProvisioner connectors, Clock clock, SnapshotLoadReceipts boundedLoads) {
            var scope = owner.original.preparation().businessScope();
            return new ScopedPipelineCaptureCoordinator(new ScopedPipelineCaptureCoordinator.Stores(scope, owner.original.cache(),
                    owner.memberStorage.sourceViews(), owner.configuration.capturePreparations(),
                    owner.original.cache().capturePreparations(scope, owner.memberStorage::requireNode), owner.configuration.requiredNotes(),owner::recoveryPinned,boundedLoads,
                    owner.configuration.rebuilds().guardedBy(owner.memberStorage::requireNode)::completedFull),
                    new ScopedPipelineCaptureCoordinator.Execution(node::currentFence, authorization, ownership,
                            cluster.getWorkloadClaimRenewInterval(), owner.memberStorage::requireNode, clock), runtime, buffer, connectors);
        }
        @Bean NativePipelineRecovery nativePipelineRecovery(NativeBusinessContext owner,PipelineFirstStatePreparer preparer,
                ConnectorProvisioner connectors,Engine engine,PipelineCaptureCoordinator captures,ExecutionAuthorization authorization) {
            return new NativePipelineRecovery(owner.configuration,owner.original.preparation().authScope(),owner.port,preparer,
                    connectors,engine,captures,authorization,owner.original::requireActive,owner::available,owner.original.cache());
        }
        @Bean io.tapstate.core.lifecycle.ParallelismBudget parallelismBudget(org.springframework.core.env.Environment environment) {
            return new DataPlaneActuationConfiguration().parallelismBudget(
                    environment.getProperty("tapstate.execution.max-local-parallelism", Integer.class, 16),
                    environment.getProperty("tapstate.execution.max-connector-instances-per-member", Integer.class, 8),
                    environment.getProperty("tapstate.execution.max-buffered-records-per-member", Long.class, 262144L),
                    environment.getProperty("tapstate.execution.max-blocking-processors-per-member", Integer.class, 128));
        }
        @Bean DagSource dagSource(NativeBusinessContext owner, NestSettings nests, ConnectionTester tester, HazelcastInstance member,
                CloudRuntimeSettings cloud, ParallelismBudget budget, ConnectorProvisioner connectors, ArtifactStore artifacts, SnapshotLoadReceipts boundedLoads,NativePipelineRecovery recovery) {
            return StoreBackedDagSource.scopedRoot(owner.port, nests, StoreReachability.probing(tester, Duration.ofSeconds(10)),
                    SourcePlacement.on(member.getCluster().getLocalMember().getAddress()), cloud.cloud(),
                    () -> ClusterMembershipGate.dataMembers(member), budget, owner.configuration.requiredNotes(), connectors, artifacts,owner::recoveryPinned,boundedLoads::forRun,recovery::inspectCaptured);
        }
        @Bean(destroyMethod = "close") BoundedPipelinePreviewExecutor boundedPipelinePreviewExecutor(StorePort store, ConnectorProvisioner connectors,
                HazelcastInstance member, NestSettings nests, Clock clock) {
            return new BoundedPipelinePreviewExecutor(store, connectors, member, nests, clock);
        }
        @Bean HazelcastExecutionPlans executionPlans(HazelcastInstance member) { return new HazelcastExecutionPlans(member); }
        @Bean EngineLifecycleActuator lifecycleActuator(NativeBusinessContext owner, Engine engine, DagSource dags,
                PipelineCaptureCoordinator captures, PipelineActuationOwnership ownership, HazelcastExecutionPlans plans,
                HazelcastInstance member, Clock clock) {
            return new EngineLifecycleActuator(engine, dags, captures, owner.port.operatorStateStores().defaultDatabase(),
                    ownership, plans, clock, new HazelcastConnectorReadiness(member, Duration.ofSeconds(60)));
        }
        @Bean ArtifactMutationService.PipelineDeletion nativePipelineDeletion(NativeBusinessContext owner,
                PipelineActuationOwnership ownership, NodeSessionLease node, Engine engine, EngineLifecycleActuator actuator,
                PipelineCaptureCoordinator captures, HazelcastInstance member, HazelcastExecutionPlans plans, ConnectorProvisioner connectors) {
            if (!(captures instanceof ScopedPipelineCaptureCoordinator nativeCaptures)) throw ThreeLayerStorePort.refused();
            HazelcastOperatorReadiness.bindRetirement(member,nativeCaptures,owner.memberStorage::requireNode);
            return new NativePipelineDeletion(owner.configuration,owner.original.cache(),owner.original.preparation().businessScope(),
                    ownership,() -> node.currentFence().orElseThrow(ThreeLayerStorePort::refused),owner.memberStorage::requireNode,
                    engine,actuator,new HazelcastOperatorReadiness(member,Duration.ofSeconds(30)),plans,connectors);
        }
        @Bean(destroyMethod = "close") NativeSharedSrsRecovery nativeSharedSrsRecovery(NativeBusinessContext owner,
                PipelineActuationOwnership ownership,NodeSessionLease node,Engine engine,EngineLifecycleActuator actuator,
                PipelineCaptureCoordinator captures,HazelcastInstance member,HazelcastExecutionPlans plans,
                ArtifactMutationService.PipelineDeletion deletion) {
            if(!(captures instanceof ScopedPipelineCaptureCoordinator nativeCaptures))throw ThreeLayerStorePort.refused();
            Runnable active=() -> {if(!owner.available())throw StorageRuntime.unavailable();owner.memberStorage.requireNode();};
            return new NativeSharedSrsRecovery(owner.configuration,owner.original.cache(),owner.original.preparation().authScope(),
                    ownership,() -> node.currentFence().orElseThrow(ThreeLayerStorePort::refused),active,engine,actuator,
                    new HazelcastOperatorReadiness(member,Duration.ofSeconds(30)),plans,nativeCaptures,member);
        }
        @Bean PipelinePositionService.WriteBack nativeSourcePositions(NativeBusinessContext owner,
                PipelineChains chains, LivePipelines live, AuditGate audit, PipelineActuationOwnership ownership,
                CaptureOwnership captures, NodeSessionLease node, Engine engine, EngineLifecycleActuator actuator,
                HazelcastInstance member, HazelcastExecutionPlans plans, Clock clock,
                ArtifactMutationService.PipelineDeletion retirement) {
            var reading = new PipelinePositionService(chains, owner.port.meta(), owner.port.artifacts(), live, audit);
            return new NativeSourcePositionEdits(owner.configuration, owner.original.cache(), owner.original.preparation().businessScope(),
                    owner.port, reading, ownership, captures, () -> node.currentFence().orElseThrow(ThreeLayerStorePort::refused),
                    owner.memberStorage::requireNode, engine, actuator, new HazelcastOperatorReadiness(member, Duration.ofSeconds(30)), plans, clock);
        }
        @Bean RebuildAdmission rebuildAdmission(ClusterProperties cluster, HazelcastProperties hazelcast,
                PipelineActuationOwnership ownership, Engine engine) {
            return new DataPlaneActuationConfiguration().rebuildAdmission(cluster, hazelcast, ownership, engine);
        }
        @Bean PipelineConverger pipelineConverger(NativeBusinessContext owner, EngineLifecycleActuator actuator, Clock clock, RebuildAdmission admission) {
            return new PipelineConverger(owner.port.desired(), owner.port.state(), actuator, clock, admission);
        }
        @Bean ObservationPublisher observationPublisher(NativeBusinessContext owner, Engine engine, PipelineCaptureCoordinator captures) {
            var readings = new RunBoundCaptureReadings(captures, pipeline -> engine.captureRun(pipeline).map(run -> {
                if (!(run.operatorManifest().executionIdentity() instanceof ExecutionFence original)) throw ThreeLayerStorePort.refused();
                return original;
            }), engine::capturedRows);
            return new RuntimeConvergenceConfiguration().observationPublisher(owner.port, engine, readings);
        }
        @Bean RateSampler rateSampler(NativeBusinessContext owner, MetricsHistoryProperties history) {
            return new RateSampler(owner.port.rateHistory(), history.getSampleInterval());
        }
        @Bean(destroyMethod = "close") MetricsExport metricsExport(MetricsExportProperties properties) {
            return RuntimeConvergenceConfiguration.metricsExportFor(properties);
        }
        @Bean ConvergenceDriver convergenceDriver(NativeBusinessContext owner, PipelineConverger converger,
                ObservationPublisher observations, RateSampler samples, MetricsExport export, NodeSessionLease node,
                ClusterMembershipGate membership, PipelineActuationOwnership ownership, EngineLifecycleActuator actuator,
                HazelcastInstance member, HazelcastExecutionPlans plans, RebuildAdmission admission, Clock clock, ClusterProperties cluster,
                ConnectorProvisioner connectors, Engine engine, PipelineCaptureCoordinator captures) {
            var operators = new HazelcastOperatorReadiness(member, Duration.ofSeconds(60));
            var scope = owner.original.preparation().businessScope();
            var changes = owner.configuration.rebuilds();
            var rebuilds = new PipelineRebuildCoordinator(new PipelineRebuildCoordinator.Stores(owner.port.artifacts(), owner.port.schemas(),
                    owner.original.cache(), owner.context.getBean(SrsStores.class).consumers(), changes,
                    owner.original.cache().pipelineRebuilds(scope, owner.memberStorage::requireNode)),
                    new PipelinePrivateNoteRetirement(owner.configuration.requiredNotes(), changes, connectors),
                    () -> node.currentFence().orElseThrow(ThreeLayerStorePort::refused), owner.memberStorage::requireNode,
                    (request, manifest) -> {
                        owner.memberStorage.requireNode();
                        var job = engine.captureRun(request.originalScope().pipelineId());
                        if (job.isPresent() && (job.orElseThrow().isCarryingAJob()
                                || manifest.isEmpty() || !job.orElseThrow().operatorManifest().equals(manifest.orElseThrow()))) throw ThreeLayerStorePort.refused();
                        if (manifest.isPresent() && manifest.orElseThrow().executionIdentity() instanceof ExecutionFence original
                                && captures.forRun(request.originalScope(), request.originalClaim(), original).isCapturing(request.originalScope().pipelineId())) throw ThreeLayerStorePort.refused();
                    }, cluster.getWorkloadClaimTtl(), clock);
            var ports = new ScopedEngineLifecycleActuator.Ports(operators::prepare, operators::releaseHot,
                    rebuilds::rebuild, (original, command) -> plans.forRun(original, () -> {
                        owner.memberStorage.requireNode(); if (!ownership.proveClaim(original.scope(), command)) throw ThreeLayerStorePort.refused();
                    }));
            return ConvergenceDriver.scoped(converger, observations, samples, export,
                    () -> owner.active.get() && node.active() && (cluster.getProfile() == ClusterProperties.Profile.SINGLE || membership.businessEligible()),
                    ownership, new ConvergenceDriver.ScopedPorts(owner.configuration.intents(), owner.original.cache(),
                            owner.memberStorage::requireNode, pass -> actuator.forPass(pass, ports), pass -> admission, admission::retain));
        }
        @Bean PipelineStatusDiagnostics nativePipelineStatusDiagnostics(NativeBusinessContext owner, Engine engine,
                PipelineActuationOwnership ownership) {
            return new NativePipelineStatusDiagnostics(owner.original.preparation().businessScope(),
                    owner.configuration.intents(), owner.configuration.preparations(), engine, ownership, owner.active::get,
                    owner.original.cache()::pipelineStates);
        }
        @Bean(initMethod = "start", destroyMethod = "")
        RetiredPipelinePayloadCleanup retiredPipelinePayloadCleanup(NativeBusinessContext owner, NodeSessionLease node) {
            var scope = owner.original.preparation().businessScope();
            if (owner.payloadCleanup != null) throw new IllegalStateException("retired payload cleanup already owned");
            owner.payloadCleanup = new RetiredPipelinePayloadCleanup(guard -> owner.original.cache().retiredPipelinePayloads(scope, () -> {
                guard.run(); owner.memberStorage.requireNode();
            }), node::currentFence, () -> owner.active.get() && node.active());
            return owner.payloadCleanup;
        }
        @Bean RequestMappingHandlerMapping runtimeRequestMappingHandlerMapping() {
            var mapping = new RequestMappingHandlerMapping(); mapping.setOrder(0);
            mapping.setPathPrefixes(Map.of("/api", HandlerTypePredicate.forAnnotation(org.springframework.web.bind.annotation.RestController.class)));
            return mapping;
        }
    }
}
