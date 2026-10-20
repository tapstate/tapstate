package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.lifecycle.*;
import io.tapstate.spi.store.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.function.Consumer;

/** A fixed admitted cache owner; ordinary queries never acquire permission for unscoped runtime writes. */
final class ThreeLayerStorePort implements StorePort {
    private final MongoConfigurationDomain configuration;
    private final StorageRuntime.Bound bound;
    private final PipelineConvergencePass pass;
    private final Consumer<PipelineIntentScope> prepare;
    private final Control control;
    private record Control(PipelineIntentScope scope, WorkloadClaimFence claim, Runnable active,
            MongoConfigurationDomain.AuthoringModels models) {}

    ThreeLayerStorePort(MongoConfigurationDomain configuration, StorageRuntime.Bound bound,
            Consumer<PipelineIntentScope> prepare) {
        this(configuration, bound, prepare, null, null);
    }
    private ThreeLayerStorePort(MongoConfigurationDomain configuration, StorageRuntime.Bound bound,
            Consumer<PipelineIntentScope> prepare, PipelineConvergencePass pass, Control control) {
        this.configuration = Objects.requireNonNull(configuration); this.bound = Objects.requireNonNull(bound);
        this.prepare = Objects.requireNonNull(prepare); this.pass = pass; this.control = control;
        bound.requireBusiness();
    }
    ThreeLayerStorePort forPass(PipelineConvergencePass original) {
        original.requirePipeline(original.scope().pipelineId());
        if (!business().equals(new CacheBusinessScope(original.scope().clusterId(), original.scope().bindingVersion(),
                original.scope().cacheIncarnation()))) throw refused();
        return new ThreeLayerStorePort(configuration, bound, prepare, original, null);
    }
    ThreeLayerStorePort forControl(PipelineIntentScope scope, WorkloadClaimFence claim, Runnable guard) {
        Objects.requireNonNull(guard).run();
        if (!scope.equals(scope(scope.pipelineId())) || claim.key().type() != WorkloadClaimType.PIPELINE_ACTUATION
                || !claim.key().resourceId().equals(scope.pipelineId()) || !claim.key().clusterId().equals(scope.clusterId())) throw refused();
        return new ThreeLayerStorePort(configuration, bound, prepare, null, new Control(scope, claim, guard, null));
    }
    ThreeLayerStorePort forUnpreparedControl(MongoConfigurationDomain.AuthoringModels models, WorkloadClaimFence node, Runnable guard) {
        var scope = models.scope();
        Objects.requireNonNull(guard).run();
        if (!scope.equals(scope(scope.pipelineId())) || node.key().type() != WorkloadClaimType.NODE_SESSION
                || !node.key().clusterId().equals(scope.clusterId())) throw refused();
        return new ThreeLayerStorePort(configuration, bound, prepare, null, new Control(scope, node, guard, models));
    }
    private void active() {
        bound.requireBusiness();
        if (pass != null) pass.requirePipeline(pass.scope().pipelineId());
        if (control != null) control.active().run();
    }
    CacheBusinessScope business() { return bound.preparation().businessScope(); }
    PipelineIntentScope scope(String pipeline) {
        active();
        if (pass != null) { pass.requirePipeline(pipeline); return pass.scope(); }
        if (control != null) { if (!control.scope().pipelineId().equals(pipeline)) throw refused(); return control.scope(); }
        var mapping = configuration.pipelineStates().read(pipeline).orElseThrow(ThreeLayerStorePort::refused);
        if (mapping.phase() != PipelineStateMapping.Phase.ACTIVE) throw refused();
        var cache = business();
        return new PipelineIntentScope(cache.clusterId(), cache.bindingVersion(), cache.cacheIncarnation(), pipeline, mapping.stateGeneration());
    }
    private boolean unprepared(String pipeline) {
        active();
        return configuration.preparations().read(pipeline).orElseThrow(ThreeLayerStorePort::refused).phase()
                == PipelinePreparation.Phase.UNASSIGNED;
    }
    @Override public ArtifactStore artifacts() { active(); return configuration.artifacts(); }
    @Override public CatalogStore catalog() { active(); return configuration.catalog(); }
    @Override public PipelineLayoutStore layouts() { active(); return configuration.layouts(); }
    @Override public PipelineDraftStore drafts() { active(); return configuration.drafts(); }
    @Override public SchemaStore schemas() { active(); return bound.cache().schemas(business(), this::active); }
    @Override public ConnectorRegistry connectors() { active(); return bound.cache().connectors(business(), this::active); }
    @Override public ConnectorCatalogStore connectorCatalog() { active(); return bound.cache().connectorCatalog(business(), this::active); }
    @Override public ConnectorSpecStore connectorSpecs() { active(); return bound.cache().connectorSpecs(business(), this::active); }
    @Override public ConnectionTestResultStore connectionTestResults() { active(); return bound.cache().connectionTestResults(business(), this::active); }
    @Override public WorkloadClaimStore workloadClaims() { active(); return bound.cache().workloadClaims(business()); }
    @Override public ClusterMembershipStore clusterMembership() { active(); return bound.cache().clusterMembership(business()); }
    @Override public SrsLogStore srsLog() { active(); return bound.cache().srs(business(), this::active).log(); }

    @Override public DesiredStore desired() {
        return new DesiredStore() {
            @Override public void save(DesiredState desired) {
                var original = scope(desired.pipelineId());
                var intents = configuration.intents().guardedBy(ThreeLayerStorePort.this::active);
                var previous = intents.load(desired.pipelineId());
                if (previous.isPresent() && !previous.orElseThrow().scope().equals(original)) throw refused();
                if (desired.targetState() == PipelineState.RUNNING) prepare.accept(original);
                if (!intents.compareAndSet(original, previous.map(PipelineIntentSnapshot::intentRevision).orElse(0L), desired)) throw refused();
            }
            @Override public Optional<DesiredState> read(String pipeline) {
                active(); return configuration.intents().load(pipeline).filter(value -> !value.deleted()).map(PipelineIntentSnapshot::desired);
            }
            @Override public List<String> pipelineIds() { active(); return configuration.intents().pipelineIds(); }
            @Override public void delete(String pipeline) { throw refused(); }
        };
    }
    @Override public StateStore state() {
        if (pass != null) return pass.state();
        return new StateStore() {
            @Override public Optional<CheckpointDoc> read(String pipeline) {
                var original = scope(pipeline);
                return unprepared(pipeline) ? Optional.empty() : bound.cache().pipelineStates(original).read(pipeline);
            }
            @Override public void create(String pipeline, String json, Instant time) { throw refused(); }
            @Override public CasOutcome compareAndSwap(String pipeline, long epoch, String json, Instant time) { throw refused(); }
            @Override public void delete(String pipeline) { throw refused(); }
        };
    }
    @Override public ObservationStore observations() {
        if (pass != null) return pass.observations();
        return new ObservationStore() {
            @Override public Optional<Observation> read(String pipeline) {
                var original = scope(pipeline);
                return unprepared(pipeline) ? Optional.empty() : bound.cache().pipelineObservations(original).read(pipeline);
            }
            @Override public void save(Observation observation) { throw refused(); }
            @Override public void delete(String pipeline) { throw refused(); }
        };
    }
    @Override public DerivedSchemaStore derivedSchemas() {
        if (pass != null) return pass.derivedSchemas();
        if (control != null) return control.claim().key().type() == WorkloadClaimType.NODE_SESSION
                ? bound.cache().unpreparedPipelineDerivedSchemas(control.models(), control.claim(), this::active)
                : bound.cache().pipelineDerivedSchemas(control.scope(), control.claim(), this::active);
        return new DerivedSchemaStore() {
            private DerivedSchemaStore view(String pipeline) {
                var original = scope(pipeline);
                return unprepared(pipeline) ? bound.cache().unpreparedPipelineDerivedSchemas(configuration.authoringModels(original), null, () -> {
                    active();
                    if (!unprepared(pipeline) || !original.equals(scope(pipeline))) throw refused();
                }) : bound.cache().pipelineDerivedSchemas(original);
            }
            @Override public Optional<DerivedSchema> latest(String pipeline, String step) {
                return view(pipeline).latest(pipeline, step);
            }
            @Override public Optional<DerivedSchema> pinned(String pipeline, String step) {
                return view(pipeline).pinned(pipeline, step);
            }
            @Override public void record(String pipeline, String step, Map<String,String> columns, String statement, String from, String by) { throw refused(); }
            @Override public void pin(String pipeline, String step, long version) { throw refused(); }
            @Override public void delete(String pipeline) { throw refused(); }
        };
    }
    @Override public RateHistoryStore rateHistory() {
        if (pass != null) return pass.rateHistory();
        return new RateHistoryStore() {
            private RateHistoryStore view(String pipeline) { return bound.cache().pipelineRateHistory(scope(pipeline)); }
            @Override public void append(RateSample sample) { throw refused(); }
            @Override public Page readPage(String pipeline, Instant from, Instant to, Key after, int limit) {
                return unprepared(pipeline) ? new Page(List.of(), false) : view(pipeline).readPage(pipeline, from, to, after, limit);
            }
            @Override public Optional<Entry> read(String pipeline, Key key) { return unprepared(pipeline) ? Optional.empty() : view(pipeline).read(pipeline, key); }
            @Override public Optional<Entry> predecessor(String pipeline, Instant at) { return unprepared(pipeline) ? Optional.empty() : view(pipeline).predecessor(pipeline, at); }
            @Override public Optional<Entry> successor(String pipeline, Instant at) { return unprepared(pipeline) ? Optional.empty() : view(pipeline).successor(pipeline, at); }
            @Override public void deleteAll(String pipeline) { throw refused(); }
            @Override public Duration retention() { active(); return bound.cache().rateRetention(); }
        };
    }
    @Override public SrsMetaStore meta() {
        return new SrsMetaStore() {
            private SrsStores stores() { active(); return bound.cache().srs(business(), ThreeLayerStorePort.this::active); }
            @Override public Optional<SrsMeta> read(String chain) {
                var stores = stores();
                return stores.chains().load(chain).map(original -> {
                    var topology = stores.consumers().attached(chain);
                    var cursors = topology.consumers().stream().map(member -> stores.consumers().cursor(member.context()).orElseThrow(ThreeLayerStorePort::refused)).toList();
                    return new SrsMeta(chain, original.checkpoint() == null ? null : original.checkpoint().position(), cursors,
                            List.of(), original.retention(), original.epoch(), original.checkpointAt(), original.checkpoint() != null);
                });
            }
            @Override public List<String> miningChainIdsWithConsumer(String pipeline) {
                if (unprepared(pipeline)) return List.of();
                return stores().consumers().forPipeline(scope(pipeline)).stream().filter(member -> !member.detached())
                        .map(member -> member.context().key().miningChainId()).distinct().toList();
            }
            @Override public void create(String chain, String retention) { throw refused(); }
            @Override public void advanceSourceReadOffset(String chain, ChainPosition position) { throw refused(); }
            @Override public void rewindSourceReadOffset(String chain, String token) { throw refused(); }
            @Override public void upsertConsumerOffset(String chain, ConsumerOffset offset) { throw refused(); }
            @Override public void advanceConsumerReadSeq(String chain, String pipeline, String table, long sequence) { throw refused(); }
            @Override public void advanceSinkAcked(String chain, String pipeline, ChainPosition position) { throw refused(); }
            @Override public void setCdcStart(String chain, String pipeline, String token, long epoch) { throw refused(); }
            @Override public long openEpoch(String chain) { throw refused(); }
            @Override public void appendSchemaVersion(String chain, SchemaVersion version) { throw refused(); }
            @Override public void markSnapshotComplete(String chain, String pipeline, String table) { throw refused(); }
            @Override public void detachConsumer(String chain, String pipeline) { throw refused(); }
            @Override public void dropChain(String chain) { throw refused(); }
        };
    }
    @Override public KeyedStateStore keyedState() { throw refused(); }
    @Override public NestDeadLetterStore nestDeadLetters() { throw refused(); }
    @Override public OperatorStateStores operatorStateStores() {
        return new OperatorStateStores() {
            @Override public String defaultDatabase() { active(); return bound.binding().cache().database(); }
            @Override public OperatorStateStore inDatabase(String database) { throw refused(); }
        };
    }
    static TapstateException refused() { return new TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null); }
}
