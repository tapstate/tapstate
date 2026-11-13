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
import java.util.function.BiConsumer;
import java.util.function.Supplier;

/** A fixed admitted cache owner; ordinary queries never acquire permission for unscoped runtime writes. */
final class ThreeLayerStorePort implements StorePort {
    private final MongoConfigurationDomain configuration;
    private final StorageRuntime.Bound bound;
    private final PipelineConvergencePass pass;
    private final Consumer<PipelineIntentScope> prepare;
    private final BiConsumer<PipelineIntentScope, PipelinePreparationInputs> prepareCaptured;
    private final Control control;
    private final boolean deferFirstStart;
    private record Control(PipelineIntentScope scope, WorkloadClaimFence claim, Runnable active,
            MongoConfigurationDomain.AuthoringModels models) {}

    ThreeLayerStorePort(MongoConfigurationDomain configuration, StorageRuntime.Bound bound,
            Consumer<PipelineIntentScope> prepare) {
        this(configuration, bound, prepare, (scope, ignored) -> prepare.accept(scope));
    }
    ThreeLayerStorePort(MongoConfigurationDomain configuration, StorageRuntime.Bound bound,
            Consumer<PipelineIntentScope> prepare, BiConsumer<PipelineIntentScope, PipelinePreparationInputs> prepareCaptured) {
        this(configuration, bound, prepare, prepareCaptured, false);
    }
    ThreeLayerStorePort(MongoConfigurationDomain configuration, StorageRuntime.Bound bound,
            Consumer<PipelineIntentScope> prepare, BiConsumer<PipelineIntentScope, PipelinePreparationInputs> prepareCaptured,
            boolean deferFirstStart) {
        this(configuration, bound, prepare, prepareCaptured, null, null, deferFirstStart);
    }
    private ThreeLayerStorePort(MongoConfigurationDomain configuration, StorageRuntime.Bound bound,
            Consumer<PipelineIntentScope> prepare, BiConsumer<PipelineIntentScope, PipelinePreparationInputs> prepareCaptured,
            PipelineConvergencePass pass, Control control, boolean deferFirstStart) {
        this.configuration = Objects.requireNonNull(configuration); this.bound = Objects.requireNonNull(bound);
        this.prepare = Objects.requireNonNull(prepare); this.prepareCaptured = Objects.requireNonNull(prepareCaptured);
        this.pass = pass; this.control = control; this.deferFirstStart = deferFirstStart;
        bound.requireBusiness();
    }
    ThreeLayerStorePort forPass(PipelineConvergencePass original) {
        original.requirePipeline(original.scope().pipelineId());
        if (!business().equals(new CacheBusinessScope(original.scope().clusterId(), original.scope().bindingVersion(),
                original.scope().cacheIncarnation()))) throw refused();
        return new ThreeLayerStorePort(configuration, bound, prepare, prepareCaptured, original, null, false);
    }
    ThreeLayerStorePort forControl(PipelineIntentScope scope, WorkloadClaimFence claim, Runnable guard) {
        Objects.requireNonNull(guard).run();
        if (!scope.equals(scope(scope.pipelineId())) || claim.key().type() != WorkloadClaimType.PIPELINE_ACTUATION
                || !claim.key().resourceId().equals(scope.pipelineId()) || !claim.key().clusterId().equals(scope.clusterId())) throw refused();
        return new ThreeLayerStorePort(configuration, bound, prepare, prepareCaptured, null, new Control(scope, claim, guard, null), false);
    }
    ThreeLayerStorePort forUnpreparedControl(MongoConfigurationDomain.AuthoringModels models, WorkloadClaimFence node, Runnable guard) {
        var scope = models.scope();
        Objects.requireNonNull(guard).run();
        if (!scope.equals(scope(scope.pipelineId())) || node.key().type() != WorkloadClaimType.NODE_SESSION
                || !node.key().clusterId().equals(scope.clusterId())) throw refused();
        return new ThreeLayerStorePort(configuration, bound, prepare, prepareCaptured, null, new Control(scope, node, guard, models), false);
    }
    private void active() {
        bound.requireBusiness();
        if (pass != null) pass.requirePipeline(pass.scope().pipelineId());
        if (control != null) control.active().run();
    }
    CacheBusinessScope business() { return bound.preparation().businessScope(); }

    void requirePublishedGeneration(String pipelineId, long generation) {
        bound.requireBusiness();
        if (control == null || !control.scope().pipelineId().equals(pipelineId)
                || control.scope().stateGeneration() != generation) throw refused();
    }

    /** For an unprepared publication, existence, generation and intent share one CONFIG snapshot. */
    Optional<PipelineIntentScope> draftAuthoringScope(String pipelineId) {
        bound.requireBusiness();
        var header = configuration.pipelineDisplayHeaders(List.of(pipelineId), true).get(pipelineId);
        if (header == null || header.recovering()
                || header.preparation().phase() != PipelinePreparation.Phase.UNASSIGNED) return Optional.empty();
        var scope = activeScope(pipelineId, header.mapping());
        if (header.intentFailure().isPresent()) throw header.intentFailure().orElseThrow();
        var desired = header.desired().orElse(null);
        if (desired != null && !io.tapstate.control.core.LivePipelines.isAtRest(null, desired)) {
            throw new TapstateException(ActuationError.SCHEMA_SYNC_WHILE_RUNNING,
                    Map.of("pipeline", scope.pipelineId(), "state", PipelineState.NEW.name(),
                            "desired", desired.targetState().name()), null);
        }
        return Optional.of(scope);
    }
    Runnable readingGuard(String pipeline) {
        return planningScope(pipeline).verify();
    }
    record PlanningScope(PipelineIntentScope scope, Runnable verify) {}
    PlanningScope planningScope(String pipeline) {
        active();long revision=configuration.configurationRevision();var original=scope(pipeline);
        return new PlanningScope(original,
                () -> {active();if(revision!=configuration.configurationRevision() || !original.equals(scope(pipeline)))throw refused();});
    }
    PlanningScope planningScope(PipelineIntentScope original) {
        active();
        if (!business().equals(new CacheBusinessScope(original.clusterId(), original.bindingVersion(), original.cacheIncarnation())))
            throw refused();
        return new PlanningScope(original, this::active);
    }

    SchemaStore startSchemas() {
        active();
        return bound.cache().admittedStartSchemas(business(), this::active);
    }

    Optional<SourceCaptureResolution> sharedReading(io.tapstate.core.model.PipelineResource pipeline,
            io.tapstate.core.model.SourceResource source,SourceCaptureResolution canonical,boolean requireAttachment) {
        return sharedReading(pipeline, source, canonical, requireAttachment,
                () -> bound.cache().srs(business(), this::active), true);
    }

    SrsStores startPlanningSrs() {
        active();
        return bound.cache().startPlanningSrs(business(), this::active);
    }

    Optional<SourceCaptureResolution> sharedReadingForStart(io.tapstate.core.model.PipelineResource pipeline,
            io.tapstate.core.model.SourceResource source, SourceCaptureResolution canonical, Supplier<SrsStores> stores) {
        return sharedReading(pipeline, source, canonical, true, stores, false);
    }

    private Optional<SourceCaptureResolution> sharedReading(io.tapstate.core.model.PipelineResource pipeline,
            io.tapstate.core.model.SourceResource source, SourceCaptureResolution canonical, boolean requireAttachment,
            Supplier<SrsStores> stores, boolean verifyMissingCapture) {
        active();
        if(pipeline.sources().stream().anyMatch(reference -> reference.id().equals(source.id())
                && reference instanceof io.tapstate.core.model.SourceRef.Spec spec && !spec.srs())
                || pipeline.settings()!=null && pipeline.settings().readMode()==io.tapstate.core.model.ReadMode.SNAPSHOT_ONLY)
            return Optional.of(canonical);
        var original=scope(pipeline.id());String capture=io.tapstate.core.model.canonical.SourceCaptureIdentity.sharedCapture(canonical.chainId().value());
        var prepared=configuration.capturePreparations().read(business(),capture);
        if(prepared.isEmpty()) {
            // Start planning can leave the absent-history proof to the transaction that creates capture.
            if (verifyMissingCapture && (bound.cache().workloadClaims(business())
                    .read(new WorkloadClaimKey(business().clusterId(), WorkloadClaimType.CAPTURE, capture)).isPresent()
                    || stores.get().chains().load(canonical.chainId().value()).isPresent()))
                throw new TapstateException(IoError.CAPTURE_STATE_REBUILD_REQUIRED,Map.of("capture",capture),null);
            return requireAttachment?Optional.empty():Optional.of(canonical);
        }
        var fixed=prepared.orElseThrow();
        if(fixed.mode()!=CapturePreparation.Mode.SHARED_SRS || !business().equals(fixed.scope()) || !capture.equals(fixed.captureId())
                || !canonical.chainId().value().equals(fixed.canonicalChainId())
                || !fixed.sourceIdentityHash().equals(io.tapstate.core.model.canonical.SourceCaptureIdentity.physical(source.connector(),source.config())))throw refused();
        var topology=stores.get().consumers().attached(fixed.chainId());
        var reader=topology.consumers().stream().filter(member -> member.context().key().pipelineId().equals(pipeline.id())
                && member.context().key().sourceId().equals(source.id())).findFirst();
        active();if(!original.equals(scope(pipeline.id())))throw refused();
        if(reader.isEmpty() && requireAttachment)return Optional.empty();
        if(reader.isPresent() && reader.orElseThrow().context().stateGeneration()!=original.stateGeneration())throw refused();
        return Optional.of(canonical.onPhysicalChain(fixed.canonicalChainId(),fixed.chainId()));
    }
    PipelineIntentScope scope(String pipeline) {
        active();
        if (pass != null) { pass.requirePipeline(pipeline); return pass.scope(); }
        if (control != null) { if (!control.scope().pipelineId().equals(pipeline)) throw refused(); return control.scope(); }
        return activeScope(pipeline, currentMapping(pipeline));
    }
    private PipelineStateMapping currentMapping(String pipeline) {
        active();
        var mapping = configuration.pipelineStates().read(pipeline).orElseThrow(ThreeLayerStorePort::refused);
        active();
        if (!pipeline.equals(mapping.pipelineId())) throw refused();
        return mapping;
    }
    private PipelineIntentScope activeScope(String pipeline, PipelineStateMapping mapping) {
        if (!pipeline.equals(mapping.pipelineId()) || mapping.phase() != PipelineStateMapping.Phase.ACTIVE) throw refused();
        var cache = business();
        return new PipelineIntentScope(cache.clusterId(), cache.bindingVersion(), cache.cacheIncarnation(), pipeline, mapping.stateGeneration());
    }
    private boolean unprepared(String pipeline) {
        active();
        return configuration.preparations().read(pipeline).orElseThrow(ThreeLayerStorePort::refused).phase()
                == PipelinePreparation.Phase.UNASSIGNED;
    }
    @Override public boolean pipelineRecoveryRequired(String pipeline) {
        var mapping = currentMapping(pipeline);
        if (mapping.phase() == PipelineStateMapping.Phase.REBUILDING) return false;
        var original = activeScope(pipeline, mapping);
        var recoveries = configuration.cacheRecoveries();
        var summary = recoveries.read(pipeline);
        if (summary.isEmpty()) return false;
        if (!summary.orElseThrow().targetScope().equals(original)) {
            if (!mapping.equals(currentMapping(pipeline)) || !summary.equals(recoveries.read(pipeline))) return false;
            throw refused();
        }
        var preparation = configuration.preparations().read(pipeline).orElseThrow(ThreeLayerStorePort::refused);
        boolean required = preparation.phase() != PipelinePreparation.Phase.COMPLETE;
        if (!required) {
            var pending = configuration.recoveryCommands().current(bound.preparation().authScope());
            required = pending.filter(plan -> plan.originalScope().equals(bound.preparation().authScope())
                    && plan.phase() != StorageRecoveryCommands.Phase.COMPLETE
                    && plan.manifest().selection().extent() == StorageRecoveryCommands.Extent.PIPELINE
                    && plan.manifest().selection().pipelineIds().contains(pipeline)).isPresent()
                    && configuration.intents().load(pipeline).filter(intent -> !intent.deleted() && intent.scope().equals(original)
                            && intent.desired().targetState() == PipelineState.STOPPED).isPresent();
        }
        return mapping.equals(currentMapping(pipeline)) && summary.equals(recoveries.read(pipeline)) && required;
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

    @Override public int endTransactionsLeftOpenBy(String processId) {
        Objects.requireNonNull(processId, "processId");
        // Rollback of a departed boot must remain possible while business readiness is unavailable.
        bound.requireActive();
        int ended = 0;
        RuntimeException failure = null;
        try { ended = bound.cache().endTransactionsLeftOpenBy(processId); }
        catch (RuntimeException unavailable) { failure = unavailable; }
        try { ended += configuration.endTransactionsLeftOpenBy(processId); }
        catch (RuntimeException unavailable) {
            if (failure == null) failure = unavailable;
            else if (failure != unavailable) failure.addSuppressed(unavailable);
        }
        if (failure != null) throw failure;
        return ended;
    }

    @Override public DesiredStore desired() {
        return new DesiredStore() {
            @Override public void save(DesiredState desired) {
                var original = scope(desired.pipelineId());
                var intents = configuration.intents().guardedBy(ThreeLayerStorePort.this::active);
                var previous = intents.load(desired.pipelineId());
                if (previous.isPresent() && !previous.orElseThrow().scope().equals(original)) throw refused();
                submit(desired, original, previous.map(PipelineIntentSnapshot::desired),
                        previous.map(PipelineIntentSnapshot::intentRevision).orElse(0L), Optional.empty());
            }
            @Override public Optional<StartEntry> startEntry(String pipeline) {
                if (!deferFirstStart) return Optional.empty();
                active();
                var snapshot = configuration.startSnapshot(pipeline);
                active();
                return snapshot.map(value -> {
                    var original = activeScope(pipeline, value.mapping());
                    var current = value.intent();
                    if (current.isPresent() && !current.orElseThrow().scope().equals(original)) throw refused();
                    var desired = current.filter(valueAtStart -> !valueAtStart.deleted()).map(PipelineIntentSnapshot::desired);
                    var intent = new StartIntent(desired, Optional.of(original), current.map(PipelineIntentSnapshot::intentRevision).orElse(0L));
                    var checkpoint = desired.isPresent() && value.preparation().phase() == PipelinePreparation.Phase.COMPLETE
                            ? bound.cache().pipelineStates(original).read(pipeline) : Optional.<CheckpointDoc>empty();
                    active();
                    return new StartEntry(value.pipeline(), intent, checkpoint);
                });
            }
            @Override public StartIntent readForStart(String pipeline) {
                var original = scope(pipeline);
                var current = configuration.intents().load(pipeline);
                if (current.isPresent() && !current.orElseThrow().scope().equals(original)) throw refused();
                return new StartIntent(current.filter(value -> !value.deleted()).map(PipelineIntentSnapshot::desired),
                        Optional.of(original), current.map(PipelineIntentSnapshot::intentRevision).orElse(0L));
            }
            @Override public void saveStart(DesiredState desired, StartIntent original) {
                saveStart(desired, original, Optional.empty());
            }
            @Override public boolean defersStart(DesiredState desired, StartIntent original, Optional<StartPreparation> captured) {
                return deferFirstStart && original.revision() == 0 && desired.targetState() == PipelineState.RUNNING
                        && !desired.purgeState() && captured.isPresent();
            }
            @Override public void saveStart(DesiredState desired, StartIntent original, Optional<StartPreparation> captured) {
                active();
                var scope = original.scope().orElseThrow(ThreeLayerStorePort::refused);
                if (!scope.pipelineId().equals(desired.pipelineId())) throw refused();
                if (defersStart(desired, original, captured)) {
                    var inputs = captured.orElseThrow();
                    if (!scope.equals(inputs.scope()) || !configuration.intents().guardedBy(ThreeLayerStorePort.this::active)
                            .compareAndSetStart(scope, original.revision(), desired, inputs.inputs())) throw refused();
                    return;
                }
                submit(desired, scope, original.desired(), original.revision(), captured);
            }
            private void submit(DesiredState desired, PipelineIntentScope original, Optional<DesiredState> previous,
                    long revision, Optional<StartPreparation> captured) {
                // An immediate start may supersede an explicitly clearing stop before convergence.
                // That same fenced intent must finish replacement before preparing the new selection.
                boolean clearsFirst = desired.purgeState() && desired.reassemble()
                        && previous.filter(value -> value.targetState() == PipelineState.STOPPED
                                && value.purgeState()).isPresent();
                if (desired.targetState() == PipelineState.RUNNING && !clearsFirst) {
                    if (captured.isEmpty()) prepare.accept(original);
                    else {
                        var inputs = captured.orElseThrow();
                        if (!original.equals(inputs.scope()) || !desired.revision().equals(inputs.inputs().pipelineHash()))
                            throw refused();
                        prepareCaptured.accept(original, inputs.inputs());
                    }
                }
                if (!configuration.intents().guardedBy(ThreeLayerStorePort.this::active)
                        .compareAndSet(original, revision, desired)) throw refused();
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
    @Override public Map<String, PipelineCatalogState> pipelineCatalogStates(List<String> ids) {
        return displayStates(ids, true);
    }

    private Map<String, PipelineCatalogState> displayStates(List<String> ids, boolean includeIntents) {
        if (pass != null || control != null || ids.isEmpty()) return Map.of();
        active();
        var result = new HashMap<String, PipelineCatalogState>();
        for (int offset = 0; offset < ids.size(); offset += 200) {
            var page = ids.subList(offset, Math.min(offset + 200, ids.size()));
            var headers = configuration.pipelineDisplayHeaders(page, includeIntents);
            var scopes = new ArrayList<PipelineIntentScope>();
            for (var entry : headers.entrySet()) {
                var header = entry.getValue();
                if (includeIntents && header.recovering()) continue;
                if (header.preparation().phase() == PipelinePreparation.Phase.COMPLETE) {
                    var original = activeScope(entry.getKey(), header.mapping());
                    if (original.equals(header.preparation().scope())) scopes.add(original);
                } else {
                    result.put(entry.getKey(), new PipelineCatalogState(startFailure(entry.getKey(), header), header.desired(), header.intentFailure(), false,
                            io.tapstate.core.lifecycle.PipelineState.NEW));
                }
            }
            if (!scopes.isEmpty()) {
                var cached = bound.cache().pipelineObservationBatch(scopes, this::active, includeIntents);
                var after = configuration.pipelineDisplayHeaders(scopes.stream().map(PipelineIntentScope::pipelineId).toList(), false);
                for (var entry : cached.entrySet()) {
                    var header = headers.get(entry.getKey());
                    var current = after.get(entry.getKey());
                    if (current != null && current.mapping().equals(header.mapping())
                            && current.preparation().equals(header.preparation())) {
                        result.put(entry.getKey(), new PipelineCatalogState(entry.getValue().observation(),
                                header.desired(), header.intentFailure(), entry.getValue().runningObservationUnavailable(),
                                entry.getValue().checkpoint(), activeScope(entry.getKey(), header.mapping())));
                    }
                }
            }
        }
        active();
        return Map.copyOf(result);
    }

    @Override public ObservationStore observations() {
        if (pass != null) return pass.observations();
        return new ObservationStore() {
            @Override public Map<String, Observation> readAll(List<String> ids) {
                var batch = displayStates(ids, false);
                var result = new HashMap<String, Observation>();
                for (var id : ids) {
                    var item = batch.get(id);
                    (item == null ? read(id) : item.observation()).ifPresent(value -> result.put(id, value));
                }
                return Map.copyOf(result);
            }
            @Override public Optional<Observation> read(String pipeline) {
                if (control != null) return readObservation(pipeline, scope(pipeline));
                active();
                var retained = configuration.pipelineStates().read(pipeline);
                active();
                if (retained.isPresent() && !pipeline.equals(retained.orElseThrow().pipelineId())) throw refused();
                if (retained.isEmpty() || retained.orElseThrow().phase() == PipelineStateMapping.Phase.RETIRED) {
                    // A retained deletion fence is not an observable run. Only CONFIG absence can
                    // classify this as unknown; a live definition with a missing mapping stays fenced.
                    boolean absent = configuration.artifacts().get(pipeline)
                            .filter(io.tapstate.core.model.PipelineResource.class::isInstance).isEmpty();
                    active();
                    if (absent) return Optional.empty();
                    throw refused();
                }
                var mapping = retained.orElseThrow();
                if (mapping.phase() == PipelineStateMapping.Phase.REBUILDING) return Optional.empty();
                var original = activeScope(pipeline, mapping);
                try {
                    var observation = readObservation(pipeline, original);
                    // A display read never carries an old observation across a concurrent state rebuild.
                    return mapping.equals(currentMapping(pipeline)) ? observation : Optional.empty();
                } catch (TapstateException failure) {
                    boolean retiredScope = failure.code() == IoError.WORKLOAD_CLAIM_FENCED
                            || failure.code() == IoError.CACHE_BUSINESS_FENCED;
                    if (!retiredScope || mapping.equals(currentMapping(pipeline))) throw failure;
                    return Optional.empty();
                }
            }
            @Override public void save(Observation observation) { throw refused(); }
            @Override public void delete(String pipeline) { throw refused(); }
        };
    }
    private static Optional<Observation> startFailure(String pipeline, MongoConfigurationDomain.PipelineDisplayHeader header) {
        return header.startFailure().map(code -> new Observation(pipeline, PipelineState.FAILED, Map.of(), Map.of(), Map.of(),
                new io.tapstate.core.lifecycle.ObservationFailure(code.code(), Map.of()), null, List.of()));
    }
    private Optional<Observation> readObservation(String pipeline, PipelineIntentScope original) {
        var preparation = configuration.preparations().read(pipeline).orElseThrow(ThreeLayerStorePort::refused);
        if (preparation.phase() != PipelinePreparation.Phase.COMPLETE) {
            if (!deferFirstStart) return Optional.empty();
            var header = configuration.pipelineDisplayHeaders(List.of(pipeline), true).get(pipeline);
            return header == null || header.mapping().stateGeneration() != original.stateGeneration()
                    ? Optional.empty() : startFailure(pipeline, header);
        }
        if (!original.equals(preparation.scope())) throw refused();
        return bound.cache().pipelineObservations(original).read(pipeline);
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
    private Optional<DirectChannelProgress> directProgress(String chain) {
        active();
        for (var resource : configuration.artifacts().list()) {
            if (!(resource instanceof io.tapstate.core.model.PipelineResource pipeline)) continue;
            var mode = pipeline.settings() == null || pipeline.settings().readMode() == null
                    ? io.tapstate.core.model.ReadMode.SNAPSHOT_AND_CDC : pipeline.settings().readMode();
            if (mode == io.tapstate.core.model.ReadMode.SNAPSHOT_ONLY) continue;
            for (var reference : pipeline.sources()) {
                if (!(reference instanceof io.tapstate.core.model.SourceRef.Spec spec) || spec.srs()) continue;
                var source = StoredArtifacts.requireSource(configuration.artifacts(), reference.id());
                var key = source.srs() == null ? null : source.srs().key();
                if (!chain.equals(io.tapstate.core.model.canonical.SourceCaptureIdentity.directChannel(source.connector(), source.config(), key, pipeline.id(), source.id()))) continue;
                if (unprepared(pipeline.id())) return Optional.empty();
                var tables = PipelineTableSelection.resolve(pipeline, source, SourceDiscovery.model(this, source));
                var channel = DirectChannelPreparation.of(scope(pipeline.id()), source, mode,
                        pipeline.settings() == null ? null : pipeline.settings().startFrom(), tables);
                var original = bound.cache().directChannels(business(), this::active).load(channel.context());
                return Optional.of(original);
            }
        }
        return Optional.empty();
    }
    private Optional<SrsMeta> directPosition(String chain) {
        return directProgress(chain).map(original -> {
            var pending = original.resumeOverride();
            var position = pending == null ? original.cursor().sinkAcked() : new ChainPosition(null, pending.token());
            return new SrsMeta(chain, position, List.of(original.cursor()), List.of(), null, original.epoch(),
                    pending == null ? null : pending.editedAt(), false);
        });
    }
    Optional<ConsumerOffset> sinkOffset(String pipeline, String source, String chain) {
        var original = scope(pipeline);
        Runnable guard = () -> {
            active();
            if (!original.equals(scope(pipeline))) throw refused();
        };
        var context = new io.tapstate.spi.store.SrsConsumerContext(
                new io.tapstate.spi.store.SrsConsumerKey(chain, pipeline, source), original.stateGeneration());
        var offset = bound.cache().srs(business(), guard).consumers().cursor(context);
        guard.run();
        return offset;
    }

    Optional<SrsMeta> startProgress(String chain, SrsStores stores) {
        var shared = stores.chains().load(chain);
        if (shared.isEmpty()) return directPosition(chain);
        return shared.map(original -> {
            var topology = stores.consumers().attached(chain);
            var cursors = topology.consumers().stream().map(member -> stores.consumers().cursor(member.context())
                    .orElseThrow(ThreeLayerStorePort::refused)).toList();
            return new SrsMeta(chain, original.resumeOverride() != null ? new ChainPosition(null, original.resumeOverride().token())
                    : original.checkpoint() == null ? null : original.checkpoint().position(), cursors,
                    List.of(), original.retention(), original.epoch(), original.resumeOverride() != null
                            ? original.resumeOverride().editedAt() : original.checkpointAt(),
                    original.resumeOverride() == null && original.checkpoint() != null);
        });
    }

    @Override public SrsMetaStore meta() {
        return new SrsMetaStore() {
            private SrsStores stores() { active(); return bound.cache().srs(business(), ThreeLayerStorePort.this::active); }
            @Override public Optional<SrsMeta> read(String chain) {
                return startProgress(chain, stores());
            }
            @Override public Optional<DurableSourceRead> durableSourceRead(String chain) {
                var shared = stores().chains().load(chain);
                if (shared.isPresent()) {
                    var original = shared.orElseThrow();
                    if (original.resumeOverride() != null || original.checkpoint() == null
                            || original.checkpoint().position().order().epoch() != original.epoch()) return Optional.empty();
                    return Optional.of(new DurableSourceRead(original.checkpoint().position(), true));
                }
                return directProgress(chain).flatMap(original -> {
                    var point = original.cursor().sinkAcked();
                    return original.resumeOverride() != null || original.current() == null || original.current().anchor() == null
                            || point == null || point.order().epoch() != original.epoch() ? Optional.empty() : Optional.of(new DurableSourceRead(point, false));
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
