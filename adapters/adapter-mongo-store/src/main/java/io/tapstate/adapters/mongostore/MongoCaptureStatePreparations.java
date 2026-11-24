package io.tapstate.adapters.mongostore;

import com.mongodb.*;
import com.mongodb.client.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.*;
import io.tapstate.core.model.canonical.*;
import io.tapstate.spi.store.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.bson.Document;

/** One-time CONFIG insertion authority, constructed only after the winning transaction commits. */
final class MongoCaptureStatePreparations implements CaptureStatePreparations {
    private static final TransactionOptions TRANSACTION=TransactionOptions.builder().readConcern(ReadConcern.SNAPSHOT).writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();
    private final MongoClient client;private final MongoDatabase database;private final String cluster;
    private final MongoStorageBindingStore bindings;private final MongoConfigurationArtifactStore artifacts;private final Runnable active;
    MongoCaptureStatePreparations(MongoClient client,MongoDatabase database,String cluster,MongoStorageBindingStore bindings,MongoConfigurationArtifactStore artifacts,Runnable active) {
        this.client=Objects.requireNonNull(client,"client");this.database=Objects.requireNonNull(database,"database");this.cluster=Objects.requireNonNull(cluster,"cluster");this.bindings=Objects.requireNonNull(bindings,"bindings");this.artifacts=Objects.requireNonNull(artifacts,"artifacts");this.active=Objects.requireNonNull(active,"active");
        if(!bindings.belongsTo(client,database.getName()) || !artifacts.belongsTo(client,database.getName()))throw new IllegalArgumentException("capture preparation requires one original CONFIG client");
    }
    static void addRetainedArtifactHashes(ClientSession session, MongoDatabase database, java.util.Set<String> retained) {
        var preparations = SystemCollections.CAPTURE_PREPARATIONS.on(database);
        preparations.find(session, new Document("artifactHash", new Document("$exists", true)))
                .forEach(row -> retained.add(MongoCapturePreparationFacts.artifactHash(row)));
    }

    @Override public CaptureStatePreparations guardedBy(Runnable guard){Objects.requireNonNull(guard,"guard");return new MongoCaptureStatePreparations(client,database,cluster,bindings,artifacts,() -> {active.run();guard.run();});}
    @Override public Optional<CapturePreparation> read(CacheBusinessScope scope,String capture) {
        Objects.requireNonNull(scope,"scope");if(capture==null || capture.isBlank())throw new IllegalArgumentException("blank capture identity");
        return transaction(session -> {requireScope(session,scope);return Optional.ofNullable(SystemCollections.CAPTURE_PREPARATIONS.on(database).find(session,new Document("_id",MongoCapturePreparationFacts.id(scope,capture))).first()).map(MongoCapturePreparationFacts::available);});
    }
    @Override public Optional<RequiredNoteOrigin> inspectSharedOrigin(CacheBusinessScope scope,String sourceId,String sourceHash) {
        return inspectSharedOrigin(scope,sourceId,sourceHash,null,null);
    }
    @Override public Optional<RequiredNoteOrigin> inspectSharedOrigin(CacheBusinessScope scope,String sourceId,String sourceHash,String pipelineId,String pipelineHash) {
        return transaction(session -> {
            requireScope(session,scope);
            var reference = verifiedReference(session,pipelineId,pipelineHash,sourceId);
            var source=artifacts.getInTransaction(session,reference.id()).filter(SourceResource.class::isInstance).map(SourceResource.class::cast).orElseThrow(MongoCaptureStatePreparations::fenced);
            if(!CanonicalHash.of(source).equals(sourceHash))throw fenced();
            var settings = PipelineSourceConfig.settings(reference,source);
            String chain = PipelineSourceConfig.chain(reference,source);
            String capture=SourceCaptureIdentity.sharedCapture(chain);
            var preparations=SystemCollections.CAPTURE_PREPARATIONS.on(database);
            var current=preparations.find(session,new Document("_id",MongoCapturePreparationFacts.id(scope,capture))).first();
            if(current!=null) {
                var original=MongoCapturePreparationFacts.available(current);
                if(original.mode()!=CapturePreparation.Mode.SHARED_SRS || !original.canonicalChainId().equals(chain)
                        || !original.sourceIdentityHash().equals(SourceCaptureIdentity.physical(source.connector(),settings)))throw fenced();
            }
            var prior=preparations.find(session,new Document("_id.clusterId",scope.clusterId())
                    .append("_id.bindingVersion",scope.bindingVersion()).append("_id.capture",OperatorStateText.encode(capture))).first();
            if(prior!=null){var original=MongoCapturePreparationFacts.decode(prior);if(original.mode()!=CapturePreparation.Mode.SHARED_SRS || !original.canonicalChainId().equals(chain)
                    || !original.sourceIdentityHash().equals(SourceCaptureIdentity.physical(source.connector(),settings)))throw fenced();}
            return artifacts.configurationNotes().inspectSharedOrigin(session,new RequiredNoteOwner(scope.bindingVersion(),"pdk.chain."+chain,RequiredNoteOwner.Kind.CHAIN,chain,0),prior!=null);
        });
    }
    @Override public CaptureSharedHistory inspectSharedHistory(CapturePreparation original, String sourceId, String sourceHash) {
        return inspectSharedHistory(original,sourceId,sourceHash,null,null);
    }
    @Override public CaptureSharedHistory inspectSharedHistory(CapturePreparation original,String sourceId,String sourceHash,String pipelineId,String pipelineHash) {
        Objects.requireNonNull(original, "original");
        if (original.mode() != CapturePreparation.Mode.SHARED_SRS) throw fenced();
        return readSnapshot(() -> {
            var captured = capturedHistory(original, sourceId, pipelineId, pipelineHash);
            if (captured.isPresent()) return inspectSharedHistory(null, original, captured.orElseThrow().reference(), sourceHash, captured);
            return transaction(session -> inspectSharedHistory(session, original, verifiedReference(session, pipelineId, pipelineHash, sourceId), sourceHash, Optional.empty()));
        });
    }

    private CaptureSharedHistory inspectSharedHistory(ClientSession session, CapturePreparation original, SourceRef reference,
            String sourceHash, Optional<CapturedHistory> captured) {
        if (captured.isEmpty()) requireScope(session, original.scope());
        var source = captured.map(CapturedHistory::source)
                .orElseGet(() -> artifacts.getInTransaction(session, reference.id()).filter(SourceResource.class::isInstance)
                        .map(SourceResource.class::cast).orElseThrow(MongoCaptureStatePreparations::fenced));
        String chain = PipelineSourceConfig.chain(reference, source);
        if (!CanonicalHash.of(source).equals(sourceHash) || !original.canonicalChainId().startsWith("mcv-") && !chain.equals(original.canonicalChainId())
                || !SourceCaptureIdentity.sharedCapture(original.canonicalChainId()).equals(original.captureId())
                || !SourceCaptureIdentity.physical(source.connector(), PipelineSourceConfig.settings(reference, source)).equals(original.sourceIdentityHash())) throw fenced();
        var table = SystemCollections.CAPTURE_PREPARATIONS.on(database);
        var retained = captured.map(CapturedHistory::capture)
                .orElseGet(() -> table.find(session,
                        new Document("_id", MongoCapturePreparationFacts.id(original.scope(), original.captureId()))).first());
        var current = MongoCapturePreparationFacts.available(retained);
        var artifactHash = MongoCapturePreparationFacts.artifactHash(retained);
        if (!current.equals(original)) throw fenced();
        boolean historical = current.replacement();
        var filter = new Document("_id.clusterId", original.scope().clusterId())
                .append("_id.bindingVersion", original.scope().bindingVersion())
                .append("_id.capture", OperatorStateText.encode(original.captureId()))
                .append("_id.cacheIncarnation", new Document("$ne", original.scope().cacheIncarnation()));
        var historyRows = captured.map(CapturedHistory::prior)
                .orElseGet(() -> table.find(session, filter).batchSize(64).into(new ArrayList<>()));
        for (var historyRow : historyRows) {
                var prior = MongoCapturePreparationFacts.decode(historyRow);
                if (prior.mode() != original.mode() || !prior.canonicalChainId().equals(original.canonicalChainId())
                        || !Objects.equals(MongoCapturePreparationFacts.artifactHash(historyRow), artifactHash)
                        || !prior.captureId().equals(original.captureId())
                        || !prior.sourceIdentityHash().equals(original.sourceIdentityHash())) throw fenced();
                historical = true;
        }
        var owner = new RequiredNoteOwner(original.scope().bindingVersion(),
                "pdk.chain." + original.canonicalChainId(), RequiredNoteOwner.Kind.CHAIN, original.canonicalChainId(), 0);
        if (captured.isPresent()) artifacts.configurationNotes().inspectCapturedSharedOrigin(captured.orElseThrow().snapshot(), owner);
        else artifacts.configurationNotes().inspectSharedOrigin(session, owner, true);
        return new CaptureSharedHistory(current, historical);
    }
    @Override public CaptureCreationResult issueForStartup(CapturePreparationRequest request, String canonicalChain) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(canonicalChain, "canonicalChain");
        var retained = retainedStart(request, canonicalChain);
        return retained.<CaptureCreationResult>map(CaptureCreationResult.AlreadyIssued::new)
                .orElseGet(() -> issueFirstCreate(request));
    }

    private record CreationScope(StorageBinding binding, Document mapping) {}

    private CreationScope creationScope(ClientSession session, PipelineIntentScope pipeline, CacheBusinessScope scope) {
        if (!cluster.equals(scope.clusterId())) throw fenced();
        if (MongoReadBudget.supportsSnapshotReads(client)) {
            var stages = MongoStartConfigRead.root();
            MongoStartConfigRead.addBinding(stages);
            stages.add(MongoStartConfigRead.boundedLookup(SystemCollections.OPERATOR_GENERATIONS, "mapping", pipeline.pipelineId()));
            var captured = MongoStartConfigRead.snapshot(database, session, stages);
            var mapping = MongoStartConfigRead.boundedRow(captured, "mapping", pipeline.pipelineId());
            var binding = MongoStartConfigRead.binding(captured, bindings, scope);
            if (mapping != null && binding.isPresent()) return new CreationScope(binding.orElseThrow(), mapping);
        }
        var loaded = bindings.readForTransaction(session).loaded().orElseThrow(MongoCaptureStatePreparations::fenced);
        if (!scope.equals(loaded.cachePreparation().businessScope())) throw fenced();
        var mapping = SystemCollections.OPERATOR_GENERATIONS.on(database).find(session, new Document("_id", pipeline.pipelineId())).first();
        return new CreationScope(loaded.binding(), mapping);
    }

    @Override public CaptureCreationResult issueFirstCreate(CapturePreparationRequest request) {
        Objects.requireNonNull(request,"request");String operation=UUID.randomUUID().toString();
        var issued=transaction(session -> {
            var scope=scopeOf(request.pipeline());
            var creation = creationScope(session, request.pipeline(), scope);
            var mapping = creation.mapping();
            var state=MongoPipelineStateMappings.decode(request.pipeline().pipelineId(),mapping);
            if(state.phase()!=PipelineStateMapping.Phase.ACTIVE || state.stateGeneration()!=request.pipeline().stateGeneration())throw fenced();
            var verified = new MongoPipelineStatePreparations(client,database,cluster,bindings,artifacts,active)
                    .verifiedInputs(session,request.pipeline(),request.inputs());
            var pipeline = verified.pipeline();
            var reference = pipeline.sourceReference(request.sourceId());
            var source = verified.sources().get(reference.id());
            if (source == null) throw fenced();
            if(!(reference instanceof SourceRef.Spec spec))throw new IllegalStateException("a capture requires a materialized source switch");
            var readMode=pipeline.settings()==null || pipeline.settings().readMode()==null?ReadMode.SNAPSHOT_AND_CDC:pipeline.settings().readMode();
            var mode=readMode==ReadMode.SNAPSHOT_ONLY?CapturePreparation.Mode.SNAPSHOT_ONLY:spec.srs()?CapturePreparation.Mode.SHARED_SRS:CapturePreparation.Mode.DIRECT;
            String key=source.srs()==null?null:source.srs().key();
            var settings = PipelineSourceConfig.settings(reference, source);
            var overrides = PipelineSourceConfig.overrides(reference, source);
            String canonical = mode==CapturePreparation.Mode.DIRECT
                    ? SourceCaptureIdentity.directChannel(source.connector(),settings,key,pipeline.id(),reference.binding(),overrides)
                    : PipelineSourceConfig.chain(reference, source);
            var sharedArtifact = request.sharedArtifact();
            if (sharedArtifact != null && mode != CapturePreparation.Mode.SHARED_SRS) throw fenced();
            if (sharedArtifact != null && sharedArtifact.handoffIntentRevision() != null) {
                var intent = new MongoPipelineIntentStore(client, database, cluster, bindings)
                        .loadRetainedInTransaction(session, request.pipeline().pipelineId()).orElseThrow(MongoCaptureStatePreparations::fenced);
                if (intent.deleted() || intent.intentRevision() != sharedArtifact.handoffIntentRevision()
                        || !intent.scope().equals(request.pipeline())) throw fenced();
            }
            String artifactHash = sharedArtifact == null ? null : sharedArtifact.sha256();
            String chain = sharedArtifact == null ? canonical : SourceCaptureIdentity.versionedSharedChain(canonical, artifactHash,
                    sharedArtifact.handoffIntentRevision() == null ? null
                            : pipeline.id() + ":" + request.pipeline().stateGeneration() + ":" + sharedArtifact.handoffIntentRevision());
            var tables=request.inputs().selectedTables().get(reference.binding());
            String capture=switch(mode){
                case SHARED_SRS -> SourceCaptureIdentity.sharedCapture(chain);
                case SNAPSHOT_ONLY -> SourceCaptureIdentity.snapshotCapture(chain,tables);
                case DIRECT -> SourceCaptureIdentity.directCapture(source.connector(),settings,key,pipeline.id(),reference.binding(),pipeline.settings()==null?null:pipeline.settings().startFrom(),tables,overrides);
            };
            var preparation=new CapturePreparation(scope,mode,chain,capture,SourceCaptureIdentity.physical(source.connector(),settings),operation);
            var table=SystemCollections.CAPTURE_PREPARATIONS.on(database);var existing=table.find(session,new Document("_id",MongoCapturePreparationFacts.id(scope,capture))).first();
            if(existing!=null) {
                var retained=MongoCapturePreparationFacts.available(existing);
                if(retained.mode()!=preparation.mode() || !retained.canonicalChainId().equals(preparation.chainId())
                        || !Objects.equals(MongoCapturePreparationFacts.artifactHash(existing), artifactHash)
                        || !retained.sourceIdentityHash().equals(preparation.sourceIdentityHash()))
                    throw MongoPipelineSourcePreparations.identityUnproven(request.pipeline());
                if (handoffCandidate(mapping, request, retained)) {
                    return new Issuance(retained,true,MongoStorageBindingStore.targetFingerprint(creation.binding().cache()),
                            source.srs()==null?null:source.srs().retention(),true);
                }
                return new Issuance(retained,false,null,null,false);
            }
            // This is the existing CONFIG contention point used by authoring, binding and rebuild.
            // A dependency changed after our snapshot must abort and revalidate before any grant.
            ConfigurationVersion.advance(session, SystemCollections.CONFIGURATION_VERSION.on(database), client);
            boolean newSharedHistory = false;
            if(mode==CapturePreparation.Mode.SHARED_SRS) {
                var prior=table.find(session,new Document("_id.clusterId",scope.clusterId()).append("_id.bindingVersion",scope.bindingVersion())
                        .append("_id.capture",OperatorStateText.encode(capture))).first();
                if(prior!=null) {
                    var previous=MongoCapturePreparationFacts.decode(prior);
                    if(previous.mode()!=mode || !previous.canonicalChainId().equals(chain)
                            || !Objects.equals(MongoCapturePreparationFacts.artifactHash(prior), artifactHash)
                            || !previous.sourceIdentityHash().equals(preparation.sourceIdentityHash()))throw MongoCapturePreparationFacts.unreadable();
                }
                artifacts.configurationNotes().reserveSharedCapture(session,new RequiredNoteOwner(scope.bindingVersion(),
                        "pdk.chain."+chain,RequiredNoteOwner.Kind.CHAIN,chain,0),prior!=null);
                newSharedHistory = prior == null;
            }
            if (sharedArtifact != null && sharedArtifact.handoffIntentRevision() != null
                    && !handoffCandidate(mapping, request, preparation)) throw fenced();
            table.insertOne(session,MongoCapturePreparationFacts.withArtifact(MongoCapturePreparationFacts.row(preparation), artifactHash));
            return new Issuance(preparation,true,MongoStorageBindingStore.targetFingerprint(creation.binding().cache()),source.srs()==null?null:source.srs().retention(),newSharedHistory);
        });
        active.run();return issued.won()?new CaptureCreationResult.Issued(new Creation(issued.preparation(),request,issued.targetFingerprint(),issued.retention(),issued.newSharedHistory(),active)):new CaptureCreationResult.AlreadyIssued(issued.preparation());
    }
    private record CapturedHistory(SourceRef reference, SourceResource source, Document capture, List<Document> prior,
            Document snapshot) {}

    private Optional<CapturedHistory> capturedHistory(CapturePreparation original, String sourceId, String pipelineId, String pipelineHash) {
        if (MongoValidationClaimObservation.lacksSnapshotRead(client)) return Optional.empty();
        var stages = MongoStartConfigRead.root();
        MongoStartConfigRead.addBinding(stages);
        var ids = pipelineId == null ? List.of(sourceId) : List.of(pipelineId, sourceId);
        MongoStartConfigRead.addArtifacts(stages, ids);
        var id = MongoCapturePreparationFacts.id(original.scope(), original.captureId());
        stages.add(MongoValidationClaimObservation.lookup(SystemCollections.CAPTURE_PREPARATIONS, "capture", id, null));
        var priorFilter = new Document("_id.clusterId", original.scope().clusterId())
                .append("_id.bindingVersion", original.scope().bindingVersion())
                .append("_id.capture", OperatorStateText.encode(original.captureId()))
                .append("_id.cacheIncarnation", new Document("$ne", original.scope().cacheIncarnation()));
        addBoundedLookup(stages, SystemCollections.CAPTURE_PREPARATIONS, "priorCaptures", priorFilter);
        var noteOwner = new RequiredNoteOwner(original.scope().bindingVersion(), "pdk.chain." + original.canonicalChainId(),
                RequiredNoteOwner.Kind.CHAIN, original.canonicalChainId(), 0);
        artifacts.configurationNotes().captureSharedOrigin(stages, noteOwner);
        var snapshot = MongoStartConfigRead.snapshot(database, stages);
        if (MongoStartConfigRead.binding(snapshot, bindings, original.scope()).isEmpty()) throw fenced();
        var rows = MongoStartConfigRead.artifacts(snapshot, ids);
        var capture = MongoStartConfigRead.row(snapshot, "capture", id);
        var prior = boundedRows(snapshot, "priorCaptures");
        if (rows.isEmpty() || rows.orElseThrow().size() != ids.size() || capture.isEmpty() || prior.isEmpty()
                || !artifacts.configurationNotes().hasCapturedSharedOrigin(snapshot)) return Optional.empty();
        var values = new LinkedHashMap<String, Resource>();
        for (var row : rows.orElseThrow()) {
            var value = artifacts.decode(row);
            values.put(value.id(), value);
        }
        SourceRef reference;
        if (pipelineId == null) {
            if (pipelineHash != null) throw fenced();
            reference = SourceRef.bare(sourceId);
        } else {
            if (!(values.get(pipelineId) instanceof PipelineResource pipeline)
                    || !CanonicalHash.of(pipeline).equals(pipelineHash) || !pipeline.sourceBindings().contains(sourceId)) throw fenced();
            reference = pipeline.sourceReference(sourceId);
        }
        if (!values.containsKey(reference.id())) return Optional.empty();
        var source = values.get(reference.id());
        if (!(source instanceof SourceResource selected)) throw fenced();
        return Optional.of(new CapturedHistory(reference, selected, capture.orElseThrow(), prior.orElseThrow(), snapshot));
    }

    private Optional<CapturePreparation> retainedStart(CapturePreparationRequest request, String canonicalChain) {
        if (request.sharedArtifact() != null || MongoValidationClaimObservation.lacksSnapshotRead(client) || request.inputs().sourceHashes().size() > 15)
            return Optional.empty();
        return readSnapshot(() -> {
            var scope = scopeOf(request.pipeline());
            var ids = new LinkedHashSet<String>();
            ids.add(request.pipeline().pipelineId()); ids.addAll(request.inputs().sourceHashes().keySet());
            var stages = MongoStartConfigRead.root();
            MongoStartConfigRead.addBinding(stages);
            MongoStartConfigRead.addArtifacts(stages, List.copyOf(ids));
            var captureKey = MongoCapturePreparationFacts.id(scope, SourceCaptureIdentity.sharedCapture(canonicalChain));
            stages.add(MongoValidationClaimObservation.lookup(SystemCollections.CAPTURE_PREPARATIONS,
                    "capture", captureKey, null));
            stages.add(MongoValidationClaimObservation.lookup(SystemCollections.OPERATOR_GENERATIONS,
                    "mapping", request.pipeline().pipelineId(), null));
            var snapshot = MongoStartConfigRead.snapshot(database, stages);
            if (MongoStartConfigRead.binding(snapshot, bindings, scope).isEmpty()) throw fenced();
            var mapping = MongoStartConfigRead.row(snapshot, "mapping", request.pipeline().pipelineId());
            if (mapping.isEmpty()) return Optional.empty();
            var state = MongoPipelineStateMappings.decode(request.pipeline().pipelineId(), mapping.orElseThrow());
            if (state.phase() != PipelineStateMapping.Phase.ACTIVE
                    || state.stateGeneration() != request.pipeline().stateGeneration()) throw fenced();
            var capturedRows = MongoStartConfigRead.artifacts(snapshot, List.copyOf(ids));
            var candidate = MongoStartConfigRead.row(snapshot, "capture", captureKey);
            if (capturedRows.isEmpty() || candidate.isEmpty()) return Optional.empty();
            var captured = capturedRows.map(rows -> {
                var values = new LinkedHashMap<String, Resource>();
                rows.forEach(row -> { var value = artifacts.decode(row); values.put(value.id(), value); });
                return Map.copyOf(values);
            });
            if (captured.orElseThrow().get(request.pipeline().pipelineId()) instanceof PipelineResource candidatePipeline
                    && !captured.orElseThrow().keySet().containsAll(candidatePipeline.sourceIds())) return Optional.empty();
            var verified = MongoPipelineStatePreparations.verifiedInputs(request.pipeline(), request.inputs(),
                    captured.orElseThrow(), captured.orElseThrow()::get);
            var pipeline = verified.pipeline();
            var reference = pipeline.sourceReference(request.sourceId());
            var source = verified.sources().get(reference.id());
            if (source == null) throw fenced();
            var mode = pipeline.settings() == null ? null : pipeline.settings().readMode();
            if (mode == ReadMode.SNAPSHOT_ONLY || !(reference instanceof SourceRef.Spec spec) || !spec.srs())
                return Optional.empty();
            String chain = PipelineSourceConfig.chain(reference, source);
            if (!canonicalChain.equals(chain)) throw MongoPipelineSourcePreparations.identityUnproven(request.pipeline());
            if (MongoCapturePreparationFacts.artifactHash(candidate.orElseThrow()) != null) return Optional.empty();
            var held = MongoCapturePreparationFacts.available(candidate.orElseThrow());
            if (held.mode() != CapturePreparation.Mode.SHARED_SRS || !chain.equals(held.canonicalChainId())
                    || !SourceCaptureIdentity.physical(source.connector(), PipelineSourceConfig.settings(reference, source)).equals(held.sourceIdentityHash()))
                throw MongoPipelineSourcePreparations.identityUnproven(request.pipeline());
            return Optional.of(held);
        });
    }

    private <T> T readSnapshot(java.util.function.Supplier<T> action) {
        return MongoStructureFailureScope.inspectOnFailure(client, () -> StoreIo.call(() -> {
            active.run();
            try {
                return action.get();
            } finally { active.run(); }
        }));
    }

    private static void addBoundedLookup(List<Document> stages, SystemCollections table, String field, Document filter) {
        var bytes = new Document("$bsonSize", "$$ROOT");
        var captured = new Document("_id", 0).append("value", new Document("$cond",
                List.of(new Document("$lte", List.of(bytes, 64 * 1024)), "$$ROOT", false)));
        stages.add(new Document("$lookup", new Document("from", table.collectionName()).append("as", field)
                .append("pipeline", List.of(new Document("$match", filter), new Document("$limit", 17),
                        new Document("$project", captured)))));
    }

    private static Optional<List<Document>> boundedRows(Document snapshot, String field) {
        if (snapshot == null) return Optional.empty();
        var values = snapshot.getList(field, Document.class);
        if (values == null || values.size() > 16 || values.stream().anyMatch(row -> !(row.get("value") instanceof Document)))
            return Optional.empty();
        return Optional.of(values.stream().map(row -> row.get("value", Document.class)).toList());
    }

    static boolean handoffCandidate(Document mapping, CapturePreparationRequest request, CapturePreparation retained) {
        var artifact = request.sharedArtifact();
        if (artifact == null || artifact.handoffIntentRevision() == null || retained.replacement()
                || !retained.canonicalChainId().startsWith("mcv-")) return false;
        var bindings = MongoPipelineStateMappings.connectorBindings(mapping).orElse(null);
        if (bindings == null || !bindings.scope().equals(request.pipeline()) || bindings.handoff() == null
                || bindings.handoff().phase() != ConnectorRunBindings.Phase.PREPARED
                || bindings.handoff().intentRevision() != artifact.handoffIntentRevision()) return false;
        return bindings.handoff().candidates().stream().anyMatch(candidate ->
                candidate.purpose() == PipelineNode.Purpose.CAPTURE && request.sourceId().equals(candidate.nodeId())
                && artifact.sha256().equals(candidate.sha256()) && retained.captureId().equals(candidate.captureId())
                && retained.chainId().equals(candidate.chainId()));
    }

    private SourceRef verifiedReference(ClientSession session,String pipelineId,String pipelineHash,String sourceId) {
        if (pipelineId == null) {
            if (pipelineHash != null) throw fenced();
            return SourceRef.bare(sourceId);
        }
        var pipeline = artifacts.getInTransaction(session,pipelineId).filter(PipelineResource.class::isInstance)
                .map(PipelineResource.class::cast).orElseThrow(MongoCaptureStatePreparations::fenced);
        if (!CanonicalHash.of(pipeline).equals(pipelineHash) || !pipeline.sourceBindings().contains(sourceId)) throw fenced();
        return pipeline.sourceReference(sourceId);
    }
    private void requireScope(ClientSession session,CacheBusinessScope expected){var current=bindings.admittedCachePreparation(session).orElseThrow(MongoCaptureStatePreparations::fenced).businessScope();if(!cluster.equals(expected.clusterId()) || !expected.equals(current))throw fenced();}
    MongoCapturePreparationFacts.Recovery reserveSharedRecovery(ClientSession session,StorageRecoveryCommands.Plan plan) {
        var previous=plan.manifest().sharedSrs().previous();requireScope(session,previous.scope());
        var table=SystemCollections.CAPTURE_PREPARATIONS.on(database);
        var row=table.find(session,new Document("_id",MongoCapturePreparationFacts.id(previous.scope(),previous.captureId()))).first();
        if(row==null || !previous.equals(MongoCapturePreparationFacts.available(row)))throw fenced();
        requireSharedRecoveryOrigin(session,previous);
        if(previous.replacement())MongoPipelineStateMappings.requireNoSharedReferences(session,database,previous.preparationId());
        var recovery=sharedReservation(plan);
        if(table.replaceOne(session,row,MongoCapturePreparationFacts.withArtifact(
                MongoCapturePreparationFacts.configurationRow(recovery), MongoCapturePreparationFacts.artifactHash(row))).getMatchedCount()!=1)throw fenced();
        return recovery;
    }
    MongoCapturePreparationFacts.Recovery requireSharedReservation(ClientSession session,StorageRecoveryCommands.Plan plan) {
        var expected=sharedReservation(plan);requireScope(session,expected.previous().scope());
        var actual=currentRecoveryForOperation(session,database,expected.previous().scope(),plan.operationId());
        if(!expected.equals(actual))throw fenced();requireSharedRecoveryOrigin(session,expected.previous());return actual;
    }
    void publishSharedRecovery(ClientSession session,StorageRecoveryCommands.Plan plan) {
        var original=requireSharedReservation(session,plan);
        var ready=new MongoCapturePreparationFacts.Recovery(original.previous(),original.replacement(),original.planHash(),MongoCapturePreparationFacts.RecoveryPhase.READY);
        replaceSharedRecoveryRow(session, original, ready);
    }
    boolean hasSharedRecoveryReservation(ClientSession session,String operation) {
        return SystemCollections.CAPTURE_PREPARATIONS.on(database).find(session,new Document("currentRecovery.operationId",operation)).first()!=null;
    }
    MongoCapturePreparationFacts.Recovery requireSharedReconfirmation(ClientSession session,StorageRecoveryCommands.Plan plan) {
        var expected=sharedReservation(plan);requireScope(session,expected.previous().scope());
        var actual=currentRecoveryForOperation(session,database,expected.previous().scope(),plan.operationId());
        if(actual.phase()!=MongoCapturePreparationFacts.RecoveryPhase.RESERVED || !actual.previous().equals(expected.previous())
                || !actual.replacement().equals(expected.replacement()))throw fenced();
        requireSharedRecoveryOrigin(session,actual.previous());return actual;
    }
    void rebindSharedRecovery(ClientSession session,StorageRecoveryCommands.Plan plan,MongoCapturePreparationFacts.Recovery original) {
        var actual=requireSharedReconfirmation(session,plan);
        if(!actual.equals(original))throw fenced();
        replaceSharedRecoveryRow(session, original, sharedReservation(plan));
    }
    private void replaceSharedRecoveryRow(ClientSession session, MongoCapturePreparationFacts.Recovery previous,
            MongoCapturePreparationFacts.Recovery next) {
        var table = SystemCollections.CAPTURE_PREPARATIONS.on(database);
        var birth = previous.previous();
        var row = table.find(session, new Document("_id", MongoCapturePreparationFacts.id(birth.scope(), birth.captureId()))).first();
        if (row == null || !MongoCapturePreparationFacts.recovery(row).filter(previous::equals).isPresent()) throw fenced();
        var replacement = MongoCapturePreparationFacts.withArtifact(MongoCapturePreparationFacts.configurationRow(next),
                MongoCapturePreparationFacts.artifactHash(row));
        if (table.replaceOne(session, row, replacement).getMatchedCount() != 1) throw fenced();
    }

    private void requireSharedRecoveryOrigin(ClientSession session,CapturePreparation previous) {
        var owner=new RequiredNoteOwner(previous.scope().bindingVersion(),"pdk.chain."+previous.canonicalChainId(),RequiredNoteOwner.Kind.CHAIN,previous.canonicalChainId(),0);
        if(artifacts.configurationNotes().inspectSharedOrigin(session,owner,true).isEmpty())
            throw new TapstateException(IoError.NOTE_SCOPE_UNAVAILABLE,Map.of(),null);
    }
    private static MongoCapturePreparationFacts.Recovery sharedReservation(StorageRecoveryCommands.Plan plan) {
        if(plan.manifest().selection().extent()!=StorageRecoveryCommands.Extent.SHARED_SRS || plan.manifest().sharedSrs()==null)throw fenced();
        var previous=plan.manifest().sharedSrs().previous();
        try {
            var replacement=new CapturePreparation(previous.scope(),CapturePreparation.Mode.SHARED_SRS,
                    MongoCapturePreparationFacts.replacementChain(previous,plan.operationId()),previous.captureId(),previous.sourceIdentityHash(),plan.operationId(),
                    previous.canonicalChainId(),previous.birthPreparationId());
            return new MongoCapturePreparationFacts.Recovery(previous,replacement,plan.planHash(),MongoCapturePreparationFacts.RecoveryPhase.RESERVED);
        }catch(IllegalArgumentException invalid){throw fenced();}
    }
    /** CONFIG-only lookup of an existing reservation; it does not issue or consume any creation permission. */
    static MongoCapturePreparationFacts.Recovery currentRecoveryForOperation(ClientSession session,MongoDatabase database,
            CacheBusinessScope expected,String operation) {
        if(!session.hasActiveTransaction())throw new IllegalStateException("capture recovery lookup requires its CONFIG transaction");
        var rows=SystemCollections.CAPTURE_PREPARATIONS.on(database).find(session,new Document("currentRecovery.operationId",operation))
                .limit(2).into(new ArrayList<>());
        if(rows.size()!=1)throw MongoCapturePreparationFacts.unreadable();
        var retained=MongoCapturePreparationFacts.recovery(rows.getFirst()).orElseThrow(MongoCapturePreparationFacts::unreadable);
        if(!expected.equals(retained.replacement().scope()) || !operation.equals(retained.replacement().preparationId()))throw MongoCapturePreparationFacts.unreadable();
        return retained;
    }
    private <T>T transaction(Function<ClientSession,T> action) {
        return MongoStructureFailureScope.inspectOnFailure(client, () -> StoreIo.call(() -> {
            try {
                active.run();
                for(int attempt=0;;attempt++)try(var session=client.startSession()) {
                    var result=session.withTransaction(() -> {MongoStructureFailureScope.beginCallback();active.run();var value=action.apply(session);active.run();return value;},TRANSACTION);active.run();return result;
                }catch(MongoException raced){if(attempt>=2 || ErrorCategory.fromErrorCode(raced.getCode())!=ErrorCategory.DUPLICATE_KEY)throw raced;}
            }catch(RuntimeException failure){active.run();throw failure;}
        }));
    }
    static FirstCaptureCreation consumeFirst(FirstCaptureCreation invocation,String targetFingerprint) {
        if(!(invocation instanceof Creation creation))throw fenced();creation.consume();if(!creation.targetFingerprint.equals(targetFingerprint))throw fenced();return creation;
    }
    private record Issuance(CapturePreparation preparation,boolean won,String targetFingerprint,String retention,boolean newSharedHistory){}
    private static final class Creation implements FirstCaptureCreation {
        private final CapturePreparation preparation;private final CapturePreparationRequest request;private final String targetFingerprint;private final String retention;private final boolean newSharedHistory;private final Runnable active;private final AtomicBoolean consumed=new AtomicBoolean();
        Creation(CapturePreparation preparation,CapturePreparationRequest request,String targetFingerprint,String retention,boolean newSharedHistory,Runnable active){this.preparation=preparation;this.request=request;this.targetFingerprint=targetFingerprint;this.retention=retention;this.newSharedHistory=newSharedHistory;this.active=active;}
        public CapturePreparation preparation(){return preparation;}public CapturePreparationRequest request(){return request;}public String retention(){return retention;}
        @Override public boolean newSharedHistory() { return newSharedHistory; }
        public void consume(){if(!consumed.compareAndSet(false,true))throw fenced();active.run();}public void requireActive(){active.run();}
        @Override public String toString(){return "FirstCaptureCreation[redacted]";}
    }
    private static CacheBusinessScope scopeOf(PipelineIntentScope scope){return new CacheBusinessScope(scope.clusterId(),scope.bindingVersion(),scope.cacheIncarnation());}
    private static TapstateException fenced(){return new TapstateException(IoError.PIPELINE_INTENT_FENCED,Map.of(),null);}
}
