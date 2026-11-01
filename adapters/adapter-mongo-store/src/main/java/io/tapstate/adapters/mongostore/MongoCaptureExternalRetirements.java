package io.tapstate.adapters.mongostore;

import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import org.bson.BsonBinaryWriter;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;

/** Bounded CONFIG opening uncertainty; ordinary CACHE takeover does not require an older close. */
final class MongoCaptureExternalRetirements implements CaptureExternalRetirements {
    static final int MAX_CONTROL_BYTES = 524288;
    static final int MAX_OPENING_BYTES = 1024;
    private static final TransactionOptions TX = TransactionOptions.builder().readConcern(ReadConcern.SNAPSHOT)
            .writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();
    private static final TransactionOptions INSTALL = TransactionOptions.builder().readConcern(ReadConcern.LOCAL)
            .writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();
    private final MongoClient client;
    private final MongoDatabase database;
    private final String cluster;
    private final MongoStorageBindingStore bindings;
    private final Runnable active;
    private final MongoRequiredNoteStore notes;

    MongoCaptureExternalRetirements(MongoClient client, MongoDatabase database, String cluster,
            MongoStorageBindingStore bindings, MongoRequiredNoteStore notes, Runnable active) {
        this.client = Objects.requireNonNull(client); this.database = Objects.requireNonNull(database);
        this.cluster = Objects.requireNonNull(cluster); this.bindings = Objects.requireNonNull(bindings);
        this.active = Objects.requireNonNull(active);
        this.notes = Objects.requireNonNull(notes);
    }

    static Document id(CapturePreparation preparation) {
        return new Document("clusterId", preparation.scope().clusterId())
                .append("bindingVersion", preparation.scope().bindingVersion())
                .append("canonicalCaptureId", preparation.captureId());
    }

    /** Only the winning original first preparation may initialize an empty opening history. */
    void initialize(ClientSession session, CapturePreparation preparation) {
        requireScope(session, preparation.scope());
        var table = SystemCollections.CAPTURE_EXTERNAL_RETIREMENTS.on(database);
        var previous = table.find(session, new Document("_id", id(preparation))).first();
        if (previous != null) {
            requireOpen(previous, preparation);
            return;
        }
        table.insertOne(session, new Document("_id", id(preparation)).append("formatVersion", 1)
                .append("revision", 1L).append("resourceEpoch", 1L).append("operationSeq", 0L)
                .append("currentPreparation", MongoCapturePreparationFacts.row(preparation))
                .append("sharedNoteScope", null).append("originDigest", null)
                .append("openingJournal", CaptureOpeningIntervals.empty().document())
                .append("aliasAuthorities", List.of()).append("operation", null)
                .append("lastResult", null).append("gcPrefix", null));
    }

    List<CapturePreparation> legacyPreparations(CacheBusinessScope scope) {
        return transaction(session -> {
            requireScope(session,scope);var schema=SystemCollections.SCHEMA_MIGRATIONS.on(database).find(session,new Document("_id","config")).first();
            if(schema==null || !Set.of(2,3).contains(schema.get("installedVersion")))throw pending(null,"unsupported-legacy-config-version");
            return SystemCollections.CAPTURE_PREPARATIONS.on(database).find(session,new Document("_id.clusterId",scope.clusterId())
                    .append("_id.bindingVersion",scope.bindingVersion()).append("_id.cacheIncarnation",scope.cacheIncarnation()))
                    .into(new ArrayList<>()).stream().map(MongoCapturePreparationFacts::available).toList();
        });
    }
    CaptureStorageUpgrade.ConfigurationReceipt adoptLegacy(CaptureStorageUpgrade.CacheObservation observation) {
        var original=MongoCaptureClearCache.requireLegacy(observation);
        original.requireCurrent();
        transaction(INSTALL,session -> {
            requireScope(session,original.scope());
            var binding=bindings.loadForTransaction(session).orElseThrow(() -> pending(null,"binding-unproven"));
            if(!original.fingerprint().equals(MongoStorageBindingStore.targetFingerprint(binding.cache())))throw pending(null,"legacy-target-changed");
            var versions=SystemCollections.SCHEMA_MIGRATIONS.on(database);var schema=versions.find(session,new Document("_id","config")).first();
            boolean creating=schema!=null && Integer.valueOf(2).equals(schema.get("installedVersion"));
            if(!creating && (schema==null || !Integer.valueOf(3).equals(schema.get("installedVersion"))))throw pending(null,"legacy-config-version");
            var current=SystemCollections.CAPTURE_PREPARATIONS.on(database).find(session,new Document("_id.clusterId",original.scope().clusterId())
                    .append("_id.bindingVersion",original.scope().bindingVersion()).append("_id.cacheIncarnation",original.scope().cacheIncarnation()))
                    .into(new ArrayList<>()).stream().map(MongoCapturePreparationFacts::available).collect(java.util.stream.Collectors.toSet());
            if(!current.equals(Set.copyOf(original.captures())))throw pending(null,"legacy-capture-history-changed");
            // This verifies encrypted origins and both sides of the existing native carry graph.
            notes.resourceNotes(session,original.scope());
            if(creating)SystemCollections.CAPTURE_EXTERNAL_RETIREMENTS.prepareCollectionOn(database,session);
            for(var preparation:original.captures()) {
                var table=SystemCollections.CAPTURE_EXTERNAL_RETIREMENTS.on(database);
                var existing=table.find(session,new Document("_id",id(preparation))).first();
                if(existing==null) {
                    if(!creating)throw pending(preparation,"legacy-control-missing-after-upgrade");
                    var coverage=new Document("sequence",1L).append("openingId","legacy-"+preparation.preparationId()).append("resourceEpoch",1L)
                            .append("kind","LEGACY_UNTRACKED").append("state","UNTRACKED");
                    coverage.putAll(original.openingCoverage(preparation));
                    if(bytes(coverage)>MAX_OPENING_BYTES)throw capacity(preparation,"legacy-opening-bytes");
                    initialize(session,preparation);
                    var row=control(session,preparation);
                    replace(session,row,new Document(row).append("openingJournal",CaptureOpeningIntervals.append(CaptureOpeningIntervals.empty(),0,coverage).document()),preparation);
                }else requireLegacyCoverage(existing,preparation);
            }
            ConfigurationVersion.advance(session,SystemCollections.CONFIGURATION_VERSION.on(database));
            if(creating && versions.replaceOne(session,schema,new Document(schema).append("installedVersion",3)).getMatchedCount()!=1)
                throw pending(null,"legacy-config-version-raced");
            return null;
        });
        return new LegacyReceipt(this,original);
    }
    private record LegacyReceipt(MongoCaptureExternalRetirements owner,MongoCaptureClearCache.LegacyObservation original)
            implements CaptureStorageUpgrade.ConfigurationReceipt {
        @Override public void requireCurrent() {
            owner.transaction(session -> {
                owner.requireScope(session,original.scope());
                var schema=SystemCollections.SCHEMA_MIGRATIONS.on(owner.database).find(session,new Document("_id","config")).first();
                if(schema==null || !Integer.valueOf(3).equals(schema.get("installedVersion")))throw pending(null,"legacy-config-not-upgraded");
                for(var preparation:original.captures())requireLegacyCoverage(owner.control(session,preparation),preparation);
                return null;
            });
        }
    }
    static void requireLegacyReceipt(CaptureStorageUpgrade.ConfigurationReceipt receipt,CaptureStorageUpgrade.CacheObservation original) {
        if(!(receipt instanceof LegacyReceipt held) || held.original()!=original)throw pending(null,"legacy-configuration-receipt-not-issued");
        receipt.requireCurrent();
    }
    private static void requireLegacyCoverage(Document control,CapturePreparation preparation) {
        requireControlShape(control,preparation);
        if(!MongoCapturePreparationFacts.annex(control.get("currentPreparation",Document.class)).equals(preparation))throw pending(preparation,"legacy-preparation-changed");
        var journal=CaptureOpeningIntervals.read(control.get("openingJournal",Document.class));
        if(journal.unresolved().stream().noneMatch(token -> Long.valueOf(1).equals(token.get("sequence")) && "LEGACY_UNTRACKED".equals(token.get("kind"))))
            throw pending(preparation,"legacy-coverage-missing");
    }

    @Override public Registration prepare(Request request) {
        Objects.requireNonNull(request);
        String opening = UUID.randomUUID().toString();
        var original = transaction(session -> {
            requireScope(session, request.preparation().scope());
            var row = control(session, request.preparation()); requireOpeningAllowed(row, request.preparation());
            requireOpeningAuthority(session,row,request.preparation());
            replace(session,row,new Document(row),request.preparation());
            return new Boundary(request, row.getLong("resourceEpoch"),
                    CaptureOpeningIntervals.read(row.get("openingJournal", Document.class)).issuedThrough(), opening);
        });
        return () -> register(original);
    }

    private record Boundary(Request request, long epoch, long expectedIssued, String openingId) {}

    @Override public List<CapturePreparation> sharedCaptures(CacheBusinessScope scope) {
        return transaction(session -> {
            requireScope(session,scope);var found=new ArrayList<CapturePreparation>();
            for(var row:SystemCollections.CAPTURE_EXTERNAL_RETIREMENTS.on(database)
                    .find(session,new Document("_id.clusterId",scope.clusterId()).append("_id.bindingVersion",scope.bindingVersion()))) {
                var original=MongoCapturePreparationFacts.annex(row.get("currentPreparation",Document.class));
                if(original.mode()==CapturePreparation.Mode.SHARED_SRS && original.scope().equals(scope))found.add(original);
            }
            return List.copyOf(found);
        });
    }
    @Override public boolean retainedPeer(CapturePreparation capture,String relinquishingPipeline) {
        return transaction(session -> {requireScope(session,capture.scope());return retainedPeer(session,capture,relinquishingPipeline);});
    }
    private boolean retainedPeer(ClientSession session,CapturePreparation capture,String relinquishingPipeline) {
        for(var row:SystemCollections.OPERATOR_GENERATIONS.on(database).find(session,new Document("sharedRecovery",new Document("$exists",true)))) {
            var marker=MongoPipelineSharedRecoveryFacts.read(row).orElseThrow(() -> pending(capture,"shared-recovery-reference-unproven"));
            var scope=marker.originalScope();
            if(scope.pipelineId().equals(relinquishingPipeline))continue;
            var recovery=MongoCaptureStatePreparations.currentRecoveryForOperation(session,database,
                    new CacheBusinessScope(scope.clusterId(),scope.bindingVersion(),scope.cacheIncarnation()),marker.operationId());
            if(recovery.previous().captureId().equals(capture.captureId()))return true;
        }
        return false;
    }
    @Override public List<Pending> pending(CacheBusinessScope scope) {
        return transaction(session -> {
            requireScope(session,scope);var result=new ArrayList<Pending>();
            for(var row:SystemCollections.CAPTURE_EXTERNAL_RETIREMENTS.on(database).find(session,
                    new Document("_id.clusterId",scope.clusterId()).append("_id.bindingVersion",scope.bindingVersion()))) {
                var op=row.get("operation",Document.class);if(op==null)continue;
                String phase=op.getString("phase");
                if(Set.of("CANCELLED","POLICY_RETAINED").contains(phase) || "READY".equals(phase) && row.get("gcPrefix")==null)continue;
                var original=held(row);if(!original.observation().previous().scope().equals(scope))continue;
                result.add(new Pending(original,phase,Boolean.TRUE.equals(op.getBoolean("automatic"))));
            }
            return List.copyOf(result);
        });
    }

    @Override public void relinquishAliases(PipelineRebuildPreparation original,List<CaptureAliasObservation> contexts) {
        transaction(session -> {
            MongoPipelineStateRebuilds.requireOriginalInTransaction(client,database,session,original);
            notes.holdAliases(session,original,contexts);
            if(!contexts.isEmpty())ConfigurationVersion.advance(session,SystemCollections.CONFIGURATION_VERSION.on(database));
            return null;
        });
    }
    @Override public void relinquishAliases(PipelineRetirementPreparation original,List<CaptureAliasObservation> contexts) {
        transaction(session -> {
            MongoPipelineRetirements.requireOriginalInTransaction(client,database,session,original);
            notes.holdAliases(session,original,contexts);
            if(!contexts.isEmpty())ConfigurationVersion.advance(session,SystemCollections.CONFIGURATION_VERSION.on(database));return null;
        });
    }
    @Override public List<CaptureResourceNotes> resourceNotes(CacheBusinessScope scope) {
        return transaction(session -> {requireScope(session,scope);return notes.resourceNotes(session,scope);});
    }
    @Override public RequiredNoteOrigin admitOrigin(RequiredNoteScope scope,RequiredNoteOrigin origin,String resourceNameHash) {
        return transaction(session -> {
            if(notes.missingOrigin(session,scope)) {
                ConfigurationVersion.advance(session,SystemCollections.CONFIGURATION_VERSION.on(database));
                for(var control:SystemCollections.CAPTURE_EXTERNAL_RETIREMENTS.on(database).find(session,new Document("_id.bindingVersion",scope.owner().bindingVersion()))) {
                    var operation=control.get("operation",Document.class);
                    if(operation==null || Set.of("READY","CANCELLED","POLICY_RETAINED").contains(operation.getString("phase")))continue;
                    if("UNKNOWN".equals(resourceNameHash) || operation.getString("resourceNameHash")==null || resourceNameHash.equals(operation.getString("resourceNameHash")))
                        throw pending(null,"external-resource-admission-frozen");
                }
            }
            return notes.prepareOrigin(session,scope,origin);
        });
    }

    @Override public List<CaptureClearPreparation> reserve(PipelineRebuildPreparation original,List<CaptureClearObservation> candidates,
            List<CaptureResourceNotes> related,String resourceNameHash,RequiredNoteOrigin expectedOrigin,boolean automatic,List<CaptureRetirementMember> members) {
        return reserve(new Authority(original.request().originalScope(),original.request().operationId(),original.confirmationHash(),original.request().originalClaim(),"CLEAR",
                session -> MongoPipelineStateRebuilds.requireOriginalInTransaction(client,database,session,original)),candidates,related,resourceNameHash,expectedOrigin,automatic,members);
    }
    @Override public List<CaptureClearPreparation> reserve(PipelineRetirementReceipt original,List<CaptureClearObservation> candidates,
            List<CaptureResourceNotes> related,String resourceNameHash,RequiredNoteOrigin expectedOrigin,boolean automatic,List<CaptureRetirementMember> members) {
        MongoCachePipelineRetirements.requireCurrentReceipt(original);
        var prepared=original.preparation();var fact=MongoPipelineRetirements.originalFact(prepared);
        return reserve(new Authority(prepared.request().scope(),prepared.request().operationId(),MongoCaptureClearControls.hash(MongoPipelineRetirementFacts.encode(fact)),
                original.retiredState().map(PipelineRetiredState::claim).orElse(null),"DELETE",
                session -> MongoPipelineRetirements.requireOriginalInTransaction(client,database,session,prepared)),candidates,related,resourceNameHash,expectedOrigin,automatic,members);
    }
    private record Authority(PipelineIntentScope scope,String operationId,String hash,WorkloadClaimFence claim,String kind,
            java.util.function.Consumer<ClientSession> requireCurrent) {}
    private List<CaptureClearPreparation> reserve(Authority original,List<CaptureClearObservation> candidates,
            List<CaptureResourceNotes> related,String resourceNameHash,RequiredNoteOrigin expectedOrigin,boolean automatic,List<CaptureRetirementMember> members) {
        if(candidates.isEmpty() || candidates.size()>32 || related.size()>64)throw capacity(null,"cohort-count");
        String operation=UUID.randomUUID().toString();
        return transaction(session -> {
            original.requireCurrent().accept(session);
            notes.requireResourceNotes(session,related);
            ConfigurationVersion.advance(session,SystemCollections.CONFIGURATION_VERSION.on(database));
            var result=new ArrayList<CaptureClearPreparation>();
            for(var candidate:candidates)result.add(reserve(session,original,candidate,related,resourceNameHash,expectedOrigin,automatic,members,operation,candidates));
            return List.copyOf(result);
        });
    }
    private CaptureClearPreparation reserve(ClientSession session,Authority original,CaptureClearObservation candidate,
            List<CaptureResourceNotes> related,String resourceNameHash,RequiredNoteOrigin expectedOrigin,boolean automatic,List<CaptureRetirementMember> members,
            String operation,List<CaptureClearObservation> cohort) {
        Objects.requireNonNull(original);Objects.requireNonNull(candidate);Objects.requireNonNull(expectedOrigin);
        if(!(candidate instanceof MongoCaptureClearCache.Observation evidence))throw pending(null,"clear-observation-not-issued");
        var observation=evidence.snapshot();
        if(members.isEmpty() || members.size()>64 || members.stream().map(CaptureRetirementMember::nodeId).distinct().count()!=members.size())
            throw capacity(observation.previous(),"member-manifest");
        if(!observation.consumers().isEmpty())throw pending(observation.previous(),"retained-consumer");
            var previous=observation.previous();requireScope(session,previous.scope());
            original.requireCurrent().accept(session);
            if(retainedPeer(session,previous,original.scope().pipelineId()))throw pending(previous,"retained-shared-recovery-peer");
            requireRelinquished(session,original,evidence);
            var row=control(session,previous);var existing=row.get("operation",Document.class);
            if(existing!=null && original.operationId().equals(existing.getString("originalOperation")))return held(row);
            requireOpen(row,previous);
            if(automatic && row.getLong("resourceEpoch")==Long.MAX_VALUE)throw capacity(previous,"resource-epoch");
            if(row.get("gcPrefix")!=null)throw capacity(previous,"previous-prefix-cleanup-pending");
            var owner=new RequiredNoteOwner(previous.scope().bindingVersion(),"pdk.chain."+previous.canonicalChainId(),
                    RequiredNoteOwner.Kind.CHAIN,previous.canonicalChainId(),0);
            var scope=notes.open(session,owner);var origin=notes.originalOrigin(session,scope);
            if(!origin.equals(expectedOrigin))throw pending(previous,"origin-changed");
            var resourceHash=MongoCaptureClearControls.hash(new Document("originDigest",originDigest(origin)).append("resourceNameHash",resourceNameHash));
            var aliases=automatic?notes.heldAliases(session,scope,related,cohort.stream().map(c -> c.snapshot().previous().canonicalChainId()).toList(),resourceHash):List.<Document>of();
            long sequence=row.getLong("operationSeq")+1;
            if(sequence<1 || sequence==Long.MAX_VALUE)throw capacity(previous,"operation-sequence");
            var next=automatic?new CapturePreparation(previous.scope(),previous.mode(),clearChain(previous,operation),previous.captureId(),
                    previous.sourceIdentityHash(),operation,previous.canonicalChainId(),previous.birthPreparationId()):previous;
            var binding=bindings.loadForTransaction(session).orElseThrow(() -> pending(previous,"binding-unproven"));
            if(!evidence.targetFingerprint().equals(MongoStorageBindingStore.targetFingerprint(binding.cache())))
                throw pending(previous,"observation-target-changed");
            var closure=new Document("memberships",evidence.memberships()).append("pipelineClaims",evidence.pipelineClaims());
            if(bytes(closure)>65536)throw capacity(previous,"clear-closure-bytes");
            var value=new Document("operationId",operation).append("sequence",sequence).append("phase",automatic?"RESERVED":"POLICY_RETAINED")
                    .append("originalScope",MongoPipelinePreparationFacts.scope(original.scope()))
                    .append("originalOperation",original.operationId()).append("authorizationHash",original.hash()).append("originalAction",original.kind())
                    .append("observation",MongoSharedSrsRecoveryFacts.encode(observation))
                    .append("dependencyClosure",closure)
                    .append("members",members.stream().map(m -> new Document("nodeId",m.nodeId()).append("bootId",m.bootId()).append("memberId",m.memberId())).toList())
                    .append("replacement",MongoCapturePreparationFacts.row(next)).append("notes",noteScope(scope))
                    .append("aliasAuthorities",aliases)
                    .append("resourceNameHash",resourceNameHash)
                    .append("resourceCoordinates",related.stream().map(r -> new Document("bindingVersion",r.scope().owner().bindingVersion())
                            .append("namespace",r.scope().owner().namespace())).toList())
                    .append("resourceFactsHash",notes.resourceFactsHash(session,related))
                    .append("cohort",cohort.stream().map(c -> id(c.snapshot().previous())).toList())
                    .append("openingCoverage",openingCoverage(session,related,cohort))
                    .append("originDigest",originDigest(origin)).append("targetFingerprint",MongoStorageBindingStore.targetFingerprint(binding.cache()))
                    .append("automatic",automatic).append("frozenIssuedThrough",-1L).append("freezeReceipt",null).append("attempt",null);
            var preparations=SystemCollections.CAPTURE_PREPARATIONS.on(database);
            var priorConfiguration=preparations.find(session,new Document("_id",MongoCapturePreparationFacts.id(previous.scope(),previous.captureId()))).first();
            if(priorConfiguration==null || !previous.equals(MongoCapturePreparationFacts.available(priorConfiguration)))
                throw pending(previous,"preparation-changed");
            value.append("previousConfiguration",priorConfiguration);
            if(bytes(value)>65536)throw capacity(previous,"frozen-manifest-bytes");
            value.append("requestHash",MongoCaptureClearControls.hash(new Document(value)));
            var updated=new Document(row).append("operationSeq",sequence).append("operation",value)
                    .append("sharedNoteScope",noteScope(scope)).append("originDigest",originDigest(origin)).append("aliasAuthorities",aliases);
            replace(session,row,updated,previous);
            var result=held(updated);
            if(automatic && preparations.replaceOne(session,priorConfiguration,clearConfiguration(result,"RESERVED",null)).getMatchedCount()!=1)
                throw pending(previous,"preparation-raced");
            return result;
    }
    @Override public List<CaptureClearPreparation> cohort(CaptureClearPreparation original) {
        return transaction(session -> cohort(session,original).stream().map(this::held).map(CaptureClearPreparation.class::cast).toList());
    }
    private List<Document> cohort(ClientSession session,CaptureClearPreparation original) {
        var first=requireHeld(session,original);var operation=first.get("operation",Document.class);
        var result=new ArrayList<Document>();
        for(var key:operation.getList("cohort",Document.class)) {
            var found=SystemCollections.CAPTURE_EXTERNAL_RETIREMENTS.on(database).find(session,new Document("_id",key)).first();
            if(found==null || !original.operationId().equals(found.get("operation",Document.class).getString("operationId")))
                throw pending(original.replacement(),"cohort-changed");
            result.add(found);
        }
        return List.copyOf(result);
    }
    private List<Document> openingCoverage(ClientSession session,List<CaptureResourceNotes> related,List<CaptureClearObservation> roots) {
        var owners=related.stream().filter(n -> n.scope().owner().kind()==RequiredNoteOwner.Kind.PIPELINE)
                .map(n -> n.scope().owner().ownerId()).collect(java.util.stream.Collectors.toSet());
        var result=new ArrayList<Document>();
        if(owners.isEmpty())return List.of();
        var binding=related.getFirst().scope().owner().bindingVersion();
        for(var control:SystemCollections.CAPTURE_EXTERNAL_RETIREMENTS.on(database).find(session,new Document("_id.bindingVersion",binding))) {
            var preparation=MongoCapturePreparationFacts.annex(control.get("currentPreparation",Document.class));
            if(roots.stream().anyMatch(r -> r.snapshot().previous().captureId().equals(preparation.captureId())))continue;
            requireControlShape(control,preparation);
            var journal=CaptureOpeningIntervals.read(control.get("openingJournal",Document.class));
            if(journal.unresolved().stream().noneMatch(entry -> "LEGACY_UNTRACKED".equals(entry.get("kind"))
                    ? roots.stream().anyMatch(root -> root.snapshot().previous().sourceIdentityHash().equals(preparation.sourceIdentityHash()))
                    : owners.contains(entry.get("originalExecutionFence",Document.class).getString("pipeline"))))continue;
            result.add(new Document("control",control.get("_id")).append("resourceEpoch",control.getLong("resourceEpoch"))
                    .append("issuedThrough",journal.issuedThrough()));
            // Compete with an original closing callback or a fresh opening on this exact aggregate.
            replace(session,control,new Document(control),preparation);
        }
        if(result.size()+roots.size()>32)throw capacity(null,"opening-coverage-count");
        return List.copyOf(result);
    }
    private void requireCoverageClosed(ClientSession session,Document operation) {
        for(var coverage:operation.getList("openingCoverage",Document.class)) {
            var row=SystemCollections.CAPTURE_EXTERNAL_RETIREMENTS.on(database).find(session,new Document("_id",coverage.get("control"))).first();
            if(row==null || !coverage.get("resourceEpoch").equals(row.get("resourceEpoch")))throw pending(null,"original-opening-coverage-unproven");
            requireControlShape(row,MongoCapturePreparationFacts.annex(row.get("currentPreparation",Document.class)));
            var journal=CaptureOpeningIntervals.read(row.get("openingJournal",Document.class));
            if(journal.closedPrefixThrough()<coverage.getLong("issuedThrough"))throw pending(null,"original-opening-coverage-unresolved");
            replace(session,row,new Document(row),MongoCapturePreparationFacts.annex(row.get("currentPreparation",Document.class)));
        }
    }

    private void requireRelinquished(ClientSession session,Authority original,MongoCaptureClearCache.Observation observation) {
        var previous=observation.snapshot().previous();var records=new MongoSrsConsumerRecords(previous.scope());
        for(var stored:observation.memberships()) {
            var member=records.membership(stored);var context=member.context();
            if(!member.detached())throw pending(previous,"retained-consumer");
            var scope=new PipelineIntentScope(previous.scope().clusterId(),previous.scope().bindingVersion(),
                    previous.scope().cacheIncarnation(),context.key().pipelineId(),context.stateGeneration());
            var mapping=SystemCollections.OPERATOR_GENERATIONS.on(database).find(session,new Document("_id",scope.pipelineId())).first();
            if(mapping==null)throw pending(previous,"detached-authority-missing");
            if(scope.equals(original.scope())) {
                if(original.claim()==null || !(original.kind().equals("CLEAR")?original.claim().equals(member.commandFence()):MongoPositionFences.retained(member.commandFence(),original.claim())))
                    throw pending(previous,"detached-original-fence");
                continue;
            }
            if(mapping.containsKey("retirement")) {
                var deletion=MongoPipelineRetirementFacts.read(mapping);
                if(deletion.phase().equals("COMPLETE") && deletion.request().scope().equals(scope)) {
                    var retired=deletion.completed().cache().orElseThrow(() -> pending(previous,"detached-delete-cache-proof-missing"));
                    var cache=observation.pipelineClaims().stream().filter(value -> MongoWorkloadClaimStore.id(member.commandFence().key()).equals(value.get("_id")))
                            .findFirst().orElseThrow(() -> pending(previous,"detached-delete-claim-missing"));
                    if(!MongoPipelineRetirementFacts.cache(retired).equals(cache.get("pipelineRetirement"))
                            || !MongoPositionFences.retained(member.commandFence(),retired.claim()))throw pending(previous,"detached-delete-fence");
                    continue;
                }
            }
            var annex=mapping.get("stateRebuild",Document.class);
            if(annex==null || !Integer.valueOf(2).equals(annex.get("formatVersion")) || !annex.containsKey("result")
                    || !MongoPipelinePreparationFacts.scope(scope).equals(annex.get("originalScope")))
                throw pending(previous,"detached-explicit-clear-unproven");
            var target=MongoPipelinePreparationFacts.scope(annex.get("targetScope",Document.class));
            var prepared=MongoPipelinePreparationFacts.decode(scope.pipelineId(),mapping,scope.clusterId());
            MongoPipelineStateRebuilds.requireCompletedPrior(mapping,prepared);
            var claim=observation.pipelineClaims().stream().filter(value -> MongoWorkloadClaimStore.id(member.commandFence().key()).equals(value.get("_id")))
                    .findFirst().orElseThrow(() -> pending(previous,"detached-cache-claim-missing"));
            var cache=claim.get("pipelineRebuild",Document.class);
            if(!target.equals(prepared.scope()) || cache==null
                    || !annex.get("confirmationHash").equals(cache.get("confirmationHash"))
                    || !annex.get("operationId").equals(cache.get("operationId"))
                    || !annex.get("preparationId").equals(cache.get("preparationId"))
                    || !annex.get("originalScope").equals(cache.get("originalScope"))
                    || !annex.get("targetScope").equals(cache.get("targetScope"))
                    || !MongoPipelineStateRebuilds.claim(member.commandFence()).equals(annex.get("originalClaim")))
                throw pending(previous,"detached-clear-receipt-unproven");
        }
    }

    @Override public void frozen(CaptureClearPreparation original,CaptureClearCache.Receipt receipt) {
        if(receipt.preparation()!=original || receipt.outcome()!=CaptureClearCache.Outcome.FROZEN)throw pending(original.replacement(),"freeze-receipt");
        receipt.requireCurrent();
        change(original,List.of("RESERVED","FROZEN"),(row,op) -> {
            op.put("phase","FROZEN");op.put("freezeReceipt",receipt.hash());
            op.put("frozenIssuedThrough",CaptureOpeningIntervals.read(row.get("openingJournal",Document.class)).issuedThrough());
        });
    }

    @Override public Optional<Attempt> beginExternal(CaptureClearPreparation original,WorkloadClaimFence node) {
        var issuer=java.util.UUID.randomUUID().toString();
        boolean issued=transaction(session -> {
            var group=cohort(session,original);
            var previous=original.observation().previous();
            for(var row:group) {
            var op=row.get("operation",Document.class);
            var oldAttempt=op.get("attempt",Document.class);
            if(oldAttempt!=null && !("UNKNOWN".equals(op.getString("phase")) && "TERMINAL".equals(oldAttempt.getString("status"))))return false;
            if(!Set.of("FROZEN","UNKNOWN").contains(op.getString("phase")))throw pending(previous,"not-frozen");
            var journal=CaptureOpeningIntervals.read(row.get("openingJournal",Document.class));
            if(journal.issuedThrough()!=op.getLong("frozenIssuedThrough") || journal.closedPrefixThrough()!=journal.issuedThrough())
                throw pending(previous,"original-openings-unresolved");
            if(!notes.resourceFactsForCoordinates(session,op.getList("resourceCoordinates",Document.class)).equals(op.getString("resourceFactsHash")))
                throw pending(previous,"resource-facts-changed");
            requireCoverageClosed(session,op);
            notes.requireHeldAliases(session,op.getList("aliasAuthorities",Document.class),group.stream()
                    .map(r -> MongoCapturePreparationFacts.annex(r.get("currentPreparation",Document.class)).canonicalChainId()).toList());
            }
            for(var row:group) {
            var op=row.get("operation",Document.class);
            var oldAttempt=op.get("attempt",Document.class);long number=oldAttempt==null?1L:oldAttempt.getLong("number")+1;
            if(number<1 || number==Long.MAX_VALUE)throw capacity(previous,"attempt-number");
            var next=new Document(row);var changed=new Document(op).append("phase","STARTED")
                    .append("attempt",new Document("id",issuer).append("number",number).append("workerNode",node.owner().nodeId())
                            .append("workerBoot",node.owner().bootId()).append("status","STARTED"));
            next.put("operation",changed);replace(session,row,next,previous);
            }
            return true;
        });
        if(!issued)return Optional.empty();
        return Optional.of(new Attempt() {
            private final java.util.concurrent.atomic.AtomicBoolean consumed=new java.util.concurrent.atomic.AtomicBoolean();
            @Override public String id(){return issuer;}
            @Override public void consume(){if(!consumed.compareAndSet(false,true))throw pending(original.replacement(),"attempt-already-consumed");}
            @Override public void ended(){attemptPhase(original,issuer,"TERMINAL",null);}
            @Override public void unknown(String reason){attemptPhase(original,issuer,"STARTED",reason);}
        });
    }

    @Override public void verified(CaptureClearPreparation original,Attempt attempt) {
        changeCohort(original,List.of("STARTED","UNKNOWN"),(row,op) -> {
            var saved=op.get("attempt",Document.class);
            if(saved==null || !attempt.id().equals(saved.getString("id")) || !"TERMINAL".equals(saved.getString("status")))
                throw pending(original.replacement(),"external-attempt-not-terminal");
            op.put("phase","VERIFIED");
        });
    }
    @Override public void complete(CaptureClearPreparation original,List<CaptureClearCache.Receipt> receipts) {
        var prepared=cohort(original);
        if(prepared.size()!=receipts.size())throw pending(original.replacement(),"cohort-receipts-incomplete");
        for(var member:prepared) {
            var receipt=receipts.stream().filter(r -> r.preparation().operationId().equals(member.operationId())
                    && r.preparation().replacement().equals(member.replacement())).findFirst().orElseThrow(() -> pending(member.replacement(),"cohort-receipt-missing"));
            MongoCaptureClearCache.requireReceipt(receipt,receipt.preparation());
        }
        transaction(session -> {
            var group=cohort(session,original);var grants=new java.util.TreeMap<String,Document>();
            if(group.stream().allMatch(r -> "READY".equals(r.get("operation",Document.class).getString("phase"))))return null;
            for(var row:group) {
            var current=held(row);var op=row.get("operation",Document.class);
            if(!"VERIFIED".equals(op.getString("phase")))throw pending(original.replacement(),"external-unverified");
            if(row.getLong("resourceEpoch")==Long.MAX_VALUE)throw capacity(current.observation().previous(),"resource-epoch");
            var origin=notes.originalOrigin(session,current.notes());
            if(!originDigest(origin).equals(op.getString("originDigest")))throw pending(original.replacement(),"origin-changed");
            var nextNotes=notes.retireShared(session,current.notes());notes.prepareOrigin(session,nextNotes,origin);
            op.getList("aliasAuthorities",Document.class).forEach(g -> grants.put(g.get("noteScope",Document.class).getString("namespace"),g));
            var receipt=receipts.stream().filter(r -> r.preparation().replacement().equals(current.replacement())).findFirst().orElseThrow();
            publishClearPreparation(session,current,receipt.hash());
            var next=new Document(row).append("currentPreparation",MongoCapturePreparationFacts.row(current.replacement()))
                    .append("sharedNoteScope",noteScope(nextNotes)).append("resourceEpoch",row.getLong("resourceEpoch")+1)
                    .append("lastResult",new Document("operationId",current.operationId()).append("sequence",current.sequence())
                            .append("outcome","RETIRED").append("receiptHash",receipt.hash()))
                    .append("gcPrefix",current.observation().previous().chainId());
            next.put("operation",new Document(op).append("phase","READY"));replace(session,row,next,current.observation().previous());
            }
            notes.completeHeldAliases(session,original.notes(),List.copyOf(grants.values()));return null;
        });
    }
    @Override public void cancelling(CaptureClearPreparation original) {
        changeCohort(original,List.of("RESERVED","FROZEN"),(row,op) -> {if(op.get("attempt")!=null)throw pending(original.replacement(),"attempt-started");op.put("phase","CANCELLING");});
    }
    @Override public void cancelled(CaptureClearPreparation original,CaptureClearCache.Receipt receipt) {
        if(receipt.preparation()!=original || receipt.outcome()!=CaptureClearCache.Outcome.CANCELLED)throw pending(original.replacement(),"cancel-receipt");
        receipt.requireCurrent();transaction(session -> {
            var row=requireHeld(session,original);var op=row.get("operation",Document.class);
            if("CANCELLED".equals(op.getString("phase")))return null;
            if(!"CANCELLING".equals(op.getString("phase")))throw pending(original.replacement(),"cannot-cancel");
            if(SystemCollections.CAPTURE_PREPARATIONS.on(database).replaceOne(session,clearConfiguration(original,"RESERVED",null),
                    op.get("previousConfiguration",Document.class)).getMatchedCount()!=1)throw pending(original.replacement(),"cancel-preparation-changed");
            replace(session,row,new Document(row).append("operation",new Document(op).append("phase","CANCELLED")),original.observation().previous());return null;
        });
    }
    @Override public void recoveredUnknown(CaptureClearPreparation original) {
        changeCohort(original,List.of("STARTED","UNKNOWN"),(row,op) -> op.put("phase","UNKNOWN"));
    }
    @Override public void cleaned(CaptureClearPreparation original) {
        transaction(session -> {
            var row=requireHeld(session,original);
            if(!"READY".equals(row.get("operation",Document.class).getString("phase")))throw pending(original.replacement(),"not-complete");
            if(!notes.reclaimRetiredToken(session,original.notes(),256))return null;
            for(var alias:row.get("operation",Document.class).getList("aliasAuthorities",Document.class)) {
                var coordinate=alias.get("noteScope",Document.class);
                var owner=new RequiredNoteOwner(coordinate.getLong("bindingVersion"),coordinate.getString("namespace"),RequiredNoteOwner.Kind.PIPELINE,
                        coordinate.getString("pipelineId"),coordinate.getLong("stateGeneration"));
                if(!notes.reclaimRetiredToken(session,new RequiredNoteScope(owner,coordinate.getLong("generation")),256))return null;
            }
            replace(session,row,new Document(row).append("gcPrefix",null),original.replacement());return null;
        });
    }

    private void attemptPhase(CaptureClearPreparation original,String issuer,String status,String reason) {
        changeCohort(original,List.of("STARTED","UNKNOWN"),(row,op) -> {
            var attempt=op.get("attempt",Document.class);
            if(attempt==null || !issuer.equals(attempt.getString("id")))throw pending(original.replacement(),"attempt-changed");
            if(!"TERMINAL".equals(attempt.getString("status")))attempt=new Document(attempt).append("status",status);
            op.put("attempt",attempt);
            if(reason!=null)op.put("phase","UNKNOWN");
        });
    }
    private void change(CaptureClearPreparation original,List<String> phases,java.util.function.BiConsumer<Document,Document> action) {
        transaction(session -> {var row=requireHeld(session,original);var op=new Document(row.get("operation",Document.class));
            if(!phases.contains(op.getString("phase")))throw pending(original.replacement(),"operation-phase");
            action.accept(row,op);replace(session,row,new Document(row).append("operation",op),original.observation().previous());return null;});
    }
    private void changeCohort(CaptureClearPreparation original,List<String> phases,java.util.function.BiConsumer<Document,Document> action) {
        transaction(session -> {for(var row:cohort(session,original)) {
            var op=new Document(row.get("operation",Document.class));
            if(!phases.contains(op.getString("phase")))throw pending(original.replacement(),"cohort-phase");
            action.accept(row,op);replace(session,row,new Document(row).append("operation",op),original.observation().previous());
        }return null;});
    }
    private Document requireHeld(ClientSession session,CaptureClearPreparation original) {
        var held=requireClear(original,null);if(held.owner!=this)throw pending(original.replacement(),"different-config-owner");
        var row=control(session,original.observation().previous());var saved=row.get("operation",Document.class);
        if(saved==null || !original.operationId().equals(saved.getString("operationId"))
                || !Long.valueOf(original.sequence()).equals(saved.get("sequence")) || !original.requestHash().equals(saved.getString("requestHash")))
            throw pending(original.replacement(),"operation-changed");
        return row;
    }
    private HeldClear held(Document row) {return new HeldClear(this,new Document(row.get("operation",Document.class)));}
    static HeldClear requireClear(CaptureClearPreparation original,String targetFingerprint) {
        if(!(original instanceof HeldClear held) || targetFingerprint!=null && !targetFingerprint.equals(held.value.getString("targetFingerprint")))
            throw pending(original.replacement(),"clear-authority");
        return held;
    }
    static final class HeldClear implements CaptureClearPreparation {
        private final MongoCaptureExternalRetirements owner;private final Document value;
        private HeldClear(MongoCaptureExternalRetirements owner,Document value){this.owner=owner;this.value=value;}
        @Override public String operationId(){return value.getString("operationId");}
        @Override public long sequence(){return value.getLong("sequence");}
        @Override public PipelineIntentScope originalScope(){return MongoPipelinePreparationFacts.scope(value.get("originalScope",Document.class));}
        @Override public String originalOperationId(){return value.getString("originalOperation");}
        @Override public List<CaptureRetirementMember> members(){return value.getList("members",Document.class).stream()
                .map(m -> new CaptureRetirementMember(m.getString("nodeId"),m.getString("bootId"),m.getString("memberId"))).toList();}
        @Override public SharedSrsRecoverySnapshot observation(){return MongoSharedSrsRecoveryFacts.decode(value.get("observation",Document.class));}
        @Override public CapturePreparation replacement(){return MongoCapturePreparationFacts.annex(value.get("replacement",Document.class));}
        @Override public RequiredNoteScope notes(){return readNoteScope(value.get("notes",Document.class));}
        @Override public String requestHash(){return value.getString("requestHash");}
        @Override public void requireCurrent(){owner.transaction(session -> {owner.requireHeld(session,this);return null;});}
        void requirePhase(String... phases){owner.transaction(session -> {var row=owner.requireHeld(session,this);
            if(!List.of(phases).contains(row.get("operation",Document.class).getString("phase")))throw pending(replacement(),"operation-phase");return null;});}
    }
    private static Document noteScope(RequiredNoteScope scope) {
        return new Document("binding",scope.owner().bindingVersion()).append("namespace",scope.owner().namespace())
                .append("owner",scope.owner().ownerId()).append("generation",scope.generation());
    }
    private static RequiredNoteScope readNoteScope(Document value) {
        return new RequiredNoteScope(new RequiredNoteOwner(value.getLong("binding"),value.getString("namespace"),
                RequiredNoteOwner.Kind.CHAIN,value.getString("owner"),0),value.getLong("generation"));
    }
    private static String originDigest(RequiredNoteOrigin origin) {
        return io.tapstate.core.model.canonical.CanonicalHash.ofText(io.tapstate.core.common.JsonWriter.write(
                List.of(origin.connectorId(),origin.artifactHash(),java.util.Base64.getEncoder().encodeToString(origin.settings()))));
    }
    static String clearChain(CapturePreparation previous,String operation) {
        return "srs-clear-"+io.tapstate.core.model.canonical.CanonicalHash.ofText(io.tapstate.core.common.JsonWriter.write(
                List.of("native-explicit-clear",previous.scope().clusterId(),previous.scope().bindingVersion(),
                        previous.scope().cacheIncarnation(),previous.captureId(),previous.preparationId(),operation)));
    }
    private void publishClearPreparation(ClientSession session,CaptureClearPreparation original,String receipt) {
        var table=SystemCollections.CAPTURE_PREPARATIONS.on(database);
        if(table.replaceOne(session,clearConfiguration(original,"RESERVED",null),clearConfiguration(original,"READY",receipt)).getMatchedCount()!=1)
            throw pending(original.replacement(),"preparation-raced");
    }
    private static Document clearConfiguration(CaptureClearPreparation original,String phase,String receipt) {
        var previous=original.observation().previous();
        var marker=new Document("operationId",original.operationId()).append("sequence",original.sequence())
                .append("previousChain",previous.chainId()).append("previousPreparationId",previous.preparationId())
                .append("nextChain",original.replacement().chainId()).append("nextPreparationId",original.replacement().preparationId())
                .append("phase",phase).append("receiptHash",receipt);
        MongoCapturePreparationFacts.requireBytes(marker,4096);
        var row=new Document("_id",MongoCapturePreparationFacts.id(previous.scope(),previous.captureId())).append("formatVersion",3)
                .append("original",MongoCapturePreparationFacts.row(previous.birth())).append("currentClear",marker);
        return row;
    }
    private Opening register(Boundary original) {
        var entry = transaction(session -> {
            var preparation = original.request().preparation();
            requireScope(session, preparation.scope());
            var row = control(session, preparation);
            requireOpeningAllowed(row,preparation);requireOpeningAuthority(session,row,preparation);
            var journal = CaptureOpeningIntervals.read(row.get("openingJournal", Document.class));
            long expectedSequence = original.expectedIssued() + 1;
            var held = journal.unresolved().stream().filter(value -> value.getLong("sequence") == expectedSequence).findFirst();
            if (held.isPresent()) {
                if (!original.openingId().equals(held.orElseThrow().getString("openingId"))) throw pending(preparation, "registration-raced");
                return held.orElseThrow();
            }
            if (original.expectedIssued() != journal.issuedThrough() || original.epoch() != row.getLong("resourceEpoch"))
                throw pending(preparation, "registration-stale");
            requireOpeningAllowed(row, preparation);
            if (journal.unresolved().size() >= CaptureOpeningIntervals.MAX_OPENINGS || journal.issuedThrough() >= Long.MAX_VALUE - 1)
                throw capacity(preparation, "opening-count");
            var request = original.request(); var pipeline = request.pipelineFence(); var capture = request.captureFence();
            var value = new Document("sequence", expectedSequence).append("openingId", original.openingId())
                    .append("resourceEpoch", original.epoch()).append("kind", "NATIVE_INSTANCE").append("state", "REGISTERED")
                    .append("nodeId", request.node().owner().nodeId()).append("bootId", request.node().owner().bootId())
                    .append("originalExecutionFence", new Document("pipeline", request.pipeline().pipelineId())
                            .append("stateGeneration", request.pipeline().stateGeneration())
                            .append("cacheIncarnation", request.pipeline().cacheIncarnation())
                            .append("claimGeneration", pipeline.claimGeneration()).append("executionGeneration", pipeline.executionGeneration())
                            .append("topologyRevision", pipeline.topologyRevision()))
                    .append("captureFence", new Document("ownerNodeId", capture.owner().nodeId()).append("ownerBootId", capture.owner().bootId())
                            .append("claimGeneration", capture.claimGeneration()).append("executionGeneration", capture.executionGeneration())
                            .append("topologyRevision", capture.topologyRevision()));
            if (bytes(value) > MAX_OPENING_BYTES) throw capacity(preparation, "opening-bytes");
            var next = new Document(row).append("openingJournal", CaptureOpeningIntervals.append(journal, original.expectedIssued(), value).document());
            replace(session, row, next, preparation);
            return value;
        });
        return new Opening() {
            private boolean accessed;
            private boolean closed;
            @Override public synchronized void accessing() {
                if (accessed || closed) throw pending(original.request().preparation(), "opening-handle-used");
                changeOpening(original, entry, false, false); accessed = true;
            }
            @Override public synchronized void closed() {
                if (!closed) { changeOpening(original, entry, true, accessed); closed = true; }
            }
        };
    }

    private void changeOpening(Boundary original, Document allocated, boolean closed, boolean ownedAccess) {
        transaction(session -> {
            // A cleanup callback may prove only its own old physical exit after its business lease ended.
            var preparation = original.request().preparation();
            var row = control(session, preparation);
            var journal = CaptureOpeningIntervals.read(row.get("openingJournal", Document.class));
            var found = journal.unresolved().stream().filter(value -> value.getLong("sequence").equals(allocated.getLong("sequence"))).findFirst();
            if (found.isEmpty()) {
                if (closed && allocated.getLong("sequence") <= journal.issuedThrough()) return null;
                throw pending(preparation, "opening-already-closed");
            }
            var entry = found.orElseThrow();
            if (original.epoch() != row.getLong("resourceEpoch") || !entry.getString("openingId").equals(original.openingId())
                    || !sameIdentity(allocated, entry)) throw pending(preparation, "opening-identity");
            CaptureOpeningIntervals.State next;
            if (closed) {
                if (!ownedAccess && !"REGISTERED".equals(entry.getString("state")))
                    throw pending(preparation, "opening-close-not-owned");
                next = CaptureOpeningIntervals.close(journal, entry);
            }
            else {
                requireOpeningAllowed(row, preparation);
                requireOpeningAuthority(session,row,preparation);
                if (!"REGISTERED".equals(entry.getString("state"))) throw pending(preparation, "opening-access-already-started");
                var entries = new ArrayList<>(journal.unresolved());
                entries.set(entries.indexOf(entry), new Document(entry).append("state", "ACCESS_STARTED"));
                next = new CaptureOpeningIntervals.State(journal.issuedThrough(), journal.closedPrefixThrough(), journal.ranges(), entries);
            }
            replace(session, row, new Document(row).append("openingJournal", next.document()), preparation);
            return null;
        });
    }

    private static boolean sameIdentity(Document original, Document current) {
        var first = new Document(original); var second = new Document(current);
        first.remove("state"); second.remove("state"); return first.equals(second);
    }
    Document control(ClientSession session, CapturePreparation preparation) {
        var row = SystemCollections.CAPTURE_EXTERNAL_RETIREMENTS.on(database).find(session, new Document("_id", id(preparation))).first();
        return requireControlShape(row,preparation);
    }
    private static Document requireControlShape(Document row,CapturePreparation preparation) {
        if (row == null || !Integer.valueOf(1).equals(row.get("formatVersion"))
                || !(row.get("revision") instanceof Long revision) || revision < 1 || revision == Long.MAX_VALUE
                || !(row.get("resourceEpoch") instanceof Long epoch) || epoch < 1)
            throw pending(preparation, "control-unproven");
        requireOpeningShapes(row);
        return row;
    }
    private static void requireOpeningShapes(Document row) {
        try {
            var journal=CaptureOpeningIntervals.read(row.get("openingJournal",Document.class));
            for(var opening:journal.unresolved()) {
                if(bytes(opening)>MAX_OPENING_BYTES)throw unreadableOpening();
                if("LEGACY_UNTRACKED".equals(opening.get("kind"))) {
                    requireLegacyOpening(row,opening);
                    continue;
                }
                if(!opening.keySet().equals(Set.of("sequence","openingId","resourceEpoch","kind","state",
                        "nodeId","bootId","originalExecutionFence","captureFence"))
                        || !"NATIVE_INSTANCE".equals(opening.get("kind"))
                        || !Set.of("REGISTERED","ACCESS_STARTED").contains(opening.get("state"))
                        || !(opening.get("resourceEpoch")instanceof Long epoch) || epoch<1 || epoch>row.getLong("resourceEpoch")
                        || !nonblank(opening.get("nodeId")) || !nonblank(opening.get("bootId")))throw unreadableOpening();
                String openingId=opening.getString("openingId");
                if(!UUID.fromString(openingId).toString().equals(openingId))throw unreadableOpening();
                var execution=opening.get("originalExecutionFence",Document.class);
                if(!execution.keySet().equals(Set.of("pipeline","stateGeneration","cacheIncarnation","claimGeneration",
                        "executionGeneration","topologyRevision"))
                        || !nonblank(execution.get("pipeline")) || !nonblank(execution.get("cacheIncarnation"))
                        || !counter(execution,"stateGeneration",1) || !counter(execution,"claimGeneration",1)
                        || !counter(execution,"executionGeneration",0) || !counter(execution,"topologyRevision",0))throw unreadableOpening();
                var capture=opening.get("captureFence",Document.class);
                if(!validCaptureFence(capture))throw unreadableOpening();
            }
        }catch(IllegalArgumentException|ClassCastException|NullPointerException invalid){throw unreadableOpening();}
    }
    private static void requireLegacyOpening(Document control,Document opening) {
        if(!opening.keySet().equals(Set.of("sequence","openingId","resourceEpoch","kind","state",
                "oldBusinessScope","previousCaptureFence","coverageDigest")) || !"UNTRACKED".equals(opening.get("state"))
                || !(opening.get("resourceEpoch")instanceof Long epoch) || epoch<1 || epoch>control.getLong("resourceEpoch")
                || !(opening.get("openingId")instanceof String openingId) || !openingId.startsWith("legacy-")
                || !UUID.fromString(openingId.substring(7)).toString().equals(openingId.substring(7))
                || !(opening.get("coverageDigest")instanceof String digest) || !digest.matches("[0-9a-f]{64}"))throw unreadableOpening();
        var scope=opening.get("oldBusinessScope",Document.class);var id=control.get("_id",Document.class);
        if(!scope.keySet().equals(Set.of("clusterId","bindingVersion","cacheIncarnation")) || !nonblank(scope.get("cacheIncarnation"))
                || !id.get("clusterId").equals(scope.get("clusterId")) || !id.get("bindingVersion").equals(scope.get("bindingVersion"))
                || !validCaptureFence(opening.get("previousCaptureFence",Document.class)))throw unreadableOpening();
    }
    private static boolean validCaptureFence(Document capture) {
        return capture.keySet().equals(Set.of("ownerNodeId","ownerBootId","claimGeneration","executionGeneration","topologyRevision"))
                && nonblank(capture.get("ownerNodeId")) && nonblank(capture.get("ownerBootId"))
                && counter(capture,"claimGeneration",1) && counter(capture,"executionGeneration",0)
                && counter(capture,"topologyRevision",0);
    }
    private static boolean nonblank(Object value) { return value instanceof String text && !text.isBlank(); }
    private static boolean counter(Document row,String key,long minimum) { return row.get(key)instanceof Long value && value>=minimum; }
    private static TapstateException unreadableOpening() {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE,Map.of("id","capture-opening-journal","field","opening"),null);
    }
    void requireOpen(Document row, CapturePreparation preparation) {
        if (!MongoCapturePreparationFacts.annex(row.get("currentPreparation", Document.class)).equals(preparation))
            throw pending(preparation, "preparation-changed");
        var operation = row.get("operation", Document.class);
        if (operation != null && !Set.of("READY", "CANCELLED", "POLICY_RETAINED").contains(operation.getString("phase")))
            throw pending(preparation, "external-operation-pending");
    }
    private void requireOpeningAllowed(Document row,CapturePreparation preparation) {
        var operation=row.get("operation",Document.class);
        if(operation!=null && "RESERVED".equals(operation.getString("phase"))) {
            if(!MongoCapturePreparationFacts.annex(row.get("currentPreparation",Document.class)).equals(preparation))
                throw pending(preparation,"preparation-changed");
            return;
        }
        requireOpen(row,preparation);
    }
    void requireAdmitted(ClientSession session,CapturePreparation preparation) {
        var row=control(session,preparation);requireOpen(row,preparation);requireAuthoritative(session,preparation);
    }
    void loadedReady(ClientSession session,CapturePreparation preparation) {
        var row=control(session,preparation);
        requireOpen(row,preparation);requireAuthoritative(session,preparation);
    }
    private Document preparation(ClientSession session,CapturePreparation expected) {
        var row=SystemCollections.CAPTURE_PREPARATIONS.on(database).find(session,
                new Document("_id",MongoCapturePreparationFacts.id(expected.scope(),expected.captureId()))).first();
        if(row==null)throw pending(expected,"authoritative-preparation-missing");return row;
    }
    private void requireAuthoritative(ClientSession session,CapturePreparation expected) {
        if(!expected.equals(MongoCapturePreparationFacts.available(preparation(session,expected))))throw pending(expected,"authoritative-preparation-changed");
    }
    private void requireOpeningAuthority(ClientSession session,Document control,CapturePreparation expected) {
        var row=preparation(session,expected);var operation=control.get("operation",Document.class);
        if(operation!=null && "RESERVED".equals(operation.getString("phase")) && row.equals(clearConfiguration(held(control),"RESERVED",null))) {
            if(!expected.equals(MongoCapturePreparationFacts.available(operation.get("previousConfiguration",Document.class))))
                throw pending(expected,"original-clear-preparation-changed");
            return;
        }
        if(!expected.equals(MongoCapturePreparationFacts.available(row)))throw pending(expected,"authoritative-preparation-changed");
    }
    void reserveRecovery(ClientSession session,CapturePreparation previous) {
        requireAuthoritative(session,previous);var row=control(session,previous);requireOpen(row,previous);
        replace(session,row,new Document(row),previous);
    }
    void reconfirmRecovery(ClientSession session,MongoCapturePreparationFacts.Recovery original) {
        if(!MongoCapturePreparationFacts.configurationRow(original).equals(preparation(session,original.previous())))
            throw pending(original.previous(),"reconfirmation-preparation-changed");
        var row=control(session,original.previous());requireOpen(row,original.previous());replace(session,row,new Document(row),original.previous());
    }
    void publishedRecovery(ClientSession session,CapturePreparation previous,CapturePreparation replacement) {
        var row=control(session,previous);requireOpen(row,previous);
        requireAuthoritative(session,replacement);
        if(!previous.scope().equals(replacement.scope()) || !previous.birth().equals(replacement.birth()) || !replacement.replacement())
            throw pending(previous,"recovery-publication-history");
        replace(session,row,new Document(row).append("currentPreparation",MongoCapturePreparationFacts.row(replacement)),previous);
    }
    void replace(ClientSession session, Document original, Document next, CapturePreparation preparation) {
        long revision=original.getLong("revision");
        if(revision>=Long.MAX_VALUE-1)throw capacity(preparation,"control-revision");
        next.put("revision", revision + 1);
        if (bytes(next) > MAX_CONTROL_BYTES) throw capacity(preparation, "control-bytes");
        if (SystemCollections.CAPTURE_EXTERNAL_RETIREMENTS.on(database).replaceOne(session, original, next).getMatchedCount() != 1)
            throw pending(preparation, "control-changed");
    }
    private void requireScope(ClientSession session, CacheBusinessScope scope) {
        var current = bindings.cachePreparationForTransaction(session).orElseThrow(() -> pending(null, "binding-unproven"));
        if (!cluster.equals(scope.clusterId()) || !scope.equals(current.businessScope())) throw pending(null, "binding-changed");
    }
    private <T> T transaction(Function<ClientSession,T> body) {
        return transaction(TX,body);
    }
    private <T> T transaction(TransactionOptions options,Function<ClientSession,T> body) {
        return StoreIo.call(() -> {
            active.run();
            try (var session = client.startSession()) {
                return session.withTransaction(() -> {
                    active.run();
                    T result=body.apply(session);
                    active.run();
                    return result;
                }, options);
            }
        });
    }
    static int bytes(Document row) {
        try (var output = new BasicOutputBuffer(); var writer = new BsonBinaryWriter(output)) {
            new DocumentCodec().encode(writer, row, EncoderContext.builder().build()); return output.getSize();
        }
    }
    static TapstateException pending(CapturePreparation preparation, String reason) {
        return refusal(IoError.CAPTURE_EXTERNAL_RETIREMENT_PENDING, preparation, reason);
    }
    static TapstateException capacity(CapturePreparation preparation, String reason) {
        return refusal(IoError.CAPTURE_RETIREMENT_CAPACITY, preparation, reason);
    }
    static void fenceCohortChange(ClientSession session,MongoDatabase database) {
        ConfigurationVersion.advance(session,SystemCollections.CONFIGURATION_VERSION.on(database));
    }
    private static TapstateException refusal(IoError code, CapturePreparation preparation, String reason) {
        return new TapstateException(code, Map.of("capture", preparation == null ? "unproven" : preparation.captureId(),
                "operation", "opening", "reason", reason), null);
    }
}
