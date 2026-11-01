package io.tapstate.adapters.mongostore;

import static org.assertj.core.api.Assertions.*;

import com.mongodb.client.MongoClients;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.model.*;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.*;
import io.tapstate.testsupport.DockerGate;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

/** Real independent CONFIG/CACHE clients; a released lease is not a physical close receipt. */
class MongoCaptureExternalRetirementsIT {
    private static MongoDBContainer server;
    private static final Duration TTL=Duration.ofMinutes(2);
    private static final WorkloadOwner A=new WorkloadOwner("node-a","boot-a");
    private static final WorkloadOwner B=new WorkloadOwner("node-b","boot-b");
    @BeforeAll static void start() {
        DockerGate.require();
        server=new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
        var owner=System.getProperty("tapstate.e2e.resource-owner");
        if(owner!=null && !owner.isBlank())server.withLabel("io.tapstate.issue-loop.owner",owner);
        server.withTmpFs(Map.of("/data/db","rw,size=512m","/data/configdb","rw,size=16m"));
        var command=new ArrayList<>(Arrays.asList(server.getCommandParts()));
        command.addAll(List.of("--wiredTigerCacheSizeGB","0.25","--oplogSize","16"));server.withCommand(command.toArray(String[]::new));
        server.withCreateContainerCmdModifier(c -> c.getHostConfig().withMemory(768L<<20).withNanoCPUs(1_000_000_000L));
        server.start();
    }
    @AfterAll static void stop(){if(server!=null)server.stop();}

    @ParameterizedTest
    @ValueSource(booleans={false,true})
    void aHeldAliasWithoutItsRetainedOriginRefusesBeforePublishingAnything(boolean completing) throws Exception {
        fixture(f -> {
            var retirement=f.configuration.captureExternalRetirements();var notes=f.configuration.requiredNotes();
            var shared=f.shared(notes);
            var source=notes.initialize(RequiredNoteOwner.forNode(f.prepared.scope().bindingVersion(),
                    new PipelineNode("flow","input"),PipelineNode.Purpose.CAPTURE,1));
            notes.prepareOrigin(source,notes.origin(shared));
            var clear=f.clear();
            var observed=retirement.resourceNotes(f.prepared.scope()).stream().filter(n -> n.scope().equals(source)).findFirst().orElseThrow();
            retirement.relinquishAliases(clear,List.of(new CaptureAliasObservation(observed,"original-slot")));
            var key=new Document("_id",new Document("bindingVersion",source.owner().bindingVersion()).append("namespace",source.owner().namespace()));
            var table=SystemCollections.PDK_NOTE_SCOPES.on(f.database);
            var hold=table.find(key).first().get("externalRetirementHold",Document.class);
            table.updateOne(key,new Document("$set",new Document("originGeneration",0L)));
            SystemCollections.PDK_NOTE_ORIGINS.on(f.database).deleteOne(key);
            var before=table.find(key).first();
            try(var client=MongoClients.create(server.getReplicaSetUrl());var session=client.startSession()) {
                var database=client.getDatabase(f.database.getName());
                var store=new MongoRequiredNoteStore(client,database,new SourceConfigCipher(new byte[32]));
                session.startTransaction();
                try {
                    assertThatThrownBy(() -> {
                        if(completing)store.completeHeldAliases(session,shared,List.of(hold));
                        else store.heldAliases(session,shared,List.of(observed),List.of(f.prepared.canonicalChainId()),hold.getString("resourceManifestHash"));
                    }).isInstanceOfSatisfying(io.tapstate.core.common.TapstateException.class,
                            failure -> assertThat(failure.code()).isEqualTo(IoError.NOTE_SCOPE_UNAVAILABLE));
                } finally {session.abortTransaction();}
            }
            assertThat(table.find(key).first()).isEqualTo(before);
            assertThat(SystemCollections.PDK_NOTE_ORIGINS.on(f.database).find(key).first()).isNull();
        });
    }

    @ParameterizedTest
    @ValueSource(booleans={false,true})
    void aliasReadmissionAndReservationCompeteWithoutChangingTheRetainedNotes(boolean reserved) throws Exception {
        fixture(f -> {
            var retirement=f.configuration.captureExternalRetirements();var notes=f.configuration.requiredNotes();var shared=f.shared(notes);
            var owner=RequiredNoteOwner.forNode(f.prepared.scope().bindingVersion(),new PipelineNode("flow","input"),PipelineNode.Purpose.CAPTURE,1);
            var source=notes.initialize(owner);var origin=notes.origin(shared);notes.prepareOrigin(source,origin);
            notes.compareAndSet(source,"slot",0,new byte[]{7,9},false);notes.prepareCarry(new RequiredNoteCarry(shared,source));
            var clear=f.clear();var observed=retirement.resourceNotes(f.prepared.scope()).stream().filter(n -> n.scope().equals(source)).findFirst().orElseThrow();
            retirement.relinquishAliases(clear,List.of(new CaptureAliasObservation(observed,"original-slot")));
            var cache=f.cache.capturePreparations(f.prepared.scope(),() -> {}).explicitClear();
            if(reserved)retirement.reserve(clear,List.of(cache.inspect(f.prepared)),retirement.resourceNotes(f.prepared.scope()),
                    CaptureResourceNotes.nameHash("original-slot"),origin,true,List.of(new CaptureRetirementMember(A.nodeId(),A.bootId(),"member-a")));
            f.configuration.rebuilds().complete(f.cache.pipelineRebuilds(f.prepared.scope(),() -> {}).prepare(clear,f.node,TTL,Instant.now()));
            var next=notes.open(RequiredNoteOwner.forNode(owner.bindingVersion(),new PipelineNode("flow","input"),PipelineNode.Purpose.CAPTURE,2));
            var before=f.control();
            if(reserved) {
                assertThatThrownBy(() -> notes.prepareOrigin(next,origin)).isInstanceOfSatisfying(io.tapstate.core.common.TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(IoError.NOTE_REFERENCED));
                assertThat(f.control()).isEqualTo(before);
            } else {
                assertThat(notes.prepareOrigin(next,origin)).isEqualTo(origin);
                assertThat(retirement.resourceNotes(f.prepared.scope()).stream().filter(n -> n.scope().equals(next))).singleElement().satisfies(n -> assertThat(n.resumable()).isTrue());
                assertThatThrownBy(() -> retirement.reserve(clear,List.of(cache.inspect(f.prepared)),retirement.resourceNotes(f.prepared.scope()),
                        CaptureResourceNotes.nameHash("original-slot"),origin,true,List.of(new CaptureRetirementMember(A.nodeId(),A.bootId(),"member-a"))))
                        .isInstanceOf(io.tapstate.core.common.TapstateException.class);
                assertThat(f.control().get("operation")).isNull();
            }
            assertThat(notes.read(next,"slot").bytes()).containsExactly(7,9);
        });
    }

    @Test void aFrozenSharedScopeMayOnlyMaterializeTheExactExistingCarry() throws Exception {
        fixture(f -> {
            var retirement=f.configuration.captureExternalRetirements();var notes=f.configuration.requiredNotes();var shared=f.shared(notes);
            var source=notes.initialize(RequiredNoteOwner.forNode(f.prepared.scope().bindingVersion(),new PipelineNode("flow","input"),PipelineNode.Purpose.CAPTURE,1));
            notes.prepareOrigin(source,notes.origin(shared));notes.compareAndSet(source,"slot",0,new byte[]{7,9},false);
            notes.prepareCarry(new RequiredNoteCarry(shared,source));notes.compareAndSet(shared,"blocked",0,null,true);
            var clear=f.clear();var observed=retirement.resourceNotes(f.prepared.scope()).stream().filter(n -> n.scope().equals(source)).findFirst().orElseThrow();
            retirement.relinquishAliases(clear,List.of(new CaptureAliasObservation(observed,"original-slot")));
            var cache=f.cache.capturePreparations(f.prepared.scope(),() -> {}).explicitClear();
            var held=retirement.reserve(clear,List.of(cache.inspect(f.prepared)),retirement.resourceNotes(f.prepared.scope()),
                    CaptureResourceNotes.nameHash("original-slot"),notes.origin(shared),true,List.of(new CaptureRetirementMember(A.nodeId(),A.bootId(),"member-a"))).getFirst();
            retirement.frozen(held,cache.freeze(held,f.node));
            assertThat(notes.compareAndSet(shared,"slot",0,new byte[]{7,9},false)).isTrue();
            for(String key:List.of("slot","absent","blocked")) {
                var before=notes.read(shared,key);
                assertThatThrownBy(() -> notes.compareAndSet(shared,key,before.revision(),new byte[]{8},false))
                        .isInstanceOfSatisfying(io.tapstate.core.common.TapstateException.class,failure -> assertThat(failure.code()).isEqualTo(IoError.NOTE_REFERENCED));
                assertThat(notes.read(shared,key)).isEqualTo(before);
            }
            assertThatThrownBy(() -> notes.compareAndSet(shared,"slot",1,null,true)).isInstanceOf(io.tapstate.core.common.TapstateException.class);
            assertThat(notes.read(shared,"slot").bytes()).containsExactly(7,9);
        });
    }

    @Test void revisionExhaustionRefusesBeforeMakingTheControlUnreadable() throws Exception {
        fixture(f -> {
            var table=SystemCollections.CAPTURE_EXTERNAL_RETIREMENTS.on(f.database);
            var registration=f.configuration.captureOpeningJournal().prepare(f.opening(A));
            table.updateOne(new Document("_id",MongoCaptureExternalRetirements.id(f.prepared)),
                    new Document("$set",new Document("revision",Long.MAX_VALUE-1)));
            var before=f.control();
            assertThatThrownBy(registration::register).isInstanceOfSatisfying(io.tapstate.core.common.TapstateException.class,
                    failure -> assertThat(failure.code()).isEqualTo(IoError.CAPTURE_RETIREMENT_CAPACITY));
            assertThat(f.control()).isEqualTo(before);
            assertThatThrownBy(() -> f.configuration.captureOpeningJournal().prepare(f.opening(A)))
                    .isInstanceOfSatisfying(io.tapstate.core.common.TapstateException.class,
                            failure -> assertThat(failure.code()).isEqualTo(IoError.CAPTURE_RETIREMENT_CAPACITY));
            assertThat(f.control()).isEqualTo(before);
        });
    }

    @ParameterizedTest
    @ValueSource(strings={"missing-state","invalid-state","kind","legacy-kind","epoch","execution-type","execution-scope","capture-counter","capture-owner"})
    void malformedOriginalOpeningsNeverAuthorizeAnotherOpening(String corruption) throws Exception {
        fixture(f -> {
            f.configuration.captureOpeningJournal().prepare(f.opening(A)).register();
            var row=f.control();var entry=row.get("openingJournal",Document.class).getList("unresolved",Document.class).getFirst();
            switch(corruption) {
                case "missing-state" -> entry.remove("state");
                case "invalid-state" -> entry.put("state","UNKNOWN");
                case "kind" -> entry.put("kind","UNKNOWN");
                case "legacy-kind" -> entry.put("kind","LEGACY_UNTRACKED");
                case "epoch" -> entry.put("resourceEpoch",0L);
                case "execution-type" -> entry.get("originalExecutionFence",Document.class).put("claimGeneration","1");
                case "execution-scope" -> entry.get("originalExecutionFence",Document.class).put("cacheIncarnation",7L);
                case "capture-counter" -> entry.get("captureFence",Document.class).put("claimGeneration",0L);
                case "capture-owner" -> entry.get("captureFence",Document.class).put("ownerBootId",null);
                default -> throw new IllegalArgumentException("unknown corruption fixture");
            }
            SystemCollections.CAPTURE_EXTERNAL_RETIREMENTS.on(f.database).replaceOne(new Document("_id",row.get("_id")),row);
            var before=f.control();
            assertThatThrownBy(() -> f.configuration.captureOpeningJournal().prepare(f.opening(A)))
                    .isInstanceOfSatisfying(io.tapstate.core.common.TapstateException.class,
                            failure -> assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
            assertThat(f.control()).isEqualTo(before);
        });
    }

    @Test void resourceEpochExhaustionRefusesBeforeReservationButStillAllowsPolicyRetention() throws Exception {
        fixture(f -> {
            var retirement=f.configuration.captureExternalRetirements();var notes=f.configuration.requiredNotes();var shared=f.shared(notes);
            SystemCollections.CAPTURE_EXTERNAL_RETIREMENTS.on(f.database).updateOne(
                    new Document("_id",MongoCaptureExternalRetirements.id(f.prepared)),new Document("$set",new Document("resourceEpoch",Long.MAX_VALUE)));
            var clear=f.clear();var cache=f.cache.capturePreparations(f.prepared.scope(),() -> {}).explicitClear();
            var observation=cache.inspect(f.prepared);var related=retirement.resourceNotes(f.prepared.scope());var before=f.control();
            var members=List.of(new CaptureRetirementMember(A.nodeId(),A.bootId(),"member-a"));
            assertThatThrownBy(() -> retirement.reserve(clear,List.of(observation),related,CaptureResourceNotes.nameHash("original-slot"),
                    notes.origin(shared),true,members)).isInstanceOfSatisfying(io.tapstate.core.common.TapstateException.class,
                            failure -> assertThat(failure.code()).isEqualTo(IoError.CAPTURE_RETIREMENT_CAPACITY));
            assertThat(f.control()).isEqualTo(before);
            retirement.reserve(clear,List.of(observation),related,CaptureResourceNotes.nameHash("original-slot"),notes.origin(shared),false,members);
            assertThat(f.control().get("operation",Document.class).getString("phase")).isEqualTo("POLICY_RETAINED");
            assertThat(f.control().get("operation",Document.class).get("attempt")).isNull();
            assertThat(f.control().getLong("resourceEpoch")).isEqualTo(before.getLong("resourceEpoch"));
            assertThat(notes.origin(shared)).isNotNull();
            assertThat(retirement.prepare(f.opening(A))).isNotNull();
        });
    }

    @Test void aNewFencedOwnerCanOpenWhileTheOriginalTokenRemainsUnresolved() throws Exception {
        fixture(f -> {
            var journal=f.configuration.captureOpeningJournal();
            var old=journal.prepare(f.opening(A));var first=old.register();first.accessing();
            assertThat(f.claims.release(f.capture)).isTrue();
            assertThat(f.claims.release(f.pipeline)).isTrue();
            var node=WorkloadClaimFence.from(f.claims.acquire(new WorkloadClaimKey("cluster",WorkloadClaimType.NODE_SESSION,B.nodeId()),B,1,TTL).claim());
            var pipeline=f.claims.acquirePipeline(f.request.pipeline(),B,1,TTL).claim();
            var running=f.claims.advanceExecution(pipeline,1,Set.of(B.nodeId())).orElseThrow();
            var capture=f.claims.acquire(f.capture.key(),B,1,TTL).claim();
            var next=journal.prepare(new CaptureOpeningJournal.Request(f.prepared,f.request.pipeline(),WorkloadClaimFence.from(running),WorkloadClaimFence.from(capture),node));
            var second=next.register();second.accessing();
            assertThat(f.entries()).hasSize(2);
            second.closed();
            assertThat(f.entries()).singleElement().satisfies(row -> assertThat(row.getString("bootId")).isEqualTo(A.bootId()));
            assertThat(f.control().get("openingJournal",Document.class).getLong("closedPrefixThrough")).isZero();
            // Only the original handle can positively close this original physical opening after lease loss.
            first.closed();
            assertThat(f.entries()).isEmpty();
            assertThat(f.control().get("openingJournal",Document.class).getLong("closedPrefixThrough")).isEqualTo(2);
            assertThatThrownBy(old::register).isInstanceOf(io.tapstate.core.common.TapstateException.class);
            assertThat(f.entries()).isEmpty();
        });
    }

    @Test void aDuplicateRegistrationCannotAccessOrCloseTheFirstPhysicalInstance() throws Exception {
        fixture(f -> {
            var registration=f.configuration.captureOpeningJournal().prepare(f.opening(A));
            var first=registration.register();first.accessing();
            var duplicate=registration.register();
            assertThatThrownBy(duplicate::accessing).isInstanceOf(io.tapstate.core.common.TapstateException.class);
            assertThatThrownBy(duplicate::closed).isInstanceOf(io.tapstate.core.common.TapstateException.class);
            assertThat(f.entries()).hasSize(1);
            first.closed();assertThat(f.entries()).isEmpty();
        });
    }

    @Test void anUnresolvedOriginalOpeningBlocksReleaseButItsExactLateCloseCanContinue() throws Exception {
        fixture(f -> {
            var retirement=f.configuration.captureExternalRetirements();
            var opening=retirement.prepare(f.opening(A)).register();opening.accessing();
            var notes=f.configuration.requiredNotes();var shared=f.shared(notes);
            var original=f.clear();var cache=f.cache.capturePreparations(f.prepared.scope(),() -> {}).explicitClear();
            var context=retirement.resourceNotes(f.prepared.scope());
            var held=retirement.reserve(original,List.of(cache.inspect(f.prepared)),context,CaptureResourceNotes.nameHash("original-slot"),
                    notes.origin(shared),true,List.of(new CaptureRetirementMember(A.nodeId(),A.bootId(),"member-a"))).getFirst();
            var frozen=cache.freeze(held,f.node);retirement.frozen(held,frozen);
            assertThatThrownBy(() -> retirement.beginExternal(held,f.node)).satisfies(thrown ->
                    assertThat(((io.tapstate.core.common.TapstateException)thrown).args()).containsEntry("reason","original-openings-unresolved"));
            assertThat(f.control().get("operation",Document.class).get("attempt")).isNull();
            opening.closed();var attempt=retirement.beginExternal(held,f.node).orElseThrow();attempt.consume();
            retirement.recoveredUnknown(held);
            assertThat(retirement.beginExternal(held,f.node)).isEmpty();
            assertThat(notes.origin(shared)).isNotNull();
            // A known source worker terminal can reconcile only this exact attempt.
            attempt.ended();retirement.verified(held,attempt);
            var receipt=cache.apply(held,f.node);retirement.complete(held,List.of(receipt));
            assertThat(f.control().get("operation",Document.class).getString("phase")).isEqualTo("READY");
            assertThatThrownBy(() -> attempt.unknown("old-callback")).isInstanceOf(io.tapstate.core.common.TapstateException.class);
            assertThat(f.control().get("operation",Document.class).getString("phase")).isEqualTo("READY");
        });
    }

    @Test void aFreshStartWaitsForConfigReadyEvenAfterTheExternalAndCacheStepsFinished() throws Exception {
        fixture(f -> {
            var retirement=f.configuration.captureExternalRetirements();var notes=f.configuration.requiredNotes();var shared=f.shared(notes);
            var original=f.clear();var cache=f.cache.capturePreparations(f.prepared.scope(),() -> {}).explicitClear();
            var held=retirement.reserve(original,List.of(cache.inspect(f.prepared)),retirement.resourceNotes(f.prepared.scope()),
                    CaptureResourceNotes.nameHash("original-slot"),notes.origin(shared),true,List.of(new CaptureRetirementMember(A.nodeId(),A.bootId(),"member-a"))).getFirst();
            var rebuilt=f.cache.pipelineRebuilds(f.prepared.scope(),() -> {}).prepare(original,f.node,TTL,Instant.now());
            f.configuration.rebuilds().complete(rebuilt);
            retirement.frozen(held,cache.freeze(held,f.node));var attempt=retirement.beginExternal(held,f.node).orElseThrow();attempt.consume();attempt.ended();
            retirement.verified(held,attempt);var receipt=cache.apply(held,f.node);
            var acquired=f.claims.acquirePipeline(original.request().targetScope(),A,1,TTL).claim();
            var running=f.claims.advanceExecution(acquired,1,Set.of(A.nodeId())).orElseThrow();
            var start=new CapturePreparationRequest(original.request().targetScope(),WorkloadClaimFence.from(running),f.node,"input",f.request.inputs());
            var before=f.control();
            assertThatThrownBy(() -> f.configuration.capturePreparations().issueFirstCreate(start))
                    .isInstanceOfSatisfying(io.tapstate.core.common.TapstateException.class,
                            pending -> assertThat(pending.code()).isEqualTo(IoError.CAPTURE_EXTERNAL_RETIREMENT_PENDING));
            assertThat(f.control()).isEqualTo(before);
            retirement.complete(held,List.of(receipt));
            assertThat(f.configuration.capturePreparations().issueFirstCreate(start)).isEqualTo(new CaptureCreationResult.AlreadyIssued(held.replacement()));
        });
    }

    @Test void exactCarriedPrivateNotesRetireAfterSharedRetirementWithoutAnExpiredMappingHandle() throws Exception {
        fixture(f -> {
            var retirement=f.configuration.captureExternalRetirements();var notes=f.configuration.requiredNotes();
            var shared=f.shared(notes);
            var owner=RequiredNoteOwner.forNode(f.prepared.scope().bindingVersion(),new PipelineNode("flow","input"),PipelineNode.Purpose.CAPTURE,1);
            var privateNotes=notes.initialize(owner);notes.prepareOrigin(privateNotes,notes.origin(shared));
            notes.compareAndSet(privateNotes,"slot",0,new byte[]{7,9},false);
            var carry=notes.prepareCarry(new RequiredNoteCarry(shared,privateNotes));
            var original=f.clear();var inventory=retirement.resourceNotes(f.prepared.scope());
            var observed=inventory.stream().filter(v -> v.scope().equals(privateNotes)).findFirst().orElseThrow();
            retirement.relinquishAliases(original,List.of(new CaptureAliasObservation(observed,"original-slot")));
            var cache=f.cache.capturePreparations(f.prepared.scope(),() -> {}).explicitClear();
            var held=retirement.reserve(original,List.of(cache.inspect(f.prepared)),retirement.resourceNotes(f.prepared.scope()),
                    CaptureResourceNotes.nameHash("original-slot"),notes.origin(shared),true,List.of(new CaptureRetirementMember(A.nodeId(),A.bootId(),"member-a"))).getFirst();
            var rebuilt=f.cache.pipelineRebuilds(f.prepared.scope(),() -> {}).prepare(original,f.node,TTL,Instant.now());
            f.configuration.rebuilds().complete(rebuilt);
            assertThat(notes.readCarried(carry,"slot").bytes()).containsExactly(7,9);
            assertThatThrownBy(() -> notes.retirement(privateNotes,original.request().operationId()).completeAfterVerifiedRelease())
                    .isInstanceOf(io.tapstate.core.common.TapstateException.class);
            retirement.frozen(held,cache.freeze(held,f.node));var attempt=retirement.beginExternal(held,f.node).orElseThrow();attempt.consume();attempt.ended();
            retirement.verified(held,attempt);retirement.complete(held,List.of(cache.apply(held,f.node)));
            assertThat(retirement.resourceNotes(f.prepared.scope()).stream().filter(n -> n.scope().owner().kind()==RequiredNoteOwner.Kind.PIPELINE))
                    .singleElement().satisfies(n -> assertThat(n.resumable()).isFalse());
            var current=notes.open(RequiredNoteOwner.forNode(owner.bindingVersion(),new PipelineNode("flow","input"),PipelineNode.Purpose.CAPTURE,2));
            assertThat(current.generation()).isEqualTo(privateNotes.generation()+1);
            assertThat(notes.read(current,"slot").bytes()).isNull();
            assertThat(notes.origin(current)).isEqualTo(notes.origin(notes.open(shared.owner())));
            assertThat(notes.carries(notes.open(shared.owner()))).isEmpty();
            assertThat(retirement.resourceNotes(f.prepared.scope()).stream().filter(n -> n.scope().owner().kind()==RequiredNoteOwner.Kind.PIPELINE))
                    .singleElement().satisfies(n -> assertThat(n.resumable()).isFalse());
            notes.prepareOrigin(current,notes.origin(current));
            assertThat(retirement.resourceNotes(f.prepared.scope()).stream().filter(n -> n.scope().owner().kind()==RequiredNoteOwner.Kind.PIPELINE))
                    .singleElement().satisfies(n -> assertThat(n.resumable()).isTrue());
        });
    }

    @Test void anAlreadyAdmittedAttachmentThatCommitsFirstCancelsWithoutRelease() throws Exception {
        fixture(f -> {
            var retirement=f.configuration.captureExternalRetirements();var notes=f.configuration.requiredNotes();var shared=f.shared(notes);
            var original=f.clear();var cache=f.cache.capturePreparations(f.prepared.scope(),() -> {}).explicitClear();
            var held=retirement.reserve(original,List.of(cache.inspect(f.prepared)),retirement.resourceNotes(f.prepared.scope()),
                    CaptureResourceNotes.nameHash("original-slot"),notes.origin(shared),true,List.of(new CaptureRetirementMember(A.nodeId(),A.bootId(),"member-a"))).getFirst();
            var consumers=f.cache.srs(f.prepared.scope(),() -> {}).consumers();
            var context=new SrsConsumerContext(new SrsConsumerKey(f.prepared.chainId(),"flow","input"),1);
            assertThat(consumers.attach(context,0,Set.of("orders"),WorkloadClaimFence.from(f.pipeline))).isPresent();
            assertThatThrownBy(() -> cache.freeze(held,f.node)).isInstanceOf(io.tapstate.core.common.TapstateException.class);
            retirement.cancelling(held);retirement.cancelled(held,cache.cancel(held,f.node));
            assertThat(f.control().get("operation",Document.class).get("attempt")).isNull();
            assertThat(consumers.find(context.key()).orElseThrow().detached()).isFalse();
            assertThat(notes.origin(shared)).isNotNull();
            assertThat(MongoCapturePreparationFacts.available(SystemCollections.CAPTURE_PREPARATIONS.on(f.database)
                    .find(new Document("_id",MongoCapturePreparationFacts.id(f.prepared.scope(),f.prepared.captureId()))).first())).isEqualTo(f.prepared);
        });
    }

    @Test void aFrozenRootRefusesTheLateAttachmentWithoutChangingItsClosure() throws Exception {
        fixture(f -> {
            var retirement=f.configuration.captureExternalRetirements();var notes=f.configuration.requiredNotes();var shared=f.shared(notes);
            var original=f.clear();var cache=f.cache.capturePreparations(f.prepared.scope(),() -> {}).explicitClear();
            var held=retirement.reserve(original,List.of(cache.inspect(f.prepared)),retirement.resourceNotes(f.prepared.scope()),
                    CaptureResourceNotes.nameHash("original-slot"),notes.origin(shared),true,List.of(new CaptureRetirementMember(A.nodeId(),A.bootId(),"member-a"))).getFirst();
            retirement.frozen(held,cache.freeze(held,f.node));
            var context=new SrsConsumerContext(new SrsConsumerKey(f.prepared.chainId(),"flow","input"),1);
            assertThatThrownBy(() -> f.cache.srs(f.prepared.scope(),() -> {}).consumers().attach(context,0,Set.of("orders"),WorkloadClaimFence.from(f.pipeline)))
                    .isInstanceOf(io.tapstate.core.common.TapstateException.class);
            assertThat(f.control().get("operation",Document.class).get("attempt")).isNull();
            assertThat(cache.frozen(held)).isPresent();
        });
    }

    @Test void twoRootsReleaseTheirExactSharedCarriesBeforeOneDeferredPrivateCompletion() throws Exception {
        fixture(true,f -> {
            var retirement=f.configuration.captureExternalRetirements();var notes=f.configuration.requiredNotes();var first=f.shared(notes);
            var request=new CapturePreparationRequest(f.request.pipeline(),f.request.originalPipeline(),f.node,"other",f.request.inputs());
            var issued=((CaptureCreationResult.Issued)f.configuration.capturePreparations().issueFirstCreate(request)).creation();
            var second=f.cache.capturePreparations(f.prepared.scope(),() -> {}).createFirst(issued).preparation();
            var otherNotes=notes.open(new RequiredNoteOwner(second.scope().bindingVersion(),"pdk.chain."+second.canonicalChainId(),
                    RequiredNoteOwner.Kind.CHAIN,second.canonicalChainId(),0));notes.prepareOrigin(otherNotes,notes.origin(first));
            var owner=RequiredNoteOwner.forNode(f.prepared.scope().bindingVersion(),new PipelineNode("flow","input"),PipelineNode.Purpose.CAPTURE,1);
            var source=notes.initialize(owner);notes.prepareOrigin(source,notes.origin(first));notes.compareAndSet(source,"slot",0,new byte[]{4,5},false);
            notes.prepareCarry(new RequiredNoteCarry(first,source));notes.prepareCarry(new RequiredNoteCarry(otherNotes,source));
            var original=f.clear();var inventory=retirement.resourceNotes(f.prepared.scope());
            retirement.relinquishAliases(original,List.of(new CaptureAliasObservation(inventory.stream().filter(n -> n.scope().equals(source)).findFirst().orElseThrow(),"same-slot")));
            var cache=f.cache.capturePreparations(f.prepared.scope(),() -> {}).explicitClear();
            var held=retirement.reserve(original,List.of(cache.inspect(f.prepared),cache.inspect(second)),retirement.resourceNotes(f.prepared.scope()),
                    CaptureResourceNotes.nameHash("same-slot"),notes.origin(first),true,List.of(new CaptureRetirementMember(A.nodeId(),A.bootId(),"member-a")));
            assertThat(held).hasSize(2);
            retirement.frozen(held.getFirst(),cache.freeze(held.getFirst(),f.node));
            assertThatThrownBy(() -> retirement.beginExternal(held.getFirst(),f.node)).isInstanceOf(io.tapstate.core.common.TapstateException.class);
            retirement.frozen(held.getLast(),cache.freeze(held.getLast(),f.node));
            var attempt=retirement.beginExternal(held.getFirst(),f.node).orElseThrow();attempt.consume();attempt.ended();retirement.verified(held.getFirst(),attempt);
            assertThat(retirement.beginExternal(held.getLast(),f.node)).isEmpty();
            var receipts=held.stream().map(p -> cache.apply(p,f.node)).toList();retirement.complete(held.getFirst(),receipts);
            assertThat(notes.carries(notes.open(first.owner()))).isEmpty();assertThat(notes.carries(notes.open(otherNotes.owner()))).isEmpty();
            var metadata=SystemCollections.PDK_NOTE_SCOPES.on(f.database).find(new Document("_id.namespace",source.owner().namespace())).first();
            assertThat(metadata.getLong("generation")).isEqualTo(source.generation()+1);
            assertThat(metadata.getLong("incomingCarries")).isZero();
            assertThat(metadata.containsKey("externalRetirementHold")).isFalse();
            retirement.complete(held.getLast(),receipts);
            assertThat(SystemCollections.PDK_NOTE_SCOPES.on(f.database).find(new Document("_id.namespace",source.owner().namespace())).first().getLong("generation"))
                    .isEqualTo(source.generation()+1);
        });
    }

    @Test void offlineLegacyAdoptionRetainsUntrackedCoverageAndSeparatesTheTwoVersionCommits() throws Exception {
        fixture(f -> {
            var notes=f.configuration.requiredNotes();var shared=f.shared(notes);
            SystemCollections.CAPTURE_EXTERNAL_RETIREMENTS.on(f.database).drop();
            SystemCollections.SCHEMA_MIGRATIONS.on(f.database).updateOne(new Document("_id","config"),new Document("$set",new Document("installedVersion",2)));
            SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(f.cacheDatabase).updateOne(new Document("_id","business"),new Document("$set",new Document("installedVersion",1)));
            var cache=f.cache.capturePreparations(f.prepared.scope(),() -> {}).explicitClear();
            var original=cache.observeLegacyUpgrade(f.configuration.legacyCapturePreparations(f.prepared.scope()));
            f.configuration.adoptLegacyCaptureStorage(original);
            assertThat(SystemCollections.SCHEMA_MIGRATIONS.on(f.database).find(new Document("_id","config")).first().getInteger("installedVersion")).isEqualTo(3);
            assertThat(SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(f.cacheDatabase).find(new Document("_id","business")).first().getInteger("installedVersion")).isEqualTo(1);
            assertThatThrownBy(() -> f.cache.workloadClaims(f.prepared.scope())).isInstanceOf(io.tapstate.core.common.TapstateException.class);
            // Re-observe and resume after the CONFIG commit, without an invented old-owner close.
            var resumed=cache.observeLegacyUpgrade(f.configuration.legacyCapturePreparations(f.prepared.scope()));
            var receipt=f.configuration.adoptLegacyCaptureStorage(resumed);resumed.complete(receipt);resumed.complete(receipt);
            var retirement=f.configuration.captureExternalRetirements();var opening=retirement.prepare(f.opening(A)).register();opening.accessing();opening.closed();
            assertThat(f.entries()).singleElement().satisfies(row -> {
                assertThat(row.keySet()).containsExactlyInAnyOrder("sequence","openingId","resourceEpoch","kind","state",
                        "oldBusinessScope","previousCaptureFence","coverageDigest");
                assertThat(row.getString("kind")).isEqualTo("LEGACY_UNTRACKED");
                assertThat(row.getString("state")).isEqualTo("UNTRACKED");
                assertThat(row.get("oldBusinessScope",Document.class)).isEqualTo(new Document("clusterId",f.prepared.scope().clusterId())
                        .append("bindingVersion",f.prepared.scope().bindingVersion()).append("cacheIncarnation",f.prepared.scope().cacheIncarnation()));
                assertThat(row.get("previousCaptureFence",Document.class)).isEqualTo(new Document("ownerNodeId",A.nodeId())
                        .append("ownerBootId",A.bootId()).append("claimGeneration",f.capture.claimGeneration())
                        .append("executionGeneration",f.capture.executionGeneration()).append("topologyRevision",f.capture.topologyRevision()));
                assertThat(row.getString("coverageDigest")).matches("[0-9a-f]{64}");
                assertThat(MongoCaptureExternalRetirements.bytes(row)).isLessThanOrEqualTo(MongoCaptureExternalRetirements.MAX_OPENING_BYTES);
            });
            assertThat(f.control().get("openingJournal",Document.class).getLong("closedPrefixThrough")).isZero();
            var clear=f.clear();
            var held=retirement.reserve(clear,List.of(cache.inspect(f.prepared)),retirement.resourceNotes(f.prepared.scope()),
                    CaptureResourceNotes.nameHash("original-slot"),notes.origin(shared),true,List.of(new CaptureRetirementMember(A.nodeId(),A.bootId(),"member-a"))).getFirst();
            retirement.frozen(held,cache.freeze(held,f.node));
            assertThatThrownBy(() -> retirement.beginExternal(held,f.node)).isInstanceOf(io.tapstate.core.common.TapstateException.class);
            assertThat(notes.origin(shared)).isNotNull();assertThat(f.control().get("operation",Document.class).get("attempt")).isNull();
        });
    }

    @Test void aLaterOriginalDeleteGrantCanFinishTheSamePinnedGenerationWithoutReleasingTheNextOne() throws Exception {
        fixture(f -> {
            var retirement=f.configuration.captureExternalRetirements();var notes=f.configuration.requiredNotes();var shared=f.shared(notes);
            var owner=RequiredNoteOwner.forNode(f.prepared.scope().bindingVersion(),new PipelineNode("flow","input"),PipelineNode.Purpose.CAPTURE,1);
            var source=notes.initialize(owner);notes.prepareOrigin(source,notes.origin(shared));notes.compareAndSet(source,"slot",0,new byte[]{8},false);
            notes.prepareCarry(new RequiredNoteCarry(shared,source));
            var clear=f.clear();var before=retirement.resourceNotes(f.prepared.scope());
            retirement.relinquishAliases(clear,List.of(new CaptureAliasObservation(before.stream().filter(n -> n.scope().equals(source)).findFirst().orElseThrow(),"same-slot")));
            var cache=f.cache.capturePreparations(f.prepared.scope(),() -> {}).explicitClear();
            var held=retirement.reserve(clear,List.of(cache.inspect(f.prepared)),retirement.resourceNotes(f.prepared.scope()),CaptureResourceNotes.nameHash("same-slot"),
                    notes.origin(shared),true,List.of(new CaptureRetirementMember(A.nodeId(),A.bootId(),"member-a"))).getFirst();
            var rebuilt=f.cache.pipelineRebuilds(f.prepared.scope(),() -> {}).prepare(clear,f.node,TTL,Instant.now());f.configuration.rebuilds().complete(rebuilt);
            var originalCache=new PipelineRetirementRequest.CacheOriginal(rebuilt.claim(),rebuilt.checkpointEpoch());
            var deletion=f.configuration.retirements().begin(new PipelineRetirementRequest("delete-after-clear",clear.request().targetScope(),
                    f.request.inputs().pipelineHash(),Optional.of(originalCache)));
            var observed=retirement.resourceNotes(f.prepared.scope()).stream().filter(n -> n.scope().owner().kind()==RequiredNoteOwner.Kind.PIPELINE).findFirst().orElseThrow();
            retirement.relinquishAliases(deletion,List.of(new CaptureAliasObservation(observed,"same-slot")));
            var deleted=f.cache.pipelineRetirements(f.prepared.scope(),() -> {}).retire(deletion,f.node,Optional.of(originalCache));
            retirement.frozen(held,cache.freeze(held,f.node));var attempt=retirement.beginExternal(held,f.node).orElseThrow();attempt.consume();attempt.ended();
            retirement.verified(held,attempt);retirement.complete(held,List.of(cache.apply(held,f.node)));
            var metadata=SystemCollections.PDK_NOTE_SCOPES.on(f.database).find(new Document("_id.namespace",owner.namespace())).first();
            assertThat(metadata.get("externalRetirementResult",Document.class).getString("kind")).isEqualTo("DELETE");
            assertThat(metadata.getLong("originGeneration")).isZero();
            for(int batch=0;batch<4 && f.control().get("gcPrefix")!=null;batch++)if(cache.reclaim(held,f.node))retirement.cleaned(held);
            f.configuration.retirements().complete(deleted);
            assertThatThrownBy(() -> notes.retirement(observed.scope(),deletion.request().operationId())).isInstanceOf(io.tapstate.core.common.TapstateException.class);
            assertThat(SystemCollections.PDK_NOTE_SCOPES.on(f.database).find(new Document("_id.namespace",owner.namespace())).first().getLong("generation"))
                    .isEqualTo(source.generation()+1);
        });
    }

    @Test void anotherLegacyControlRemainsCoveredWhenAPrivateAliasIsRelinquished() throws Exception {
        fixture(true,f -> {
            var notes=f.configuration.requiredNotes();var first=f.shared(notes);
            var request=new CapturePreparationRequest(f.request.pipeline(),f.request.originalPipeline(),f.node,"other",f.request.inputs());
            var issued=((CaptureCreationResult.Issued)f.configuration.capturePreparations().issueFirstCreate(request)).creation();
            var second=f.cache.capturePreparations(f.prepared.scope(),() -> {}).createFirst(issued).preparation();
            var secondNotes=notes.open(new RequiredNoteOwner(second.scope().bindingVersion(),"pdk.chain."+second.canonicalChainId(),
                    RequiredNoteOwner.Kind.CHAIN,second.canonicalChainId(),0));notes.prepareOrigin(secondNotes,notes.origin(first));
            SystemCollections.CAPTURE_EXTERNAL_RETIREMENTS.on(f.database).drop();
            SystemCollections.SCHEMA_MIGRATIONS.on(f.database).updateOne(new Document("_id","config"),new Document("$set",new Document("installedVersion",2)));
            SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(f.cacheDatabase).updateOne(new Document("_id","business"),new Document("$set",new Document("installedVersion",1)));
            var cache=f.cache.capturePreparations(f.prepared.scope(),() -> {}).explicitClear();
            var observed=cache.observeLegacyUpgrade(f.configuration.legacyCapturePreparations(f.prepared.scope()));
            observed.complete(f.configuration.adoptLegacyCaptureStorage(observed));
            var retirement=f.configuration.captureExternalRetirements();
            var owner=RequiredNoteOwner.forNode(f.prepared.scope().bindingVersion(),new PipelineNode("flow","input"),PipelineNode.Purpose.CAPTURE,1);
            var privateNotes=notes.initialize(owner);notes.prepareOrigin(privateNotes,notes.origin(first));
            notes.compareAndSet(privateNotes,"slot",0,new byte[]{7,9},false);notes.prepareCarry(new RequiredNoteCarry(first,privateNotes));
            var clear=f.clear();var inventory=retirement.resourceNotes(f.prepared.scope());
            retirement.relinquishAliases(clear,List.of(new CaptureAliasObservation(inventory.stream()
                    .filter(value -> value.scope().equals(privateNotes)).findFirst().orElseThrow(),"original-slot")));
            var held=retirement.reserve(clear,List.of(cache.inspect(f.prepared)),retirement.resourceNotes(f.prepared.scope()),
                    CaptureResourceNotes.nameHash("original-slot"),notes.origin(first),true,
                    List.of(new CaptureRetirementMember(A.nodeId(),A.bootId(),"member-a"))).getFirst();
            assertThat(f.control().get("operation",Document.class).getList("openingCoverage",Document.class)).singleElement()
                    .satisfies(value -> assertThat(value.get("control")).isEqualTo(MongoCaptureExternalRetirements.id(second)));
            retirement.frozen(held,cache.freeze(held,f.node));
            assertThatThrownBy(() -> retirement.beginExternal(held,f.node)).isInstanceOf(io.tapstate.core.common.TapstateException.class);
            assertThat(f.control().get("operation",Document.class).get("attempt")).isNull();
            var other=SystemCollections.CAPTURE_EXTERNAL_RETIREMENTS.on(f.database).find(new Document("_id",MongoCaptureExternalRetirements.id(second))).first();
            assertThat(other.get("openingJournal",Document.class).getList("unresolved",Document.class)).singleElement()
                    .satisfies(value -> assertThat(value.getString("state")).isEqualTo("UNTRACKED"));
            assertThat(notes.origin(first)).isNotNull();
        });
    }

    @Test void missingLegacyRootIsNotAdoptedOrRecreated() throws Exception {
        fixture(f -> {
            f.shared(f.configuration.requiredNotes());
            SystemCollections.CAPTURE_EXTERNAL_RETIREMENTS.on(f.database).drop();
            SystemCollections.SCHEMA_MIGRATIONS.on(f.database).updateOne(new Document("_id","config"),new Document("$set",new Document("installedVersion",2)));
            SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(f.cacheDatabase).updateOne(new Document("_id","business"),new Document("$set",new Document("installedVersion",1)));
            SystemCollections.SRS_CHAINS.on(f.cacheDatabase).deleteOne(new Document("_id",f.prepared.chainId()));
            assertThatThrownBy(() -> f.cache.capturePreparations(f.prepared.scope(),() -> {}).explicitClear()
                    .observeLegacyUpgrade(f.configuration.legacyCapturePreparations(f.prepared.scope())))
                    .isInstanceOf(io.tapstate.core.common.TapstateException.class);
            assertThat(SystemCollections.SRS_CHAINS.on(f.cacheDatabase).find(new Document("_id",f.prepared.chainId())).first()).isNull();
            assertThat(SystemCollections.SCHEMA_MIGRATIONS.on(f.database).find(new Document("_id","config")).first().getInteger("installedVersion")).isEqualTo(2);
        });
    }

    @Test void aMissingCurrentGateCannotMasqueradeAsOldUntrackedHistory() throws Exception {
        fixture(f -> {
            f.shared(f.configuration.requiredNotes());SystemCollections.CAPTURE_EXTERNAL_RETIREMENTS.on(f.database).drop();
            SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(f.cacheDatabase).updateOne(new Document("_id","business"),new Document("$set",new Document("installedVersion",1)));
            var original=f.cache.capturePreparations(f.prepared.scope(),() -> {}).explicitClear().observeLegacyUpgrade(f.configuration.legacyCapturePreparations(f.prepared.scope()));
            assertThatThrownBy(() -> f.configuration.adoptLegacyCaptureStorage(original)).isInstanceOf(io.tapstate.core.common.TapstateException.class);
            assertThat(SystemCollections.CAPTURE_EXTERNAL_RETIREMENTS.on(f.database).countDocuments()).isZero();
            assertThat(SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(f.cacheDatabase).find(new Document("_id","business")).first().getInteger("installedVersion")).isEqualTo(1);
        });
    }

    private static void fixture(Scenario scenario) throws Exception {
        fixture(false,scenario);
    }
    private static void fixture(boolean cohort,Scenario scenario) throws Exception {
        String id=UUID.randomUUID().toString().replace("-","");
        var configName="opening_cfg_"+id;var cacheName="opening_cache_"+id;
        var settings=new MongoConnectionSettings(server.getReplicaSetUrl(configName),null,Duration.ofSeconds(5));
        var connection=new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB,server.getReplicaSetUrl(cacheName),cacheName,null);
        try(var admin=MongoClients.create(server.getReplicaSetUrl());
                var configuration=MongoConfigurationBootstrap.prepare(settings,"cluster","install","setup",1);
                var cache=MongoCacheDomain.connect(connection,Duration.ofSeconds(5),"opening-test")) {
            var master=new SourceResource("views",null,"mongodb",Map.of("uri","mongodb://unused.invalid/master"),null,null,null,null);
            var binding=configuration.bindings().commitWithFirstCache(new StorageSetupScope("cluster","install","setup",1),StorageBinding.Mode.OP,connection,master);
            cache.prepareFirstAuth(binding.firstCreation().orElseThrow());var scope=cache.prepareInitialBusiness(binding.preparation(),() -> {});
            var claims=cache.workloadClaims(scope);var node=WorkloadClaimFence.from(claims.acquire(new WorkloadClaimKey("cluster",WorkloadClaimType.NODE_SESSION,A.nodeId()),A,0,TTL).claim());
            var source=new SourceResource("input",null,"mysql",Map.of("host","unused.invalid"),SourceMode.CDC,List.of(TableRef.literal("orders")),
                    cohort?new Srs("primary",null,null,null,null):null,null);
            var pipeline=(PipelineResource)new DslParser().parse("version: tapstate/v1\nkind: pipeline\nid: flow\nsource: ["+
                    (cohort?"{id: input, srs: true}, {id: other, srs: true}":"{id: input, srs: true}")+"]\nsettings: {read_mode: snapshot_and_cdc}\n");
            configuration.artifacts().create(source);configuration.artifacts().create(pipeline);
            var definitions=new LinkedHashMap<String,SourceResource>();definitions.put("input",source);
            var selected=new LinkedHashMap<String,Set<String>>();selected.put("input",Set.of("orders"));
            if(cohort) {
                var other=new SourceResource("other",null,"mysql",source.config(),SourceMode.CDC,source.tables(),new Srs("secondary",null,null,null,null),null);
                configuration.artifacts().create(other);definitions.put("other",other);selected.put("other",Set.of("orders"));
            }
            var intent=new PipelineIntentScope("cluster",scope.bindingVersion(),scope.cacheIncarnation(),"flow",1);
            configuration.intents().compareAndSet(intent,0,new DesiredState("flow",PipelineState.RUNNING,CanonicalHash.of(pipeline),false));
            var inputs=PipelinePreparationInputs.of(intent,pipeline,definitions,selected);
            var first=((FirstPipelineCreationResult.Issued)configuration.preparations().issueFirstCreate(intent,node,inputs)).creation();
            var preparer=cache.pipelinePreparations(scope,() -> {});var reserved=preparer.reserveFirst(first,TTL);
            configuration.preparations().complete(preparer.seed(reserved,configuration.preparations(),Instant.now()));
            var owner=claims.acquirePipeline(intent,A,1,TTL).claim();var running=claims.advanceExecution(owner,1,Set.of(A.nodeId())).orElseThrow();
            var request=new CapturePreparationRequest(intent,WorkloadClaimFence.from(running),node,"input",inputs);
            var issued=((CaptureCreationResult.Issued)configuration.capturePreparations().issueFirstCreate(request)).creation();
            var prepared=cache.capturePreparations(scope,() -> {}).createFirst(issued).preparation();
            var capture=claims.acquire(new WorkloadClaimKey("cluster",WorkloadClaimType.CAPTURE,prepared.captureId()),A,1,TTL).claim();
            scenario.run(new Fixture(configuration,admin.getDatabase(configName),admin.getDatabase(cacheName),cache,claims,request,prepared,running,capture,node));
            admin.getDatabase(configName).drop();admin.getDatabase(cacheName).drop();
        }
    }
    private record Fixture(MongoConfigurationDomain configuration,com.mongodb.client.MongoDatabase database,com.mongodb.client.MongoDatabase cacheDatabase,MongoCacheDomain cache,WorkloadClaimStore claims,
            CapturePreparationRequest request,CapturePreparation prepared,WorkloadClaim pipeline,WorkloadClaim capture,WorkloadClaimFence node) {
        CaptureOpeningJournal.Request opening(WorkloadOwner ignored){return new CaptureOpeningJournal.Request(prepared,request.pipeline(),WorkloadClaimFence.from(pipeline),WorkloadClaimFence.from(capture),node);}
        Document control(){return SystemCollections.CAPTURE_EXTERNAL_RETIREMENTS.on(database).find(new Document("_id",MongoCaptureExternalRetirements.id(prepared))).first();}
        List<Document> entries(){return control().get("openingJournal",Document.class).getList("unresolved",Document.class);}
        RequiredNoteScope shared(RequiredNoteStore notes) {
            var scope=notes.open(new RequiredNoteOwner(prepared.scope().bindingVersion(),"pdk.chain."+prepared.canonicalChainId(),
                    RequiredNoteOwner.Kind.CHAIN,prepared.canonicalChainId(),0));
            notes.prepareOrigin(scope,new RequiredNoteOrigin("postgres","a".repeat(64),"{}".getBytes(java.nio.charset.StandardCharsets.UTF_8)));
            return scope;
        }
        PipelineRebuildPreparation clear() {
            var current=configuration.intents().load("flow").orElseThrow();
            assertThat(configuration.intents().compareAndSet(request.pipeline(),current.intentRevision(),
                    new DesiredState("flow",PipelineState.STOPPED,request.inputs().pipelineHash(),true))).isTrue();
            var next=configuration.intents().load("flow").orElseThrow();
            return configuration.rebuilds().begin(new PipelineRebuildRequest("explicit-clear",next,WorkloadClaimFence.from(pipeline),0,request.inputs()));
        }
    }
    @FunctionalInterface private interface Scenario {void run(Fixture fixture)throws Exception;}
}
