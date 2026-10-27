package io.tapstate.control.core;

import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

/** Real service composition under the shared decoder; native transaction behavior has separate integration coverage. */
class StorageControlDispatcherTest {
    private static final StorageSetupScope SCOPE = StorageSetupServiceTest.SCOPE;
    static final class Recoveries implements StorageRecoveryCommands {
        Plan plan; int plans; int confirms;
        @Override public StorageRecoveryCommands guardedBy(Runnable active) { active.run(); return this; }
        @Override public Plan plan(CacheAuthScope scope, Manifest manifest, String id, String previous) {
            plans++; plan = new Plan(id == null ? UUID.randomUUID().toString() : id, previous, scope, manifest, "c".repeat(64), manifest.configurationRevision()+1, Phase.PLANNED); return plan;
        }
        @Override public Optional<Plan> current(CacheAuthScope scope) { return Optional.ofNullable(plan); }
        @Override public Optional<Plan> find(CacheAuthScope scope, String id) { return plan != null && plan.originalScope().equals(scope) && plan.operationId().equals(id) ? Optional.of(plan) : Optional.empty(); }
        @Override public Plan confirm(Plan original) { confirms++; plan = new Plan(original.operationId(), original.previousOperationId(), original.originalScope(), original.manifest(), original.planHash(), original.controlRevision(), Phase.CONFIRMED); return plan; }
        @Override public Plan start(Plan original) { throw new AssertionError("no transport starts recovery"); }
        @Override public Plan complete(Plan original) { throw new AssertionError("no transport completes recovery"); }
    }
    static final class Fixture {
        final StorageSetupServiceTest.Fixture setup = new StorageSetupServiceTest.Fixture();
        final ConfigurationImportServiceTest.Fixture imports = new ConfigurationImportServiceTest.Fixture();
        final Recoveries recoveries = new Recoveries();
        StorageControlDispatcher.Snapshot snapshot = new StorageControlDispatcher.Snapshot(StorageBinding.Mode.OP, SCOPE.clusterId(), SCOPE.operationId(), 1, 1, 1, 7,
                "COMPLETE", StorageControlDispatcher.BusinessPhase.READY, Map.of(StorageSetupService.Layer.CONFIG, StorageControlDispatcher.Health.AVAILABLE,
                StorageSetupService.Layer.CACHE, StorageControlDispatcher.Health.AVAILABLE, StorageSetupService.Layer.MASTER, StorageControlDispatcher.Health.AVAILABLE), List.of("storage.recovery.plan"), false);
        final StorageRecoveryService recovery = new StorageRecoveryService(recoveries, new StorageRecoveryService.Inspector() {
            @Override public StorageRecoveryCommands.Manifest inspect(CacheAuthScope scope, StorageRecoveryCommands.Selection selection) {
                if(selection.extent()==StorageRecoveryCommands.Extent.SHARED_SRS) {
                    var business=new CacheBusinessScope(scope.clusterId(),scope.bindingVersion(),scope.cacheIncarnation());var owner=new WorkloadOwner("node","boot");
                    var previous=new CapturePreparation(business,CapturePreparation.Mode.SHARED_SRS,selection.physicalChain(),"capture","mc-"+"a".repeat(64),UUID.randomUUID().toString(),selection.canonicalChain(),UUID.randomUUID().toString());
                    var pipeline=new PipelineIntentScope(scope.clusterId(),scope.bindingVersion(),scope.cacheIncarnation(),"pipeline",1);
                    var claim=new WorkloadClaimFence(new WorkloadClaimKey(scope.clusterId(),WorkloadClaimType.PIPELINE_ACTUATION,"pipeline"),owner,1,1,1);
                    var snapshot=new SharedSrsRecoverySnapshot(previous,new WorkloadClaimFence(new WorkloadClaimKey(scope.clusterId(),WorkloadClaimType.CAPTURE,"capture"),owner,1,0,1),"a".repeat(64),
                            List.of(new SrsConsumerMembership(new SrsConsumerContext(new SrsConsumerKey(selection.physicalChain(),"pipeline","source"),1),1,false,Set.of("orders"),claim)),
                            Map.of("pipeline",new SharedSrsRecoverySnapshot.Pipeline(pipeline,claim)));
                    return new StorageRecoveryCommands.Manifest(selection,Set.of("pipeline"),Map.of("pipeline","a".repeat(64)),"b".repeat(64),5,snapshot);
                }
                return new StorageRecoveryCommands.Manifest(selection, Set.of("pipeline"), Map.of("pipeline", "a".repeat(64)), "b".repeat(64), 5);
            }
            @Override public void requireCurrent(CacheAuthScope scope, StorageRecoveryCommands.Manifest original) {}
        }, imports.audit);
        StorageControlDispatcher dispatcher() { return new StorageControlDispatcher(ControlOperations.registry(), setup.service(), recovery, imports.service, (caller, active) -> { active.run(); return snapshot; }); }
        StorageControlAccess admin() {
            return StorageControlAccess.ordinary(StorageBinding.Mode.OP, SCOPE, new CacheAuthScope(SCOPE.clusterId(),1,StorageBinding.Mode.OP,"incarnation",1),
                    TapstatePrincipal.humanJwt(new VerifiedToken("admin",Scope.ADMIN)),null,() -> {});
        }
        StorageControlAccess owner() {
            var authority = new SetupCapabilityService(setup.states, setup.time, () -> {}, () -> {}, () -> {});
            var issued = new SetupCapabilityService.Issued(SCOPE, StorageSetupStateStore.Purpose.SETUP, setup.time.instant().plusSeconds(60), "s".repeat(43));
            authority.restore(issued); return StorageControlAccess.management(authority.authorize(issued.token(), SCOPE, issued.purpose()));
        }
    }
    private static Map<String,Object> candidate() {
        var out = new LinkedHashMap<String,Object>(); var value = StorageSetupServiceTest.CANDIDATE;
        out.put("configuration", connection(value.configuration())); out.put("cache",connection(value.cache())); out.put("master",connection(value.master())); return out;
    }
    private static Map<String,Object> connection(StorageBinding.Connection value) { return Map.of("kind",value.kind().name(),"uri",value.uri(),"database",value.database()); }
    @Test void statusEnclosesTheEntireProjectionInTwoOriginalAuthorizationChecks() {
        var f = new Fixture(); var checks = new AtomicInteger();
        var caller = StorageControlAccess.ordinary(StorageBinding.Mode.OP, SCOPE,
                new CacheAuthScope(SCOPE.clusterId(), 1, StorageBinding.Mode.OP, "incarnation", 1),
                TapstatePrincipal.humanJwt(new VerifiedToken("admin", Scope.ADMIN)), null, checks::incrementAndGet);
        var dispatcher = new StorageControlDispatcher(ControlOperations.registry(), f.setup.service(), f.recovery, f.imports.service,
                (ignored, active) -> { assertThat(checks).hasValue(1); return f.snapshot; });
        assertThat(dispatcher.dispatch("storage.status", caller, Map.of())).containsEntry("businessPhase", "READY");
        assertThat(checks).hasValue(2);
    }

    @Test void initializationProgressIsAnOptionalScopedReadOnlyProjectionWithoutSubmissionProofs() {
        var f = new Fixture();
        var progress = new StorageInitializationProgress(java.time.Clock.systemUTC());
        progress.begin(SCOPE, 1, true);
        progress.checked(SCOPE, StorageSetupService.Layer.MASTER);
        var original = f.snapshot;
        f.snapshot = new StorageControlDispatcher.Snapshot(original.mode(), original.clusterId(), original.operationId(),
                original.setupGeneration(), original.bindingVersion(), original.authGeneration(), original.stateVersion(),
                "STORAGE_BOUND", StorageControlDispatcher.BusinessPhase.PREPARING, original.health(), original.allowedActions(),
                false, original.applicationDataVerification(), progress.snapshot(SCOPE).orElseThrow());
        var result = f.dispatcher().dispatch("storage.status", f.admin(), Map.of());
        var view = (Map<?,?>) result.get("initializationProgress");
        assertThat(view.get("operationId")).isEqualTo(SCOPE.operationId());
        assertThat((List<?>) view.get("milestones")).hasSize(4);
        assertThat((List<?>) view.get("connectionChecks")).hasSize(1);
        assertThat(JsonWriter.write(result)).doesNotContain("testStamp", "sentinel-secret", "mongodb://");
        var schema = (Map<?,?>) StorageOperationSchemas.definitions().get("StorageStatusResult");
        assertThat(((List<?>) schema.get("required")).contains("initializationProgress")).isFalse();
        assertThatThrownBy(() -> new StorageControlDispatcher.Snapshot(original.mode(), original.clusterId(), "different",
                original.setupGeneration(), original.bindingVersion(), original.authGeneration(), original.stateVersion(),
                "STORAGE_BOUND", StorageControlDispatcher.BusinessPhase.PREPARING, original.health(), original.allowedActions(),
                false, original.applicationDataVerification(), progress.snapshot(SCOPE).orElseThrow()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test void aRevocationOrStorageFenceAfterProjectionPreventsReturningItsCapturedValues() {
        for (var code : List.of(ControlError.UNAUTHENTICATED, ControlError.STORAGE_UNAVAILABLE)) {
            var f = new Fixture(); var checks = new AtomicInteger(); var revoked = new java.util.concurrent.atomic.AtomicBoolean();
            var caller = StorageControlAccess.ordinary(StorageBinding.Mode.OP, SCOPE,
                    new CacheAuthScope(SCOPE.clusterId(), 1, StorageBinding.Mode.OP, "incarnation", 1),
                    TapstatePrincipal.humanJwt(new VerifiedToken("admin", Scope.ADMIN)), null, () -> {
                        checks.incrementAndGet();
                        if (revoked.get()) throw new TapstateException(code, Map.of(), null);
                    });
            var dispatcher = new StorageControlDispatcher(ControlOperations.registry(), f.setup.service(), f.recovery, f.imports.service,
                    (ignored, active) -> { var captured = f.snapshot; revoked.set(true); return captured; });
            assertThatThrownBy(() -> dispatcher.dispatch("storage.status", caller, Map.of()))
                    .isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.code()).isEqualTo(code));
            assertThat(checks).hasValue(2);
        }
    }

    @Test void sharedRecoveryCarriesTheExactOriginalPhysicalSelectionWithoutAcceptingBrowserInfluence() {
        var f=new Fixture();var dispatcher=f.dispatcher();
        var input=Map.<String,Object>of("scope","SHARED_SRS","pipelineIds",List.of(),"canonicalChain","canonical","physicalChain","old-physical");
        var result=dispatcher.dispatch("storage.recovery.plan",f.admin(),input);
        assertThat(result).containsEntry("canonicalChain","canonical").containsEntry("physicalChain","old-physical").containsEntry("affectedPipelineIds",List.of("pipeline"));
        assertThat(f.recoveries.plan.manifest().selection()).isEqualTo(new StorageRecoveryCommands.Selection(StorageRecoveryCommands.Extent.SHARED_SRS,Set.of(),"canonical","old-physical"));
        var query=dispatcher.dispatch("storage.recovery.status",f.admin(),Map.of("operationId",result.get("operationId")));
        assertThat(query).containsEntry("physicalChain","old-physical");assertThat(JsonWriter.write(query)).doesNotContain("captureFence","sourceIdentityHash");
    }
    @Test void malformedSharedCoordinatesAndInventedAffectedPipelinesNeverReachPlanning() {
        var f=new Fixture();var dispatcher=f.dispatcher();
        var valid=Map.<String,Object>of("scope","SHARED_SRS","pipelineIds",List.of(),"canonicalChain","canonical","physicalChain","old-physical");
        for(String field:List.of("canonicalChain","physicalChain")) {
            var missing=new LinkedHashMap<>(valid);missing.remove(field);assertMalformed(() -> dispatcher.dispatch("storage.recovery.plan",f.admin(),missing));
        }
        var ids=new LinkedHashMap<>(valid);ids.put("pipelineIds",List.of("attacker"));assertMalformed(() -> dispatcher.dispatch("storage.recovery.plan",f.admin(),ids));
        var invented=new LinkedHashMap<>(valid);invented.put("affectedPipelineIds",List.of());assertMalformed(() -> dispatcher.dispatch("storage.recovery.plan",f.admin(),invented));
        var cluster=new LinkedHashMap<>(valid);cluster.put("scope","CLUSTER");assertMalformed(() -> dispatcher.dispatch("storage.recovery.plan",f.admin(),cluster));
        assertThat(f.recoveries.plans).isZero();
    }
    @Test void statusProjectsTheActualVerificationTimesWithoutConnectionSettings() {
        var f = new Fixture(); var old = f.snapshot;
        var checked = java.time.Instant.parse("2026-10-26T01:00:00Z");
        f.snapshot = new StorageControlDispatcher.Snapshot(old.mode(), old.clusterId(), old.operationId(), old.setupGeneration(),
                old.bindingVersion(), old.authGeneration(), old.stateVersion(), old.setupPhase(), old.businessPhase(), old.health(),
                old.allowedActions(), old.restartRequired(), new DefaultSourceVerification.Verification(
                        DefaultSourceVerification.Status.PASSED, checked, checked.plusSeconds(600)));
        var result = f.dispatcher().dispatch("storage.status", f.admin(), Map.of());
        assertThat(result.get("applicationDataVerification")).isEqualTo(Map.of("status", "PASSED", "checkedAt",
                "2026-10-26T01:00:00Z", "validUntil", "2026-10-26T01:10:00Z"));
        assertThat(JsonWriter.write(result)).doesNotContain("sentinel-secret", "mongodb://");
    }

    @Test void setupTestsCommitAndOriginalQueryUseTheSameServicesWithoutExposingConnections() {
        var f = new Fixture(); var dispatcher = f.dispatcher(); var access = f.owner(); var proofs = new LinkedHashMap<String,Object>();
        for (var layer : StorageSetupService.Layer.values()) {
            var result = dispatcher.dispatch("storage.test",access,Map.of("operationId",SCOPE.operationId(),"generation",1,"candidate",candidate(),"layer",layer.name()));
            assertThat(JsonWriter.write(result)).doesNotContain("sentinel-secret", "mongodb://"); proofs.put(layer.name(),result.get("testStamp"));
        }
        var bound = dispatcher.dispatch("storage.setup.commit",access,Map.of("operationId",SCOPE.operationId(),"generation",1,"candidate",candidate(),"testStamps",proofs));
        assertThat(bound).containsEntry("setupPhase","STORAGE_BOUND");
        assertThat(dispatcher.dispatch("storage.setup.status",access,Map.of("operationId",SCOPE.operationId()))).isEqualTo(bound);
        assertThat(f.setup.binding.inserts).isEqualTo(1); assertThat(JsonWriter.write(bound)).doesNotContain("sentinel-secret", "uri");
    }
    @Test void recoveryRequiresExplicitOriginalConfirmationAndUnknownQueriesNeverCreateOperations() {
        var f = new Fixture(); var dispatcher = f.dispatcher(); var access = f.admin();
        String unknown = UUID.randomUUID().toString();
        assertThat(dispatcher.dispatch("storage.recovery.status",access,Map.of("operationId",unknown))).containsEntry("phase","UNKNOWN");
        assertThat(f.recoveries.plans).isZero();
        var plan = dispatcher.dispatch("storage.recovery.plan",access,Map.of("scope","PIPELINE","pipelineIds",List.of("pipeline")));
        assertThat(plan).containsEntry("phase","PLANNED").containsEntry("controlRevision",6L);
        var confirmation = new LinkedHashMap<String,Object>(Map.of("operationId",plan.get("operationId"),"planHash",plan.get("planHash"),"controlRevision",plan.get("controlRevision"),"confirmed",true));
        confirmation.put("confirmed",false); assertMalformed(() -> dispatcher.dispatch("storage.recovery.confirm",access,confirmation));
        assertThat(f.recoveries.confirms).isZero(); confirmation.put("confirmed",true);
        assertThat(dispatcher.dispatch("storage.recovery.confirm",access,confirmation)).containsEntry("phase","CONFIRMED");
        dispatcher.dispatch("storage.recovery.confirm",access,confirmation); assertThat(f.recoveries.confirms).isEqualTo(1);
    }
    @Test void completionReturnsOnlyItsReceiptAndTheRetiredSetupCredentialCannotQueryAgain() {
        var f = new Fixture();
        f.setup.continuation = committed -> f.setup.states.state = new StorageSetupStateStore.Status(SCOPE, StorageSetupStateStore.Phase.COMPLETE, 1);
        var dispatcher = f.dispatcher(); var access = f.owner(); var proofs = new LinkedHashMap<String,Object>();
        for (var layer : StorageSetupService.Layer.values()) {
            var result = dispatcher.dispatch("storage.test", access, Map.of("operationId", SCOPE.operationId(), "generation", 1,
                    "candidate", candidate(), "layer", layer.name()));
            proofs.put(layer.name(), result.get("testStamp"));
        }
        var input = Map.<String,Object>of("operationId", SCOPE.operationId(), "generation", 1, "candidate", candidate(), "testStamps", proofs);
        assertThat(dispatcher.dispatch("storage.setup.commit", access, input)).containsEntry("setupPhase", "COMPLETE");
        assertThatThrownBy(() -> dispatcher.dispatch("storage.setup.status", access, Map.of("operationId", SCOPE.operationId())))
                .isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> dispatcher.dispatch("storage.setup.commit", access, input)).isInstanceOf(TapstateException.class);
        assertThat(f.setup.binding.inserts).isOne();
    }
    @Test void importsReturnTheOriginalReceiptAndNeitherKeysNorDecryptedDefinitions() {
        var f = new Fixture(); var dispatcher = f.dispatcher(); var access = f.admin();
        f.imports.codec.packages.put("input", new ConfigurationImportStore.Payload(List.of(),List.of(new User("admin2","sentinel-hash","admin")),List.of(),Map.of(),Map.of()));
        var request = new LinkedHashMap<String,Object>(Map.of("transfer","input","transferKey","sentinel-key","defaultSourceMapping",Map.of()));
        var preflight = dispatcher.dispatch("configuration.import.preflight",access,request);
        @SuppressWarnings("unchecked") var operation = (Map<String,Object>) preflight.get("operation");
        @SuppressWarnings("unchecked") var ticket = (Map<String,Object>) operation.get("ticket");
        request.put("importId",ticket.get("id")); request.put("transfer",preflight.get("preparedTransfer"));
        var receipt = dispatcher.dispatch("configuration.import.commit",access,request); f.imports.definitions.unavailable = true;
        assertThat(dispatcher.dispatch("configuration.import.commit",access,request)).isEqualTo(receipt);
        assertThat(JsonWriter.write(receipt)).doesNotContain("sentinel-hash","sentinel-key");
        assertThat(dispatcher.dispatch("configuration.import.status",access,Map.of("importId",ticket.get("id")))).containsEntry("receipt",receipt);
    }
    @Test void rejectsSpoofedAuthorityUnknownFieldsAndNonIntegralGenerationsBeforeAnyProbeOrAudit() {
        var f = new Fixture(); var dispatcher = f.dispatcher(); var access = f.owner();
        for (Object generation : List.of(1.0,"1",0)) assertMalformed(() -> dispatcher.dispatch("storage.test",access,
                Map.of("operationId",SCOPE.operationId(),"generation",generation,"candidate",candidate(),"layer","CONFIG")));
        assertMalformed(() -> dispatcher.dispatch("storage.status",access,Map.of("managementChannel",true)));
        var bad = candidate(); bad.put("principal","admin");
        assertMalformed(() -> dispatcher.dispatch("storage.test",access,Map.of("operationId",SCOPE.operationId(),"generation",1,"candidate",bad,"layer","CONFIG")));
        assertThat(f.setup.audit).isEmpty(); assertThat(f.setup.target.tests).isZero();
    }
    @Test void configuredDriverDiagnosticsCannotLeakUriUserinfoOrBecomeAuthenticationFailures() {
        var f = new Fixture();
        var caller = StorageControlAccess.ordinary(StorageBinding.Mode.OP,SCOPE,new CacheAuthScope(SCOPE.clusterId(),1,StorageBinding.Mode.OP,"incarnation",1),
                TapstatePrincipal.humanJwt(new VerifiedToken("admin",Scope.ADMIN)),null,
                () -> { throw new TapstateException(IoError.STORE_UNAVAILABLE,Map.of("detail","mongodb://user:sentinel-password@host/config"),new IllegalArgumentException("sentinel-ca")); });
        assertThatThrownBy(() -> f.dispatcher().dispatch("storage.status",caller,Map.of())).isInstanceOfSatisfying(TapstateException.class,error -> {
            assertThat(error.code()).isEqualTo(ControlError.STORAGE_UNAVAILABLE); assertThat(error.args()).isEmpty(); assertThat(error.getCause()).isNull();
        });
    }
    @Test void statusCannotCrossItsVerifiedClusterOrAuthenticationGeneration() {
        var f = new Fixture(); var dispatcher = f.dispatcher(); var active = new AtomicInteger();
        var access = StorageControlAccess.ordinary(StorageBinding.Mode.OP,SCOPE,new CacheAuthScope(SCOPE.clusterId(),1,StorageBinding.Mode.OP,"incarnation",2),
                TapstatePrincipal.humanJwt(new VerifiedToken("admin",Scope.ADMIN)),null,active::incrementAndGet);
        assertThatThrownBy(() -> dispatcher.dispatch("storage.status",access,Map.of())).isInstanceOfSatisfying(TapstateException.class,
                failure -> assertThat(failure.code()).isEqualTo(ControlError.STORAGE_SCOPE_CONFLICT));
        assertThat(active).hasValueGreaterThan(0);
    }
    @Test void cloudUserImportAndBodyClaimedScopeAreRefusedByTheSharedPath() {
        var f = new Fixture(); var dispatcher = f.dispatcher();
        var cloud = StorageControlAccess.ordinary(StorageBinding.Mode.CLOUD,SCOPE,new CacheAuthScope(SCOPE.clusterId(),1,StorageBinding.Mode.CLOUD,"incarnation",1),
                TapstatePrincipal.cloudSession(new VerifiedToken("cloud-user",Scope.WRITE)),"org",() -> {});
        f.imports.codec.packages.put("input",new ConfigurationImportStore.Payload(List.of(),List.of(new User("forged","hash","admin")),List.of(),Map.of(),Map.of()));
        var request = new LinkedHashMap<String,Object>(Map.of("transfer","input","transferKey","sentinel-key","defaultSourceMapping",Map.of()));
        assertThatThrownBy(() -> dispatcher.dispatch("configuration.import.preflight",cloud,request)).isInstanceOf(TapstateException.class);
        request.put("organizationId","other"); assertMalformed(() -> dispatcher.dispatch("configuration.import.preflight",cloud,request));
        assertThat(f.imports.imports.admissions).isZero(); assertThat(f.imports.records).isEmpty();
    }
    private static void assertMalformed(org.assertj.core.api.ThrowableAssert.ThrowingCallable action) {
        assertThatThrownBy(action).isInstanceOfSatisfying(TapstateException.class,failure -> {
            assertThat(failure.code()).isEqualTo(ControlError.MALFORMED_REQUEST); assertThat(failure.getCause()).isNull();
        });
    }
}
