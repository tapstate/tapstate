package io.tapstate.adapters.mongostore;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;
import com.mongodb.event.CommandFailedEvent;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.*;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.*;
import io.tapstate.spi.store.ConfigurationImportStore.*;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import static org.assertj.core.api.Assertions.*;

/** Real CONFIG publication witnesses, without transport, connector or runtime-start acceptance. */
@RequiresDocker
class MongoConfigurationImportStoreIT {
    @Container private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
            .withCommand("--replSet","docker-rs","--setParameter","enableTestCommands=1");
    private static final StorageSetupScope SETUP = new StorageSetupScope("cluster","install","setup",1);
    private static final User ADMIN = new User("operator","existing-password-hash","admin");
    private static final SourceResource SOURCE = new SourceResource("imported-source",null,"mysql",Map.of("host","source.invalid","password","import-private-password"),
            SourceMode.CDC,List.of(TableRef.literal("orders")),null,null);
    private static SourceResource target(String id) { return new SourceResource(id,null,"mongodb",Map.of("uri","mongodb://master.invalid/master"),null,null,null,null); }
    private static PipelineResource pipeline(String source,String target) {
        return new PipelineResource("imported-pipeline",null,List.of(SourceRef.spec(source,true)),null,null,
                new ServeBlock.Inline(null,FromRef.literal("orders"),List.of(new SyncElement("sync",target,null,null,null)),null,null),null,null);
    }
    private static Payload small() { return new Payload(List.of(SOURCE),List.of(),List.of(),Map.of(),Map.of()); }
    private static Payload complete() {
        return new Payload(List.of(SOURCE,target("imported-target"),pipeline(SOURCE.id(),"imported-target")),
                List.of(new User("imported-user","import-private-user-hash","admin")),
                List.of(new PipelineLayout("imported-pipeline",Map.of("orders",new PipelineLayout.NodePosition(1,2)),null)),Map.of(),Map.of());
    }
    private static Scope scope(MongoConfigurationDomain domain,StorageBinding.Mode mode) {
        return new Scope("cluster",domain.bindings().load().orElseThrow().bindingVersion(),mode,mode == StorageBinding.Mode.CLOUD ? "organization" : null);
    }
    @Test void definitionsUsersLayoutsRevisionAndReceiptPublishTogetherAndNoSecretsEnterTheOperation() {
        withConfiguration(StorageBinding.Mode.OP,(domain,db) -> {
            long revision = revision(db); Payload payload = complete(); var imports = domain.imports(); var scope = scope(domain,StorageBinding.Mode.OP);
            var pending = imports.preflight(scope,payload); assertThat(pending.phase()).isEqualTo(Phase.PENDING);
            assertThat(revision(db)).isEqualTo(revision); assertThat(domain.artifacts().get(SOURCE.id())).isEmpty();
            var receipt = imports.commit(pending.ticket(),payload);
            assertThat(receipt.configurationRevision()).isEqualTo(revision+1); assertThat(revision(db)).isEqualTo(revision+1);
            assertThat(receipt.resourceHashes()).containsEntry(SOURCE.id(),CanonicalHash.of(SOURCE));
            assertThat(domain.users().find("imported-user")).contains(payload.users().getFirst());
            assertThat(domain.layouts().get("imported-pipeline")).contains(payload.layouts().getFirst());
            assertThat(domain.artifacts().get("imported-pipeline")).isPresent();
            var row = SystemCollections.CONFIGURATION_IMPORTS.on(db).find(new Document("_id",pending.ticket().id())).first();
            assertThat(row).containsEntry("phase","COMMITTED").doesNotContainKey("activeSlot");
            assertThat(row.toJson()).doesNotContain("import-private-password","import-private-user-hash","source.invalid","mongodb://");
            assertThat(SystemCollections.SOURCES.on(db).find(new Document("_id",SOURCE.id())).first().toJson()).doesNotContain("import-private-password");
        });
    }
    @Test void originalCommittedResultDoesNotReplayWritesAfterTheCallerDiscardsItsReplyAndDefinitionsChange() {
        withConfiguration(StorageBinding.Mode.OP,(domain,db) -> {
            var imports = domain.imports(); var ticket = imports.preflight(scope(domain,StorageBinding.Mode.OP),small()).ticket();
            var original = imports.commit(ticket,small());
            var changed = new SourceResource(SOURCE.id(),null,"mysql",Map.of("host","changed.invalid"),SourceMode.CDC,SOURCE.tables(),null,null);
            domain.artifacts().save(changed); long revision = revision(db);
            assertThat(imports.find(ticket.scope(),ticket.id()).orElseThrow().receipt()).isEqualTo(original);
            assertThat(imports.commit(ticket,small())).isEqualTo(original); assertThat(revision(db)).isEqualTo(revision);
            assertThat(domain.artifacts().get(SOURCE.id())).contains(changed);
            assertRefused(() -> imports.commit(ticket,new Payload(List.of(changed),List.of(),List.of(),Map.of(),Map.of())),"input-mismatch");
        });
    }
    @Test void aDestinationChangeAfterPreflightRefusesTheEntireImportWithoutAUserOrPartialDefinition() {
        withConfiguration(StorageBinding.Mode.OP,(domain,db) -> {
            var imports = domain.imports(); var ticket = imports.preflight(scope(domain,StorageBinding.Mode.OP),complete()).ticket();
            domain.users().save(new User("unrelated","hash","admin")); long revision = revision(db);
            assertRefused(() -> imports.commit(ticket,complete()),"destination-changed");
            assertThat(domain.artifacts().get(SOURCE.id())).isEmpty(); assertThat(domain.users().find("imported-user")).isEmpty();
            assertThat(domain.layouts().get("imported-pipeline")).isEmpty(); assertThat(revision(db)).isEqualTo(revision);
            assertThat(imports.find(ticket.scope(),ticket.id()).orElseThrow().phase()).isEqualTo(Phase.ABORTED);
        });
    }
    @Test void cloudRejectsAnyUserBeforeRegisteringOrWritingThePackage() {
        withConfiguration(StorageBinding.Mode.CLOUD,(domain,db) -> {
            long revision = revision(db);
            assertRefused(() -> domain.imports().preflight(scope(domain,StorageBinding.Mode.CLOUD),complete()),"cloud-users");
            assertThat(SystemCollections.CONFIGURATION_IMPORTS.on(db).countDocuments()).isZero();
            assertThat(SystemCollections.CONFIG_USERS.on(db).countDocuments()).isZero(); assertThat(revision(db)).isEqualTo(revision);
            assertThat(domain.artifacts().get(SOURCE.id())).isEmpty();
        });
    }
    @Test void originalScopeAndUnknownIdsNeverBecomePermissionToCreate() {
        withConfiguration(StorageBinding.Mode.OP,(domain,db) -> {
            var imports = domain.imports(); var ticket = imports.preflight(scope(domain,StorageBinding.Mode.OP),small()).ticket();
            var unknown = new Ticket(UUID.randomUUID().toString(),ticket.scope(),ticket.inputHash(),ticket.mappingHash(),ticket.destinationRevision(),ticket.createdAt(),ticket.expiresAt());
            assertThat(imports.find(ticket.scope(),unknown.id())).isEmpty(); assertRefused(() -> imports.commit(unknown,small()),"unknown-operation");
            var foreign = new Ticket(ticket.id(),new Scope("cluster",2,StorageBinding.Mode.OP,null),ticket.inputHash(),ticket.mappingHash(),ticket.destinationRevision(),ticket.createdAt(),ticket.expiresAt());
            assertRefused(() -> imports.commit(foreign,small()),"scope-mismatch"); assertThat(domain.artifacts().get(SOURCE.id())).isEmpty();
        });
    }
    @Test void aPipelineWhoseActualSourceWasOmittedCannotUseAnEmptyCallerReferenceMapAsProof() {
        withConfiguration(StorageBinding.Mode.OP,(domain,db) -> {
            var payload = new Payload(List.of(pipeline("missing","missing")),List.of(),List.of(),Map.of(),Map.of());
            assertRefused(() -> domain.imports().preflight(scope(domain,StorageBinding.Mode.OP),payload),"reference-unproven");
            assertThat(domain.artifacts().get("imported-pipeline")).isEmpty(); assertThat(SystemCollections.CONFIGURATION_IMPORTS.on(db).countDocuments()).isZero();
        });
    }
    @Test void aMissingReusableDefinitionCannotBeHiddenByAnEmptyCallerReferenceMap() {
        withConfiguration(StorageBinding.Mode.OP,(domain,db) -> {
            var pipeline = new PipelineResource("imported-pipeline",null,List.of(SourceRef.spec(SOURCE.id(),true)),
                    List.of(Step.use("filter","absent-transform",FromClause.list(FromRef.literal("orders")))),null,
                    new ServeBlock.Inline(null,FromRef.literal("filter"),List.of(new SyncElement("sync","imported-target",null,null,null)),null,null),null,null);
            var payload = new Payload(List.of(SOURCE,target("imported-target"),pipeline),List.of(),List.of(),Map.of(),Map.of());
            assertRefused(() -> domain.imports().preflight(scope(domain,StorageBinding.Mode.OP),payload),"reference-unproven");
        });
    }
    @Test void defaultTargetMappingMustNameTheActualCurrentBindingTarget() {
        withConfiguration(StorageBinding.Mode.OP,(domain,db) -> {
            var payload = new Payload(List.of(SOURCE),List.of(),List.of(),Map.of(),Map.of("old-default","missing"));
            assertRefused(() -> domain.imports().preflight(scope(domain,StorageBinding.Mode.OP),payload),"default-target-mismatch");
            assertThat(SystemCollections.CONFIGURATION_IMPORTS.on(db).countDocuments()).isZero();
        });
    }
    @Test void anExpiredUnknownAttemptCanBeQueriedButCannotMakeItsFirstDataCommit() {
        withConfiguration(StorageBinding.Mode.OP,(domain,db) -> {
            var scope = scope(domain,StorageBinding.Mode.OP); var ticket = domain.imports().preflight(scope,small()).ticket();
            long revision = revision(db);
            var unavailable = domain.imports().guardedBy(() -> {
                Document row = SystemCollections.CONFIGURATION_IMPORTS.on(db).find(new Document("_id",ticket.id())).first();
                if (row != null && "COMMITTING".equals(row.get("phase"))) throw new TapstateException(IoError.STORE_UNAVAILABLE,Map.of("detail","test-unavailable"),null);
            });
            assertThatThrownBy(() -> unavailable.commit(ticket,small())).isInstanceOf(TapstateException.class);
            assertThat(domain.imports().find(scope,ticket.id()).orElseThrow().phase()).isEqualTo(Phase.COMMITTING);
            var expired = new Ticket(ticket.id(),ticket.scope(),ticket.inputHash(),ticket.mappingHash(),ticket.destinationRevision(),
                    ticket.createdAt().minusSeconds(3600),ticket.expiresAt().minusSeconds(3600));
            SystemCollections.CONFIGURATION_IMPORTS.on(db).updateOne(new Document("_id",ticket.id()),new Document("$set",new Document("createdAt",Date.from(expired.createdAt()))
                    .append("expiresAt",Date.from(expired.expiresAt()))));
            assertRefused(() -> domain.imports().commit(expired,small()),"expired");
            assertThat(domain.artifacts().get(SOURCE.id())).isEmpty(); assertThat(revision(db)).isEqualTo(revision);
            assertThat(domain.imports().find(scope,ticket.id()).orElseThrow().phase()).isEqualTo(Phase.COMMITTING);
        });
    }
    @Test void actualArtifactWriteThenGuardLossRollsBackDefinitionsUsersLayoutsVersionAndReceipt() {
        withConfiguration(StorageBinding.Mode.OP,(domain,db) -> {
            var ticket = domain.imports().preflight(scope(domain,StorageBinding.Mode.OP),complete()).ticket(); long revision = revision(db);
            var written = new AtomicBoolean(); var requests = ConcurrentHashMap.<Integer>newKeySet();
            CommandListener witness = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent event) {
                    if (event.getDatabaseName().equals(db.getName()) && event.getCommandName().equals("update")
                            && "sources".equals(event.getCommand().getString("update").getValue())) requests.add(event.getRequestId());
                }
                @Override public void commandSucceeded(CommandSucceededEvent event) { if (requests.remove(event.getRequestId())) written.set(true); }
                @Override public void commandFailed(CommandFailedEvent event) { requests.remove(event.getRequestId()); }
            };
            try (var observed = MongoClients.create(MongoClientSettings.builder().applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl())).addCommandListener(witness).build())) {
                var observedDb = observed.getDatabase(db.getName());
                var material = SystemCollections.CLUSTER_KEYS.on(db).find().first().getString("material");
                var cipher = SourceConfigCipherProvider.fixed(new SourceConfigCipher(java.util.Base64.getUrlDecoder().decode(material)));
                var bindings = new MongoStorageBindingStore(observed,observedDb,cipher);
                var artifacts = new MongoConfigurationArtifactStore(observed,observedDb,cipher);
                var imports = new MongoConfigurationImportStore(observed,observedDb,"cluster",bindings,artifacts,() -> { if (written.get()) throw fenced(); });
                assertThatThrownBy(() -> imports.commit(ticket,complete())).isInstanceOf(TapstateException.class);
            }
            assertThat(written).isTrue(); assertThat(domain.artifacts().get(SOURCE.id())).isEmpty();
            assertThat(domain.artifacts().get("imported-pipeline")).isEmpty(); assertThat(domain.users().find("imported-user")).isEmpty();
            assertThat(domain.layouts().get("imported-pipeline")).isEmpty(); assertThat(revision(db)).isEqualTo(revision);
            assertThat(domain.imports().find(ticket.scope(),ticket.id()).orElseThrow().phase()).isEqualTo(Phase.COMMITTING);
        });
    }
    @Test void concurrentPreflightsCannotExceedTheDeclaredPendingBudget() {
        withConfiguration(StorageBinding.Mode.OP,(domain,db) -> {
            var scope = scope(domain,StorageBinding.Mode.OP); var begin = new CountDownLatch(1);
            var results = new ArrayList<java.util.concurrent.Future<Optional<Status>>>();
            try (var executor = Executors.newFixedThreadPool(4)) {
                for (int i = 0; i < 18; i++) results.add(executor.submit(() -> {
                    begin.await();
                    try { return Optional.of(domain.imports().preflight(scope,small())); }
                    catch (TapstateException failure) { assertThat(failure.code()).isEqualTo(IoError.CONFIGURATION_IMPORT_LIMIT); return Optional.empty(); }
                }));
                begin.countDown(); int admitted = 0;
                for (var result : results) if (result.get(30,TimeUnit.SECONDS).isPresent()) admitted++;
                assertThat(admitted).isEqualTo(16);
            }
            assertThat(SystemCollections.CONFIGURATION_IMPORTS.on(db).countDocuments()).isEqualTo(16);
            assertThat(domain.artifacts().get(SOURCE.id())).isEmpty();
        });
    }
    @Test void missingIndexOrOperationFieldsAreNotRebuiltByAnOrdinaryRequest() {
        withConfiguration(StorageBinding.Mode.OP,(domain,db) -> {
            var imports = domain.imports(); var scope = scope(domain,StorageBinding.Mode.OP); var ticket = imports.preflight(scope,small()).ticket();
            SystemCollections.CONFIGURATION_IMPORTS.on(db).updateOne(new Document("_id",ticket.id()),new Document("$unset",new Document("activeSlot",1)));
            assertThatThrownBy(() -> imports.find(scope,ticket.id())).isInstanceOfSatisfying(TapstateException.class,failure -> assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
            SystemCollections.CONFIGURATION_IMPORTS.on(db).dropIndex("configuration_import_active_slot");
            assertThatThrownBy(() -> imports.preflight(scope,small())).isInstanceOfSatisfying(TapstateException.class,failure -> assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
            assertThat(SystemCollections.CONFIGURATION_IMPORTS.on(db).listIndexes().into(new ArrayList<>())).noneMatch(index -> "configuration_import_active_slot".equals(index.get("name")));
        });
    }
    @Test void boundDefaultTargetIsAReadOnlyHashCheckedDependencyOfTheFinalPipeline() {
        withConfiguration(StorageBinding.Mode.OP,(domain,db) -> {
            var target = (SourceResource) domain.artifacts().get(StorageBinding.defaultSourceId(StorageBinding.Mode.OP)).orElseThrow();
            var payload = new Payload(List.of(SOURCE,pipeline(SOURCE.id(),target.id())),List.of(),List.of(),
                    Map.of(target.id(),CanonicalHash.of(target)),Map.of("old-default",target.id()));
            long revision = revision(db);
            var ticket = domain.imports().preflight(scope(domain,StorageBinding.Mode.OP),payload).ticket();
            var receipt = domain.imports().commit(ticket,payload);
            assertThat(receipt.resourceHashes()).containsOnlyKeys(SOURCE.id(),"imported-pipeline");
            assertThat(domain.artifacts().get(target.id())).contains(target);
            assertThat(revision(db)).isEqualTo(revision+1);
        });
    }
    @Test void actualReferenceKindsAndReusableServeSinkDependenciesCannotBeOmitted() {
        withConfiguration(StorageBinding.Mode.OP,(domain,db) -> {
            var wrong = new Payload(List.of(SOURCE,pipeline(SOURCE.id(),"imported-pipeline")),List.of(),List.of(),Map.of(),Map.of());
            assertRefused(() -> domain.imports().preflight(scope(domain,StorageBinding.Mode.OP),wrong),"reference-kind");
            var serve = new ServeResource("reusable-serve",null,List.of(new SyncElement("sync","missing-target",null,null,null)),null,null,null);
            var pipeline = new PipelineResource("imported-pipeline",null,List.of(SourceRef.spec(SOURCE.id(),true)),null,null,
                    new ServeBlock.Use(null,serve.id(),FromRef.literal("orders")),null,null);
            var missing = new Payload(List.of(SOURCE,serve,pipeline),List.of(),List.of(),Map.of(),Map.of());
            assertRefused(() -> domain.imports().preflight(scope(domain,StorageBinding.Mode.OP),missing),"reference-unproven");
            assertThat(SystemCollections.CONFIGURATION_IMPORTS.on(db).countDocuments()).isZero();
        });
    }
    @Test void twoIndependentClientsCanPublishOneOriginalTicketOnlyOnce() {
        withConfiguration(StorageBinding.Mode.OP,(domain,db) -> {
            var ticket = domain.imports().preflight(scope(domain,StorageBinding.Mode.OP),complete()).ticket();
            long revision = revision(db); var begin = new CountDownLatch(1);
            try (var second = MongoConfigurationDomain.open(settings(db),"cluster"); var executor = Executors.newFixedThreadPool(2)) {
                var first = executor.submit(() -> { begin.await(); return concurrentCommit(domain.imports(),ticket,complete()); });
                var other = executor.submit(() -> { begin.await(); return concurrentCommit(second.imports(),ticket,complete()); });
                begin.countDown(); var a = first.get(30,TimeUnit.SECONDS); var b = other.get(30,TimeUnit.SECONDS);
                var receipt = domain.imports().find(ticket.scope(),ticket.id()).orElseThrow().receipt();
                assertThat(receipt).isNotNull(); assertThat(a.isPresent() || b.isPresent()).isTrue();
                a.ifPresent(value -> assertThat(value).isEqualTo(receipt)); b.ifPresent(value -> assertThat(value).isEqualTo(receipt));
                assertThat(second.imports().commit(ticket,complete())).isEqualTo(receipt);
            }
            assertThat(revision(db)).isEqualTo(revision+1);
            assertThat(SystemCollections.CONFIGURATION_IMPORTS.on(db).countDocuments()).isOne();
            assertThat(SystemCollections.CONFIG_USERS.on(db).countDocuments(new Document("_id","imported-user"))).isOne();
            assertThat(domain.artifacts().get("imported-pipeline")).isPresent();
        });
    }
    @Test void competingImportsForOneUsernameCannotPublishTheLosingDefinition() {
        withConfiguration(StorageBinding.Mode.OP,(domain,db) -> {
            var user = new User("same-user","hash","admin");
            var left = new Payload(List.of(target("left-source")),List.of(user),List.of(),Map.of(),Map.of());
            var right = new Payload(List.of(target("right-source")),List.of(user),List.of(),Map.of(),Map.of());
            var scope = scope(domain,StorageBinding.Mode.OP); var imports = domain.imports();
            var a = imports.preflight(scope,left).ticket(); var b = imports.preflight(scope,right).ticket();
            long revision = revision(db); var begin = new CountDownLatch(1);
            try (var second = MongoConfigurationDomain.open(settings(db),"cluster"); var executor = Executors.newFixedThreadPool(2)) {
                var first = executor.submit(() -> { begin.await(); return competingCommit(imports,a,left); });
                var other = executor.submit(() -> { begin.await(); return competingCommit(second.imports(),b,right); });
                begin.countDown(); boolean firstWon = first.get(30,TimeUnit.SECONDS); boolean secondWon = other.get(30,TimeUnit.SECONDS);
                assertThat(firstWon ^ secondWon).isTrue();
                assertThat(domain.artifacts().get("left-source").isPresent()).isEqualTo(firstWon);
                assertThat(domain.artifacts().get("right-source").isPresent()).isEqualTo(secondWon);
                assertThat(imports.find(scope,firstWon ? b.id() : a.id()).orElseThrow().phase()).isEqualTo(Phase.ABORTED);
            }
            assertThat(revision(db)).isEqualTo(revision+1); assertThat(domain.users().find(user.username())).contains(user);
        });
    }
    @Test void threeUnknownAttemptsRetainTheOriginalOperationAndCannotObtainAnotherAttempt() {
        withConfiguration(StorageBinding.Mode.OP,(domain,db) -> {
            var imports = domain.imports(); var scope = scope(domain,StorageBinding.Mode.OP);
            var ticket = imports.preflight(scope,small()).ticket(); long revision = revision(db);
            for (int attempt = 1; attempt <= 3; attempt++) {
                int threshold = attempt;
                var unavailable = imports.guardedBy(() -> {
                    var row = SystemCollections.CONFIGURATION_IMPORTS.on(db).find(new Document("_id",ticket.id())).first();
                    if (row != null && row.getInteger("attempts") >= threshold) throw fenced();
                });
                assertThatThrownBy(() -> unavailable.commit(ticket,small())).isInstanceOf(TapstateException.class);
                var status = imports.find(scope,ticket.id()).orElseThrow();
                assertThat(status.phase()).isEqualTo(Phase.COMMITTING); assertThat(status.attempts()).isEqualTo(attempt);
            }
            assertThatThrownBy(() -> imports.commit(ticket,small())).isInstanceOfSatisfying(TapstateException.class,
                    failure -> assertThat(failure.code()).isEqualTo(IoError.CONFIGURATION_IMPORT_LIMIT));
            assertThat(SystemCollections.CONFIGURATION_IMPORTS.on(db).countDocuments()).isOne();
            assertThat(imports.find(scope,ticket.id()).orElseThrow().attempts()).isEqualTo(3);
            assertThat(domain.artifacts().get(SOURCE.id())).isEmpty(); assertThat(revision(db)).isEqualTo(revision);
        });
    }
    @Test void retainedResultsAreBoundedWithoutDeletingOrReplayingTheOriginalCommittedOutcome() {
        withConfiguration(StorageBinding.Mode.OP,(domain,db) -> {
            var imports = domain.imports(); var scope = scope(domain,StorageBinding.Mode.OP);
            var ticket = imports.preflight(scope,small()).ticket(); var receipt = imports.commit(ticket,small());
            var rows = SystemCollections.CONFIGURATION_IMPORTS.on(db); var original = rows.find(new Document("_id",ticket.id())).first();
            var copies = new ArrayList<Document>();
            // Synthetic retained outcomes isolate the budget check from 255 unrelated publications.
            for (int slot = 1; slot < ConfigurationImportStore.MAX_RESULTS; slot++) {
                var copy = new Document(original); copy.put("receipt",new Document(original.get("receipt",Document.class))); var id = UUID.randomUUID().toString();
                copy.put("_id",id); copy.put("historySlot",slot);
                var body = copy.get("receipt",Document.class); body.put("id",id);
                copy.put("receiptHash",CanonicalHash.ofText(io.tapstate.core.common.JsonWriter.write(body)));
                copies.add(copy);
            }
            rows.insertMany(copies); long revision = revision(db);
            assertThatThrownBy(() -> imports.preflight(scope,small())).isInstanceOfSatisfying(TapstateException.class,
                    failure -> assertThat(failure.code()).isEqualTo(IoError.CONFIGURATION_IMPORT_LIMIT));
            assertThat(rows.countDocuments()).isEqualTo(ConfigurationImportStore.MAX_RESULTS);
            assertThat(imports.find(scope,ticket.id()).orElseThrow().receipt()).isEqualTo(receipt);
            assertThat(imports.commit(ticket,small())).isEqualTo(receipt); assertThat(revision(db)).isEqualTo(revision);
            assertThat(rows.listIndexes().into(new ArrayList<>())).noneMatch(index -> index.containsKey("expireAfterSeconds"));
        });
    }
    @Test void aDriverTransactionRetryRechecksTheOriginalGuardBeforeASecondDataWrite() {
        withConfiguration(StorageBinding.Mode.OP,(domain,db) -> {
            var ticket = domain.imports().preflight(scope(domain,StorageBinding.Mode.OP),small()).ticket(); long revision = revision(db);
            var marker = new AtomicBoolean(); var armed = new AtomicBoolean(); var lost = new AtomicBoolean();
            var started = new AtomicInteger(); var failed = new AtomicInteger();
            try (var admin = MongoClients.create(MONGO.getReplicaSetUrl())) {
                CommandListener witness = new CommandListener() {
                    @Override public void commandStarted(CommandStartedEvent event) {
                        if (!event.getDatabaseName().equals(db.getName()) || !event.getCommandName().equals("update")) return;
                        if (armed.get()) started.incrementAndGet();
                        if ("configuration_imports".equals(event.getCommand().getString("update").getValue())) {
                            var replacement = event.getCommand().getArray("updates").getFirst().asDocument().getDocument("u");
                            if (replacement.containsKey("phase") && "COMMITTING".equals(replacement.getString("phase").getValue())) marker.set(true);
                        }
                    }
                    @Override public void commandSucceeded(CommandSucceededEvent event) {
                        if (event.getCommandName().equals("commitTransaction") && marker.get() && armed.compareAndSet(false,true))
                            admin.getDatabase("admin").runCommand(new Document("configureFailPoint","failCommand")
                                    .append("mode",new Document("times",1)).append("data",new Document("failCommands",List.of("update"))
                                            .append("errorCode",112).append("errorLabels",List.of("TransientTransactionError"))));
                    }
                    @Override public void commandFailed(CommandFailedEvent event) {
                        if (armed.get() && event.getCommandName().equals("update")) { failed.incrementAndGet(); lost.set(true); }
                    }
                };
                try (var observed = MongoClients.create(MongoClientSettings.builder().applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl())).addCommandListener(witness).build())) {
                    var observedDb = observed.getDatabase(db.getName());
                    var material = SystemCollections.CLUSTER_KEYS.on(db).find().first().getString("material");
                    var cipher = SourceConfigCipherProvider.fixed(new SourceConfigCipher(java.util.Base64.getUrlDecoder().decode(material)));
                    var bindings = new MongoStorageBindingStore(observed,observedDb,cipher);
                    var artifacts = new MongoConfigurationArtifactStore(observed,observedDb,cipher);
                    var imports = new MongoConfigurationImportStore(observed,observedDb,"cluster",bindings,artifacts,() -> { if (lost.get()) throw fenced(); });
                    try { assertThatThrownBy(() -> imports.commit(ticket,small())).isInstanceOfSatisfying(TapstateException.class,
                            failure -> assertThat(failure.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED)); }
                    finally { admin.getDatabase("admin").runCommand(new Document("configureFailPoint","failCommand").append("mode","off")); }
                }
            }
            assertThat(armed).isTrue(); assertThat(failed.get()).isOne(); assertThat(started.get()).isOne();
            assertThat(domain.artifacts().get(SOURCE.id())).isEmpty(); assertThat(revision(db)).isEqualTo(revision);
            var status = domain.imports().find(ticket.scope(),ticket.id()).orElseThrow();
            assertThat(status.phase()).isEqualTo(Phase.COMMITTING); assertThat(status.attempts()).isOne();
        });
    }
    private static Optional<Receipt> concurrentCommit(ConfigurationImportStore imports,Ticket ticket,Payload payload) {
        try { return Optional.of(imports.commit(ticket,payload)); }
        catch (TapstateException failure) {
            assertThat(failure.code()).isEqualTo(IoError.CONFIGURATION_IMPORT_REFUSED);
            assertThat(failure.args()).containsEntry("reason","attempt-fenced"); return Optional.empty();
        }
    }
    private static boolean competingCommit(ConfigurationImportStore imports,Ticket ticket,Payload payload) {
        try { imports.commit(ticket,payload); return true; }
        catch (TapstateException failure) {
            assertThat(failure.code()).isEqualTo(IoError.CONFIGURATION_IMPORT_REFUSED);
            assertThat(failure.args().get("reason")).isIn("destination-changed","user-exists"); return false;
        }
    }
    private static TapstateException fenced() { return new TapstateException(IoError.WORKLOAD_CLAIM_FENCED,Map.of(),null); }
    private static void assertRefused(Runnable action,String reason) {
        assertThatThrownBy(action::run).isInstanceOfSatisfying(TapstateException.class,failure -> {
            assertThat(failure.code()).isEqualTo(IoError.CONFIGURATION_IMPORT_REFUSED); assertThat(failure.args()).containsEntry("reason",reason);
        });
    }
    private static long revision(MongoDatabase db) { return SystemCollections.CONFIGURATION_VERSION.on(db).find().first().getLong("revision"); }
    private static MongoConnectionSettings settings(MongoDatabase db) { return new MongoConnectionSettings(MONGO.getReplicaSetUrl(db.getName()),null,Duration.ofSeconds(5)); }
    private static void withConfiguration(StorageBinding.Mode mode,Scenario scenario) {
        try (var admin = MongoClients.create(MONGO.getReplicaSetUrl())) {
            var db = admin.getDatabase("configuration_import_"+UUID.randomUUID().toString().replace("-",""));
            try (var domain = MongoConfigurationBootstrap.prepare(settings(db),"cluster","install","setup",1)) {
                var cache = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB,"mongodb://cache.invalid/cache","cache",null);
                domain.bindings().commit(SETUP,mode,cache,target(StorageBinding.defaultSourceId(mode))); domain.bindings().complete(SETUP);
                if (mode == StorageBinding.Mode.OP) domain.firstAdmin().create(SETUP,ADMIN);
                scenario.run(domain,db);
            } finally { db.drop(); }
        } catch (Exception failure) { throw new AssertionError(failure); }
    }
    @FunctionalInterface private interface Scenario { void run(MongoConfigurationDomain domain,MongoDatabase db) throws Exception; }
}
