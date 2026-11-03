package io.tapstate.adapters.mongostore;

import com.mongodb.client.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.*;
import io.tapstate.spi.store.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import io.tapstate.testsupport.RequiresDocker;
import static org.assertj.core.api.Assertions.*;

/** Native count summaries share the original CONFIG report fence and never create row roots. */
@RequiresDocker
class MongoValidationCountStoreIT {
    @Container static final MongoDBContainer MONGO=new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    @Test void exactCountsAndZeroArePersistedWithoutSyntheticRootsAndReplayCannotReplaceFacts() {
        try(var f=new Fixture()) {
            var first=f.write("one",9007199254740993L,9007199254740992L);
            assertThat(f.first.publishCount(first)).isEqualTo(ValidationResultStore.Publication.APPLIED);
            assertThat(f.second.publishCount(first)).isEqualTo(ValidationResultStore.Publication.DUPLICATE);
            assertThatThrownBy(()->f.second.publishCount(f.write("one",1,1))).isInstanceOf(TapstateException.class).hasMessageContaining("conflictingCountReplay");
            assertThat(f.second.publishCount(f.write("two",0,0))).isEqualTo(ValidationResultStore.Publication.APPLIED);
            var report=f.report();var summaries=report.getList("countSummaries",Document.class);
            assertThat(summaries).hasSize(2);assertThat(summaries.getFirst()).containsEntry("sourceCount",9007199254740993L);
            assertThat(summaries.get(1)).containsEntry("sourceCount",0L).containsEntry("targetCount",0L);
            assertThat(report.getLong("resultBytes")).isEqualTo(report.getLong("countSummaryBytes")).isPositive();
            assertThat(report.getLong("rootCount")).isZero();
            assertThat(SystemCollections.VALIDATION_DIFFERENCES.on(f.database).countDocuments()).isZero();
        }
    }
    @Test void publishCompetesWithSealAndCannotResurrectAnExpiredReport()throws Exception {
        try(var f=new Fixture();var workers=Executors.newFixedThreadPool(2)) {
            var gate=new CountDownLatch(1);
            var publish=workers.submit(()->{gate.await();return f.first.publishCount(f.write("one",3,2));});
            var seal=workers.submit(()->{gate.await();return f.second.seal(f.start.report());});gate.countDown();
            assertThat(publish.get(10,TimeUnit.SECONDS)).isIn(ValidationResultStore.Publication.APPLIED,ValidationResultStore.Publication.CLOSED_OR_EXPIRED);
            assertThat(seal.get(10,TimeUnit.SECONDS)).isEqualTo(ValidationResultStore.Seal.SEALED);
            assertThat(f.first.publishCount(f.write("two",0,0))).isEqualTo(ValidationResultStore.Publication.CLOSED_OR_EXPIRED);
            var reports=SystemCollections.VALIDATION_REPORTS.on(f.database);reports.deleteOne(new Document("_id",MongoValidationAdmissionStore.reportId(f.start.run())));
            assertThat(f.second.publishCount(f.write("one",3,2))).isEqualTo(ValidationResultStore.Publication.CLOSED_OR_EXPIRED);
            assertThat(reports.countDocuments()).isZero();
        }
    }
    @Test void theSnapshotMappingAndOriginalReportEpochAreNotBorrowable() {
        try(var f=new Fixture()) {
            var check=f.write("one",3,2).check();
            var wrong=new ValidationResultStore.Report(f.start.run(),2);
            assertThat(f.first.publishCount(new ValidationResultStore.CountWrite(wrong,f.spec,check))).isEqualTo(ValidationResultStore.Publication.CLOSED_OR_EXPIRED);
            assertThatThrownBy(()->new ValidationResultStore.CountWrite(f.start.report(),f.spec,
                    new ValidationCountCheck(f.start.run(),"foreign","out",3,2,check.sourceRead(),check.targetRead())))
                    .isInstanceOf(TapstateException.class);
            assertThat(f.report()).doesNotContainKey("countSummaries");
        }
    }
    @Test void aCorruptedSummaryLedgerCannotBeAcceptedAsAnIdempotentReplay() {
        try(var f=new Fixture()) {
            var write=f.write("one",3,2);f.first.publishCount(write);
            SystemCollections.VALIDATION_REPORTS.on(f.database).updateOne(new Document("_id",MongoValidationAdmissionStore.reportId(f.start.run())),
                    new Document("$inc",new Document("countSummaryBytes",1L)));
            assertThatThrownBy(()->f.first.publishCount(write)).isInstanceOf(TapstateException.class).hasMessageContaining("count-summary-accounting");
        }
    }
    static final class Fixture implements AutoCloseable {
        final MongoClient client=MongoClients.create(MONGO.getReplicaSetUrl()),other=MongoClients.create(MONGO.getReplicaSetUrl());
        final MongoDatabase database=client.getDatabase("count_results_"+UUID.randomUUID().toString().replace("-",""));
        final ValidationReportAdmission.Task task=new ValidationReportAdmission.Task("cluster","flow","task",UUID.randomUUID());
        final MongoValidationAdmissionStore admission=new MongoValidationAdmissionStore(client,database,"cluster",()->{});
        final MongoValidationResultStore first=new MongoValidationResultStore(client,database,"cluster",()->{});
        final MongoValidationResultStore second=new MongoValidationResultStore(other,other.getDatabase(database.getName()),"cluster",()->{});
        final ValidationRunSpec spec;
        final ValidationReportAdmission.Start start;
        Fixture() {
            try(var session=client.startSession()) {
                for(var table:List.of(SystemCollections.VALIDATION_TASKS,SystemCollections.VALIDATION_REPORTS,SystemCollections.VALIDATION_DIFFERENCES)) {
                    table.prepareCollectionOn(database,session);for(var index:table.indexes())IndexEnsure.createInTransaction(session,table.on(database),index);
                }
            }
            var table=new ValidationTaskSpec.Table("input","input","db",null,"records",null,null);
            var target=new ValidationTaskSpec.Table("out","target","db",null,"records",null,null);
            var comparison=new ValidationTaskSpec("flow",Map.of("flow","a".repeat(64),"input","b".repeat(64),"target","c".repeat(64)),1,"rules",
                    List.of(new ValidationTaskSpec.Mapping("one",ValidationTaskSpec.OutputKind.SYNC,"out",table,target,null),
                            new ValidationTaskSpec.Mapping("two",ValidationTaskSpec.OutputKind.SYNC,"out",
                                    new ValidationTaskSpec.Table("input","input","db",null,"other",null,null),
                                    new ValidationTaskSpec.Table("out","target","db",null,"other",null,null),null)));
            spec=new ValidationRunSpec(comparison,ValidationMode.FULL,ValidationMethod.COUNT);
            var run=new ValidationRunIdentity(new ValidationOwner("cluster","flow","node","boot",1,1,0),"task","run",ValidationMode.FULL,spec.fingerprint(),1,"rules");
            start=new ValidationReportAdmission.Start(task,1,1,"command",run);
            SystemCollections.VALIDATION_TASKS.on(database).insertOne(new Document("_id",MongoValidationAdmissionStore.taskId(task)).append("formatVersion",1)
                    .append("taskGeneration",task.generation().toString()).append("revision",1L)
                    .append("modes",new Document("full",new Document("nextStartSequence",1L)).append("incremental",new Document("nextStartSequence",1L))));
            assertThat(admission.reserve(start)).isEqualTo(ValidationReportAdmission.Reservation.RESERVED);
            assertThat(admission.open(new ValidationReportAdmission.Open(start,spec))).isEqualTo(ValidationReportAdmission.Birth.CREATED);
        }
        ValidationResultStore.CountWrite write(String mapping,long source,long target) {
            var left=new ValidationCheckEvent.ReadBoundary("source-read","fixed-native-count-v1",Instant.EPOCH,Instant.EPOCH);
            var right=new ValidationCheckEvent.ReadBoundary("target-read","fixed-native-count-v1",Instant.EPOCH,Instant.EPOCH);
            return new ValidationResultStore.CountWrite(start.report(),spec,new ValidationCountCheck(start.run(),mapping,"out",source,target,left,right));
        }
        Document report(){return SystemCollections.VALIDATION_REPORTS.on(database).find(new Document("_id",MongoValidationAdmissionStore.reportId(start.run()))).first();}
        public void close(){try{database.drop();}finally{other.close();client.close();}}
    }
}
