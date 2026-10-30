package io.tapstate.app;

import com.mongodb.client.MongoClients;
import io.tapstate.spi.store.AlertMaintenance;
import io.tapstate.testsupport.RequiresDocker;
import java.time.Duration;
import java.util.*;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import static org.assertj.core.api.Assertions.*;

/** Actual CONFIG cleanup under the production round driver and a controlled five-minute clock. */
@RequiresDocker
class AlertMaintenanceRoundIT {
    @Container static final org.testcontainers.containers.MongoDBContainer MONGO=AlertEvaluationHostIT.MONGO;
    @Test void oneRoundKeepsUpWithTwentyAcceptedPayloadsAndFiveQuotasBesideLiveConfiguration(){
        AlertEvaluationHostIT.withFixture(f->{
            AlertNotificationPlannerIT.route(f,Duration.ZERO);var planner=AlertNotificationPlannerIT.planner(f,"mixed-cleanup-planner");
            var settings=new AlertRetentionSettings();settings.setRetention(Map.of("accepted-payload","PT15M"));var policy=settings.resolve(f.storeHost().deployment());
            var evaluator=new AlertEvaluationHost(f.store(),io.tapstate.core.alerting.BuiltinAlarmTypes.catalog(),f.storeHost().deployment(),f.clock(),"mixed-cleanup-evaluator",()->{},policy).withPlanner(planner);
            var h=io.tapstate.core.alerting.AlertConfiguration.Header.create(io.tapstate.core.alerting.AlertConfiguration.Identity.create(
                    io.tapstate.core.alerting.AlertConfiguration.Kind.RULE,f.storeHost().scope(),"protected-definition"),f.storeHost().deployment(),f.clock().instant(),"operator");
            var extra=new io.tapstate.core.alerting.AlertConfiguration.Rule(h,"Protected definition","pipeline.failed",1,f.rule().resources(),Map.of(),null,true,null)
                    .validated(io.tapstate.core.alerting.BuiltinAlarmTypes.catalog()).enrolled(null);
            assertThat(f.store().saveDefinition(null,extra,AlertNotificationPlannerIT.access())).isEqualTo(io.tapstate.spi.store.AlertStore.Outcome.STAGED);
            var ids=new ArrayList<io.tapstate.core.alerting.AlertIntent.Identity>();var start=f.clock().instant();
            for(int cycle=0;cycle<5;cycle++)for(int phase=0;phase<2;phase++){
                f.now().set(start.plusSeconds(cycle*10L+phase));
                var condition=phase==0?io.tapstate.core.alerting.AlertSignal.Condition.BAD:io.tapstate.core.alerting.AlertSignal.Condition.GOOD;
                assertThat(evaluator.accept(f.binding(()->new io.tapstate.spi.store.AlertStore.EvaluationProof(f.intent(),()->{})),cycle*2+phase+1,
                        new io.tapstate.core.alerting.AlertInput.Conditions(f.clock().instant(),condition,null))).isEqualTo(AlertEvaluationHost.Result.ACCEPTED);
                planner.tick();var pending=AlertNotificationPlannerIT.intents(f);assertThat(pending).hasSize(2);
                for(var row:pending){ids.add(row.value().frozen().identity());AlertNotificationPlannerIT.grant(f,row);AlertNotificationPlannerIT.accepted(f,row.value().frozen().identity());}
            }
            assertThat(ids).hasSize(20);f.now().set(start.plusSeconds(1200));
            for(var id:ids){var record=f.store().intent(id,null).orElseThrow().value();
                assertThat(record.frozen().retention().value().acceptedPayload()).isEqualTo(Duration.ofMinutes(15));
                assertThat(record.state().retained().payloadUntil()).isBefore(f.clock().instant());}
            try(var client=MongoClients.create(f.settings().uri())){
                var db=client.getDatabase(new com.mongodb.ConnectionString(f.settings().uri()).getDatabase());var scope=f.storeHost().scope();
                for(int i=0;i<5;i++){var minute=start.minusSeconds(7200+i*60L).truncatedTo(java.time.temporal.ChronoUnit.MINUTES);
                    var id=new Document("authorityId",scope.authorityId()).append("clusterId",scope.clusterId()).append("minute",minute.toString());
                    db.getCollection("alert_test_quotas").insertOne(new Document("_id",id).append("formatVersion",1).append("recordRevision",1L).append("cleanupAt",Date.from(minute.plusSeconds(3660)))
                            .append("body",new Document("actors",List.of(new Document("actor","original-actor").append("accepted",1)))));
                }
                try(var worker=new AlertMaintenanceRuntime(f.store().maintenance().orElseThrow(),f.clock(),()->{})){
                    worker.tick();
                    assertThat(ids.stream().filter(id->f.store().intent(id,null).orElseThrow().value().state().payload()!=null).map(io.tapstate.core.alerting.AlertIntent.Identity::deliveryId).toList())
                            .as("remaining eligible payloads; health=%s",worker.health()).isEmpty();
                    assertThat(db.getCollection("alert_test_quotas").countDocuments()).isZero();
                    assertThat(((Number)worker.health().get("scanned")).longValue()).isLessThanOrEqualTo(1000);
                    assertThat(((Number)worker.health().get("ioBytes")).longValue()).isLessThanOrEqualTo(16L*1024*1024);
                }
            }
        });
    }
    @Test void twentyFiveSmallCandidatesFitOneOriginalRoundAlongsideTheOtherResources(){
        AlertEvaluationHostIT.withFixture(f->{var scope=f.storeHost().scope();var start=f.clock().instant();
            try(var client=MongoClients.create(f.settings().uri())){
                var db=client.getDatabase(new com.mongodb.ConnectionString(f.settings().uri()).getDatabase());
                // Twenty-five cheap mutations are the five-minute floor of the admitted engineering load.
                // Quota rows isolate transaction-discovery overhead without simulating transport or an SLO pass.
                for(int i=0;i<25;i++){var minute=start.minusSeconds(7200+i*60L).truncatedTo(java.time.temporal.ChronoUnit.MINUTES);
                    var id=new Document("authorityId",scope.authorityId()).append("clusterId",scope.clusterId()).append("minute",minute.toString());
                    db.getCollection("alert_test_quotas").insertOne(new Document("_id",id).append("formatVersion",1).append("recordRevision",1L).append("cleanupAt",Date.from(minute.plusSeconds(3660)))
                            .append("body",new Document("actors",List.of(new Document("actor","original-actor").append("accepted",1)))));
                }
                try(var worker=new AlertMaintenanceRuntime(f.store().maintenance().orElseThrow(),f.clock(),()->{})){
                    worker.tick();assertThat(worker.health()).containsEntry("changed",25L).containsEntry("failures",0L);
                    assertThat(db.getCollection("alert_test_quotas").countDocuments()).isZero();
                    assertThat(((Number)worker.health().get("scanned")).longValue()).isLessThanOrEqualTo(1000);
                    assertThat(((Number)worker.health().get("ioBytes")).longValue()).isLessThanOrEqualTo(16L*1024*1024);
                }
            }
        });
    }
    @Test void thirtyEligibleControlRowsAreReclaimedWithinThreeUnexpandedRounds() {
        AlertEvaluationHostIT.withFixture(f->{var scope=f.storeHost().scope();var start=f.clock().instant();
            try(var client=MongoClients.create(f.settings().uri())){
                var db=client.getDatabase(new com.mongodb.ConnectionString(f.settings().uri()).getDatabase());
                for(int i=0;i<30;i++){var minute=start.minusSeconds(7200+i*60L).truncatedTo(java.time.temporal.ChronoUnit.MINUTES);
                    var id=new Document("authorityId",scope.authorityId()).append("clusterId",scope.clusterId()).append("minute",minute.toString());
                    db.getCollection("alert_test_quotas").insertOne(new Document("_id",id).append("formatVersion",1).append("recordRevision",1L).append("cleanupAt",Date.from(minute.plusSeconds(3660)))
                            .append("body",new Document("actors",List.of(new Document("actor","original-actor").append("accepted",1)))));
                }
                try(var worker=new AlertMaintenanceRuntime(f.store().maintenance().orElseThrow(),f.clock(),()->{})){
                    for(int round=0;round<3;round++){f.now().set(start.plusSeconds(round*300L));worker.tick();}
                    assertThat(db.getCollection("alert_test_quotas").countDocuments()).isZero();
                    assertThat(worker.health()).containsEntry("changed",30L).containsEntry("failures",0L);
                    assertThat(((Number)worker.health().get("scanned")).longValue()).isLessThanOrEqualTo(3000);
                    assertThat(((Number)worker.health().get("ioBytes")).longValue()).isLessThanOrEqualTo(48L*1024*1024);
                }
            }
        });
    }
    @Test void aCandidateWhoseMutationExceedsTheRemainingBudgetIsRetriedBeforeAdvancingItsCursor(){
        AlertEvaluationHostIT.withFixture(f->{var scope=f.storeHost().scope();var minute=f.clock().instant().minusSeconds(7200).truncatedTo(java.time.temporal.ChronoUnit.MINUTES);
            try(var client=MongoClients.create(f.settings().uri())){
                var db=client.getDatabase(new com.mongodb.ConnectionString(f.settings().uri()).getDatabase());
                var id=new Document("authorityId",scope.authorityId()).append("clusterId",scope.clusterId()).append("minute",minute.toString());
                db.getCollection("alert_test_quotas").insertOne(new Document("_id",id).append("formatVersion",1).append("recordRevision",1L).append("cleanupAt",Date.from(minute.plusSeconds(3660)))
                        .append("body",new Document("actors",List.of(new Document("actor","original-actor").append("accepted",1)))));
                var maintenance=f.store().maintenance().orElseThrow();
                maintenance.maintain(new AlertMaintenance.Cursor(1,null,null),200,4L*1024*1024,Duration.ofSeconds(10));
                var version=db.getCollection("configuration_version").find().first();
                var yielded=maintenance.maintain(new AlertMaintenance.Cursor(10,null,null),44,4L*1024*1024,Duration.ofSeconds(5));
                assertThat(yielded.changed()).isZero();assertThat(yielded.scanned()).isLessThanOrEqualTo(44);assertThat(yielded.next()).isNotNull();
                assertThat(yielded.oldestCandidate()).isEqualTo(minute.plusSeconds(3660));
                assertThat(db.getCollection("configuration_version").find().first()).isEqualTo(version);
                assertThat(db.getCollection("alert_test_quotas").countDocuments(new Document("_id",id))).isOne();
                var resumed=maintenance.maintain(yielded.next(),200,4L*1024*1024,Duration.ofSeconds(5));
                assertThat(resumed.changed()).isOne();
                assertThat(db.getCollection("alert_test_quotas").countDocuments(new Document("_id",id))).isZero();
            }
        });
    }
    @Test void aHeaderValidBodyThatCannotFitAnyFreshBatchIsIsolatedWithoutBlockingTheNextCandidate(){
        AlertEvaluationHostIT.withFixture(f->{var scope=f.storeHost().scope();var minute=f.clock().instant().minusSeconds(7200).truncatedTo(java.time.temporal.ChronoUnit.MINUTES);
            try(var client=MongoClients.create(f.settings().uri())){
                var db=client.getDatabase(new com.mongodb.ConnectionString(f.settings().uri()).getDatabase());
                var earlier=new Document("authorityId",scope.authorityId()).append("clusterId",scope.clusterId()).append("minute",minute.minusSeconds(60).toString());
                var later=new Document("authorityId",scope.authorityId()).append("clusterId",scope.clusterId()).append("minute",minute.toString());
                String padding="x".repeat(4*1024*1024-16*1024);
                var poisoned=new Document("_id",earlier).append("formatVersion",1).append("recordRevision",1L).append("cleanupAt",Date.from(minute.plusSeconds(3600)))
                        .append("body",new Document("actors",List.of(new Document("actor","original-actor").append("accepted",1)))).append("padding",padding);
                db.getCollection("alert_test_quotas").insertOne(poisoned);
                db.getCollection("alert_test_quotas").insertOne(new Document("_id",later).append("formatVersion",1).append("recordRevision",1L).append("cleanupAt",Date.from(minute.plusSeconds(3660)))
                        .append("body",new Document("actors",List.of(new Document("actor","original-actor").append("accepted",1)))));
                var maintenance=f.store().maintenance().orElseThrow();var cursor=new AlertMaintenance.Cursor(10,null,null);long changed=0,isolated=0;
                for(int round=0;round<3;round++){var result=maintenance.maintain(cursor,200,4L*1024*1024,Duration.ofSeconds(5));
                    changed+=result.changed();isolated+=result.skipped().getOrDefault("unreadable",0L);assertThat(result.bytes()).isLessThanOrEqualTo(4L*1024*1024);
                    if(result.next()==null)break;cursor=result.next();}
                assertThat(changed).isOne();assertThat(isolated).isPositive();
                assertThat(db.getCollection("alert_test_quotas").countDocuments(new Document("_id",later))).isZero();
                assertThat(db.getCollection("alert_test_quotas").find(new Document("_id",earlier)).projection(new Document("padding",1)).first().getString("padding")).isEqualTo(padding);
            }
        });
    }
}
