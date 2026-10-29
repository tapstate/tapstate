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
}
