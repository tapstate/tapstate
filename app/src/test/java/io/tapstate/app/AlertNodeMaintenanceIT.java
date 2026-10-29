package io.tapstate.app;

import com.mongodb.client.MongoClients;
import io.tapstate.core.alerting.*;
import io.tapstate.spi.store.*;
import io.tapstate.spi.store.AlertNodeLifecycleStore.*;
import io.tapstate.testsupport.RequiresDocker;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import static org.assertj.core.api.Assertions.*;

/** Original lifecycle CAS and complete CONFIG reference protection, with controlled current membership proof. */
@RequiresDocker
class AlertNodeMaintenanceIT {
    @Container static final org.testcontainers.containers.MongoDBContainer MONGO=AlertEvaluationHostIT.MONGO;
    @Test void plannedNodeStillPresentOrReferencedIsProtectedAndExactClosedBootCanBeReclaimed() {
        AlertEvaluationHostIT.withFixture(f->{
            var host=f.storeHost();var cache=f.configuration().bindings().cachePreparation().orElseThrow().authScope();
            var lifecycle=f.configuration().alertNodeLifecycles(host.scope(),host.setup(),()->{});
            var key=new Key(host.scope(),"retiring-peer","old-boot");
            var fence=new WorkloadClaimFence(new WorkloadClaimKey(host.scope().clusterId(),WorkloadClaimType.NODE_SESSION,key.nodeId()),new WorkloadOwner(key.nodeId(),key.bootId()),1,1,0);
            var registration=new Registration(key,host.setup(),cache,fence,UUID.randomUUID().toString(),1);
            var row=lifecycle.register(registration,f.clock().instant(),()->{});row=lifecycle.retire(registration,row.revision(),UUID.randomUUID().toString(),f.clock().instant(),()->{});
            var original=row;var present=new AtomicBoolean(true);
            var scoped=new AlertStore.Host(host.scope(),host.setup(),host.deployment(),()->{},null,null,resource->resource.equals(key.resource())?Optional.of(new AlertStore.NodeEvidence(resource,key.nodeId(),key.bootId(),1,registration.producerIncarnation(),()->{},present.get(),original.revision(),null)):Optional.empty(),Set.of());
            var store=f.configuration().alerts(scoped,BuiltinAlarmTypes.catalog(),f.clock());var port=store.maintenance().orElseThrow();
            clean(port,11);assertThat(lifecycle.read(key)).contains(original);
            present.set(false);
            try(var client=MongoClients.create(f.settings().uri())) {
                var db=client.getDatabase(new com.mongodb.ConnectionString(f.settings().uri()).getDatabase());
                var unknown=new Document("_id",new Document("authorityId",host.scope().authorityId()).append("clusterId",host.scope().clusterId()).append("resourceKind","cluster-node").append("resourceId",key.nodeId()).append("resourceIncarnation",key.bootId()).append("typeId","future.type").append("qualifier","future"))
                        .append("formatVersion",999);
                db.getCollection("alert_instances").insertOne(unknown);clean(port,11);assertThat(lifecycle.read(key)).contains(original);
                db.getCollection("alert_instances").deleteOne(new Document("_id",unknown.get("_id")));clean(port,11);
                assertThat(lifecycle.read(key)).isEmpty();
            }
        });
    }
    private static void clean(AlertMaintenance port,int slot){port.maintain(new AlertMaintenance.Cursor(slot,null,null),200,4L*1024*1024,Duration.ofSeconds(5));}
}
