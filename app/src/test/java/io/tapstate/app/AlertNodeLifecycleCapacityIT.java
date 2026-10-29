package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.*;
import io.tapstate.spi.store.AlertNodeLifecycleStore.*;
import io.tapstate.testsupport.RequiresDocker;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import static org.assertj.core.api.Assertions.*;
import static io.tapstate.spi.store.AlertNodeLifecycleStore.MAX_ROWS;

/** Retained node boots consume their original bounded authority budget, including planned exits. */
@RequiresDocker
class AlertNodeLifecycleCapacityIT {
    @Container static final org.testcontainers.containers.MongoDBContainer MONGO=AlertEvaluationHostIT.MONGO;
    @Test void aFullRegistryRefusesNewBootsButStillAllowsOriginalCasAndExactCommandReplay() {
        AlertEvaluationHostIT.withFixture(f->{
            var scope=f.storeHost().scope();var setup=f.storeHost().setup();
            var cache=f.configuration().bindings().cachePreparation().orElseThrow().authScope();
            var store=f.configuration().alertNodeLifecycles(scope,setup,()->{});
            var rows=new ArrayList<Row>();
            for(int i=0;i<MAX_ROWS;i++) {
                var registration=registration(f,cache,"node-"+i);
                rows.add(store.register(registration,f.clock().instant(),()->{}));
            }
            assertThat(store.list()).hasSize(MAX_ROWS);
            var extra=registration(f,cache,"extra");
            assertThatThrownBy(()->store.register(extra,f.clock().instant(),()->{})).isInstanceOf(TapstateException.class);
            assertThat(store.read(extra.key())).isEmpty();
            var original=rows.getFirst();String command=UUID.randomUUID().toString();
            var retired=store.retire(original.registration(),original.revision(),command,f.clock().instant(),()->{});
            assertThat(store.retire(original.registration(),original.revision(),command,f.clock().instant(),()->{})).isEqualTo(retired);
            assertThatThrownBy(()->store.retire(original.registration(),original.revision(),UUID.randomUUID().toString(),f.clock().instant(),()->{}))
                    .isInstanceOf(TapstateException.class);
            assertThat(store.list()).hasSize(MAX_ROWS);
            assertThat(store.read(rows.getLast().registration().key())).contains(rows.getLast());
        });
    }
    private static Registration registration(AlertEvaluationHostIT.Fixture f,CacheAuthScope cache,String node) {
        String boot="boot-"+node;
        var session=new WorkloadClaimFence(new WorkloadClaimKey(f.storeHost().scope().clusterId(),WorkloadClaimType.NODE_SESSION,node),
                new WorkloadOwner(node,boot),1,1,0);
        return new Registration(new Key(f.storeHost().scope(),node,boot),f.storeHost().setup(),cache,session,UUID.randomUUID().toString(),1);
    }
}
