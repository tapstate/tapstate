package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClients;
import io.tapstate.testsupport.RequiresDocker;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

@RequiresDocker
class PreparedNativeStoreDisposalIT {
    @Test void expiredDisposalKeepsTheOriginalDatabasesAvailableForRetry() {
        var store=PreparedNativeStore.create();
        try(var client=MongoClients.create(store.configurationUri())){
            var names=List.of(new ConnectionString(store.configurationUri()).getDatabase(),
                    new ConnectionString(store.cacheUri()).getDatabase(),new ConnectionString(store.masterUri()).getDatabase());
            var before=names.stream().map(name->client.getDatabase(name).listCollectionNames().into(new java.util.ArrayList<>())).toList();
            assertThatThrownBy(()->store.close(System.nanoTime()-1)).isInstanceOf(AssertionError.class)
                    .hasMessageContaining("disposal deadline expired");
            var retained=names.stream().map(name->client.getDatabase(name).listCollectionNames().into(new java.util.ArrayList<>())).toList();
            assertThat(retained).isEqualTo(before);
            store.close(System.nanoTime()+Duration.ofSeconds(10).toNanos());
            assertThat(client.listDatabaseNames().into(new java.util.ArrayList<>())).doesNotContainAnyElementsOf(names);
        }finally{store.close();}
    }
}
