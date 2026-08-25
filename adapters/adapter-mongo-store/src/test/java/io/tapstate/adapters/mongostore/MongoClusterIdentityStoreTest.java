package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoCollection;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.ClusterIdentity;
import io.tapstate.spi.store.IoError;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

class MongoClusterIdentityStoreTest {

    @Test
    void missingUpsertResultIsReportedAsUnreadableDocument() {
        MongoCollection<Document> collection = (MongoCollection<Document>) Proxy.newProxyInstance(
                MongoCollection.class.getClassLoader(),
                new Class<?>[] {MongoCollection.class},
                (proxy, method, args) -> method.getName().equals("findOneAndUpdate") ? null : null);

        Throwable thrown = catchThrowable(() -> new MongoClusterIdentityStore(collection)
                .createIfAbsent(new ClusterIdentity("01J5FIXTURE")));

        assertThat(thrown).isInstanceOf(TapstateException.class);
        TapstateException coded = (TapstateException) thrown;
        assertThat(coded.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);
        assertThat(coded.args()).containsEntry("id", "cluster");
        assertThat(coded.getCause()).isNull();
    }
}
