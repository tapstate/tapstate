package io.tapstate.adapters.mongostore;

import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.*;
import org.bson.Document;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import java.util.Map;
import static org.assertj.core.api.Assertions.*;
import java.lang.reflect.Proxy;
import java.lang.reflect.InvocationHandler;
import java.util.concurrent.atomic.AtomicInteger;

/** Closing the cache owner is diagnosed before touching its already closed Mongo client. */
class MongoSrsOwnerLifecycleTest {
    private static final CacheBusinessScope SCOPE = new CacheBusinessScope("cluster",1,"cache");
    private static final Runnable CLOSED = () -> { throw new TapstateException(IoError.CACHE_BUSINESS_FENCED,Map.of(),null); };

    @ParameterizedTest @ValueSource(strings={"chain","model","consumer","log"})
    void aStoppedOwnerRefusesBeforeStartingAnySession(String facet) {
        var sessions = new AtomicInteger();
        var client = proxy(MongoClient.class,(object,method,args) -> {
            if (method.getName().equals("startSession")) { sessions.incrementAndGet(); throw new IllegalStateException("the client has closed"); }
            throw new AssertionError(method.getName());
        });
        var database = proxy(MongoDatabase.class,(object,method,args) -> switch(method.getName()) {
            case "getName" -> "cache";
            case "getCollection" -> collection((String)args[0]);
            default -> throw new AssertionError(method.getName());
        });
        var models = new MongoSrsModelStore(client,database,SCOPE).guardedBy(CLOSED);
        var log = new MongoSrsLogStore(client,collection("srs_events"),collection("srs_log_bounds"),collection("workload_claims"),models);
        assertThatThrownBy(() -> {
            switch(facet) {
                case "chain" -> new MongoSrsChainStore(client,database,SCOPE).guardedBy(CLOSED).load("chain");
                case "model" -> models.load(new SrsModelKey("chain","orders","cache",1,0));
                case "consumer" -> new MongoSrsConsumerStore(client,database,SCOPE).guardedBy(CLOSED).find(new SrsConsumerKey("chain","pipeline","source"));
                case "log" -> log.bounds("ring");
                default -> throw new AssertionError(facet);
            }
        }).isInstanceOfSatisfying(TapstateException.class,failure -> assertThat(failure.code()).isEqualTo(IoError.CACHE_BUSINESS_FENCED));
        assertThat(sessions.get()).isZero();
    }

    @SuppressWarnings("unchecked") private static MongoCollection<Document> collection(String name) {
        return proxy(MongoCollection.class,(object,method,args) -> switch(method.getName()) {
            case "getNamespace" -> new MongoNamespace("cache",name);
            case "withReadConcern", "withWriteConcern" -> object;
            default -> throw new AssertionError(method.getName());
        });
    }
    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(),new Class<?>[]{type},handler));
    }
}
