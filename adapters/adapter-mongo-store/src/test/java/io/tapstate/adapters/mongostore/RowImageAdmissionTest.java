package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoCollection;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Op;
import io.tapstate.core.event.RowValueError;
import io.tapstate.spi.store.NestDeadLetterRecord;
import io.tapstate.spi.store.SrsLogRecord;
import java.lang.reflect.Proxy;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RowImageAdmissionTest {
    @Test
    void refusesTheWholeStoreBatchBeforeAnyWriteForEitherImage() {
        for (boolean before : new boolean[] {false, true}) {
            AtomicInteger calls = new AtomicInteger();
            MongoSrsLogStore store = new MongoSrsLogStore(collection(calls));
            SrsLogRecord valid = new SrsLogRecord("first", Op.INSERT, 0, null, Map.of("id", 1), 0);
            Map<String, Object> row = invalidRow();
            SrsLogRecord invalid = new SrsLogRecord("second", Op.UPDATE, 1,
                    before ? row : Map.of("id", 2), before ? Map.of("id", 2) : row, 0);

            assertThatThrownBy(() -> store.storeAll("ring", 0, List.of(valid, invalid)))
                    .isInstanceOfSatisfying(TapstateException.class, failure -> {
                        assertThat(failure.code()).isEqualTo(RowValueError.NON_STRING_MAP_KEY);
                        assertThat(failure.args()).containsEntry("path", "$/dict");
                    });
            assertThat(calls).hasValue(0);
            assertThat((Map<?, ?>) row.get("dict")).hasSize(2);
        }
    }

    @Test
    void refusesADeadLetterBeforeReplacingTheStoredRecord() {
        AtomicInteger calls = new AtomicInteger();
        MongoNestDeadLetterStore store = new MongoNestDeadLetterStore(collection(calls));
        NestDeadLetterRecord record = new NestDeadLetterRecord("ns", "el", "chain", "order", 0, 1,
                invalidRow());
        assertThatThrownBy(() -> store.record(record))
                .isInstanceOfSatisfying(TapstateException.class, failure ->
                        assertThat(failure.code()).isEqualTo(RowValueError.NON_STRING_MAP_KEY));
        assertThat(calls).hasValue(0);
    }

    private static Map<String, Object> invalidRow() {
        Map<Object, Object> nested = new LinkedHashMap<>();
        nested.put(7, "number-key");
        nested.put("7", "text-key");
        return Map.of("dict", nested);
    }

    @SuppressWarnings("unchecked")
    private static MongoCollection<Document> collection(AtomicInteger calls) {
        return (MongoCollection<Document>) Proxy.newProxyInstance(MongoCollection.class.getClassLoader(),
                new Class<?>[] {MongoCollection.class}, (proxy, method, args) -> {
                    if (method.getName().equals("withReadConcern") || method.getName().equals("withWriteConcern")) {
                        return proxy;
                    }
                    calls.incrementAndGet();
                    throw new AssertionError("Invalid input reached MongoCollection." + method.getName());
                });
    }
}
