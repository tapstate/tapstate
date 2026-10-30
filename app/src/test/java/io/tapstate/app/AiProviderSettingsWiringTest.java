package io.tapstate.app;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.MongoConnection;
import io.tapstate.adapters.mongostore.MongoConnectionSettings;
import io.tapstate.adapters.mongostore.MongoAiProviderSettingsStore;
import io.tapstate.adapters.mongostore.SourceConfigKeyringHandle;
import io.tapstate.control.core.AiProviderService;
import io.tapstate.control.core.AuditGate;
import io.tapstate.spi.store.AiProviderSettingsStore;
import io.tapstate.spi.store.AuditStore;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class AiProviderSettingsWiringTest {
    @Test void assemblesTheEncryptedMongoStoreAndControlServiceFromTheirProductionFactories() throws Exception {
        MongoCollection<Document> collection = proxy(MongoCollection.class, (self, method, args) -> {
            if (List.of("withReadPreference", "withReadConcern", "withWriteConcern").contains(method.getName())) return self;
            throw new AssertionError("the provider store factory must not issue Mongo operations: " + method.getName());
        });
        MongoDatabase database = proxy(MongoDatabase.class, (self, method, args) -> {
            if (method.getName().equals("getCollection")) return collection;
            throw new AssertionError("unexpected database operation: " + method.getName());
        });
        MongoClient client = proxy(MongoClient.class, (self, method, args) -> {
            if (method.getName().equals("getDatabase")) return database;
            throw new AssertionError("unexpected client operation: " + method.getName());
        });
        MongoConnection connection = new MongoConnection(new MongoConnectionSettings(
                "mongodb://localhost/tapstate", null, Duration.ofSeconds(1)));
        set(connection, "client", client);
        set(connection, "databaseName", "tapstate");
        set(connection, "sourceConfigKeyring", mock(SourceConfigKeyringHandle.class));

        AiProviderSettingsStore store = new StoreConfiguration().aiProviderSettingsStore(connection);
        AuditStore audit = record -> { };
        AiProviderService service = new ControlPlaneConfiguration().aiProviderService(settingsProvider(store),
                new AuditGate(audit, Clock.fixed(Instant.EPOCH, ZoneOffset.UTC)));

        assertThat(store).isInstanceOf(MongoAiProviderSettingsStore.class);
        assertThat(service).isNotNull();
    }

    private static void set(Object target, String fieldName, Object value) throws Exception {
        var field = MongoConnection.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> type, Invocation invocation) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, (self, method, args) ->
                invocation.invoke(self, method, args));
    }

    @FunctionalInterface
    private interface Invocation {
        Object invoke(Object self, java.lang.reflect.Method method, Object[] args) throws Throwable;
    }
    @org.junit.jupiter.api.Test
    void anUnconfiguredOptionalProviderDoesNotPreventTheNativeRuntimeFromStarting() {
        var audit = org.mockito.Mockito.mock(io.tapstate.control.core.AuditGate.class);
        org.assertj.core.api.Assertions.assertThat(new ControlPlaneConfiguration().aiProviderService(
                new org.springframework.beans.factory.support.StaticListableBeanFactory()
                        .getBeanProvider(io.tapstate.spi.store.AiProviderSettingsStore.class), audit)).isNull();
    }

    private static org.springframework.beans.factory.ObjectProvider<io.tapstate.spi.store.AiProviderSettingsStore>
            settingsProvider(io.tapstate.spi.store.AiProviderSettingsStore store) {
        var factory = new org.springframework.beans.factory.support.StaticListableBeanFactory();
        factory.addBean("settings", store);
        return factory.getBeanProvider(io.tapstate.spi.store.AiProviderSettingsStore.class);
    }

}
