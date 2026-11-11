package io.tapstate.adapters.mongostore;

import com.mongodb.TransactionOptions;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.TransactionBody;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.*;
import java.util.Optional;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class MongoStorageSetupStateStoreTest {
    private static final StorageSetupScope SCOPE = new StorageSetupScope("cluster", "installation", "operation", 1);
    private static Document row(long generation, String phase) {
        return new Document("clusterId", SCOPE.clusterId()).append("installationId", SCOPE.installationId())
                .append("operationId", SCOPE.operationId()).append("generation", generation).append("phase", phase);
    }
    private static final class Fixture {
        final MongoClient client = mock(MongoClient.class);
        final ClientSession session = mock(ClientSession.class);
        final MongoStorageBindingStore bindings = mock(MongoStorageBindingStore.class);
        final MongoStorageSetupStateStore store = new MongoStorageSetupStateStore(client, mock(MongoDatabase.class), bindings, () -> {});
        Fixture() {
            when(client.startSession()).thenReturn(session);
            when(session.withTransaction(any(), any(TransactionOptions.class)))
                    .thenAnswer(call -> ((TransactionBody<?>) call.getArgument(0)).execute());
            when(bindings.readForTransaction(session)).thenReturn(
                    new MongoStorageBindingStore.TransactionFacts(Optional.empty(), row(1, "CONFIG_PREPARED")));
        }
    }

    @Test void statusBindingAndPreparationComeFromOneValidatedRead() {
        var f = new Fixture();
        var cache = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, "mongodb://localhost/cache", "cache", null);
        var binding = new StorageBinding(SCOPE, StorageBinding.Mode.CLOUD, 1, "atlas-store", cache);
        var preparation = new CachePreparation(SCOPE.operationId(), new CacheAuthScope("cluster", 1, StorageBinding.Mode.CLOUD, "incarnation", 1));
        var loaded = mock(MongoStorageBindingStore.Loaded.class);
        when(loaded.binding()).thenReturn(binding); when(loaded.cachePreparation()).thenReturn(preparation);
        when(f.bindings.readForTransaction(f.session)).thenReturn(
                new MongoStorageBindingStore.TransactionFacts(Optional.of(loaded), row(1, "STORAGE_BOUND")));
        var view = f.store.requireCurrentView(SCOPE, StorageSetupStateStore.Purpose.SETUP);
        assertThat(view.status()).isEqualTo(new StorageSetupStateStore.Status(SCOPE, StorageSetupStateStore.Phase.STORAGE_BOUND, 1));
        assertThat(view.binding()).contains(binding); assertThat(view.cachePreparation()).contains(preparation);
        verify(f.bindings, times(1)).readForTransaction(f.session);
        verify(f.bindings, never()).load(); verify(f.bindings, never()).cachePreparation();
    }

    @Test void transactionRetryRechecksTheNewGeneration() {
        var f = new Fixture();
        when(f.bindings.readForTransaction(f.session)).thenReturn(
                new MongoStorageBindingStore.TransactionFacts(Optional.empty(), row(1, "CONFIG_PREPARED")),
                new MongoStorageBindingStore.TransactionFacts(Optional.empty(), row(2, "CONFIG_PREPARED")));
        when(f.session.withTransaction(any(), any(TransactionOptions.class))).thenAnswer(call -> {
            var body = (TransactionBody<?>) call.getArgument(0);
            body.execute();
            return body.execute();
        });
        assertThatThrownBy(() -> f.store.requireCurrentView(SCOPE, StorageSetupStateStore.Purpose.SETUP))
                .isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.code()).isEqualTo(StoreError.INITIALIZATION_REFUSED));
        verify(f.bindings, times(2)).readForTransaction(f.session);
    }

    @Test void unreadableFactsCannotBecomeAnEmptyBinding() {
        var f = new Fixture();
        when(f.bindings.readForTransaction(f.session)).thenThrow(
                new TapstateException(IoError.DOCUMENT_UNREADABLE, java.util.Map.of("id", "cluster", "field", "binding"), null));
        assertThatThrownBy(f.store::loadView).isInstanceOfSatisfying(TapstateException.class,
                failure -> assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
    }
}
