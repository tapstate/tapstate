package io.tapstate.adapters.mongostore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.mongodb.TransactionOptions;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.TransactionBody;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.CachePreparation;
import io.tapstate.spi.store.IoError;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class MongoCaptureOpeningGuardTest {
    @Test void domainRevocationDuringTheBodyPreventsTheTransactionCallbackFromReturning() {
        var client=mock(MongoClient.class);var database=mock(MongoDatabase.class);
        var session=mock(ClientSession.class);var bindings=mock(MongoStorageBindingStore.class);
        var notes=mock(MongoRequiredNoteStore.class);var prepared=mock(CachePreparation.class);
        var scope=new CacheBusinessScope("cluster",1,"original-cache");
        var revoked=new AtomicBoolean();var callbackReturned=new AtomicBoolean();
        when(client.startSession()).thenReturn(session);
        when(prepared.businessScope()).thenReturn(scope);
        when(bindings.cachePreparationForTransaction(session)).thenReturn(Optional.of(prepared));
        when(notes.resourceNotes(session,scope)).thenAnswer(invocation -> {
            revoked.set(true);
            return List.of();
        });
        when(session.withTransaction(any(TransactionBody.class),any(TransactionOptions.class))).thenAnswer(invocation -> {
            Object result=((TransactionBody<?>)invocation.getArgument(0)).execute();
            callbackReturned.set(true);
            return result;
        });
        // This is the original domain's lifecycle guard, not a new business-lease requirement.
        Runnable active=() -> {
            if(revoked.get())throw new TapstateException(IoError.WORKLOAD_CLAIM_FENCED,Map.of(),null);
        };
        var retirement=new MongoCaptureExternalRetirements(client,database,"cluster",bindings,notes,active);
        assertThatThrownBy(() -> retirement.resourceNotes(scope)).isInstanceOfSatisfying(TapstateException.class,
                failure -> assertThat(failure.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED));
        assertThat(callbackReturned).isFalse();
    }
}
