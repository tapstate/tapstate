package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoCacheDomain;
import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class ThreeLayerDepartedTransactionsTest {
    private final MongoConfigurationDomain configuration = mock(MongoConfigurationDomain.class);
    private final MongoCacheDomain cache = mock(MongoCacheDomain.class);
    private final StorageRuntime.Bound bound = mock(StorageRuntime.Bound.class);
    private final ThreeLayerStorePort port;

    ThreeLayerDepartedTransactionsTest() {
        when(bound.cache()).thenReturn(cache);
        port = new ThreeLayerStorePort(configuration, bound, ignored -> {
            throw new AssertionError("departed transaction cleanup must not prepare state");
        });
        clearInvocations(bound);
        doThrow(StorageRuntime.unavailable()).when(bound).requireBusiness();
    }

    @Test void anActiveBindingCleansCacheBeforeConfigurationWithoutBusinessReadiness() {
        when(cache.endTransactionsLeftOpenBy("gone")).thenReturn(2);
        when(configuration.endTransactionsLeftOpenBy("gone")).thenReturn(1);
        assertThat(port.endTransactionsLeftOpenBy("gone")).isEqualTo(3);
        var ordered = inOrder(bound, cache, configuration);
        ordered.verify(bound).requireActive();
        ordered.verify(bound).cache();
        ordered.verify(cache).endTransactionsLeftOpenBy("gone");
        ordered.verify(configuration).endTransactionsLeftOpenBy("gone");
        verify(bound, never()).requireBusiness();
    }

    @ParameterizedTest @ValueSource(booleans = {true, false})
    void eitherDomainFailureStillAttemptsTheOtherOriginalServer(boolean cacheFails) {
        var failure = new TapstateException(IoError.CACHE_BUSINESS_FENCED, Map.of(), null);
        if (cacheFails) when(cache.endTransactionsLeftOpenBy("gone")).thenThrow(failure);
        else when(configuration.endTransactionsLeftOpenBy("gone")).thenThrow(failure);
        assertThatThrownBy(() -> port.endTransactionsLeftOpenBy("gone")).isSameAs(failure);
        var ordered = inOrder(cache, configuration);
        ordered.verify(cache).endTransactionsLeftOpenBy("gone");
        ordered.verify(configuration).endTransactionsLeftOpenBy("gone");
    }

    @Test void twoFailuresAreReportedTogetherInsteadOfClaimingPartialSuccess() {
        var cacheFailure = new TapstateException(IoError.CACHE_BUSINESS_FENCED, Map.of(), null);
        var configFailure = new TapstateException(IoError.PIPELINE_INTENT_FENCED, Map.of(), null);
        when(cache.endTransactionsLeftOpenBy("gone")).thenThrow(cacheFailure);
        when(configuration.endTransactionsLeftOpenBy("gone")).thenThrow(configFailure);
        assertThatThrownBy(() -> port.endTransactionsLeftOpenBy("gone")).isSameAs(cacheFailure);
        assertThat(cacheFailure.getSuppressed()).containsExactly(configFailure);
    }

    @Test void aClosedOrReplacedBindingCannotIssueCleanupToEitherServer() {
        var failure = StorageRuntime.unavailable();
        doThrow(failure).when(bound).requireActive();
        assertThatThrownBy(() -> port.endTransactionsLeftOpenBy("gone")).isSameAs(failure);
        verifyNoInteractions(cache, configuration);
        verify(bound, never()).cache();
    }
}
