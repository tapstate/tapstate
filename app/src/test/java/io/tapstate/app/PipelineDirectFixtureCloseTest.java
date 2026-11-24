package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoCacheDomain;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.DeadlineWorkloadClaimStore;
import io.tapstate.spi.store.IoError;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class PipelineDirectFixtureCloseTest {
    @Test void aBusyCloseMustAwaitOriginalOwnersBeforeRetrying() {
        var cache = mock(MongoCacheDomain.class); var claims = mock(DeadlineWorkloadClaimStore.class);
        var busy = new TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null);
        doThrow(busy).doNothing().when(cache).close();
        PipelineDirectPreparationIT.closeCacheClaims(cache, claims);
        var order = inOrder(cache, claims);
        order.verify(cache).close(); order.verify(claims).awaitQuiescence(anyLong()); order.verify(cache).close();
    }
    @Test void unknownCleanupCannotBeReportedAsClosed() {
        var cache = mock(MongoCacheDomain.class); var claims = mock(DeadlineWorkloadClaimStore.class);
        var busy = new TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null);
        var unknown = new IllegalStateException("original cleanup remains unknown");
        doThrow(busy).when(cache).close(); doThrow(unknown).when(claims).awaitQuiescence(anyLong());
        assertThatThrownBy(() -> PipelineDirectPreparationIT.closeCacheClaims(cache, claims)).isSameAs(unknown);
        assertThat(unknown.getSuppressed()).containsExactly(busy); verify(cache, times(1)).close();
    }
    @Test void unrelatedCloseFailuresAreNeverRetried() {
        var cache = mock(MongoCacheDomain.class); var claims = mock(DeadlineWorkloadClaimStore.class);
        var failure = new IllegalStateException("cache close invariant failed");
        doThrow(failure).when(cache).close();
        assertThatThrownBy(() -> PipelineDirectPreparationIT.closeCacheClaims(cache, claims)).isSameAs(failure);
        verifyNoInteractions(claims); verify(cache, times(1)).close();
    }
}
