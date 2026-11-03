package io.tapstate.app;

import io.tapstate.adapters.pdk.ConnectorError;
import io.tapstate.adapters.pdk.PdkConnectionTester;
import io.tapstate.control.core.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.runtime.probe.ConnectionProbe;
import io.tapstate.spi.store.*;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class DefaultSourceTestAdmissionTest {
    @Test void aBusyDefaultSlotCannotReplaceTheCurrentAttemptOrWriteAnAuditForUnacceptedWork() {
        var observer = mock(ConnectionTestService.Observer.class);
        when(observer.manages(any())).thenReturn(true);
        var probe = mock(ConnectionProbe.class); var results = mock(ConnectionTestResultStore.class);
        var tester = mock(PdkConnectionTester.class); when(tester.defaultTestAvailable()).thenReturn(false);
        var audit = new ArrayList<AuditRecord>();
        @SuppressWarnings("unchecked") ObjectProvider<ConnectionTestService.Observer> observers = mock(ObjectProvider.class);
        when(observers.getIfAvailable(any())).thenReturn(observer);
        var service = new ControlPlaneConfiguration().connectionTestService(probe, results, new AuditGate(audit::add, Clock.systemUTC()),
                null, SourceConnectionResolver.forAdHocConnections(), observers, tester);
        assertThatThrownBy(() -> service.test("views", "mongodb", Map.of("uri", "mongodb://host/views"), "admin"))
                .isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.code()).isEqualTo(ConnectorError.INSTANCES_BUSY));
        verify(observer, never()).begin(any()); verifyNoInteractions(probe, results); assertThat(audit).isEmpty();
    }
    @Test void theDefaultSlotDoesNotGateAnOrdinarySourceTest() {
        var observer = mock(ConnectionTestService.Observer.class); when(observer.manages(any())).thenReturn(false);
        when(observer.begin(any())).thenReturn(ConnectionTestService.Attempt.NONE);
        var probe = mock(ConnectionProbe.class); var results = mock(ConnectionTestResultStore.class);
        when(probe.probe(any())).thenReturn(new ConnectionTestResult("ordinary", "mongodb", ConnectionTestResult.Outcome.PASSED, List.of(), System.currentTimeMillis()));
        var tester = mock(PdkConnectionTester.class); when(tester.defaultTestAvailable()).thenReturn(false);
        @SuppressWarnings("unchecked") ObjectProvider<ConnectionTestService.Observer> observers = mock(ObjectProvider.class);
        when(observers.getIfAvailable(any())).thenReturn(observer);
        var service = new ControlPlaneConfiguration().connectionTestService(probe, results, new AuditGate(ignored -> {}, Clock.systemUTC()),
                null, SourceConnectionResolver.forAdHocConnections(), observers, tester);
        assertThat(service.test("ordinary", "mongodb", Map.of("uri", "mongodb://host/data"), "admin").outcome()).isEqualTo(ConnectionTestReport.Outcome.PASSED);
        verify(probe).probe(any()); verify(results).save(any());
    }
}
