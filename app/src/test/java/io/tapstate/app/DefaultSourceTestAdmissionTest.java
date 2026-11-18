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
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
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
                null, SourceConnectionResolver.forAdHocConnections(), observers, tester,
                new DefaultListableBeanFactory().getBeanProvider(IsolatedConnectionDiagnostics.Active.class));
        assertThatThrownBy(() -> service.test("views", "mongodb", Map.of("uri", "mongodb://host/views"), "admin"))
                .isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.code()).isEqualTo(ConnectorError.INSTANCES_BUSY));
        verify(observer, never()).begin(any()); verifyNoInteractions(probe, results); assertThat(audit).isEmpty();
    }
    @Test void theDefaultSlotDoesNotGateAnOrdinarySourceTest() {
        var observer = mock(ConnectionTestService.Observer.class); when(observer.manages(any())).thenReturn(false);
        when(observer.begin(any())).thenReturn(ConnectionTestService.Attempt.NONE);
        var probe = mock(ConnectionProbe.class); var results = mock(ConnectionTestResultStore.class);
        doCallRealMethod().when(results).beginOrdinaryTest(anyString());
        when(probe.probe(any())).thenReturn(new ConnectionTestResult("ordinary", "mongodb", ConnectionTestResult.Outcome.PASSED, List.of(), System.currentTimeMillis()));
        var tester = mock(PdkConnectionTester.class); when(tester.defaultTestAvailable()).thenReturn(false);
        @SuppressWarnings("unchecked") ObjectProvider<ConnectionTestService.Observer> observers = mock(ObjectProvider.class);
        when(observers.getIfAvailable(any())).thenReturn(observer);
        var service = new ControlPlaneConfiguration().connectionTestService(probe, results, new AuditGate(ignored -> {}, Clock.systemUTC()),
                null, SourceConnectionResolver.forAdHocConnections(), observers, tester,
                new DefaultListableBeanFactory().getBeanProvider(IsolatedConnectionDiagnostics.Active.class));
        assertThat(service.test("ordinary", "mongodb", Map.of("uri", "mongodb://host/data"), "admin").outcome()).isEqualTo(ConnectionTestReport.Outcome.PASSED);
        verify(probe).probe(any()); verify(results).save(any());
    }

    @Test
    void theCandidateRequiresTheExactClusterAndCacheDatabase() {
        var selector = new IsolatedConnectionDiagnostics("cluster", "cache");
        var setup = new StorageSetupScope("cluster", "installation", "setup", 1);
        var cache = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, "mongodb://cache/cache", "cache", null);

        assertThat(selector.matches(new StorageBinding(setup, StorageBinding.Mode.OP, 1, "views", cache))).isTrue();
        assertThat(new IsolatedConnectionDiagnostics("cluster", "different").matches(
                new StorageBinding(setup, StorageBinding.Mode.OP, 1, "views", cache))).isFalse();
        assertThat(new IsolatedConnectionDiagnostics("elsewhere", "cache").matches(
                new StorageBinding(setup, StorageBinding.Mode.OP, 1, "views", cache))).isFalse();
    }

    @Test
    void onlyTheSelectedAssemblyAddsTheLocalizedRefusal() {
        var factory = new DefaultListableBeanFactory();
        var probe = mock(ConnectionProbe.class);
        var failedProbe = new TapstateException(ConnectorError.TEST_FAILED,
                Map.of("connector", "mongodb", "detail", "driver detail"), null);
        when(probe.probe(any())).thenThrow(failedProbe);
        var results = mock(ConnectionTestResultStore.class);
        doCallRealMethod().when(results).beginOrdinaryTest(anyString());
        var control = new ControlPlaneConfiguration();
        var gate = new AuditGate(ignored -> {}, Clock.systemUTC());
        var settings = Map.<String, Object>of("uri", "mongodb://host/data");
        var service = control.connectionTestService(probe, results, gate, null, SourceConnectionResolver.forAdHocConnections(),
                factory.getBeanProvider(ConnectionTestService.Observer.class), mock(PdkConnectionTester.class),
                factory.getBeanProvider(IsolatedConnectionDiagnostics.Active.class));

        assertThatThrownBy(() -> service.test("ordinary", "mongodb", settings, "admin"))
                .isExactlyInstanceOf(TapstateException.class);

        factory.registerSingleton("activeConnectionDiagnostics", IsolatedConnectionDiagnostics.Active.INSTANCE);
        var candidate = control.connectionTestService(probe, results, gate, null, SourceConnectionResolver.forAdHocConnections(),
                factory.getBeanProvider(ConnectionTestService.Observer.class), mock(PdkConnectionTester.class),
                factory.getBeanProvider(IsolatedConnectionDiagnostics.Active.class));
        org.springframework.context.i18n.LocaleContextHolder.setLocale(java.util.Locale.SIMPLIFIED_CHINESE);
        try {
            assertThatThrownBy(() -> candidate.test("ordinary", "mongodb", settings, "admin"))
                    .isInstanceOfSatisfying(ConnectionTestRefusal.class, refusal -> {
                        assertThat(refusal.code()).isEqualTo(ConnectorError.TEST_FAILED);
                        assertThat(refusal.diagnostic()).containsEntry("code", "TS0060001");
                        var catalog = io.tapstate.messages.ConnectionDiagnosticCatalog.bundled();
                        assertThat(refusal.diagnostic()).isEqualTo(
                                catalog.snapshot(failedProbe, java.util.Locale.SIMPLIFIED_CHINESE));
                        assertThat(refusal.diagnostic()).isNotEqualTo(
                                catalog.snapshot(failedProbe, java.util.Locale.ENGLISH));
                    });
        } finally {
            org.springframework.context.i18n.LocaleContextHolder.resetLocaleContext();
        }
        verify(results, never()).save(any());
    }
}
