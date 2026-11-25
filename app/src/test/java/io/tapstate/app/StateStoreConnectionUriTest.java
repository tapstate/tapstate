package io.tapstate.app;

import io.tapstate.control.core.ApplyService;
import io.tapstate.control.core.ArtifactQueryService;
import io.tapstate.control.core.AuditGate;
import io.tapstate.control.core.ConnectionTestService;
import io.tapstate.control.core.ControlError;
import io.tapstate.control.core.DeploymentProfile;
import io.tapstate.control.core.Scope;
import io.tapstate.control.core.SourceError;
import io.tapstate.control.core.SourceRepresentation;
import io.tapstate.control.core.StateStoreSetupService;
import io.tapstate.control.core.StoredResource;
import io.tapstate.control.core.TapstatePrincipal;
import io.tapstate.control.core.VerifiedToken;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.SourceResource;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class StateStoreConnectionUriTest {
    private static final String STORE = "views";
    private static final String URI = "mongodb://user:sample@localhost/views";
    private static final TapstatePrincipal ADMIN = TapstatePrincipal.humanJwt(new VerifiedToken("owner", Scope.ADMIN));
    private final ArtifactQueryService artifacts = mock(ArtifactQueryService.class);
    private final StateStoreSetupService store = new StateStoreSetupService(mock(ApplyService.class), artifacts,
            mock(SourceRepresentation.class), DeploymentProfile.ON_PREM, mock(ConnectionTestService.class));
    private final AuditGate audit = new AuditGate(record -> {}, Clock.systemUTC());

    @Test
    void machineAdministratorAndHumanWriterCannotReadTheBoundStore() {
        var machine = TapstatePrincipal.machineToken(new VerifiedToken("machine", Scope.ADMIN));
        var writer = TapstatePrincipal.humanJwt(new VerifiedToken("writer", Scope.WRITE));
        for (var caller : List.of(machine, writer)) {
            assertThatThrownBy(() -> store.connectionUri(caller, STORE, () -> {}, audit))
                    .isInstanceOfSatisfying(TapstateException.class, e -> assertThat(e.code()).isEqualTo(ControlError.FORBIDDEN));
        }
        verifyNoInteractions(artifacts);
    }

    @Test
    void aBindingChangeBetweenSelectionAndReadRefusesTheFormerDestination() {
        inventory("other", "mongodb", URI);
        assertUnavailable();
    }

    @Test
    void unsupportedConnectorsAndNonMongoUrisAreNeverReturned() {
        inventory(STORE, "postgres", URI);
        assertUnavailable();
        inventory(STORE, "mongodb", "https://localhost/");
        assertUnavailable();
    }

    @Test
    void sessionRevocationDuringTheReadPreventsDisclosure() {
        inventory(STORE, "mongodb", URI);
        var calls = new AtomicInteger();
        assertThatThrownBy(() -> store.connectionUri(ADMIN, STORE, () -> {
            if (calls.incrementAndGet() == 2) throw new TapstateException(ControlError.UNAUTHENTICATED, Map.of(), null);
        }, audit)).isInstanceOfSatisfying(TapstateException.class, e -> assertThat(e.code()).isEqualTo(ControlError.UNAUTHENTICATED));
        assertThat(calls.get()).isEqualTo(2);
    }

    @Test
    void failedAuditPreventsReadingCredentials() {
        var unavailableAudit = new AuditGate(record -> { throw new IllegalStateException("audit unavailable"); }, Clock.systemUTC());
        assertThatThrownBy(() -> store.connectionUri(ADMIN, STORE, () -> {}, unavailableAudit))
                .isInstanceOfSatisfying(TapstateException.class, e -> assertThat(e.code()).isEqualTo(ControlError.AUDIT_BLOCKED));
        verifyNoInteractions(artifacts);
    }

    private void assertUnavailable() {
        assertThatThrownBy(() -> store.connectionUri(ADMIN, STORE, () -> {}, audit))
                .isInstanceOfSatisfying(TapstateException.class, e -> assertThat(e.code()).isEqualTo(SourceError.NOT_FOUND));
    }

    private void inventory(String boundId, String connector, String uri) {
        when(artifacts.managedDefaultSourceId()).thenReturn(Optional.of(STORE));
        var source = new SourceResource(STORE, null, connector, Map.of("uri", uri), null, null, null, null);
        when(artifacts.resourceInventory("source", STORE)).thenReturn(new ArtifactQueryService.ResourceInventory(
                List.of(new StoredResource(source, "fixture")), Optional.empty(), Optional.of(boundId), Set.of(STORE)));
    }
}
