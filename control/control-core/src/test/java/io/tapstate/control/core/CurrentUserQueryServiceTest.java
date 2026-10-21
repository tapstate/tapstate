package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CurrentUserQueryServiceTest {
    @Test
    void eachModeProjectsOnlyItsVerifiedUserAndScopes() {
        VerifiedToken user = new VerifiedToken("stable-user", Scope.WRITE);
        assertThat(new CurrentUserQueryService(AuthenticationMode.CLOUD).get(TapstatePrincipal.cloudSession(user)))
                .isEqualTo(new CurrentUserView("cloud", "stable-user", List.of("read", "write")));
        assertThat(new CurrentUserQueryService(AuthenticationMode.ON_PREM).get(TapstatePrincipal.humanJwt(user)))
                .isEqualTo(new CurrentUserView("on-prem", "stable-user", List.of("read", "write")));
    }

    @Test
    void aMachineAndTheOtherModesIdentityAreNeverReinterpretedAsTheCurrentUser() {
        VerifiedToken user = new VerifiedToken("credential-subject", Scope.ADMIN);
        for (AuthenticationMode mode : AuthenticationMode.values()) {
            var service = new CurrentUserQueryService(mode);
            assertRefused(() -> service.get(TapstatePrincipal.machineToken(user)));
            assertRefused(() -> service.get(mode == AuthenticationMode.CLOUD
                    ? TapstatePrincipal.humanJwt(user) : TapstatePrincipal.cloudSession(user)));
        }
    }

    @Test
    void theCurrentUserQueryUsesTheHttpInventoryWithoutOpeningMcpOrRequiringAnAudit() {
        Operation operation = ControlOperations.registry().resolve("auth.current-user");
        assertThat(operation.scope()).isEqualTo(Scope.READ);
        assertThat(operation.audited()).isFalse();
        assertThat(operation.exposure()).containsExactlyEntriesOf(java.util.Map.of(Frontend.CLI, Maturity.CURRENT));
        assertThat(ControlOperations.registry().exposedOn(Frontend.MCP)).doesNotContain(operation);
    }

    private static void assertRefused(Runnable query) {
        assertThatThrownBy(query::run).isInstanceOfSatisfying(TapstateException.class, failure -> {
            assertThat(failure.code()).isEqualTo(ControlError.UNAUTHENTICATED);
            assertThat(failure.args()).isEmpty();
            assertThat(failure.getCause()).isNull();
        }).hasMessageNotContaining("credential-subject");
    }
}
