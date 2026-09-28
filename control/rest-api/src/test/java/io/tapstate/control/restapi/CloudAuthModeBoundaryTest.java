package io.tapstate.control.restapi;

import io.tapstate.control.core.AuthenticationMode;
import io.tapstate.control.core.ControlError;
import io.tapstate.core.common.TapstateException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CloudAuthModeBoundaryTest {

    @Test
    void cloudModeDoesNotFallBackToAnyOnPremAuthenticationEntryPoint() {
        DefaultListableBeanFactory beans = new DefaultListableBeanFactory();
        beans.registerSingleton("authenticationMode", AuthenticationMode.CLOUD);
        AuthController controller = new AuthController(
                null, null, null, null, beans.getBeanProvider(AuthenticationMode.class));

        assertUnavailable(() -> controller.login(null));
        assertUnavailable(() -> controller.session(null));
        assertUnavailable(() -> controller.logout(null));
        assertUnavailable(() -> controller.bootstrap(null, null));
    }

    private static void assertUnavailable(Runnable action) {
        assertThatThrownBy(action::run)
                .isInstanceOfSatisfying(TapstateException.class, error -> {
                    assertThat(error.code()).isEqualTo(ControlError.AUTH_MODE_UNAVAILABLE);
                    assertThat(error.args()).containsEntry("mode", "cloud");
                });
    }
}
