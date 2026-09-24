package io.tapstate.control.restapi;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.support.StaticListableBeanFactory;
import org.springframework.boot.availability.ApplicationAvailability;
import org.springframework.boot.availability.ApplicationAvailabilityBean;
import org.springframework.boot.availability.AvailabilityChangeEvent;
import org.springframework.boot.availability.LivenessState;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What the liveness probe answers for each liveness the application may keep.
 *
 * <p>Only a liveness declared broken fails it. Broken is what a process that is still up, but cannot recover by
 * itself, is declared, and a failing probe is what has whatever watches it restart the process. Every other
 * reading answers as the probe always has, none at all included, because an unknown liveness is not a reason to
 * have a process restarted.
 */
@DisplayName("what the liveness probe answers for each liveness the application keeps")
class HealthControllerTest {

    @Test
    void aProcessDeclaredBrokenFailsTheProbe() {
        ResponseEntity<String> probe = probeKeeping(declared(LivenessState.BROKEN));

        assertThat(probe.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        assertThat(probe.getBody())
                .as("the probe says the process is broken and nothing about why")
                .isEqualTo("broken");
    }

    @Test
    void aProcessDeclaredCorrectPassesTheProbe() {
        ResponseEntity<String> probe = probeKeeping(declared(LivenessState.CORRECT));

        assertThat(probe.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(probe.getBody()).isEqualTo("ok");
    }

    @Test
    void aLivenessNobodyHasDeclaredYetPassesTheProbe() {
        ResponseEntity<String> probe = probeKeeping(new ApplicationAvailabilityBean());

        assertThat(probe.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(probe.getBody()).isEqualTo("ok");
    }

    @Test
    void aContextThatKeepsNoLivenessAtAllPassesTheProbe() {
        ResponseEntity<String> probe =
                new HealthController(new StaticListableBeanFactory().getBeanProvider(ApplicationAvailability.class))
                        .healthz();

        assertThat(probe.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(probe.getBody()).isEqualTo("ok");
    }

    /** The application's availability, told its liveness the way the application tells it: by an event. */
    private static ApplicationAvailability declared(LivenessState liveness) {
        ApplicationAvailabilityBean availability = new ApplicationAvailabilityBean();
        availability.onApplicationEvent(new AvailabilityChangeEvent<>(HealthControllerTest.class, liveness));
        return availability;
    }

    private static ResponseEntity<String> probeKeeping(ApplicationAvailability availability) {
        StaticListableBeanFactory context = new StaticListableBeanFactory(Map.of("availability", availability));
        return new HealthController(context.getBeanProvider(ApplicationAvailability.class)).healthz();
    }
}
